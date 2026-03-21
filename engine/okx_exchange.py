"""
OKX 거래소 어댑터 (ccxt)

현물(Spot)   : 롱 전용  — {COIN}/USDT 자동 생성
선물(Futures): 롱 + 숏  — {COIN}/USDT:USDT 자동 생성 (무기한 스왑)

포지션 모드: 단방향 (one-way) — long/short 동시 보유 없음
레버리지   : 기본 1x (config에서 조정 가능)
마진 모드  : isolated (포지션별 독립 마진)
심볼 변환  : KRW-BTC → BTC/USDT (spot) / BTC/USDT:USDT (futures) 자동 생성
             비표준 심볼은 _SPOT_OVERRIDE / _FUTURES_OVERRIDE 에 등록
"""
import os
import logging
import pandas as pd
import ccxt
from typing import Optional, List
from .exchange_base import ExchangeBase

logger = logging.getLogger(__name__)

# 비표준 심볼 오버라이드 (표준 패턴과 다른 경우만 등록)
# MATIC → OKX에서 POL로 리브랜딩
_SPOT_OVERRIDE: dict = {
    "KRW-MATIC": "POL/USDT",
}
_FUTURES_OVERRIDE: dict = {
    "KRW-MATIC": "POL/USDT:USDT",
}


class OKXExchange(ExchangeBase):
    """
    Parameters
    ----------
    paper_trading : True이면 실제 주문 없이 로그만 출력
    leverage      : 선물 레버리지 (기본 1)
    use_short     : 숏 전략 사용 여부 (True이면 선물 마켓 사용)
    markets       : 선물 레버리지 초기 설정할 마켓 리스트 (예: ["KRW-BTC", "KRW-ETH", ...])
    """

    def __init__(
        self,
        paper_trading: bool = True,
        leverage: int = 1,
        use_short: bool = True,
        markets: Optional[List[str]] = None,
    ):
        super().__init__("okx", paper_trading, quote_currency="USDT")
        self.leverage = leverage
        self.use_short = use_short
        self._markets = markets or []
        self._spot = None
        self._futures = None

        base_cfg = {
            "apiKey":   os.getenv("OKX_API_KEY", ""),
            "secret":   os.getenv("OKX_SECRET_KEY", ""),
            "password": os.getenv("OKX_PASSPHRASE", ""),
        }

        if not paper_trading:
            if not all(base_cfg.values()):
                raise ValueError("OKX_API_KEY / OKX_SECRET_KEY / OKX_PASSPHRASE 미설정")

            self._spot = ccxt.okx({**base_cfg, "options": {"defaultType": "spot"}})
            if use_short:
                self._futures = ccxt.okx({**base_cfg, "options": {
                    "defaultType": "swap",
                    "posMode": "net",          # 단방향(one-way) 모드 명시
                }})
                self._init_futures_settings()
            logger.info(f"[OKX] 실전 모드 | 레버리지: {leverage}x | 숏: {use_short} | 종목: {len(self._markets)}개")
        else:
            # 페이퍼 모드에서도 공개 API(현재가)는 사용
            self._spot = ccxt.okx({"options": {"defaultType": "spot"}})
            logger.info(f"[OKX] 페이퍼 트레이딩 모드 | 종목: {len(self._markets)}개")

    def _init_futures_settings(self):
        """선물 레버리지 및 마진 모드 초기 설정 (등록된 모든 마켓 × long/short)"""
        for market in self._markets:
            symbol = self._futures_symbol(market)
            for pos_side in ["long", "short"]:
                try:
                    self._futures.set_leverage(
                        self.leverage, symbol,
                        params={"mgnMode": "isolated", "posSide": pos_side}
                    )
                    logger.info(f"[OKX] {symbol} {pos_side} | isolated {self.leverage}x 설정 완료")
                except Exception as e:
                    logger.warning(f"[OKX] 레버리지 설정 실패 ({symbol} {pos_side}): {e}")

    # ── 심볼 변환 (KRW-BTC → BTC/USDT 자동 생성) ──────

    def _spot_symbol(self, market: str) -> str:
        if market in _SPOT_OVERRIDE:
            return _SPOT_OVERRIDE[market]
        coin = market.replace("KRW-", "")
        return f"{coin}/USDT"

    def _futures_symbol(self, market: str) -> str:
        if market in _FUTURES_OVERRIDE:
            return _FUTURES_OVERRIDE[market]
        coin = market.replace("KRW-", "")
        return f"{coin}/USDT:USDT"

    # ── 잔고 조회 ──────────────────────────────────────

    _bal_cache: dict = {}   # {"ts": float, "val": float}
    _BAL_TTL = 30           # 초: 30초 내 재호출 시 캐시 반환 (Rate Limit 방지)

    def get_balance_quote(self) -> float:
        """
        OKX 총 USDT 자산 = spot total + futures total (마진 + 미실현손익 포함)
        spot.free만 쓰면 선물 계좌에 이체된 증거금이 누락됨.
        30초 TTL 캐시로 20종목 × 반복 호출로 인한 Rate Limit(50011) 방지.
        """
        if self.paper_trading:
            return 0.0
        import time
        now = time.time()
        if now - self._bal_cache.get("ts", 0) < self._BAL_TTL:
            return self._bal_cache["val"]
        total = 0.0
        try:
            spot_bal = self._spot.fetch_balance()
            total += float(spot_bal.get("USDT", {}).get("total", 0) or 0)
        except Exception as e:
            logger.error(f"[OKX] Spot USDT 잔고 오류: {e}")
        if self._futures:
            try:
                fut_bal = self._futures.fetch_balance()
                total += float(fut_bal.get("USDT", {}).get("total", 0) or 0)
            except Exception as e:
                logger.error(f"[OKX] Futures USDT 잔고 오류: {e}")
        self._bal_cache = {"ts": now, "val": total}
        return total

    def get_balance_coin(self, market: str) -> float:
        if self.paper_trading:
            return 0.0
        coin = self._spot_symbol(market).split("/")[0]
        try:
            bal = self._spot.fetch_balance()
            return float(bal.get(coin, {}).get("free", 0) or 0)
        except Exception as e:
            logger.error(f"[OKX] {coin} 잔고 오류: {e}")
            return 0.0

    def get_futures_position(self, market: str) -> dict:
        """
        현재 선물 포지션 조회

        Returns
        -------
        {"side": "long"|"short"|None, "volume": float, "entry_price": float}
        """
        if self.paper_trading or not self._futures:
            return {"side": None, "volume": 0.0, "entry_price": 0.0}
        try:
            symbol = self._futures_symbol(market)
            positions = self._futures.fetch_positions([symbol])
            for p in positions:
                contracts = float(p.get("contracts", 0) or 0)
                if contracts > 0:
                    return {
                        "side": p["side"],
                        "volume": contracts,
                        "entry_price": float(p.get("entryPrice", 0) or 0),
                    }
            return {"side": None, "volume": 0.0, "entry_price": 0.0}
        except Exception as e:
            logger.error(f"[OKX] 포지션 조회 오류: {e}")
            return {"side": None, "volume": 0.0, "entry_price": 0.0}

    def _to_inst_id(self, ccxt_futures_symbol: str) -> str:
        """ccxt 선물 심볼 → OKX instId (BTC/USDT:USDT → BTC-USDT-SWAP)"""
        if ":USDT" in ccxt_futures_symbol:
            base = ccxt_futures_symbol.split("/")[0]
            return f"{base}-USDT-SWAP"
        return ccxt_futures_symbol.replace("/", "-")

    def fetch_top_trader_ratio(self, market: str, period: str = "1H") -> float:
        """
        OKX Top Traders 롱 비율 조회 (0.0 ~ 1.0)
        0.5 = 중립, > 0.5 = 롱 우위, < 0.5 = 숏 우위
        실패 시 중립값 0.5 반환
        """
        inst_id = self._to_inst_id(self._futures_symbol(market))
        try:
            resp = self._spot.publicGetRubikStatContractsLongShortAccountRatioContractTopTrader({
                "instId": inst_id,
                "period": period,
            })
            # resp가 dict이면 "data" 키 추출, list이면 직접 사용
            data = resp if isinstance(resp, list) else resp.get("data", [])
            if data:
                item = data[0]
                ratio = float(item.get("longRatio", 0.5) if isinstance(item, dict) else 0.5)
                logger.info(f"[OKX] Top Trader 롱 비율 ({market}): {ratio:.1%}")
                return ratio
        except Exception as e:
            logger.warning(f"[OKX] Top Trader 비율 조회 실패 ({market}): {e}")
        return 0.5

    def fetch_ohlcv(self, market: str, interval: str = "day", count: int = 210) -> pd.DataFrame:
        """OKX ccxt로 OHLCV 수집 (USDT 기준 가격)"""
        interval_map = {
            "day": "1d", "week": "1w", "month": "1M",
            "minute60": "1h", "minute240": "4h",
            "minute30": "30m", "minute15": "15m",
            "minute5": "5m", "minute1": "1m",
        }
        tf = interval_map.get(interval, "1D")
        symbol = self._spot_symbol(market)
        try:
            raw = self._spot.fetch_ohlcv(symbol, tf, limit=count)
            df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df.index = pd.to_datetime(df["timestamp"], unit="ms")
            df.index.name = "datetime"
            return df[["open", "high", "low", "close", "volume"]]
        except Exception as e:
            logger.error(f"[OKX] OHLCV 수집 실패 ({market}): {e}")
            return pd.DataFrame()

    def get_avg_buy_price(self, market: str) -> float:
        return 0.0

    def get_current_price(self, market: str) -> Optional[float]:
        try:
            ticker = self._spot.fetch_ticker(self._spot_symbol(market))
            return float(ticker["last"])
        except Exception as e:
            logger.error(f"[OKX] 현재가 오류: {e}")
            return None

    # ── 현물 매수/매도 (롱 전략) ───────────────────────

    def buy_market_order(self, market: str, amount_usdt: float) -> Optional[dict]:
        """현물 시장가 매수 (롱 진입)"""
        if amount_usdt < 1.0:
            logger.warning(f"[OKX] 최소 주문금액 미달: {amount_usdt:.2f} USDT")
            return None
        if self.paper_trading:
            logger.info(f"[OKX PAPER] 현물 매수 | {market} | {amount_usdt:.2f} USDT")
            return {"type": "paper_buy", "market": market, "amount": amount_usdt}
        try:
            symbol = self._spot_symbol(market)
            price  = self.get_current_price(market)
            volume = amount_usdt / price
            result = self._spot.create_market_buy_order(symbol, volume)
            logger.info(f"[OKX] 현물 매수 | {symbol} | {amount_usdt:.2f} USDT")
            return result
        except Exception as e:
            logger.error(f"[OKX] 현물 매수 실패: {e}")
            return None

    def sell_market_order(self, market: str, volume: float) -> Optional[dict]:
        """현물 시장가 매도 (롱 청산)"""
        if self.paper_trading:
            logger.info(f"[OKX PAPER] 현물 매도 | {market} | {volume:.6f}")
            return {"type": "paper_sell", "market": market, "volume": volume}
        try:
            symbol = self._spot_symbol(market)
            result = self._spot.create_market_sell_order(symbol, volume)
            logger.info(f"[OKX] 현물 매도 | {symbol} | {volume:.6f}")
            return result
        except Exception as e:
            logger.error(f"[OKX] 현물 매도 실패: {e}")
            return None

    # ── 동적 레버리지 설정 ─────────────────────────────

    def set_leverage_dynamic(self, market: str, leverage: int, pos_side: str = "long") -> bool:
        """포지션 진입 직전 레버리지 동적 변경 (hedge mode: pos_side 명시)"""
        if not self._futures:
            return False
        try:
            symbol = self._futures_symbol(market)
            self._futures.set_leverage(leverage, symbol,
                params={"mgnMode": "isolated", "posSide": pos_side})
            logger.info(f"[OKX] {market} {pos_side} 레버리지 {leverage}x 설정")
            return True
        except Exception as e:
            logger.warning(f"[OKX] 레버리지 동적 설정 실패 ({market} {leverage}x {pos_side}): {e}")
            return False

    # ── 선물 롱 진입/청산 ──────────────────────────────

    def open_long_futures(self, market: str, amount_usdt: float, leverage: int = 1) -> Optional[dict]:
        """선물 롱 진입 (시장가 매수 포지션 오픈)"""
        if amount_usdt < 1.0:
            logger.warning(f"[OKX] 선물 롱 최소 주문금액 미달: {amount_usdt:.2f} USDT")
            return None
        if self.paper_trading:
            logger.info(f"[OKX PAPER] 선물 롱 진입 | {market} | {amount_usdt:.2f} USDT {leverage}x")
            return {"type": "paper_long_futures_open", "market": market, "amount": amount_usdt}
        if not self._futures:
            logger.error("[OKX] 선물 클라이언트 미초기화")
            return None
        try:
            self.set_leverage_dynamic(market, leverage, pos_side="long")
            symbol = self._futures_symbol(market)
            price  = self.get_current_price(market)
            volume = amount_usdt / price
            result = self._futures.create_market_buy_order(
                symbol, volume,
                params={"tdMode": "isolated", "posSide": "long"}
            )
            logger.info(f"[OKX] 선물 롱 진입 | {symbol} | {amount_usdt:.2f} USDT | {leverage}x")
            return result
        except Exception as e:
            logger.error(f"[OKX] 선물 롱 진입 실패: {e}")
            return None

    def close_long_futures(self, market: str, volume: float) -> Optional[dict]:
        """선물 롱 청산 (시장가 매도 포지션 클로즈)"""
        if self.paper_trading:
            logger.info(f"[OKX PAPER] 선물 롱 청산 | {market} | {volume:.6f}")
            return {"type": "paper_long_futures_close", "market": market, "volume": volume}
        if not self._futures:
            return None
        try:
            symbol = self._futures_symbol(market)
            result = self._futures.create_market_sell_order(
                symbol, volume,
                params={"tdMode": "isolated", "posSide": "long", "reduceOnly": True}
            )
            logger.info(f"[OKX] 선물 롱 청산 | {symbol} | {volume:.6f}")
            return result
        except Exception as e:
            logger.error(f"[OKX] 선물 롱 청산 실패: {e}")
            return None

    # ── 선물 숏 진입/청산 ──────────────────────────────

    def open_short(self, market: str, amount_usdt: float, leverage: int = None) -> Optional[dict]:
        """
        선물 숏 진입 (시장가 매도 포지션 오픈)

        Parameters
        ----------
        amount_usdt : 진입 금액 (USDT 기준)
        leverage    : 레버리지 (None이면 기본 self.leverage 사용)
        """
        lev = leverage if leverage is not None else self.leverage
        if self.paper_trading:
            logger.info(f"[OKX PAPER] 숏 진입 | {market} | {amount_usdt:.2f} USDT {lev}x")
            return {"type": "paper_short_open", "market": market, "amount": amount_usdt}
        if not self._futures:
            logger.error("[OKX] 선물 클라이언트 미초기화")
            return None
        try:
            self.set_leverage_dynamic(market, lev, pos_side="short")
            symbol = self._futures_symbol(market)
            price  = self.get_current_price(market)
            result = self._futures.create_market_sell_order(
                symbol,
                amount_usdt / price,
                params={"tdMode": "isolated", "posSide": "short"}
            )
            logger.info(f"[OKX] 숏 진입 | {symbol} | {amount_usdt:.2f} USDT | {lev}x")
            return result
        except Exception as e:
            logger.error(f"[OKX] 숏 진입 실패: {e}")
            return None

    def close_short(self, market: str, volume: float) -> Optional[dict]:
        """
        선물 숏 청산 (시장가 매수 포지션 클로즈)

        Parameters
        ----------
        volume : 청산할 코인 수량
        """
        if self.paper_trading:
            logger.info(f"[OKX PAPER] 숏 청산 | {market} | {volume:.6f}")
            return {"type": "paper_short_close", "market": market, "volume": volume}
        if not self._futures:
            return None
        try:
            symbol = self._futures_symbol(market)
            result = self._futures.create_market_buy_order(
                symbol,
                volume,
                params={"tdMode": "isolated", "posSide": "short", "reduceOnly": True}
            )
            logger.info(f"[OKX] 숏 청산 | {symbol} | {volume:.6f}")
            return result
        except Exception as e:
            logger.error(f"[OKX] 숏 청산 실패: {e}")
            return None
