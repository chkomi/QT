"""
OKX 거래소 어댑터 (ccxt)

현물(Spot)   : 롱 전용  — BTC/USDT, ETH/USDT
선물(Futures): 롱 + 숏  — BTC/USDT:USDT, ETH/USDT:USDT (무기한 스왑)

포지션 모드: 단방향 (one-way) — long/short 동시 보유 없음
레버리지   : 기본 1x (config에서 조정 가능)
마진 모드  : isolated (포지션별 독립 마진)
"""
import os
import logging
import pandas as pd
import ccxt
from typing import Optional
from .exchange_base import ExchangeBase

logger = logging.getLogger(__name__)

# 업비트 마켓명 → OKX 심볼 변환
SPOT_MAP = {
    "KRW-BTC": "BTC/USDT",
    "KRW-ETH": "ETH/USDT",
}
FUTURES_MAP = {
    "KRW-BTC": "BTC/USDT:USDT",
    "KRW-ETH": "ETH/USDT:USDT",
}


class OKXExchange(ExchangeBase):
    """
    Parameters
    ----------
    paper_trading : True이면 실제 주문 없이 로그만 출력
    leverage      : 선물 레버리지 (기본 1)
    use_short     : 숏 전략 사용 여부 (True이면 선물 마켓 사용)
    """

    def __init__(
        self,
        paper_trading: bool = True,
        leverage: int = 1,
        use_short: bool = True,
    ):
        super().__init__("okx", paper_trading, quote_currency="USDT")
        self.leverage = leverage
        self.use_short = use_short
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
                self._futures = ccxt.okx({**base_cfg, "options": {"defaultType": "swap"}})
                self._init_futures_settings()
            logger.info(f"[OKX] 실전 모드 | 레버리지: {leverage}x | 숏: {use_short}")
        else:
            # 페이퍼 모드에서도 공개 API(현재가)는 사용
            self._spot = ccxt.okx({"options": {"defaultType": "spot"}})
            logger.info("[OKX] 페이퍼 트레이딩 모드")

    def _init_futures_settings(self):
        """선물 레버리지 및 마진 모드 초기 설정"""
        for market in FUTURES_MAP.values():
            try:
                # isolated 마진 모드 + 레버리지 설정
                self._futures.set_leverage(
                    self.leverage, market,
                    params={"mgnMode": "isolated"}
                )
                logger.info(f"[OKX] {market} | isolated {self.leverage}x 설정 완료")
            except Exception as e:
                logger.warning(f"[OKX] 마진/레버리지 설정 실패 ({market}): {e}")

    # ── 심볼 변환 ──────────────────────────────────────

    def _spot_symbol(self, market: str) -> str:
        return SPOT_MAP.get(market, market)

    def _futures_symbol(self, market: str) -> str:
        return FUTURES_MAP.get(market, market)

    # ── 잔고 조회 ──────────────────────────────────────

    def get_balance_quote(self) -> float:
        if self.paper_trading:
            return 0.0
        try:
            bal = self._spot.fetch_balance()
            return float(bal.get("USDT", {}).get("free", 0) or 0)
        except Exception as e:
            logger.error(f"[OKX] USDT 잔고 오류: {e}")
            return 0.0

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

    def fetch_ohlcv(self, market: str, interval: str = "day", count: int = 210) -> pd.DataFrame:
        """OKX ccxt로 OHLCV 수집 (USDT 기준 가격)"""
        interval_map = {
            "day": "1d", "week": "1w", "month": "1M",
            "minute60": "1h", "minute240": "4h",
            "minute30": "30m", "minute15": "15m",
            "minute5": "5m", "minute1": "1m",
        }
        tf = interval_map.get(interval, "1D")
        symbol = SPOT_MAP.get(market, market)
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

    # ── 선물 숏 진입/청산 ──────────────────────────────

    def open_short(self, market: str, amount_usdt: float) -> Optional[dict]:
        """
        선물 숏 진입 (시장가 매도 포지션 오픈)

        Parameters
        ----------
        amount_usdt : 진입 금액 (USDT 기준)
        """
        if self.paper_trading:
            logger.info(f"[OKX PAPER] 숏 진입 | {market} | {amount_usdt:.2f} USDT")
            return {"type": "paper_short_open", "market": market, "amount": amount_usdt}
        if not self._futures:
            logger.error("[OKX] 선물 클라이언트 미초기화")
            return None
        try:
            symbol = self._futures_symbol(market)
            price  = self.get_current_price(market)
            # 계약 수량 계산 (1 계약 = 0.01 BTC or 0.1 ETH 등 OKX 기준)
            # create_market_sell_order에 USDT 금액을 넘기면 ccxt가 자동 변환
            result = self._futures.create_market_sell_order(
                symbol,
                amount_usdt / price,          # 코인 수량
                params={
                    "tdMode": "isolated",
                    "posSide": "short",
                }
            )
            logger.info(f"[OKX] 숏 진입 | {symbol} | {amount_usdt:.2f} USDT | 레버리지: {self.leverage}x")
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
                params={
                    "tdMode": "isolated",
                    "posSide": "short",
                }
            )
            logger.info(f"[OKX] 숏 청산 | {symbol} | {volume:.6f}")
            return result
        except Exception as e:
            logger.error(f"[OKX] 숏 청산 실패: {e}")
            return None
