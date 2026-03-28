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


# ── OKX USDT 무기한 선물 계약 크기 (심볼당 코인 수) ──
# ccxt create_market_order의 amount 파라미터는 '계약 수' 단위임
# amount_usdt / price / contract_size = contracts (정수)
_FUTURES_CONTRACT_SIZES: dict = {}  # 런타임에 load_markets로 채워짐

# 폴백용 기본값 (API 실패 시 사용)
_CONTRACT_SIZE_FALLBACK = {
    "BTC/USDT:USDT":  0.01,
    "ETH/USDT:USDT":  0.1,
    "BNB/USDT:USDT":  0.1,
    "SOL/USDT:USDT":  1.0,
    "XRP/USDT:USDT":  100.0,
    "DOGE/USDT:USDT": 1000.0,
    "LINK/USDT:USDT": 10.0,
    "SUI/USDT:USDT":  100.0,
    "POL/USDT:USDT":  100.0,
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
                self._load_contract_sizes()    # 계약 크기 로드
            logger.info(f"[OKX] 실전 모드 | 레버리지: {leverage}x | 숏: {use_short} | 종목: {len(self._markets)}개")
        else:
            # 페이퍼 모드에서도 공개 API(현재가)는 사용
            self._spot = ccxt.okx({"options": {"defaultType": "spot"}})
            logger.info(f"[OKX] 페이퍼 트레이딩 모드 | 종목: {len(self._markets)}개")

    def _load_contract_sizes(self):
        """OKX USDT 선물 계약 크기 로드 (ccxt markets 기반)"""
        global _FUTURES_CONTRACT_SIZES
        try:
            markets = self._futures.load_markets()
            for sym, mkt in markets.items():
                if ":USDT" in sym:
                    cs = float(mkt.get("contractSize") or 1.0)
                    _FUTURES_CONTRACT_SIZES[sym] = cs
            logger.info(f"[OKX] 계약 크기 로드 완료: {len(_FUTURES_CONTRACT_SIZES)}개")
        except Exception as e:
            logger.warning(f"[OKX] 계약 크기 로드 실패, 폴백 사용: {e}")
            _FUTURES_CONTRACT_SIZES.update(_CONTRACT_SIZE_FALLBACK)

    def _get_contract_size(self, symbol: str) -> float:
        """심볼의 계약 크기 반환 (미로드 시 폴백)"""
        cs = _FUTURES_CONTRACT_SIZES.get(symbol)
        if cs:
            return cs
        return _CONTRACT_SIZE_FALLBACK.get(symbol, 1.0)

    def _init_futures_settings(self):
        """선물 레버리지 및 마진 모드 초기 설정 (hedge mode: long/short 각각 설정)"""
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
    _avail_cache: dict = {} # {"ts": float, "val": float}
    _AVAIL_TTL = 5          # 초: 진입 직전 가용잔고 — 짧은 TTL로 최신값 유지

    # 가용잔고 대비 최대 투자 비율 (마진 과다 사용 방지)
    # 예: 0.85 → 가용잔고의 85%까지만 단일 진입 허용
    MAX_MARGIN_RATIO = 0.85

    def get_balance_quote(self) -> float:
        """
        OKX 총 USDT 자산.
        OKX Unified Trading Account(UTA) 사용 시 spot/futures fetch_balance() 가
        동일한 계좌 잔고를 각각 반환하므로 합산하면 2배가 됨.
        → 선물 계좌 우선 사용, 없으면 스팟으로 폴백.
        30초 TTL 캐시로 Rate Limit(50011) 방지.
        """
        if self.paper_trading:
            return 0.0
        import time
        now = time.time()
        if now - self._bal_cache.get("ts", 0) < self._BAL_TTL:
            return self._bal_cache["val"]
        total = 0.0
        if self._futures:
            # 선물(거래) 계좌만 조회 — UTA 이중 계산 방지
            try:
                fut_bal = self._futures.fetch_balance()
                total = float(fut_bal.get("USDT", {}).get("total", 0) or 0)
            except Exception as e:
                logger.error(f"[OKX] Futures USDT 잔고 오류: {e}")
        else:
            try:
                spot_bal = self._spot.fetch_balance()
                total = float(spot_bal.get("USDT", {}).get("total", 0) or 0)
            except Exception as e:
                logger.error(f"[OKX] Spot USDT 잔고 오류: {e}")
        self._bal_cache = {"ts": now, "val": total}
        return total

    def _get_available_balance(self) -> float:
        """
        선물 계좌 가용 USDT 잔고 (free).
        진입 직전 호출 — 5초 TTL 캐시로 Rate Limit 방지.
        """
        if self.paper_trading or not self._futures:
            return 0.0
        import time
        now = time.time()
        if now - self._avail_cache.get("ts", 0) < self._AVAIL_TTL:
            return self._avail_cache["val"]
        try:
            bal = self._futures.fetch_balance()
            free = float(bal.get("USDT", {}).get("free", 0) or 0)
        except Exception as e:
            logger.warning(f"[OKX] 가용잔고 조회 실패: {e}")
            free = 0.0
        self._avail_cache = {"ts": now, "val": free}
        return free

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
                        "error": False,
                    }
            return {"side": None, "volume": 0.0, "entry_price": 0.0, "error": False}
        except Exception as e:
            logger.error(f"[OKX] 포지션 조회 오류: {e}")
            return {"side": None, "volume": 0.0, "entry_price": 0.0, "error": True}

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

    def set_leverage(self, market: str, leverage: int) -> bool:
        """v2 호환 별칭 — set_leverage_dynamic (long/short 공용)"""
        return self.set_leverage_dynamic(market, leverage, pos_side="long")

    def open_long(self, market: str, amount_usdt: float, leverage: int = 1) -> Optional[dict]:
        """v2 호환 별칭 — open_long_futures"""
        return self.open_long_futures(market, amount_usdt, leverage=leverage)

    def close_long(self, market: str, volume: float) -> bool:
        """v2 호환 별칭 — close_long_futures (SL/TP 청산용)"""
        result = self.close_long_futures(market, volume)
        return result is not None

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
            # ── 가용잔고 기반 투자액 캡 (마진 과다 사용 방지) ────────────
            avail = self._get_available_balance()
            max_invest = avail * self.MAX_MARGIN_RATIO
            if amount_usdt > max_invest:
                logger.warning(
                    f"[OKX] 롱 투자액 캡: {amount_usdt:.2f}→{max_invest:.2f} USDT "
                    f"(가용잔고: {avail:.2f})"
                )
                amount_usdt = max_invest
            if amount_usdt < 1.0:
                logger.warning(f"[OKX] 가용잔고 부족으로 롱 진입 스킵 (가용: {avail:.2f} USDT)")
                return None
            self.set_leverage_dynamic(market, leverage, pos_side="long")
            symbol = self._futures_symbol(market)
            price  = self.get_current_price(market)
            cs     = self._get_contract_size(symbol)
            contracts = int(amount_usdt / price / cs)
            if contracts < 1:
                logger.warning(f"[OKX] 선물 롱 최소 계약 미달: {amount_usdt:.2f} USDT (1계약={price*cs:.2f} USDT) → 스킵")
                return None
            result = self._futures.create_market_buy_order(
                symbol, contracts,
                params={"tdMode": "isolated", "posSide": "long"}
            )
            logger.info(f"[OKX] 선물 롱 진입 | {symbol} | {contracts}계약 ({amount_usdt:.2f} USDT) | {leverage}x")
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
            # ── 가용잔고 기반 투자액 캡 (마진 과다 사용 방지) ────────────
            avail = self._get_available_balance()
            max_invest = avail * self.MAX_MARGIN_RATIO
            if amount_usdt > max_invest:
                logger.warning(
                    f"[OKX] 숏 투자액 캡: {amount_usdt:.2f}→{max_invest:.2f} USDT "
                    f"(가용잔고: {avail:.2f})"
                )
                amount_usdt = max_invest
            if amount_usdt < 1.0:
                logger.warning(f"[OKX] 가용잔고 부족으로 숏 진입 스킵 (가용: {avail:.2f} USDT)")
                return None
            self.set_leverage_dynamic(market, lev, pos_side="short")
            symbol = self._futures_symbol(market)
            price  = self.get_current_price(market)
            cs     = self._get_contract_size(symbol)
            contracts = int(amount_usdt / price / cs)
            if contracts < 1:
                logger.warning(f"[OKX] 숏 최소 계약 미달: {amount_usdt:.2f} USDT (1계약={price*cs:.2f} USDT) → 스킵")
                return None
            result = self._futures.create_market_sell_order(
                symbol, contracts,
                params={"tdMode": "isolated", "posSide": "short"}
            )
            logger.info(f"[OKX] 숏 진입 | {symbol} | {contracts}계약 ({amount_usdt:.2f} USDT) | {lev}x")
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
