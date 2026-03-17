"""업비트 거래소 어댑터"""
import os
import logging
import pyupbit
from typing import Optional
from .exchange_base import ExchangeBase

logger = logging.getLogger(__name__)


class UpbitExchange(ExchangeBase):

    def __init__(self, paper_trading: bool = True):
        super().__init__("upbit", paper_trading, quote_currency="KRW")
        self._client = None
        if not paper_trading:
            access = os.getenv("UPBIT_ACCESS_KEY", "")
            secret = os.getenv("UPBIT_SECRET_KEY", "")
            if not access or not secret:
                raise ValueError("UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY 미설정")
            self._client = pyupbit.Upbit(access, secret)
            logger.info("[Upbit] 실전 모드 연결 완료")
        else:
            logger.info("[Upbit] 페이퍼 트레이딩 모드")

    def get_balance_quote(self) -> float:
        if self.paper_trading:
            return 0.0
        try:
            return float(self._client.get_balance("KRW") or 0)
        except Exception as e:
            logger.error(f"[Upbit] KRW 잔고 오류: {e}")
            return 0.0

    def get_balance_coin(self, market: str) -> float:
        if self.paper_trading:
            return 0.0
        ticker = market.split("-")[-1]
        try:
            return float(self._client.get_balance(ticker) or 0)
        except Exception as e:
            logger.error(f"[Upbit] {ticker} 잔고 오류: {e}")
            return 0.0

    def get_avg_buy_price(self, market: str) -> float:
        if self.paper_trading:
            return 0.0
        ticker = market.split("-")[-1]
        try:
            return float(self._client.get_avg_buy_price(ticker) or 0)
        except Exception as e:
            logger.error(f"[Upbit] 평균매수가 오류: {e}")
            return 0.0

    def get_current_price(self, market: str) -> Optional[float]:
        try:
            return pyupbit.get_current_price(market)
        except Exception as e:
            logger.error(f"[Upbit] 현재가 오류: {e}")
            return None

    def buy_market_order(self, market: str, amount: float) -> Optional[dict]:
        if amount < 5000:
            logger.warning(f"[Upbit] 최소 주문금액(5,000원) 미달: {amount:,.0f}원")
            return None
        if self.paper_trading:
            logger.info(f"[Upbit PAPER] 매수 | {market} | {amount:,.0f}원")
            return {"type": "paper_buy", "market": market, "amount": amount}
        try:
            result = self._client.buy_market_order(market, amount)
            logger.info(f"[Upbit] 매수 | {market} | {amount:,.0f}원")
            return result
        except Exception as e:
            logger.error(f"[Upbit] 매수 실패: {e}")
            return None

    def sell_market_order(self, market: str, volume: float) -> Optional[dict]:
        if self.paper_trading:
            logger.info(f"[Upbit PAPER] 매도 | {market} | {volume:.6f}")
            return {"type": "paper_sell", "market": market, "volume": volume}
        try:
            result = self._client.sell_market_order(market, volume)
            logger.info(f"[Upbit] 매도 | {market} | {volume:.6f}")
            return result
        except Exception as e:
            logger.error(f"[Upbit] 매도 실패: {e}")
            return None
