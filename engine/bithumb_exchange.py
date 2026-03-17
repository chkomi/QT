"""
빗썸 거래소 어댑터 (pybithumb)

마켓 형식: "BTC" | "ETH" (업비트의 "KRW-BTC"와 달리 ticker만 사용)
내부적으로 "KRW-BTC" → "BTC" 변환 처리
"""
import os
import logging
import pybithumb
from typing import Optional
from .exchange_base import ExchangeBase

logger = logging.getLogger(__name__)


def _ticker(market: str) -> str:
    """'KRW-BTC' → 'BTC'"""
    return market.split("-")[-1]


class BithumbExchange(ExchangeBase):

    def __init__(self, paper_trading: bool = True):
        super().__init__("bithumb", paper_trading, quote_currency="KRW")
        self._client = None
        if not paper_trading:
            con_key = os.getenv("BITHUMB_CON_KEY", "")
            sec_key = os.getenv("BITHUMB_SEC_KEY", "")
            if not con_key or not sec_key:
                raise ValueError("BITHUMB_CON_KEY / BITHUMB_SEC_KEY 미설정")
            self._client = pybithumb.Bithumb(con_key, sec_key)
            logger.info("[Bithumb] 실전 모드 연결 완료")
        else:
            logger.info("[Bithumb] 페이퍼 트레이딩 모드")

    def get_balance_quote(self) -> float:
        if self.paper_trading:
            return 0.0
        try:
            balance = self._client.get_balance("BTC")  # (avail, lock, avg, avail_krw, ...)
            # 인덱스 3 = 원화 사용가능 잔고
            return float(balance[3] or 0)
        except Exception as e:
            logger.error(f"[Bithumb] KRW 잔고 오류: {e}")
            return 0.0

    def get_balance_coin(self, market: str) -> float:
        if self.paper_trading:
            return 0.0
        try:
            balance = self._client.get_balance(_ticker(market))
            return float(balance[0] or 0)  # 인덱스 0 = 코인 사용가능 잔고
        except Exception as e:
            logger.error(f"[Bithumb] {_ticker(market)} 잔고 오류: {e}")
            return 0.0

    def get_avg_buy_price(self, market: str) -> float:
        if self.paper_trading:
            return 0.0
        try:
            balance = self._client.get_balance(_ticker(market))
            return float(balance[2] or 0)  # 인덱스 2 = 평균 매수가
        except Exception as e:
            logger.error(f"[Bithumb] 평균매수가 오류: {e}")
            return 0.0

    def get_current_price(self, market: str) -> Optional[float]:
        try:
            return pybithumb.get_current_price(_ticker(market))
        except Exception as e:
            logger.error(f"[Bithumb] 현재가 오류: {e}")
            return None

    def buy_market_order(self, market: str, amount: float) -> Optional[dict]:
        if amount < 5000:
            logger.warning(f"[Bithumb] 최소 주문금액(5,000원) 미달: {amount:,.0f}원")
            return None
        if self.paper_trading:
            logger.info(f"[Bithumb PAPER] 매수 | {market} | {amount:,.0f}원")
            return {"type": "paper_buy", "market": market, "amount": amount}
        try:
            result = self._client.buy_market_order(_ticker(market), amount)
            logger.info(f"[Bithumb] 매수 | {market} | {amount:,.0f}원")
            return result
        except Exception as e:
            logger.error(f"[Bithumb] 매수 실패: {e}")
            return None

    def sell_market_order(self, market: str, volume: float) -> Optional[dict]:
        if self.paper_trading:
            logger.info(f"[Bithumb PAPER] 매도 | {market} | {volume:.6f}")
            return {"type": "paper_sell", "market": market, "volume": volume}
        try:
            result = self._client.sell_market_order(_ticker(market), volume)
            logger.info(f"[Bithumb] 매도 | {market} | {volume:.6f}")
            return result
        except Exception as e:
            logger.error(f"[Bithumb] 매도 실패: {e}")
            return None
