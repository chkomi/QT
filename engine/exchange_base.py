"""
거래소 추상 기반 클래스
모든 거래소 어댑터는 ExchangeBase를 상속받아 구현합니다.
"""
from abc import ABC, abstractmethod
from typing import Optional


class ExchangeBase(ABC):
    """
    Attributes
    ----------
    name          : 거래소 이름 ("upbit" | "bithumb" | "okx")
    paper_trading : True이면 실제 주문 없이 로그만 출력
    quote_currency: 기준 통화 ("KRW" | "USDT")
    """

    def __init__(self, name: str, paper_trading: bool, quote_currency: str = "KRW"):
        self.name = name
        self.paper_trading = paper_trading
        self.quote_currency = quote_currency

    @abstractmethod
    def get_balance_quote(self) -> float:
        """기준 통화 잔고 (KRW 또는 USDT)"""
        pass

    @abstractmethod
    def get_balance_coin(self, market: str) -> float:
        """코인 잔고 수량"""
        pass

    @abstractmethod
    def get_avg_buy_price(self, market: str) -> float:
        """평균 매수가"""
        pass

    @abstractmethod
    def get_current_price(self, market: str) -> Optional[float]:
        """현재가"""
        pass

    @abstractmethod
    def buy_market_order(self, market: str, amount: float) -> Optional[dict]:
        """시장가 매수 (amount = 기준통화 금액)"""
        pass

    @abstractmethod
    def sell_market_order(self, market: str, volume: float) -> Optional[dict]:
        """시장가 매도 (volume = 코인 수량)"""
        pass

    def __repr__(self):
        mode = "PAPER" if self.paper_trading else "LIVE"
        return f"{self.__class__.__name__}({self.name}, {mode})"
