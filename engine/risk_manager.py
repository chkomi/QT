"""
리스크 관리 모듈

기능:
  - 일일 최대 손실 한도 초과 시 거래 중단
  - 종목별 최대 투자 비중 제한
  - 포지션 크기 계산
  - 손절/익절 가격 계산
"""
import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


class RiskManager:
    """
    Parameters
    ----------
    initial_capital       : 초기 자본
    max_position_ratio    : 종목당 최대 비중 (기본 50%)
    stop_loss_pct         : 손절 비율 (기본 -3%)
    take_profit_pct       : 익절 비율 (기본 +5%)
    daily_loss_limit_pct  : 일일 최대 손실 한도 (기본 -10%)
    """

    def __init__(
        self,
        initial_capital: float,
        max_position_ratio: float = 0.5,
        stop_loss_pct: float = -0.03,
        take_profit_pct: float = 0.05,
        daily_loss_limit_pct: float = -0.10,
        min_order_amount: float = 10_000,
    ):
        self.initial_capital = initial_capital
        self.max_position_ratio = max_position_ratio
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.daily_loss_limit_pct = daily_loss_limit_pct
        self.min_order_amount = min_order_amount

        self._daily_start_capital: float = initial_capital
        self._last_reset_date: Optional[date] = None
        self._trading_halted: bool = False

    def reset_daily(self, current_capital: float):
        """매일 장 시작 시 호출 — 일일 손실 추적 초기화"""
        today = date.today()
        if self._last_reset_date != today:
            self._daily_start_capital = current_capital
            self._trading_halted = False
            self._last_reset_date = today
            logger.info(f"[RiskManager] 일일 초기화 | 자본: {current_capital:,.0f}원")

    def check_daily_loss(self, current_capital: float) -> bool:
        """
        일일 손실 한도 초과 여부 확인

        Returns
        -------
        True : 거래 가능
        False: 일일 손실 한도 초과 → 거래 중단
        """
        if self._trading_halted:
            return False

        daily_return = (current_capital - self._daily_start_capital) / self._daily_start_capital
        if daily_return <= self.daily_loss_limit_pct:
            self._trading_halted = True
            logger.warning(
                f"[RiskManager] 일일 손실 한도 초과! "
                f"({daily_return*100:.2f}% <= {self.daily_loss_limit_pct*100:.1f}%) "
                f"→ 당일 거래 중단"
            )
            return False
        return True

    def calc_position_size(self, available_cash: float, price: float) -> float:
        """
        매수 가능 금액 계산

        Parameters
        ----------
        available_cash : 사용 가능한 현금
        price          : 현재 코인 가격

        Returns
        -------
        투자 금액 (원)
        """
        max_invest = available_cash * self.max_position_ratio
        if max_invest < self.min_order_amount:
            logger.warning(f"[RiskManager] 투자 가능 금액 부족 (최소 {self.min_order_amount:,.0f})")
            return 0.0
        return max_invest

    def calc_stop_loss_price(self, entry_price: float) -> float:
        """손절 가격"""
        return entry_price * (1 + self.stop_loss_pct)

    def calc_take_profit_price(self, entry_price: float) -> float:
        """익절 가격"""
        return entry_price * (1 + self.take_profit_pct)

    def should_stop_loss(self, entry_price: float, current_price: float) -> bool:
        """현재가가 손절 가격 이하인지 확인"""
        return current_price <= self.calc_stop_loss_price(entry_price)

    def should_take_profit(self, entry_price: float, current_price: float) -> bool:
        """현재가가 익절 가격 이상인지 확인"""
        return current_price >= self.calc_take_profit_price(entry_price)

    def is_trading_allowed(self, current_capital: float) -> bool:
        """거래 허용 여부 종합 판단"""
        return self.check_daily_loss(current_capital)

    def get_status(self) -> dict:
        return {
            "trading_halted": self._trading_halted,
            "daily_start_capital": self._daily_start_capital,
            "stop_loss_pct": self.stop_loss_pct,
            "take_profit_pct": self.take_profit_pct,
            "daily_loss_limit_pct": self.daily_loss_limit_pct,
        }
