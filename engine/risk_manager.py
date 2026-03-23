"""
리스크 관리 모듈 (Multi-Timeframe v2)

기능:
  - Tier별 일일 최대 손실 한도 관리
  - Tier별 ATR 기반 SL/TP 계산
  - 보유시간 초과 확인
  - 종목별 최대 투자 비중 제한
  - 포지션 크기 계산
"""
import logging
from datetime import date
from typing import Optional, Dict

logger = logging.getLogger(__name__)

# 기본 Tier 리스크 파라미터
_DEFAULT_TIER_PARAMS = {
    "daily": {"atr_sl_mult": 2.0, "atr_tp_mult": 4.0, "atr_period": 14,
              "max_leverage": 3, "daily_loss_limit_pct": -0.15},
    "4h":    {"atr_sl_mult": 1.5, "atr_tp_mult": 3.0, "atr_period": 14,
              "max_leverage": 3, "daily_loss_limit_pct": -0.12},
    "1h":    {"atr_sl_mult": 1.2, "atr_tp_mult": 2.4, "atr_period": 14,
              "max_leverage": 5, "daily_loss_limit_pct": -0.10},
    "15m":   {"atr_sl_mult": 0.8, "atr_tp_mult": 1.2, "atr_period": 14,
              "max_leverage": 3, "daily_loss_limit_pct": -0.08},
}


class RiskManager:
    """
    Parameters
    ----------
    initial_capital       : 초기 자본
    max_position_ratio    : 종목당 최대 비중 (기본 50%)
    stop_loss_pct         : 손절 비율 (기본 -3%) — ATR 미사용 시 폴백
    take_profit_pct       : 익절 비율 (기본 +5%) — ATR 미사용 시 폴백
    daily_loss_limit_pct  : 일일 최대 손실 한도 (기본 -10%)
    tier_params           : config.yaml의 risk_tiers 섹션 (없으면 기본값)
    """

    def __init__(
        self,
        initial_capital: float,
        max_position_ratio: float = 0.5,
        stop_loss_pct: float = -0.03,
        take_profit_pct: float = 0.05,
        daily_loss_limit_pct: float = -0.10,
        min_order_amount: float = 10_000,
        tier_params: Optional[Dict] = None,
    ):
        self.initial_capital = initial_capital
        self.max_position_ratio = max_position_ratio
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.daily_loss_limit_pct = daily_loss_limit_pct
        self.min_order_amount = min_order_amount

        # Tier별 파라미터 병합
        self.tier_params: Dict[str, dict] = {}
        for tier, defaults in _DEFAULT_TIER_PARAMS.items():
            overrides = (tier_params or {}).get(tier, {})
            self.tier_params[tier] = {**defaults, **overrides}

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

    def should_stop_loss(
        self,
        entry_price: float,
        current_price: float,
        stop_price: Optional[float] = None,
    ) -> bool:
        """
        현재가가 손절 가격 이하인지 확인.

        Parameters
        ----------
        stop_price : 절대 손절 가격 (ATR 기반 동적 SL). None이면 비율 기준 사용.
        """
        threshold = stop_price if stop_price is not None else self.calc_stop_loss_price(entry_price)
        return current_price <= threshold

    def should_take_profit(
        self,
        entry_price: float,
        current_price: float,
        take_price: Optional[float] = None,
    ) -> bool:
        """
        현재가가 익절 가격 이상인지 확인.

        Parameters
        ----------
        take_price : 절대 익절 가격 (ATR 기반 동적 TP). None이면 비율 기준 사용.
        """
        threshold = take_price if take_price is not None else self.calc_take_profit_price(entry_price)
        return current_price >= threshold

    def is_trading_allowed(self, current_capital: float) -> bool:
        """거래 허용 여부 종합 판단"""
        return self.check_daily_loss(current_capital)

    # ── Tier별 ATR SL/TP 계산 ─────────────────────────────────

    def get_tier_params(self, tier: str) -> dict:
        """Tier 파라미터 반환 (없으면 daily 기본값)."""
        return self.tier_params.get(tier, self.tier_params.get("daily", {}))

    def calc_atr_sl_tp(
        self,
        entry_price: float,
        atr: float,
        tier: str,
        direction: str = "long",
    ) -> tuple:
        """
        ATR 기반 SL/TP 계산.

        Returns
        -------
        (sl_price, tp_price)
        """
        tp = self.get_tier_params(tier)
        sl_mult = tp.get("atr_sl_mult", 1.5)
        tp_mult = tp.get("atr_tp_mult", 3.0)

        if direction == "long":
            sl = entry_price - atr * sl_mult
            tp_price = entry_price + atr * tp_mult
        else:
            sl = entry_price + atr * sl_mult
            tp_price = entry_price - atr * tp_mult
        return (round(sl, 8), round(tp_price, 8))

    def get_max_leverage(self, tier: str) -> int:
        return self.get_tier_params(tier).get("max_leverage", 1)

    def get_status(self) -> dict:
        return {
            "trading_halted": self._trading_halted,
            "daily_start_capital": self._daily_start_capital,
            "stop_loss_pct": self.stop_loss_pct,
            "take_profit_pct": self.take_profit_pct,
            "daily_loss_limit_pct": self.daily_loss_limit_pct,
            "tier_params": self.tier_params,
        }
