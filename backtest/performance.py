"""
백테스팅 성과 지표 계산 모듈 (v2)

주요 지표:
  - 총 수익률 / CAGR / 샤프 비율 / 최대 낙폭
  - 승률 / 손익비
  - Calmar Ratio / 평균 보유기간 / 최대 연패 / MDD 회복일
  - 롱/숏 분리 지표
"""
import numpy as np
import pandas as pd
from typing import Dict


def calc_total_return(equity_curve: pd.Series) -> float:
    """총 수익률 (%)"""
    return (equity_curve.iloc[-1] / equity_curve.iloc[0] - 1) * 100


def calc_cagr(equity_curve: pd.Series) -> float:
    """연환산 복리 수익률 (%)"""
    total_days = (equity_curve.index[-1] - equity_curve.index[0]).days
    if total_days <= 0:
        return 0.0
    years = total_days / 365.25
    total_return = equity_curve.iloc[-1] / equity_curve.iloc[0]
    if total_return <= 0:
        return -100.0
    return (total_return ** (1 / years) - 1) * 100


def calc_sharpe(returns: pd.Series, risk_free: float = 0.03) -> float:
    """연환산 샤프 비율"""
    if returns.std() == 0:
        return 0.0
    daily_rf = risk_free / 252
    excess = returns - daily_rf
    return (excess.mean() / excess.std()) * np.sqrt(252)


def calc_max_drawdown(equity_curve: pd.Series) -> float:
    """최대 낙폭 MDD (%)"""
    rolling_max = equity_curve.cummax()
    drawdown = (equity_curve - rolling_max) / rolling_max
    return drawdown.min() * 100


def calc_calmar(equity_curve: pd.Series) -> float:
    """Calmar Ratio = CAGR / |MDD|"""
    cagr = calc_cagr(equity_curve)
    mdd = abs(calc_max_drawdown(equity_curve))
    if mdd == 0:
        return 0.0
    return cagr / mdd


def calc_win_rate(trades: pd.DataFrame) -> float:
    """승률 (%)"""
    if trades.empty or "pnl" not in trades.columns:
        return 0.0
    wins = (trades["pnl"] > 0).sum()
    return wins / len(trades) * 100


def calc_profit_factor(trades: pd.DataFrame) -> float:
    """손익비 (총 이익 / 총 손실 절대값)"""
    if trades.empty or "pnl" not in trades.columns:
        return 0.0
    total_profit = trades.loc[trades["pnl"] > 0, "pnl"].sum()
    total_loss = abs(trades.loc[trades["pnl"] < 0, "pnl"].sum())
    if total_loss == 0:
        return float("inf") if total_profit > 0 else 0.0
    return total_profit / total_loss


def calc_max_consecutive_losses(trades: pd.DataFrame) -> int:
    """최대 연패 횟수"""
    if trades.empty or "pnl" not in trades.columns:
        return 0
    is_loss = (trades["pnl"] <= 0).astype(int)
    streak = 0
    max_streak = 0
    for v in is_loss:
        if v == 1:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def calc_avg_holding_period(trades: pd.DataFrame) -> float:
    """평균 보유기간 (일)"""
    if trades.empty or "entry_date" not in trades.columns:
        return 0.0
    if "holding_bars" in trades.columns:
        return trades["holding_bars"].mean()
    durations = (pd.to_datetime(trades["exit_date"]) - pd.to_datetime(trades["entry_date"])).dt.days
    return durations.mean() if len(durations) > 0 else 0.0


def calc_mdd_recovery_days(equity_curve: pd.Series) -> int:
    """MDD 회복까지 걸린 최대 일수"""
    rolling_max = equity_curve.cummax()
    in_drawdown = equity_curve < rolling_max
    max_recovery = 0
    current_dd_start = None

    for i, (idx, val) in enumerate(equity_curve.items()):
        if in_drawdown.iloc[i]:
            if current_dd_start is None:
                current_dd_start = idx
        else:
            if current_dd_start is not None:
                recovery_days = (idx - current_dd_start).days
                max_recovery = max(max_recovery, recovery_days)
                current_dd_start = None

    # 아직 회복 안 된 경우
    if current_dd_start is not None:
        recovery_days = (equity_curve.index[-1] - current_dd_start).days
        max_recovery = max(max_recovery, recovery_days)

    return max_recovery


def calc_side_metrics(trades: pd.DataFrame, side: str) -> dict:
    """롱/숏 분리 지표"""
    if trades.empty or "side" not in trades.columns:
        return {"거래수": 0, "승률": 0.0, "평균손익": 0.0}
    side_trades = trades[trades["side"] == side]
    if side_trades.empty:
        return {"거래수": 0, "승률": 0.0, "평균손익": 0.0}
    return {
        "거래수": len(side_trades),
        "승률": round(calc_win_rate(side_trades), 1),
        "평균손익": round(side_trades["pnl"].mean(), 2),
    }


def summarize(
    equity_curve: pd.Series,
    returns: pd.Series,
    trades: pd.DataFrame,
) -> Dict:
    """전체 성과 요약 딕셔너리 반환"""
    long_m = calc_side_metrics(trades, "long")
    short_m = calc_side_metrics(trades, "short")

    return {
        "총 수익률 (%)": round(calc_total_return(equity_curve), 2),
        "연환산 수익률 CAGR (%)": round(calc_cagr(equity_curve), 2),
        "샤프 비율": round(calc_sharpe(returns), 3),
        "Calmar Ratio": round(calc_calmar(equity_curve), 3),
        "최대 낙폭 MDD (%)": round(calc_max_drawdown(equity_curve), 2),
        "MDD 회복 최대일": calc_mdd_recovery_days(equity_curve),
        "승률 (%)": round(calc_win_rate(trades), 2),
        "손익비 (Profit Factor)": round(calc_profit_factor(trades), 3),
        "총 거래 횟수": len(trades),
        "최대 연패": calc_max_consecutive_losses(trades),
        "평균 보유기간 (일)": round(calc_avg_holding_period(trades), 1),
        "롱": f"{long_m['거래수']}건 승률{long_m['승률']}% 평균{long_m['평균손익']}%",
        "숏": f"{short_m['거래수']}건 승률{short_m['승률']}% 평균{short_m['평균손익']}%",
    }
