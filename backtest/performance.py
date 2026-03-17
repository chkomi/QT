"""
백테스팅 성과 지표 계산 모듈

주요 지표:
  - 총 수익률 (Total Return)
  - 연환산 수익률 (CAGR)
  - 샤프 비율 (Sharpe Ratio)
  - 최대 낙폭 (Max Drawdown)
  - 승률 (Win Rate)
  - 손익비 (Profit Factor)
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
    return (total_return ** (1 / years) - 1) * 100


def calc_sharpe(returns: pd.Series, risk_free: float = 0.03) -> float:
    """
    연환산 샤프 비율

    Parameters
    ----------
    returns    : 일별 수익률 Series
    risk_free  : 무위험 수익률 (연간, 기본 3%)
    """
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


def calc_win_rate(trades: pd.DataFrame) -> float:
    """승률 (%) — trades 에 'pnl' 컬럼 필요"""
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
        return float("inf")
    return total_profit / total_loss


def summarize(
    equity_curve: pd.Series,
    returns: pd.Series,
    trades: pd.DataFrame,
) -> Dict:
    """전체 성과 요약 딕셔너리 반환"""
    return {
        "총 수익률 (%)": round(calc_total_return(equity_curve), 2),
        "연환산 수익률 CAGR (%)": round(calc_cagr(equity_curve), 2),
        "샤프 비율": round(calc_sharpe(returns), 3),
        "최대 낙폭 MDD (%)": round(calc_max_drawdown(equity_curve), 2),
        "승률 (%)": round(calc_win_rate(trades), 2),
        "손익비 (Profit Factor)": round(calc_profit_factor(trades), 3),
        "총 거래 횟수": len(trades),
    }
