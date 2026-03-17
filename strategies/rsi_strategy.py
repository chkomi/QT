"""
RSI 역추세 전략 (Mean Reversion)

원리:
  RSI(14)가 과매도(30 이하) 진입 → 매수
  RSI(14)가 과매수(70 이상) 진입 → 매도/청산

특징:
  횡보장에서 강점 / 강한 추세장에서는 손실 가능
"""
import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy


class RSIStrategy(BaseStrategy):
    """
    Parameters
    ----------
    period     : RSI 계산 기간 (기본 14)
    oversold   : 과매도 기준 (기본 30)
    overbought : 과매수 기준 (기본 70)
    """

    def __init__(self, period: int = 14, oversold: int = 30, overbought: int = 70):
        super().__init__(
            name="RSI",
            params={"period": period, "oversold": oversold, "overbought": overbought},
        )
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def _compute_rsi(self, series: pd.Series) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.ewm(com=self.period - 1, min_periods=self.period).mean()
        avg_loss = loss.ewm(com=self.period - 1, min_periods=self.period).mean()

        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df["rsi"] = self._compute_rsi(df["close"])

        # 과매도 구간 진입(30 하향) → 매수 신호
        df["was_oversold"] = df["rsi"].shift(1) > self.oversold
        df["now_oversold"] = df["rsi"] <= self.oversold
        df["buy_signal"] = df["was_oversold"] & df["now_oversold"]

        # 과매수 구간 진입(70 상향) → 매도 신호
        df["was_overbought"] = df["rsi"].shift(1) < self.overbought
        df["now_overbought"] = df["rsi"] >= self.overbought
        df["sell_signal"] = df["was_overbought"] & df["now_overbought"]

        df["signal"] = 0
        df.loc[df["buy_signal"], "signal"] = 1
        df.loc[df["sell_signal"], "signal"] = -1

        # 포지션 유지
        df["position"] = 0
        position = 0
        for i in range(len(df)):
            sig = df["signal"].iloc[i]
            if sig == 1:
                position = 1
            elif sig == -1:
                position = 0
            df.iloc[i, df.columns.get_loc("position")] = position

        # 포지션 보유 중 수익률
        df["strategy_return"] = np.where(
            df["position"] == 1,
            df["close"].pct_change().shift(-1),
            0.0,
        )

        return df.dropna(subset=["rsi"])
