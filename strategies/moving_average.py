"""
이동평균 크로스 전략 (Golden Cross / Dead Cross)

원리:
  단기 이동평균(MA)이 장기 이동평균을 상향 돌파 → 매수 (골든크로스)
  단기 이동평균이 장기 이동평균을 하향 돌파 → 매도 (데드크로스)

포지션:
  크로스 신호 발생 시 다음날 시가에 매수/매도
  매도 시 현금으로 전환 (공매도 없음)
"""
import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy


class MovingAverageCrossStrategy(BaseStrategy):
    """
    Parameters
    ----------
    short_window : 단기 이동평균 기간 (기본 5일)
    long_window  : 장기 이동평균 기간 (기본 20일)
    """

    def __init__(self, short_window: int = 5, long_window: int = 20):
        super().__init__(
            name="MovingAverageCross",
            params={"short_window": short_window, "long_window": long_window},
        )
        self.short_window = short_window
        self.long_window = long_window

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df["ma_short"] = df["close"].rolling(self.short_window).mean()
        df["ma_long"] = df["close"].rolling(self.long_window).mean()

        # 크로스 감지
        df["above"] = df["ma_short"] > df["ma_long"]
        df["cross_up"] = df["above"] & ~df["above"].shift(1).fillna(False)    # 골든크로스
        df["cross_down"] = ~df["above"] & df["above"].shift(1).fillna(True)   # 데드크로스

        df["signal"] = 0
        df.loc[df["cross_up"], "signal"] = 1    # 매수
        df.loc[df["cross_down"], "signal"] = -1  # 매도

        # 포지션 상태 유지 (매수 후 매도 전까지 1 유지)
        df["position"] = 0
        position = 0
        for i in range(len(df)):
            sig = df["signal"].iloc[i]
            if sig == 1:
                position = 1
            elif sig == -1:
                position = 0
            df.iloc[i, df.columns.get_loc("position")] = position

        # 수익률: 다음날 시가 기준 진입/청산 (신호 발생 다음날 실행)
        df["entry_price"] = df["open"].shift(-1)
        df["exit_price"] = df["open"].shift(-1)

        # 포지션 보유 중 수익률 = (다음날 종가 - 당일 종가) / 당일 종가
        df["strategy_return"] = np.where(
            df["position"] == 1,
            df["close"].pct_change().shift(-1),
            0.0,
        )

        return df.dropna(subset=["ma_long"])
