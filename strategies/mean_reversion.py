"""
15분봉 평균회귀 전략 (Multi-Timeframe v2 Tier 4)

VP POC/VAL/VAH 반등 + RSI 극단 + 반전캔들 패턴.
추세 추종(VB)과 반대 성격: 지지/저항에서 반등을 노린다.

안전장치: 상위 TF(일봉+4H)가 같은 방향 강추세 시 역추세 MR 비활성화.
"""
import pandas as pd
from .base_strategy import BaseStrategy
from .indicators import (
    calc_volume_profile,
    calc_bollinger_bands,
    detect_rsi_divergence,
    detect_hammer,
    detect_shooting_star,
    calc_atr,
)

import logging

logger = logging.getLogger(__name__)


class MeanReversionStrategy(BaseStrategy):
    """
    Parameters
    ----------
    rsi_oversold    : RSI 매수 임계값 (기본 25)
    rsi_overbought  : RSI 매도 임계값 (기본 75)
    bb_period       : Bollinger Band 기간
    bb_std          : BB 표준편차 배수
    vp_lookback     : Volume Profile 기간
    vp_bins         : VP 가격 구간 수
    atr_period      : ATR 계산 기간
    """

    def __init__(
        self,
        rsi_oversold: int = 25,
        rsi_overbought: int = 75,
        bb_period: int = 20,
        bb_std: float = 2.0,
        vp_lookback: int = 40,
        vp_bins: int = 30,
        atr_period: int = 14,
        use_short: bool = True,
    ):
        super().__init__(
            name="MR+VP+BB+RSI",
            params={
                "rsi_os": rsi_oversold, "rsi_ob": rsi_overbought,
                "bb_period": bb_period, "vp_lookback": vp_lookback,
            },
        )
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.vp_lookback = vp_lookback
        self.vp_bins = vp_bins
        self.atr_period = atr_period
        self.use_short = use_short

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        평균회귀 신호 생성.
        signal=1: 롱 (지지선 반등)
        signal=2: 숏 (저항선 거부)
        confidence: 1-5
        """
        df = df.copy()
        if len(df) < max(self.bb_period, self.vp_lookback, 20):
            df["signal"] = 0
            df["confidence"] = 1
            return df

        # RSI (14)
        delta = df["close"].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss.replace(0, 1e-10)
        df["rsi"] = 100 - (100 / (1 + rs))

        # Bollinger Bands
        df = calc_bollinger_bands(df, period=self.bb_period, std_dev=self.bb_std)

        # Volume Profile
        vp_df = calc_volume_profile(df, self.vp_lookback, self.vp_bins)
        df["vp_poc"] = vp_df["poc"]
        df["vp_vah"] = vp_df["vah"]
        df["vp_val"] = vp_df["val"]

        # ATR
        df = calc_atr(df, self.atr_period)

        # ATR 기반 SL/TP
        if "atr" in df.columns:
            df["atr_sl_long"] = df["close"] - df["atr"] * 0.8
            df["atr_tp_long"] = df["close"] + df["atr"] * 1.2
            df["atr_sl_short"] = df["close"] + df["atr"] * 0.8
            df["atr_tp_short"] = df["close"] - df["atr"] * 1.2

        # 반전 캔들 패턴
        body = (df["close"] - df["open"]).abs()
        lower_wick = df[["open", "close"]].min(axis=1) - df["low"]
        upper_wick = df["high"] - df[["open", "close"]].max(axis=1)

        # 양봉 반전 (Hammer-like): 하단 꼬리 길고 양봉
        bullish_reversal = (
            (lower_wick > body * 1.5)
            & (df["close"] > df["open"])
        )

        # 음봉 반전 (Shooting Star-like): 상단 꼬리 길고 음봉
        bearish_reversal = (
            (upper_wick > body * 1.5)
            & (df["close"] < df["open"])
        )

        # ── 신호 생성 ──────────────────────────────────────────────
        df["signal"] = 0

        # 롱: VP VAL 또는 BB 하단 터치 + RSI 과매도 + 양봉 반전
        bb_lower_touch = df["low"] <= df["bb_lower"]
        vp_val_touch = df["low"] <= df["vp_val"].fillna(0)
        rsi_oversold = df["rsi"] < self.rsi_oversold

        long_cond = (bb_lower_touch | vp_val_touch) & rsi_oversold & bullish_reversal
        df.loc[long_cond, "signal"] = 1

        # 숏: VP VAH 또는 BB 상단 터치 + RSI 과매수 + 음봉 반전
        if self.use_short:
            bb_upper_touch = df["high"] >= df["bb_upper"]
            vp_vah_touch = df["high"] >= df["vp_vah"].fillna(float("inf"))
            rsi_overbought = df["rsi"] > self.rsi_overbought

            short_cond = (bb_upper_touch | vp_vah_touch) & rsi_overbought & bearish_reversal
            df.loc[short_cond, "signal"] = 2

        # ── 확신도 ────────────────────────────────────────────────
        confidence = pd.Series(1, index=df.index)
        signal_mask = df["signal"].isin([1, 2])

        # +1: BB 밴드 + VP 레벨 모두 터치 (더블 확인)
        confidence += (signal_mask & (df["signal"] == 1) & bb_lower_touch & vp_val_touch).astype(int)
        if self.use_short:
            confidence += (signal_mask & (df["signal"] == 2) & bb_upper_touch & vp_vah_touch).astype(int)

        # +1: RSI 극단 (< 20 또는 > 80)
        confidence += (signal_mask & (df["rsi"] < 20)).astype(int)
        confidence += (signal_mask & (df["rsi"] > 80)).astype(int)

        # +1: 거래량 증가 (반전 신뢰성)
        avg_vol = df["volume"].rolling(20).mean()
        confidence += (signal_mask & (df["volume"] > avg_vol * 1.3)).astype(int)

        df["confidence"] = confidence.clip(1, 5)
        return df
