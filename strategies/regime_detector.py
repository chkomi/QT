"""
시장 레짐 판별 모듈 (Market Regime Detection)

3가지 레짐:
  TRENDING  — ADX > 25, BB width 확장 → 추세 추종 전략
  RANGING   — ADX < 20, BB width 수축 → 평균회귀 전략
  VOLATILE  — ATR 상위 80%ile 또는 거래량 급변 → 보수적 진입

레짐에 따라 전략 선택, 포지션 크기, SL/TP 배수가 달라진다.
"""
import logging
from typing import Tuple

import pandas as pd

from strategies.indicators import (
    calc_adx,
    calc_atr,
    calc_atr_percentile,
    calc_bollinger_bands,
)

logger = logging.getLogger(__name__)


class RegimeDetector:
    """
    Parameters (from config.yaml → regime)
    """

    def __init__(self, config: dict):
        rc = config.get("regime", {})
        self.adx_trending = rc.get("adx_trending", 25)
        self.adx_ranging = rc.get("adx_ranging", 20)
        self.atr_volatile_pctl = rc.get("atr_volatile_pctl", 0.80)
        self.vol_spike_mult = rc.get("vol_spike_mult", 3.0)

        sm = rc.get("size_mult", {})
        self.size_mult = {
            "TRENDING": sm.get("trending", 1.0),
            "RANGING": sm.get("ranging", 0.7),
            "VOLATILE": sm.get("volatile", 0.5),
            "NEUTRAL": 0.8,
        }

        sl_tp = rc.get("sl_tp_mult", {})
        self.sl_tp = {
            "TRENDING": sl_tp.get("trending", {"sl": 1.5, "tp": 3.0}),
            "RANGING": sl_tp.get("ranging", {"sl": 0.8, "tp": 1.2}),
            "VOLATILE": sl_tp.get("volatile", {"sl": 2.0, "tp": 2.0}),
            "NEUTRAL": {"sl": 1.2, "tp": 2.0},
        }

    def detect(
        self,
        df_4h: pd.DataFrame,
        df_1h: pd.DataFrame = None,
    ) -> Tuple[str, dict]:
        """
        시장 레짐 판별.

        Parameters
        ----------
        df_4h : 4시간봉 (ADX, BB width 계산용)
        df_1h : 1시간봉 (ATR percentile, 거래량 급변 계산용)

        Returns
        -------
        (regime: str, meta: dict)
            regime: "TRENDING" | "RANGING" | "VOLATILE" | "NEUTRAL"
            meta: {"adx": float, "bb_width_pct": float, "atr_pct": float, "vol_change": float}
        """
        meta = {"adx": 0, "bb_width_pct": 0.5, "atr_pct": 0.5, "vol_change": 1.0}

        # ADX from 4H
        adx_val = 0
        if df_4h is not None and len(df_4h) >= 20:
            df_4h = calc_adx(df_4h)
            adx_val = float(df_4h["adx"].iloc[-1]) if "adx" in df_4h.columns else 0
            meta["adx"] = round(adx_val, 1)

        # BB Width Percentile from 4H
        bb_pct = 0.5
        if df_4h is not None and len(df_4h) >= 25:
            df_4h = calc_bollinger_bands(df_4h)
            if "bb_width" in df_4h.columns:
                bb_series = df_4h["bb_width"].dropna().tail(100)
                if len(bb_series) > 10:
                    current_bb = bb_series.iloc[-1]
                    bb_pct = float((bb_series < current_bb).sum() / len(bb_series))
            meta["bb_width_pct"] = round(bb_pct, 2)

        # ATR Percentile from 1H
        atr_pct = 0.5
        if df_1h is not None and len(df_1h) >= 20:
            atr_pct = calc_atr_percentile(df_1h, atr_period=14, lookback=100)
            meta["atr_pct"] = round(atr_pct, 2)

        # Volume Change Rate from 1H
        vol_change = 1.0
        if df_1h is not None and len(df_1h) >= 25:
            avg_vol = df_1h["volume"].rolling(20).mean().iloc[-1]
            if avg_vol > 0:
                vol_change = float(df_1h["volume"].iloc[-1] / avg_vol)
            meta["vol_change"] = round(vol_change, 2)

        # 판별 로직
        if atr_pct > self.atr_volatile_pctl or vol_change > self.vol_spike_mult:
            regime = "VOLATILE"
        elif adx_val > self.adx_trending and bb_pct > 0.30:
            regime = "TRENDING"
        elif adx_val < self.adx_ranging and bb_pct < 0.30:
            regime = "RANGING"
        else:
            regime = "NEUTRAL"

        return regime, meta

    def get_size_mult(self, regime: str) -> float:
        return self.size_mult.get(regime, 0.8)

    def get_sl_tp_mult(self, regime: str) -> dict:
        return self.sl_tp.get(regime, {"sl": 1.2, "tp": 2.0})
