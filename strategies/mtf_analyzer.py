"""
Multi-Timeframe Analyzer — 7TF 신호 융합

7개 타임프레임(1D/4H/1H/30m/15m/5m/1m)의 bias를 가중 합산하여
Alignment Score(-10 ~ +10)를 산출.

양수 = 롱 우세, 음수 = 숏 우세.
|score| > threshold 이면 해당 방향 진입 허용.
"""
import logging
from typing import Dict, Tuple, Optional

import pandas as pd

from strategies.indicators import (
    calc_multi_ema,
    calc_atr,
    calc_supertrend,
    calc_macd,
    calc_bollinger_bands,
    calc_rsi,
    calc_obv,
    volume_surge,
)
from strategies.patterns import (
    calc_pattern_score,
    detect_rsi_macd_divergence,
    detect_multi_tf_divergence,
    detect_double_bottom,
    detect_double_top,
    detect_higher_lows,
    detect_lower_highs,
)

logger = logging.getLogger(__name__)

# 기본 TF 가중치
_DEFAULT_WEIGHTS = {
    "day":       3.0,
    "minute240": 2.5,
    "minute60":  2.0,
    "minute30":  1.5,
    "minute15":  1.0,
    "minute5":   0.5,
    "minute1":   0.0,   # 모니터링 전용
}


class MTFAnalyzer:
    """
    Parameters
    ----------
    config : config.yaml 전체 dict
    """

    def __init__(self, config: dict):
        mtf = config.get("mtf_analysis", {})
        self.weights = mtf.get("tf_weights", _DEFAULT_WEIGHTS)
        thresholds = mtf.get("alignment_threshold", {})
        self.long_threshold = thresholds.get("long", 5.0)
        self.short_threshold = thresholds.get("short", -5.0)

        rev = config.get("reversal_detection", {})
        self.reversal_min = rev.get("min_conditions", 3)
        self.dead_cat_min = rev.get("dead_cat_bounce_conditions", 3)

    def calc_tf_bias(self, df: pd.DataFrame) -> float:
        """
        단일 타임프레임의 bias 점수 계산 (-3 ~ +3).

        지표 조합:
          1. 추세 방향 (EMA 20 vs 55, close vs MA200)
          2. 모멘텀 (MACD histogram 방향, RSI 수준)
          3. Supertrend 방향
          4. 거래량 확인
        """
        if df is None or len(df) < 30:
            return 0.0

        score = 0.0

        # 지표 계산 (이미 있으면 재활용)
        if "ema_20" not in df.columns:
            df = calc_multi_ema(df, [20, 55, 200])
        if "rsi" not in df.columns:
            df = calc_rsi(df)
        if "macd_hist" not in df.columns:
            df = calc_macd(df)
        if "supertrend_dir" not in df.columns:
            try:
                df = calc_atr(df, 14)
                df = calc_supertrend(df, 7, 3.0)
            except Exception:
                pass

        last = df.iloc[-1]
        close = float(last["close"])

        # 1. 추세 방향 (+/-1.0)
        ema20 = float(last.get("ema_20", 0) or 0)
        ema55 = float(last.get("ema_55", 0) or 0)
        ma200 = float(last.get("ema_200", 0) or 0)

        if ema20 > 0 and ema55 > 0:
            if ema20 > ema55:
                score += 0.5
            elif ema20 < ema55:
                score -= 0.5

        if ma200 > 0:
            if close > ma200:
                score += 0.5
            elif close < ma200:
                score -= 0.5

        # 2. 모멘텀 (+/-1.0)
        macd_h = float(last.get("macd_hist", 0) or 0)
        prev_macd = float(df["macd_hist"].iloc[-2]) if len(df) > 1 and "macd_hist" in df.columns else 0

        if macd_h > 0:
            score += 0.3
            if macd_h > prev_macd:
                score += 0.2  # 가속
        elif macd_h < 0:
            score -= 0.3
            if macd_h < prev_macd:
                score -= 0.2

        rsi = float(last.get("rsi", 50) or 50)
        if rsi > 60:
            score += 0.3
        elif rsi < 40:
            score -= 0.3

        # 3. Supertrend (+/-0.5)
        st = int(last.get("supertrend_dir", 0) or 0)
        if st == 1:
            score += 0.5
        elif st == -1:
            score -= 0.5

        # 4. 거래량 확인 (+/-0.5)
        vol = float(last.get("volume", 0) or 0)
        avg_vol = float(df["volume"].rolling(20).mean().iloc[-1]) if len(df) >= 20 else vol
        bullish_candle = close > float(last.get("open", close))

        if avg_vol > 0 and vol > avg_vol * 1.5:
            if bullish_candle:
                score += 0.5
            else:
                score -= 0.5

        return max(-3.0, min(3.0, score))

    def calc_alignment(self, ohlcv_dict: Dict[str, pd.DataFrame]) -> Tuple[float, Dict]:
        """
        7TF bias를 가중 합산하여 Alignment Score 계산.

        Parameters
        ----------
        ohlcv_dict : {"day": df_1d, "minute240": df_4h, "minute60": df_1h, ...}

        Returns
        -------
        (score: float, breakdown: dict)
            score: -10 ~ +10 (정규화)
            breakdown: TF별 bias 상세
        """
        raw_sum = 0.0
        max_possible = 0.0
        breakdown = {}

        for tf_key, weight in self.weights.items():
            if weight <= 0:
                continue
            df = ohlcv_dict.get(tf_key)
            bias = self.calc_tf_bias(df)
            weighted = bias * weight
            raw_sum += weighted
            max_possible += 3.0 * weight  # 최대 ±3 × weight
            breakdown[tf_key] = round(bias, 2)

        # 정규화: raw_sum / max_possible × 10
        if max_possible > 0:
            score = (raw_sum / max_possible) * 10
        else:
            score = 0.0

        return round(score, 2), breakdown

    def should_long(self, score: float) -> bool:
        return score >= self.long_threshold

    def should_short(self, score: float) -> bool:
        return score <= self.short_threshold

    # ── Bear→Bull 전환 판별 ──────────────────────────────────

    def check_reversal_to_bull(
        self,
        df_1d: pd.DataFrame,
        df_4h: pd.DataFrame,
        df_1h: pd.DataFrame,
    ) -> Tuple[bool, int, list]:
        """
        하락→상승 전환 확인. 5개 조건 중 min_conditions 이상 충족 시 True.

        Returns: (is_reversal, met_count, reasons)
        """
        conditions_met = 0
        reasons = []

        # 1. 일봉 RSI 30 아래에서 상승 다이버전스
        if df_1d is not None and len(df_1d) >= 20:
            df_1d = calc_rsi(df_1d)
            df_1d = calc_macd(df_1d)
            div = detect_rsi_macd_divergence(df_1d)
            if div["bullish"]:
                conditions_met += 1
                reasons.append("1D RSI+MACD bullish divergence")

        # 2. 4H MACD 히스토그램 음→양 크로스
        if df_4h is not None and len(df_4h) >= 30:
            df_4h = calc_macd(df_4h)
            if "macd_hist" in df_4h.columns:
                h = df_4h["macd_hist"]
                if len(h) >= 2 and h.iloc[-2] < 0 and h.iloc[-1] > 0:
                    conditions_met += 1
                    reasons.append("4H MACD histogram cross ↑")

        # 3. 1H에서 이중바닥 or 고점/저점 상승
        if df_1h is not None and len(df_1h) >= 30:
            if detect_double_bottom(df_1h) or detect_higher_lows(df_1h):
                conditions_met += 1
                reasons.append("1H double bottom or higher lows")

        # 4. 일봉 거래량 급증 (항복 매도 후 반등)
        if df_1d is not None and len(df_1d) >= 25:
            avg_vol = df_1d["volume"].rolling(20).mean().iloc[-1]
            if avg_vol > 0 and df_1d["volume"].iloc[-1] > avg_vol * 2:
                conditions_met += 1
                reasons.append("1D volume surge >2x (capitulation bounce)")

        # 5. OBV 선행 상승
        if df_1d is not None and len(df_1d) >= 15:
            df_1d["obv"] = calc_obv(df_1d)
            if "obv" in df_1d.columns:
                obv = df_1d["obv"]
                price = df_1d["close"]
                if obv.iloc[-1] > obv.iloc[-5] and price.iloc[-1] <= price.iloc[-5]:
                    conditions_met += 1
                    reasons.append("OBV rising while price flat/down")

        return (conditions_met >= self.reversal_min, conditions_met, reasons)

    def check_dead_cat_bounce(
        self,
        df_1d: pd.DataFrame,
        df_4h: pd.DataFrame,
    ) -> Tuple[bool, int, list]:
        """
        가짜 반등(Dead Cat Bounce) 판별. 3개 이상 해당이면 True → 롱 차단.

        Returns: (is_dead_cat, met_count, reasons)
        """
        conditions_met = 0
        reasons = []

        if df_1d is None or len(df_1d) < 30:
            return (False, 0, [])

        close = float(df_1d["close"].iloc[-1])

        # 1. 일봉 MA200 아래 + 4H Supertrend 하락
        ma200 = float(df_1d["close"].rolling(200).mean().iloc[-1]) if len(df_1d) >= 200 else 0
        if ma200 > 0 and close < ma200:
            if df_4h is not None and "supertrend_dir" not in df_4h.columns:
                df_4h = calc_atr(df_4h, 14)
                df_4h = calc_supertrend(df_4h, 7, 3.0)
            if df_4h is not None and "supertrend_dir" in df_4h.columns:
                if int(df_4h["supertrend_dir"].iloc[-1]) == -1:
                    conditions_met += 1
                    reasons.append("Below MA200 + 4H Supertrend bearish")

        # 2. 반등 거래량 < 하락 거래량의 50%
        if len(df_1d) >= 10:
            recent_vol = df_1d["volume"].iloc[-3:].mean()
            prev_vol = df_1d["volume"].iloc[-8:-3].mean()
            if prev_vol > 0 and recent_vol < prev_vol * 0.5:
                conditions_met += 1
                reasons.append("Bounce volume < 50% of decline volume")

        # 3. RSI 50 돌파 실패
        df_1d = calc_rsi(df_1d)
        if "rsi" in df_1d.columns:
            rsi = float(df_1d["rsi"].iloc[-1])
            rsi_max_5 = float(df_1d["rsi"].iloc[-5:].max())
            if rsi_max_5 < 55 and rsi < rsi_max_5:
                conditions_met += 1
                reasons.append(f"RSI failed at {rsi_max_5:.1f}, now {rsi:.1f}")

        # 4. OBV 신저점 (가격 반등에도)
        df_1d = calc_obv(df_1d)
        if "obv" in df_1d.columns:
            obv = df_1d["obv"]
            if obv.iloc[-1] < obv.iloc[-10:].min() * 1.01:
                conditions_met += 1
                reasons.append("OBV making new low despite price bounce")

        return (conditions_met >= self.dead_cat_min, conditions_met, reasons)
