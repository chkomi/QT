"""
Precision Entry Engine — 5단계 진입 판단

Step 1: Regime Check (시장 상태별 전략 분기)
Step 2: MTF Alignment (7TF 방향 합치 확인)
Step 3: Entry Trigger (하위 TF 진입 트리거)
Step 4: Confirmation (거래량, OBV, 패턴 확인)
Step 5: Risk Sizing (regime별 적응형 SL/TP + 포지션 크기)

Bear→Bull 전환 감지 + Dead Cat Bounce 필터 내장.
"""
import logging
from typing import Dict, Optional, Tuple

import pandas as pd

from strategies.regime_detector import RegimeDetector
from strategies.mtf_analyzer import MTFAnalyzer
from strategies.patterns import calc_pattern_score, detect_rsi_macd_divergence
from strategies.indicators import (
    calc_rsi,
    calc_bollinger_bands,
    calc_atr,
    calc_macd,
    volume_surge,
    calc_volume_profile,
    calc_obv,
)

logger = logging.getLogger(__name__)


class EntryDecision:
    """진입 판단 결과."""
    __slots__ = (
        "should_enter", "direction", "confidence",
        "regime", "alignment", "trigger_reason",
        "sl_mult", "tp_mult", "size_mult",
        "pattern_score", "reasons",
    )

    def __init__(self):
        self.should_enter = False
        self.direction = ""      # "long" | "short"
        self.confidence = 0      # 0-10
        self.regime = "NEUTRAL"
        self.alignment = 0.0     # -10 ~ +10
        self.trigger_reason = ""
        self.sl_mult = 1.5
        self.tp_mult = 3.0
        self.size_mult = 1.0
        self.pattern_score = 0
        self.reasons: list = []


class EntryEngine:
    """
    5단계 진입 판단 엔진.

    Parameters
    ----------
    config : config.yaml 전체 dict
    """

    def __init__(self, config: dict):
        self.regime_detector = RegimeDetector(config)
        self.mtf_analyzer = MTFAnalyzer(config)

    def evaluate(
        self,
        market: str,
        ohlcv_dict: Dict[str, pd.DataFrame],
    ) -> EntryDecision:
        """
        종목에 대한 진입 여부를 5단계로 판단.

        Parameters
        ----------
        market      : "KRW-BTC" 등
        ohlcv_dict  : {"day": df, "minute240": df, ..., "minute5": df}

        Returns
        -------
        EntryDecision
        """
        d = EntryDecision()
        coin = market.replace("KRW-", "")

        df_1d = ohlcv_dict.get("day")
        df_4h = ohlcv_dict.get("minute240")
        df_1h = ohlcv_dict.get("minute60")
        df_30m = ohlcv_dict.get("minute30")
        df_15m = ohlcv_dict.get("minute15")
        df_5m = ohlcv_dict.get("minute5")

        # ── Step 1: Regime Check ─────────────────────────────
        regime, regime_meta = self.regime_detector.detect(df_4h, df_1h)
        d.regime = regime
        d.size_mult = self.regime_detector.get_size_mult(regime)
        sl_tp = self.regime_detector.get_sl_tp_mult(regime)
        d.sl_mult = sl_tp["sl"]
        d.tp_mult = sl_tp["tp"]
        d.reasons.append(f"Regime={regime} (ADX={regime_meta['adx']})")

        # ── Step 2: MTF Alignment ────────────────────────────
        alignment, breakdown = self.mtf_analyzer.calc_alignment(ohlcv_dict)
        d.alignment = alignment

        if alignment >= self.mtf_analyzer.long_threshold:
            d.direction = "long"
        elif alignment <= self.mtf_analyzer.short_threshold:
            d.direction = "short"
        else:
            d.reasons.append(f"MTF alignment={alignment:.1f} (관망)")
            return d  # 진입 안 함

        d.reasons.append(f"MTF={alignment:+.1f} → {d.direction.upper()}")

        # Regime vs Direction 필터
        if regime == "TRENDING":
            # 추세장에서는 추세 방향만 진입
            # 일봉 bias로 추세 방향 결정
            daily_bias = breakdown.get("day", 0)
            if d.direction == "long" and daily_bias < 0:
                d.reasons.append("TRENDING but daily bearish → skip long")
                return d
            if d.direction == "short" and daily_bias > 0:
                d.reasons.append("TRENDING but daily bullish → skip short")
                return d

        # ── Dead Cat Bounce / Reversal Check ──────────────────
        if d.direction == "long":
            is_dead_cat, dc_count, dc_reasons = self.mtf_analyzer.check_dead_cat_bounce(df_1d, df_4h)
            if is_dead_cat:
                d.reasons.append(f"Dead Cat Bounce ({dc_count}/4): {', '.join(dc_reasons[:2])}")
                return d  # 롱 차단

            is_reversal, rev_count, rev_reasons = self.mtf_analyzer.check_reversal_to_bull(df_1d, df_4h, df_1h)
            if is_reversal:
                d.size_mult *= 1.3  # 전환 확인되면 크기 30% 증가
                d.reasons.append(f"Bull reversal confirmed ({rev_count}/5)")

        # ── Step 3: Entry Trigger ────────────────────────────
        trigger_found, trigger_reason = self._check_entry_trigger(
            regime, d.direction, df_15m, df_5m, df_1h, df_30m,
        )
        if not trigger_found:
            d.reasons.append("No entry trigger on lower TF")
            return d

        d.trigger_reason = trigger_reason
        d.reasons.append(f"Trigger: {trigger_reason}")

        # ── Step 4: Confirmation ─────────────────────────────
        conf_df = df_15m if df_15m is not None else df_1h
        conf_score = self._check_confirmation(d.direction, conf_df, ohlcv_dict)
        d.pattern_score = conf_score

        # 최종 confidence = alignment 정규화 + 패턴 + 확인
        raw_conf = abs(alignment) / 10 * 5  # 0-5
        raw_conf += max(0, conf_score)       # 0-3
        raw_conf = max(1, min(10, int(raw_conf)))
        d.confidence = raw_conf
        d.reasons.append(f"Confidence={raw_conf} (align={abs(alignment):.1f} pattern={conf_score})")

        # ── Step 5: Risk Sizing ──────────────────────────────
        # regime에 따른 SL/TP는 이미 설정됨
        # confidence에 따른 추가 크기 조절
        if d.confidence >= 7:
            d.size_mult *= 1.2
        elif d.confidence <= 3:
            d.size_mult *= 0.6

        d.should_enter = True
        return d

    def _check_entry_trigger(
        self,
        regime: str,
        direction: str,
        df_15m: pd.DataFrame,
        df_5m: pd.DataFrame,
        df_1h: pd.DataFrame,
        df_30m: pd.DataFrame,
    ) -> Tuple[bool, str]:
        """
        하위 TF에서 진입 트리거 확인.
        """
        # 가장 짧은 사용 가능한 TF 선택
        entry_df = df_5m if df_5m is not None and len(df_5m) >= 20 else \
                   df_15m if df_15m is not None and len(df_15m) >= 20 else \
                   df_30m if df_30m is not None and len(df_30m) >= 20 else \
                   df_1h

        if entry_df is None or len(entry_df) < 20:
            return (False, "")

        # 지표 계산
        entry_df = calc_rsi(entry_df)
        entry_df = calc_bollinger_bands(entry_df)
        entry_df = calc_atr(entry_df)

        last = entry_df.iloc[-1]
        rsi = float(last.get("rsi", 50) or 50)
        close = float(last["close"])
        bb_lower = float(last.get("bb_lower", 0) or 0)
        bb_upper = float(last.get("bb_upper", 0) or 0)

        # VP 계산
        try:
            vp = calc_volume_profile(entry_df, lookback=20, bins=20)
            val = float(vp["val"].iloc[-1]) if "val" in vp.columns else 0
            vah = float(vp["vah"].iloc[-1]) if "vah" in vp.columns else float("inf")
        except Exception:
            val, vah = 0, float("inf")

        if regime == "TRENDING":
            if direction == "long":
                # Pullback → VP VAL 반등 or BB 중단선 반등 + RSI 40-60
                bb_mid = float(last.get("bb_middle", 0) or 0)
                if (close <= val * 1.01 or (bb_mid > 0 and close <= bb_mid * 1.005)) and 35 < rsi < 55:
                    return (True, f"trend_pullback_bounce (RSI={rsi:.0f})")
                # 또는 RSI가 이전봉보다 반등 (모멘텀 회복)
                if rsi > 45 and len(entry_df) > 2:
                    prev_rsi = float(entry_df["rsi"].iloc[-2])
                    if rsi > prev_rsi + 3 and prev_rsi < 45:
                        return (True, f"rsi_momentum_recovery ({prev_rsi:.0f}→{rsi:.0f})")
            else:  # short
                if (close >= vah * 0.99 or (bb_upper > 0 and close >= bb_upper * 0.995)) and 45 < rsi < 65:
                    return (True, f"trend_rally_rejection (RSI={rsi:.0f})")
                if rsi < 55 and len(entry_df) > 2:
                    prev_rsi = float(entry_df["rsi"].iloc[-2])
                    if rsi < prev_rsi - 3 and prev_rsi > 55:
                        return (True, f"rsi_momentum_fade ({prev_rsi:.0f}→{rsi:.0f})")

        elif regime == "RANGING":
            if direction == "long":
                if (close <= bb_lower or close <= val * 1.01) and rsi < 35:
                    # 양봉 반전 확인
                    if close > float(last["open"]):
                        return (True, f"range_bottom_bounce (RSI={rsi:.0f})")
            else:
                if (close >= bb_upper or close >= vah * 0.99) and rsi > 65:
                    if close < float(last["open"]):
                        return (True, f"range_top_rejection (RSI={rsi:.0f})")

        else:  # VOLATILE / NEUTRAL
            if direction == "long":
                if rsi < 30 and close > float(last["open"]):
                    return (True, f"oversold_bounce (RSI={rsi:.0f})")
            else:
                if rsi > 70 and close < float(last["open"]):
                    return (True, f"overbought_rejection (RSI={rsi:.0f})")

        return (False, "")

    def _check_confirmation(
        self,
        direction: str,
        entry_df: pd.DataFrame,
        ohlcv_dict: Dict[str, pd.DataFrame],
    ) -> int:
        """
        확인 단계: 거래량 + OBV + 패턴 점수.
        Returns: -3 ~ +3
        """
        score = 0

        if entry_df is None or len(entry_df) < 15:
            return 0

        # 1. 패턴 점수
        score += calc_pattern_score(entry_df, direction)

        # 2. 거래량 서지
        try:
            vs = volume_surge(entry_df, 20, 1.5)
            if hasattr(vs, 'iloc'):
                if bool(vs.iloc[-1]):
                    score += 1
            elif vs:
                score += 1
        except Exception:
            pass

        # 3. OBV 방향
        try:
            entry_df["obv"] = calc_obv(entry_df)
            if "obv" in entry_df.columns:
                obv = entry_df["obv"]
                if len(obv) >= 5:
                    obv_trend = obv.iloc[-1] > obv.iloc[-5]
                    if direction == "long" and obv_trend:
                        score += 1
                    elif direction == "short" and not obv_trend:
                        score += 1
        except Exception:
            pass

        # 4. Multi-TF 다이버전스 보너스
        tf_divs = {k: v for k, v in ohlcv_dict.items()
                   if v is not None and len(v) >= 20 and k in ("minute60", "minute240", "day")}
        if tf_divs:
            from strategies.patterns import detect_multi_tf_divergence
            mtf_div = detect_multi_tf_divergence(tf_divs)
            if direction == "long" and mtf_div["bullish_count"] >= 2:
                score += 2
            elif direction == "short" and mtf_div["bearish_count"] >= 2:
                score += 2

        return max(-3, min(3, score))
