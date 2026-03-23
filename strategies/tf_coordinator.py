"""
Multi-Timeframe Coordinator (v2)

4개 타임프레임(일봉/4H/1H/15m)의 OHLCV 캐시 관리,
상위 TF bias 쿼리, Confluence Score 계산을 담당.
"""
import time
import logging
from typing import Dict, Optional, Tuple
import pandas as pd

from strategies.volatility_breakout import VolatilityBreakoutStrategy
from strategies.mean_reversion import MeanReversionStrategy

logger = logging.getLogger(__name__)

# ── OHLCV 캐시 (API Rate Limit 대응) ────────────────────────────────────
_OHLCV_CACHE: Dict[str, Tuple[float, pd.DataFrame]] = {}
_CACHE_TTL = {
    "day":       3600,   # 1시간
    "minute240":  900,   # 15분
    "minute60":   300,   # 5분
    "minute15":   120,   # 2분
}

# Tier → candle interval 매핑
TIER_INTERVAL = {
    "daily": "day",
    "4h":    "minute240",
    "1h":    "minute60",
    "15m":   "minute15",
}


def get_cached_ohlcv(
    fetch_fn,
    exchange: str,
    market: str,
    interval: str,
    count: int = 210,
) -> Optional[pd.DataFrame]:
    """OHLCV를 캐시에서 반환하거나, 만료 시 fetch_fn 호출 후 캐시."""
    key = f"{exchange}:{market}:{interval}"
    ttl = _CACHE_TTL.get(interval, 300)
    now = time.time()

    cached = _OHLCV_CACHE.get(key)
    if cached and (now - cached[0]) < ttl:
        return cached[1]

    try:
        df = fetch_fn(market, interval=interval, count=count)
        if df is not None and not df.empty:
            _OHLCV_CACHE[key] = (now, df)
            return df
    except Exception as e:
        logger.warning(f"[TFCoord] OHLCV fetch 실패 {key}: {e}")

    # 캐시에 이전 데이터 있으면 stale이라도 반환
    if cached:
        return cached[1]
    return None


class TimeframeCoordinator:
    """
    4TF 합류점수 계산기.

    Parameters
    ----------
    config : config.yaml 전체 dict
    """

    def __init__(self, config: dict):
        conf = config.get("confluence", {})
        self.daily_bias_weight = conf.get("daily_bias_weight", 3)
        self.tactical_4h_weight = conf.get("tactical_4h_weight", 2)
        self.entry_signal_weight = conf.get("entry_signal_weight", 2)
        self.macro_weight = conf.get("macro_weight", 1)
        self.top_trader_weight = conf.get("top_trader_weight", 1)
        self.volume_weight = conf.get("volume_weight", 1)
        self.min_score = conf.get("min_score", 3)

        # Tier별 전략 인스턴스 생성
        tf_strats = config.get("timeframe_strategies", {})
        self.strategies: Dict[str, object] = {}

        for tier in ["daily", "4h", "1h"]:
            p = tf_strats.get(tier, {})
            self.strategies[tier] = VolatilityBreakoutStrategy(
                k=p.get("k", 0.4),
                ma_period=p.get("ma_period", 200),
                use_short=True,
                volume_lookback=p.get("volume_lookback", 20),
                volume_multiplier=p.get("volume_multiplier", 1.5),
                vp_lookback=p.get("vp_lookback", 20),
                vp_bins=p.get("vp_bins", 50),
                fib_lookback=p.get("fib_lookback", 50),
                use_supertrend=p.get("use_supertrend", True),
                supertrend_period=p.get("supertrend_period", 7),
                supertrend_mult=p.get("supertrend_multiplier", 3.0),
                use_macd_filter=p.get("use_macd_filter", False),
                use_atr_sl=True,
                atr_period=p.get("atr_period", 14),
                atr_sl_mult=config.get("risk_tiers", {}).get(tier, {}).get("atr_sl_mult", 1.5),
                atr_tp_mult=config.get("risk_tiers", {}).get(tier, {}).get("atr_tp_mult", 3.0),
                use_rsi_div=p.get("use_rsi_divergence", True),
                use_bb_squeeze=p.get("use_bb_squeeze", False),
            )

        # 15m: MeanReversion
        p15 = tf_strats.get("15m", {})
        self.strategies["15m"] = MeanReversionStrategy(
            rsi_oversold=p15.get("rsi_oversold", 25),
            rsi_overbought=p15.get("rsi_overbought", 75),
            bb_period=p15.get("bb_period", 20),
            bb_std=p15.get("bb_std", 2.0),
            vp_lookback=p15.get("vp_lookback", 40),
            vp_bins=p15.get("vp_bins", 30),
            atr_period=p15.get("atr_period", 14),
            use_short=True,
        )

    def get_strategy(self, tier: str):
        return self.strategies.get(tier)

    def calc_daily_bias(self, df_daily: pd.DataFrame) -> Tuple[str, int]:
        """일봉 데이터 → (direction, strength)"""
        strat = self.strategies.get("daily")
        if strat is None or df_daily is None or df_daily.empty:
            return ("neutral", 0)
        # generate_signals_v2로 지표 계산 후 bias 산출
        try:
            sig_df = strat.generate_signals_v2(df_daily)
            return VolatilityBreakoutStrategy.calc_bias(sig_df)
        except Exception as e:
            logger.warning(f"[TFCoord] daily bias 계산 실패: {e}")
            return ("neutral", 0)

    def calc_4h_bias(self, df_4h: pd.DataFrame) -> Tuple[str, int]:
        """4H 데이터 → (direction, strength)"""
        strat = self.strategies.get("4h")
        if strat is None or df_4h is None or df_4h.empty:
            return ("neutral", 0)
        try:
            sig_df = strat.generate_signals_v2(df_4h)
            return VolatilityBreakoutStrategy.calc_bias(sig_df, ma_period=200)
        except Exception as e:
            logger.warning(f"[TFCoord] 4H bias 계산 실패: {e}")
            return ("neutral", 0)

    def calc_confluence(
        self,
        signal: int,
        entry_confidence: int,
        daily_bias: Tuple[str, int],
        h4_bias: Tuple[str, int],
        has_vol_surge: bool = False,
        macro_delta: int = 0,
        top_trader_delta: int = 0,
    ) -> int:
        """
        Confluence Score 계산 (0-10).

        Parameters
        ----------
        signal            : 1(롱) 또는 2(숏)
        entry_confidence  : 진입 TF 전략의 confidence (1-5)
        daily_bias        : ("bull"/"bear"/"neutral", strength 0-3)
        h4_bias           : ("bull"/"bear"/"neutral", strength 0-3)
        has_vol_surge     : 진입 캔들 거래량 서지 여부
        macro_delta       : 매크로 보정 (-3 ~ +3)
        top_trader_delta  : Top Trader 보정 (-1 ~ +2)
        """
        score = 0
        direction = "long" if signal == 1 else "short"

        # 1. 일봉 방향 일치 (0 ~ daily_bias_weight)
        d_dir, d_str = daily_bias
        if (direction == "long" and d_dir == "bull") or \
           (direction == "short" and d_dir == "bear"):
            score += min(d_str, self.daily_bias_weight)
        elif d_dir == "neutral":
            score += 0  # 중립 = 가산 없음
        # 반대 방향이면 감점 없음 (가산 0)

        # 2. 4H 방향 확인 (0 ~ tactical_4h_weight)
        h4_dir, h4_str = h4_bias
        if (direction == "long" and h4_dir == "bull") or \
           (direction == "short" and h4_dir == "bear"):
            score += min(h4_str, self.tactical_4h_weight)

        # 3. 진입 TF 신호 강도 (0 ~ entry_signal_weight)
        # confidence 1-5 → 0, 0, 1, 1, 2 매핑
        signal_pts = max(0, min(self.entry_signal_weight, (entry_confidence - 1) // 2))
        score += signal_pts

        # 4. 매크로 환경 (-macro_weight ~ +macro_weight)
        score += max(-self.macro_weight, min(self.macro_weight, macro_delta))

        # 5. Top Trader 정렬 (-top_trader_weight ~ +top_trader_weight)
        score += max(-self.top_trader_weight, min(self.top_trader_weight, top_trader_delta))

        # 6. 거래량 확인 (0 or +volume_weight)
        if has_vol_surge:
            score += self.volume_weight

        return max(0, min(10, score))

    def should_trade(self, confluence_score: int) -> bool:
        """진입 여부 판단."""
        return confluence_score >= self.min_score

    def is_strong_trend(
        self,
        daily_bias: Tuple[str, int],
        h4_bias: Tuple[str, int],
    ) -> Tuple[bool, str]:
        """
        일봉+4H 같은 방향 강추세 여부 (15m MR 안전장치).

        Returns
        -------
        (is_strong: bool, direction: str)
        """
        d_dir, d_str = daily_bias
        h_dir, h_str = h4_bias
        if d_dir == h_dir and d_dir != "neutral" and d_str >= 2 and h_str >= 2:
            return (True, d_dir)
        return (False, "neutral")
