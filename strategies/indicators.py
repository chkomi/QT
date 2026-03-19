"""
기술 지표 계산 모듈
워뇨띠 + 웅크웅크 매매법 기반

포함 지표:
  - Multi-EMA (20, 55, 100, 200)
  - VWAP (롤링 방식)
  - Volume Profile (POC / VAH / VAL)
  - Fibonacci 되돌림 (스윙 고저점 자동 탐지)
"""
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
from typing import Dict


# ── 다중 EMA ────────────────────────────────────────────────────────────────

def calc_multi_ema(df: pd.DataFrame, periods: list = [20, 55, 100, 200]) -> pd.DataFrame:
    """
    여러 기간의 EMA를 계산하여 DataFrame에 추가합니다.

    Returns
    -------
    ema_20, ema_55, ema_100, ema_200 컬럼이 추가된 DataFrame
    """
    df = df.copy()
    for p in periods:
        df[f"ema_{p}"] = df["close"].ewm(span=p, adjust=False).mean()
    return df


def ema_aligned_long(df: pd.DataFrame) -> pd.Series:
    """
    롱 진입 EMA 정렬 조건
    ema_20 > ema_55 AND close > ema_200
    → 단기 추세가 중기 위에 있고, 장기 추세도 상승
    """
    return (df["ema_20"] > df["ema_55"]) & (df["close"] > df["ema_200"])


def ema_aligned_short(df: pd.DataFrame) -> pd.Series:
    """
    숏 진입 EMA 정렬 조건
    ema_20 < ema_55 AND close < ema_200
    → 단기 추세가 중기 아래, 장기 추세도 하락
    """
    return (df["ema_20"] < df["ema_55"]) & (df["close"] < df["ema_200"])


# ── VWAP ────────────────────────────────────────────────────────────────────

def calc_vwap(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """
    롤링 VWAP (Volume Weighted Average Price)
    웅크웅크의 POC 근사 지지/저항선으로 활용

    Parameters
    ----------
    window : 롤링 기간 (기본 20일)

    Returns
    -------
    Series: VWAP 값
    """
    tp = (df["high"] + df["low"] + df["close"]) / 3  # Typical Price
    tp_vol = tp * df["volume"]
    vwap = (
        tp_vol.rolling(window).sum() / df["volume"].rolling(window).sum()
    )
    return vwap


# ── Volume Profile (POC / VAH / VAL) ────────────────────────────────────────

def calc_volume_profile(
    df: pd.DataFrame,
    lookback: int = 20,
    n_bins: int = 50,
    value_area_pct: float = 0.70,
) -> pd.DataFrame:
    """
    최근 lookback 봉의 Volume Profile을 계산합니다.
    웅크웅크 FRVP의 VAH / POC / VAL 자동화 근사.

    Parameters
    ----------
    lookback       : 분석 기간 (봉 수)
    n_bins         : 가격 구간 수 (세밀도)
    value_area_pct : Value Area 기준 비율 (기본 70%)

    Returns
    -------
    DataFrame: poc, vah, val 컬럼 (각 행마다 해당 시점 기준 계산)
    """
    results = []

    for i in range(len(df)):
        if i < lookback - 1:
            results.append({"poc": np.nan, "vah": np.nan, "val": np.nan})
            continue

        window = df.iloc[i - lookback + 1: i + 1]

        price_min = window["low"].min()
        price_max = window["high"].max()

        if price_max <= price_min:
            results.append({"poc": np.nan, "vah": np.nan, "val": np.nan})
            continue

        bins = np.linspace(price_min, price_max, n_bins + 1)
        bin_centers = (bins[:-1] + bins[1:]) / 2
        vol_by_bin = np.zeros(n_bins)

        for _, row in window.iterrows():
            # 각 캔들의 거래량을 고저 구간에 균등 분배
            mask = (bin_centers >= row["low"]) & (bin_centers <= row["high"])
            count = mask.sum()
            if count > 0:
                vol_by_bin[mask] += row["volume"] / count

        poc_idx = np.argmax(vol_by_bin)
        poc = bin_centers[poc_idx]

        # Value Area: POC 기준 위아래로 누적 70%
        total_vol = vol_by_bin.sum()
        target = total_vol * value_area_pct

        upper = poc_idx
        lower = poc_idx
        accumulated = vol_by_bin[poc_idx]

        while accumulated < target and (upper < n_bins - 1 or lower > 0):
            up_vol = vol_by_bin[upper + 1] if upper < n_bins - 1 else 0
            dn_vol = vol_by_bin[lower - 1] if lower > 0 else 0
            if up_vol >= dn_vol and upper < n_bins - 1:
                upper += 1
                accumulated += vol_by_bin[upper]
            elif lower > 0:
                lower -= 1
                accumulated += vol_by_bin[lower]
            else:
                upper += 1
                accumulated += vol_by_bin[upper]

        results.append({
            "poc": poc,
            "vah": bin_centers[upper],
            "val": bin_centers[lower],
        })

    return pd.DataFrame(results, index=df.index)


# ── 피보나치 되돌림 ──────────────────────────────────────────────────────────

def detect_swing(series: pd.Series, window: int = 5):
    """
    로컬 스윙 고점 / 저점 탐지

    Parameters
    ----------
    window : 좌우 비교 봉 수 (클수록 큰 스윙만 탐지)

    Returns
    -------
    (swing_highs_idx, swing_lows_idx)
    """
    arr = series.values
    highs = argrelextrema(arr, np.greater_equal, order=window)[0]
    lows  = argrelextrema(arr, np.less_equal,    order=window)[0]
    return highs, lows


def calc_fibonacci(
    df: pd.DataFrame,
    lookback: int = 50,
    swing_window: int = 5,
) -> pd.DataFrame:
    """
    최근 lookback 봉의 스윙 고저점을 기반으로 피보나치 되돌림 레벨을 계산합니다.
    웅크웅크 기법 3 (피보나치 0.382 / 0.5 / 0.618 레벨).

    Returns
    -------
    DataFrame: fib_high, fib_low, fib_236, fib_382, fib_500, fib_618 컬럼
    """
    levels_list = []
    fib_ratios = [0.236, 0.382, 0.500, 0.618]

    for i in range(len(df)):
        if i < lookback - 1:
            levels_list.append({f"fib_{int(r*1000):04d}": np.nan
                                 for r in fib_ratios} | {"fib_high": np.nan, "fib_low": np.nan})
            continue

        window = df.iloc[i - lookback + 1: i + 1]
        highs_idx, lows_idx = detect_swing(window["close"], window=swing_window)

        if len(highs_idx) == 0 or len(lows_idx) == 0:
            levels_list.append({f"fib_{int(r*1000):04d}": np.nan
                                 for r in fib_ratios} | {"fib_high": np.nan, "fib_low": np.nan})
            continue

        swing_high = window["close"].iloc[highs_idx].max()
        swing_low  = window["close"].iloc[lows_idx].min()
        rng = swing_high - swing_low

        row = {"fib_high": swing_high, "fib_low": swing_low}
        for r in fib_ratios:
            # 상승 추세 기준: 고점에서 되돌림 (고점 - 되돌림 비율)
            row[f"fib_{int(r*1000):04d}"] = swing_high - rng * r
        levels_list.append(row)

    return pd.DataFrame(levels_list, index=df.index)


def near_fib_level(
    price: float,
    fib_row: dict,
    tolerance: float = 0.01,
) -> bool:
    """
    현재가가 피보나치 레벨 ± tolerance 이내에 있는지 확인.
    신호 강도 가중치 부여에 사용.

    Parameters
    ----------
    tolerance : 레벨 근접 허용 오차 (기본 1%)
    """
    key_levels = ["fib_0236", "fib_0382", "fib_0500", "fib_0618"]
    for k in key_levels:
        level = fib_row.get(k, np.nan)
        if np.isnan(level) or level == 0:
            continue
        if abs(price - level) / level <= tolerance:
            return True
    return False


# ── 거래량 필터 ──────────────────────────────────────────────────────────────

def volume_surge(df: pd.DataFrame, lookback: int = 20, multiplier: float = 1.5) -> pd.Series:
    """
    당일 거래량이 최근 N일 평균의 multiplier 배 이상인지 확인.
    워뇨띠 + 웅크웅크 공통 — 신호 신뢰도 향상 핵심 필터.

    Returns
    -------
    Boolean Series (True = 거래량 급증 → 신뢰도 높음)
    """
    avg_vol = df["volume"].rolling(lookback).mean()
    return df["volume"] > avg_vol * multiplier


# ── ATR (Average True Range) ─────────────────────────────────────────────────

def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    ATR (Average True Range) — 변동성 측정 지표.
    Supertrend 계산 및 동적 SL/TP 산출에 사용.

    True Range = max(high-low, |high-prev_close|, |low-prev_close|)
    ATR = EMA(TR, period)

    Returns
    -------
    'atr' 컬럼이 추가된 DataFrame
    """
    df = df.copy()
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr"] = tr.ewm(span=period, adjust=False).mean()
    return df


# ── Supertrend ───────────────────────────────────────────────────────────────

def calc_supertrend(
    df: pd.DataFrame,
    period: int = 7,
    multiplier: float = 3.0,
) -> pd.DataFrame:
    """
    Supertrend — ATR 기반 동적 추세선.
    MA200보다 최근 가격 변화에 빠르게 반응하는 추세 방향 필터.

    Parameters
    ----------
    period     : ATR 기간 (기본 7)
    multiplier : ATR 배수 (기본 3.0 — TradingView 기본값)

    Returns
    -------
    'supertrend'     : 추세선 가격 (float)
    'supertrend_dir' : +1 상승추세 / -1 하락추세
    """
    df = df.copy()
    if "atr" not in df.columns:
        df = calc_atr(df, period)

    hl2 = (df["high"] + df["low"]) / 2
    upper_band = hl2 + multiplier * df["atr"]
    lower_band = hl2 - multiplier * df["atr"]

    supertrend = pd.Series(np.nan, index=df.index)
    direction  = pd.Series(1,      index=df.index)  # 1=상승, -1=하락

    for i in range(1, len(df)):
        prev_upper = upper_band.iloc[i - 1]
        prev_lower = lower_band.iloc[i - 1]
        prev_close = df["close"].iloc[i - 1]

        # 밴드 고정 (이전 밴드가 더 좋으면 유지)
        upper_band.iloc[i] = (
            upper_band.iloc[i]
            if upper_band.iloc[i] < prev_upper or prev_close > prev_upper
            else prev_upper
        )
        lower_band.iloc[i] = (
            lower_band.iloc[i]
            if lower_band.iloc[i] > prev_lower or prev_close < prev_lower
            else prev_lower
        )

        # 방향 결정
        prev_dir = direction.iloc[i - 1]
        if prev_dir == -1 and df["close"].iloc[i] > upper_band.iloc[i]:
            direction.iloc[i] = 1
        elif prev_dir == 1 and df["close"].iloc[i] < lower_band.iloc[i]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = prev_dir

        supertrend.iloc[i] = lower_band.iloc[i] if direction.iloc[i] == 1 else upper_band.iloc[i]

    df["supertrend"]     = supertrend
    df["supertrend_dir"] = direction
    return df


# ── MACD ─────────────────────────────────────────────────────────────────────

def calc_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """
    MACD (Moving Average Convergence/Divergence) — 모멘텀 방향 지표.

    macd_hist > 0 : 매수 모멘텀 강화 (롱 신뢰도↑)
    macd_hist < 0 : 매도 모멘텀 강화 (숏 신뢰도↑)

    Returns
    -------
    'macd', 'macd_signal', 'macd_hist' 컬럼이 추가된 DataFrame
    """
    df = df.copy()
    ema_fast   = df["close"].ewm(span=fast,   adjust=False).mean()
    ema_slow   = df["close"].ewm(span=slow,   adjust=False).mean()
    df["macd"]        = ema_fast - ema_slow
    df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
    df["macd_hist"]   = df["macd"] - df["macd_signal"]
    return df


# ── Bollinger Bands ──────────────────────────────────────────────────────────

def calc_bollinger_bands(
    df: pd.DataFrame,
    period: int = 20,
    std_dev: float = 2.0,
) -> pd.DataFrame:
    """
    볼린저 밴드 — 변동성 수축(Squeeze) 후 폭발적 움직임 포착.

    bb_width 좁아짐 = Squeeze = 대형 움직임 임박 신호
    스캘핑에서 Squeeze 후 돌파 방향 확인에 활용.

    Returns
    -------
    'bb_upper', 'bb_middle', 'bb_lower', 'bb_width' 컬럼이 추가된 DataFrame
    """
    df = df.copy()
    df["bb_middle"] = df["close"].rolling(period).mean()
    std             = df["close"].rolling(period).std()
    df["bb_upper"]  = df["bb_middle"] + std_dev * std
    df["bb_lower"]  = df["bb_middle"] - std_dev * std
    df["bb_width"]  = (df["bb_upper"] - df["bb_lower"]) / df["bb_middle"]
    return df


# ── RSI Divergence ───────────────────────────────────────────────────────────

def detect_rsi_divergence(
    df: pd.DataFrame,
    rsi_period: int = 14,
    lookback: int = 5,
) -> pd.DataFrame:
    """
    RSI 다이버전스 — 추세 전환 조기 감지.

    상승 다이버전스 (rsi_bull_div):
      가격 신저점 but RSI 신저점 아님 → 하락 동력 약화 → 반등 가능
    하락 다이버전스 (rsi_bear_div):
      가격 신고점 but RSI 신고점 아님 → 상승 동력 약화 → 조정 가능

    Parameters
    ----------
    rsi_period : RSI 계산 기간 (기본 14)
    lookback   : 스윙 포인트 탐지 좌우 비교 봉 수 (기본 5)

    Returns
    -------
    'rsi', 'rsi_bull_div', 'rsi_bear_div' 컬럼이 추가된 DataFrame
    """
    df = df.copy()

    # RSI 계산
    delta  = df["close"].diff()
    gain   = delta.clip(lower=0)
    loss   = (-delta).clip(lower=0)
    avg_g  = gain.ewm(span=rsi_period, adjust=False).mean()
    avg_l  = loss.ewm(span=rsi_period, adjust=False).mean()
    rs     = avg_g / avg_l.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))

    # 스윙 포인트 탐지 (기존 detect_swing 활용)
    price_highs, price_lows = detect_swing(df["close"], window=lookback)
    rsi_highs,   rsi_lows   = detect_swing(df["rsi"],   window=lookback)

    rsi_bull = np.zeros(len(df), dtype=bool)
    rsi_bear = np.zeros(len(df), dtype=bool)

    # 상승 다이버전스: 최근 가격 저점이 이전보다 낮지만 RSI 저점은 높을 때
    if len(price_lows) >= 2 and len(rsi_lows) >= 2:
        for i in range(1, len(price_lows)):
            pl_cur  = price_lows[i]
            pl_prev = price_lows[i - 1]
            # 인접한 RSI 저점 찾기
            rsi_prev_lows = rsi_lows[rsi_lows < pl_cur]
            if len(rsi_prev_lows) == 0:
                continue
            rl_prev = rsi_prev_lows[-1]
            # 조건: 가격 저점 하락 & RSI 저점 상승
            if (df["close"].iloc[pl_cur]  < df["close"].iloc[pl_prev] and
                    df["rsi"].iloc[pl_cur] > df["rsi"].iloc[rl_prev]):
                rsi_bull[pl_cur] = True

    # 하락 다이버전스: 최근 가격 고점이 이전보다 높지만 RSI 고점은 낮을 때
    if len(price_highs) >= 2 and len(rsi_highs) >= 2:
        for i in range(1, len(price_highs)):
            ph_cur  = price_highs[i]
            ph_prev = price_highs[i - 1]
            rsi_prev_highs = rsi_highs[rsi_highs < ph_cur]
            if len(rsi_prev_highs) == 0:
                continue
            rh_prev = rsi_prev_highs[-1]
            if (df["close"].iloc[ph_cur]  > df["close"].iloc[ph_prev] and
                    df["rsi"].iloc[ph_cur] < df["rsi"].iloc[rh_prev]):
                rsi_bear[ph_cur] = True

    df["rsi_bull_div"] = rsi_bull
    df["rsi_bear_div"] = rsi_bear
    return df


# ── EmperorBTC 기법 ────────────────────────────────────────────────────────────

def calc_obv(df: pd.DataFrame) -> pd.Series:
    """On-Balance Volume 계산. (EmperorBTC: OBV Divergence 기반 추세 확인)"""
    obv = [0]
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i - 1]:
            obv.append(obv[-1] + df["volume"].iloc[i])
        elif df["close"].iloc[i] < df["close"].iloc[i - 1]:
            obv.append(obv[-1] - df["volume"].iloc[i])
        else:
            obv.append(obv[-1])
    return pd.Series(obv, index=df.index)


def detect_obv_divergence(df: pd.DataFrame, lookback: int = 14) -> str:
    """
    OBV 다이버전스 감지.
    Bearish: 가격 상승 + OBV 하락 → 'bearish'  (롱 확신도 -1)
    Bullish: 가격 하락 + OBV 상승 → 'bullish'  (숏 확신도 -1)
    없으면: 'none'
    """
    if len(df) < lookback:
        return "none"
    obv = calc_obv(df)
    recent = df.tail(lookback)
    recent_obv = obv.tail(lookback)
    price_trend = recent["close"].iloc[-1] - recent["close"].iloc[0]
    obv_trend = recent_obv.iloc[-1] - recent_obv.iloc[0]
    if price_trend > 0 and obv_trend < 0:
        return "bearish"
    elif price_trend < 0 and obv_trend > 0:
        return "bullish"
    return "none"


def detect_hammer(df: pd.DataFrame, trend_lookback: int = 10) -> bool:
    """
    Hammer 캔들 감지. (EmperorBTC: 하락추세 반전 신호, 롱 확신도 +1)
    조건: 하락추세 + 아랫꼬리 ≥ 2×몸통 + 윗꼬리 < 0.3×몸통
    """
    if len(df) < trend_lookback + 1:
        return False
    last = df.iloc[-1]
    body = abs(last["close"] - last["open"])
    if body == 0:
        return False
    lower_wick = min(last["close"], last["open"]) - last["low"]
    upper_wick = last["high"] - max(last["close"], last["open"])
    is_hammer_shape = (lower_wick >= 2 * body) and (upper_wick < 0.3 * body)
    is_downtrend = df["close"].iloc[-trend_lookback] > df["close"].iloc[-2]
    return is_hammer_shape and is_downtrend


def detect_shooting_star(df: pd.DataFrame, trend_lookback: int = 10) -> bool:
    """
    Shooting Star 캔들 감지. (EmperorBTC: 상승추세 천장 경고, 롱 확신도 -1)
    조건: 상승추세 + 윗꼬리 ≥ 2×몸통 + 아랫꼬리 < 0.3×몸통 + 거래량 급증
    """
    if len(df) < trend_lookback + 1:
        return False
    last = df.iloc[-1]
    body = abs(last["close"] - last["open"])
    if body == 0:
        return False
    upper_wick = last["high"] - max(last["close"], last["open"])
    lower_wick = min(last["close"], last["open"]) - last["low"]
    is_star_shape = (upper_wick >= 2 * body) and (lower_wick < 0.3 * body)
    is_uptrend = df["close"].iloc[-trend_lookback] < df["close"].iloc[-2]
    avg_vol = df["volume"].rolling(14).mean().iloc[-1]
    vol_spike = df["volume"].iloc[-1] > 1.5 * avg_vol
    return is_star_shape and is_uptrend and vol_spike


def calc_ema_stack(df: pd.DataFrame) -> dict:
    """
    13/21 EMA 정렬 상태 반환. (EmperorBTC: 추세 방향 확인)
    bullish: 13 > 21, 모두 우상향 → 롱 확신도 +1
    bearish: 13 < 21, 모두 우하향 → 숏 확신도 +1 / 롱 확신도 -1
    """
    if len(df) < 21:
        return {"bullish": False, "bearish": False}
    ema13 = df["close"].ewm(span=13, adjust=False).mean()
    ema21 = df["close"].ewm(span=21, adjust=False).mean()
    bullish = (
        ema13.iloc[-1] > ema21.iloc[-1]
        and ema13.iloc[-1] > ema13.iloc[-3]
        and ema21.iloc[-1] > ema21.iloc[-3]
    )
    bearish = (
        ema13.iloc[-1] < ema21.iloc[-1]
        and ema13.iloc[-1] < ema13.iloc[-3]
        and ema21.iloc[-1] < ema21.iloc[-3]
    )
    return {"bullish": bullish, "bearish": bearish}
