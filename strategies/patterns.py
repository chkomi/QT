"""
캔들스틱 패턴 + 구조적 패턴 인식 모듈.

캔들 패턴: Engulfing, Doji, Morning/Evening Star, Three Soldiers/Crows
구조적 패턴: Double Bottom/Top, Higher Low/Lower High
다이버전스 캐스케이드: 여러 TF에서 동시 RSI+MACD+OBV 다이버전스
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple


# ══════════════════════════════════════════════════════════
# 캔들스틱 패턴 (단일/복합)
# ══════════════════════════════════════════════════════════

def detect_engulfing(df: pd.DataFrame) -> pd.DataFrame:
    """
    상승/하락 장악형 패턴 감지.
    Returns: df with 'bullish_engulfing', 'bearish_engulfing' bool columns.
    """
    df = df.copy()
    o, c = df["open"], df["close"]
    po, pc = o.shift(1), c.shift(1)

    # 상승 장악형: 전봉 음봉 + 현봉 양봉 + 현봉이 전봉 완전 감쌈
    df["bullish_engulfing"] = (pc < po) & (c > o) & (c > po) & (o < pc)

    # 하락 장악형: 전봉 양봉 + 현봉 음봉 + 현봉이 전봉 완전 감쌈
    df["bearish_engulfing"] = (pc > po) & (c < o) & (c < po) & (o > pc)

    return df


def detect_doji(df: pd.DataFrame, threshold: float = 0.1) -> pd.DataFrame:
    """
    도지 패턴 감지 (몸통이 전체 범위의 threshold 이하).
    """
    df = df.copy()
    body = (df["close"] - df["open"]).abs()
    total = df["high"] - df["low"]
    df["doji"] = (body < total * threshold) & (total > 0)
    return df


def detect_morning_star(df: pd.DataFrame) -> pd.DataFrame:
    """
    샛별형 (3봉 바닥 반전): 큰 음봉 → 작은 몸통 → 큰 양봉.
    """
    df = df.copy()
    o, c, h, l = df["open"], df["close"], df["high"], df["low"]
    body = (c - o).abs()
    avg_body = body.rolling(10).mean()

    big_bear = (c.shift(2) < o.shift(2)) & (body.shift(2) > avg_body.shift(2))
    small_body = body.shift(1) < avg_body.shift(1) * 0.5
    big_bull = (c > o) & (body > avg_body) & (c > (o.shift(2) + c.shift(2)) / 2)

    df["morning_star"] = big_bear & small_body & big_bull
    return df


def detect_evening_star(df: pd.DataFrame) -> pd.DataFrame:
    """
    석별형 (3봉 천장 반전): 큰 양봉 → 작은 몸통 → 큰 음봉.
    """
    df = df.copy()
    o, c = df["open"], df["close"]
    body = (c - o).abs()
    avg_body = body.rolling(10).mean()

    big_bull = (c.shift(2) > o.shift(2)) & (body.shift(2) > avg_body.shift(2))
    small_body = body.shift(1) < avg_body.shift(1) * 0.5
    big_bear = (c < o) & (body > avg_body) & (c < (o.shift(2) + c.shift(2)) / 2)

    df["evening_star"] = big_bull & small_body & big_bear
    return df


def detect_three_soldiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    적삼병 (3연속 양봉, 점진적 상승).
    """
    df = df.copy()
    c, o = df["close"], df["open"]
    bull1 = c.shift(2) > o.shift(2)
    bull2 = c.shift(1) > o.shift(1)
    bull3 = c > o
    rising = (c > c.shift(1)) & (c.shift(1) > c.shift(2))
    df["three_soldiers"] = bull1 & bull2 & bull3 & rising
    return df


def detect_three_crows(df: pd.DataFrame) -> pd.DataFrame:
    """
    흑삼병 (3연속 음봉, 점진적 하락).
    """
    df = df.copy()
    c, o = df["close"], df["open"]
    bear1 = c.shift(2) < o.shift(2)
    bear2 = c.shift(1) < o.shift(1)
    bear3 = c < o
    falling = (c < c.shift(1)) & (c.shift(1) < c.shift(2))
    df["three_crows"] = bear1 & bear2 & bear3 & falling
    return df


# ══════════════════════════════════════════════════════════
# 구조적 패턴
# ══════════════════════════════════════════════════════════

def detect_double_bottom(df: pd.DataFrame, lookback: int = 30, tolerance: float = 0.02) -> bool:
    """
    이중 바닥 (W자 패턴) 감지.
    최근 lookback 봉 내에서 2개의 유사한 저점이 있고, 중간에 반등이 있으면 True.
    """
    if len(df) < lookback:
        return False

    window = df.tail(lookback)
    lows = window["low"].values
    closes = window["close"].values

    # 저점 찾기 (5봉 기준 로컬 최저)
    local_mins = []
    for i in range(2, len(lows) - 2):
        if lows[i] <= min(lows[i - 2:i]) and lows[i] <= min(lows[i + 1:i + 3]):
            local_mins.append((i, lows[i]))

    if len(local_mins) < 2:
        return False

    # 최근 2개 저점 비교
    (i1, v1), (i2, v2) = local_mins[-2], local_mins[-1]
    if abs(i2 - i1) < 5:
        return False

    # 두 저점이 tolerance 이내로 유사
    if abs(v2 - v1) / max(v1, 1e-10) > tolerance:
        return False

    # 중간에 반등이 있어야 함 (저점보다 2% 이상 높은 고점)
    mid_high = max(closes[i1:i2])
    if (mid_high - min(v1, v2)) / min(v1, v2) < tolerance:
        return False

    return True


def detect_double_top(df: pd.DataFrame, lookback: int = 30, tolerance: float = 0.02) -> bool:
    """
    이중 천장 (M자 패턴) 감지.
    """
    if len(df) < lookback:
        return False

    window = df.tail(lookback)
    highs = window["high"].values
    closes = window["close"].values

    local_maxs = []
    for i in range(2, len(highs) - 2):
        if highs[i] >= max(highs[i - 2:i]) and highs[i] >= max(highs[i + 1:i + 3]):
            local_maxs.append((i, highs[i]))

    if len(local_maxs) < 2:
        return False

    (i1, v1), (i2, v2) = local_maxs[-2], local_maxs[-1]
    if abs(i2 - i1) < 5:
        return False

    if abs(v2 - v1) / max(v1, 1e-10) > tolerance:
        return False

    mid_low = min(closes[i1:i2])
    if (max(v1, v2) - mid_low) / max(v1, v2) < tolerance:
        return False

    return True


def detect_higher_lows(df: pd.DataFrame, lookback: int = 20) -> bool:
    """
    고점/저점 상승 패턴 (상승 추세 구조).
    최근 3개 스윙 저점이 순차적으로 상승하면 True.
    """
    if len(df) < lookback:
        return False

    window = df.tail(lookback)
    lows = window["low"].values

    swing_lows = []
    for i in range(2, len(lows) - 2):
        if lows[i] <= min(lows[max(0, i - 3):i]) and lows[i] <= min(lows[i + 1:min(len(lows), i + 4)]):
            swing_lows.append(lows[i])

    if len(swing_lows) < 3:
        return False

    recent = swing_lows[-3:]
    return recent[0] < recent[1] < recent[2]


def detect_lower_highs(df: pd.DataFrame, lookback: int = 20) -> bool:
    """
    고점/저점 하락 패턴 (하락 추세 구조).
    최근 3개 스윙 고점이 순차적으로 하락하면 True.
    """
    if len(df) < lookback:
        return False

    window = df.tail(lookback)
    highs = window["high"].values

    swing_highs = []
    for i in range(2, len(highs) - 2):
        if highs[i] >= max(highs[max(0, i - 3):i]) and highs[i] >= max(highs[i + 1:min(len(highs), i + 4)]):
            swing_highs.append(highs[i])

    if len(swing_highs) < 3:
        return False

    recent = swing_highs[-3:]
    return recent[0] > recent[1] > recent[2]


# ══════════════════════════════════════════════════════════
# 다이버전스 캐스케이드
# ══════════════════════════════════════════════════════════

def detect_rsi_macd_divergence(df: pd.DataFrame, lookback: int = 14) -> Dict[str, bool]:
    """
    RSI + MACD 동시 다이버전스 감지.
    Returns: {"bullish": bool, "bearish": bool}
    """
    if len(df) < lookback + 5:
        return {"bullish": False, "bearish": False}

    close = df["close"].values[-lookback:]
    rsi_vals = df["rsi"].values[-lookback:] if "rsi" in df.columns else None
    macd_vals = df["macd_hist"].values[-lookback:] if "macd_hist" in df.columns else None

    if rsi_vals is None or macd_vals is None:
        return {"bullish": False, "bearish": False}

    # 가격 신저점 but RSI/MACD 올라감 = 상승 다이버전스
    price_lower = close[-1] < min(close[:-3])
    rsi_higher = rsi_vals[-1] > min(rsi_vals[:-3])
    macd_higher = macd_vals[-1] > min(macd_vals[:-3])
    bullish = price_lower and rsi_higher and macd_higher

    # 가격 신고점 but RSI/MACD 내려감 = 하락 다이버전스
    price_higher = close[-1] > max(close[:-3])
    rsi_lower = rsi_vals[-1] < max(rsi_vals[:-3])
    macd_lower = macd_vals[-1] < max(macd_vals[:-3])
    bearish = price_higher and rsi_lower and macd_lower

    return {"bullish": bullish, "bearish": bearish}


def detect_multi_tf_divergence(dfs: Dict[str, pd.DataFrame]) -> Dict[str, int]:
    """
    여러 타임프레임에서 동시 다이버전스 감지.

    Parameters
    ----------
    dfs : {"1h": df_1h, "4h": df_4h, "1d": df_1d, ...}

    Returns
    -------
    {"bullish_count": int, "bearish_count": int}
    다이버전스가 2개 TF 이상에서 동시 발생하면 강력한 반전 신호.
    """
    bull_count = 0
    bear_count = 0

    for tf_name, df in dfs.items():
        if df is None or df.empty or len(df) < 20:
            continue
        div = detect_rsi_macd_divergence(df)
        if div["bullish"]:
            bull_count += 1
        if div["bearish"]:
            bear_count += 1

    return {"bullish_count": bull_count, "bearish_count": bear_count}


# ══════════════════════════════════════════════════════════
# 종합 패턴 점수
# ══════════════════════════════════════════════════════════

def calc_pattern_score(df: pd.DataFrame, direction: str = "long") -> int:
    """
    현재 봉 기준 캔들+구조 패턴 합산 점수.

    Returns: -3 ~ +3 (양수 = direction 유리, 음수 = 불리)
    """
    score = 0

    # 캔들 패턴
    df = detect_engulfing(df)
    df = detect_doji(df)
    df = detect_morning_star(df)
    df = detect_evening_star(df)
    df = detect_three_soldiers(df)
    df = detect_three_crows(df)

    last = df.iloc[-1] if len(df) > 0 else {}

    if direction == "long":
        if last.get("bullish_engulfing", False): score += 1
        if last.get("morning_star", False): score += 2
        if last.get("three_soldiers", False): score += 1
        if last.get("bearish_engulfing", False): score -= 1
        if last.get("evening_star", False): score -= 2
        if last.get("three_crows", False): score -= 1
        if detect_double_bottom(df): score += 2
        if detect_higher_lows(df): score += 1
        if detect_double_top(df): score -= 1
    else:  # short
        if last.get("bearish_engulfing", False): score += 1
        if last.get("evening_star", False): score += 2
        if last.get("three_crows", False): score += 1
        if last.get("bullish_engulfing", False): score -= 1
        if last.get("morning_star", False): score -= 2
        if last.get("three_soldiers", False): score -= 1
        if detect_double_top(df): score += 2
        if detect_lower_highs(df): score += 1
        if detect_double_bottom(df): score -= 1

    return max(-3, min(3, score))
