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
