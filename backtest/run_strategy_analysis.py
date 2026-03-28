"""
전략 파라미터 분석 백테스트 (v2)

목적:
  1) AND체인 vs 점수제 성과 비교
  2) 점수 임계값별 성과 분석
  3) 필터별 차단 분석
  4) 8종목 전체 분석

사용법:
  python backtest/run_strategy_analysis.py
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import pyupbit

logging.basicConfig(level=logging.WARNING)

from strategies.indicators import (
    calc_multi_ema, ema_aligned_long, ema_aligned_short,
    calc_volume_profile, calc_fibonacci, volume_surge,
)
from strategies import VolatilityBreakoutStrategy
from backtest.backtester import Backtester

# ── 설정 ──────────────────────────────────────────────────────────
MARKETS = ["KRW-BTC", "KRW-ETH", "KRW-SOL", "KRW-XRP",
           "KRW-DOGE", "KRW-LINK", "KRW-AVAX", "KRW-SUI"]
START_DATE = "2022-01-01"
END_DATE   = "2024-12-31"
CAPITAL    = 1_000_000
COMMISSION = 0.0005
SLIPPAGE   = 0.001


def fetch(market: str) -> pd.DataFrame:
    df = pyupbit.get_ohlcv(market, interval="day", count=1200)
    if df is None or df.empty:
        return pd.DataFrame()
    df.columns = [c.lower() for c in df.columns]
    df = df[START_DATE:END_DATE]
    return df


def filter_analysis(df: pd.DataFrame, market: str):
    """필터 단계별 신호 차단 수 분석"""
    print(f"\n{'─'*55}")
    print(f"  필터 차단 분석: {market}  ({len(df)}봉)")
    print(f"{'─'*55}")

    d = df.copy()
    d["ma200"] = d["close"].rolling(200).mean()
    d = calc_multi_ema(d, [20, 55, 100, 200])
    d["prev_range"] = d["high"].shift(1) - d["low"].shift(1)
    d["target_long"] = d["open"] + d["prev_range"] * 0.4
    d["target_short"] = d["open"] - d["prev_range"] * 0.4
    d["vol_surge"] = volume_surge(d, 20, 1.5)
    vp = calc_volume_profile(d, 20, 50)
    d["vp_vah"] = vp["vah"]
    d["vp_val"] = vp["val"]

    valid = d["prev_range"].notna() & (d["prev_range"] > 0)
    total = valid.sum()

    uptrend   = d["close"] > d["ma200"]
    downtrend = d["close"] < d["ma200"]
    ema_l     = ema_aligned_long(d)
    ema_s     = ema_aligned_short(d)
    vol_ok    = d["vol_surge"]
    vp_long_ok  = d["close"] >= d["vp_val"].fillna(0)
    vp_short_ok = d["close"] <= d["vp_vah"].fillna(float("inf"))
    long_break  = (d["high"] >= d["target_long"]) & (d["open"] < d["target_long"])
    short_break = (d["low"] <= d["target_short"]) & (d["open"] > d["target_short"])

    steps_long = [
        ("유효봉",          valid),
        ("+MA200 상승추세",  valid & uptrend),
        ("+EMA 정렬",       valid & uptrend & ema_l),
        ("+거래량 급증",     valid & uptrend & ema_l & vol_ok),
        ("+VP VAL 위",      valid & uptrend & ema_l & vol_ok & vp_long_ok),
        ("+변동성 돌파",     valid & uptrend & ema_l & vol_ok & vp_long_ok & long_break),
    ]
    steps_short = [
        ("유효봉",          valid),
        ("+MA200 하락추세",  valid & downtrend),
        ("+EMA 역정렬",     valid & downtrend & ema_s),
        ("+거래량 급증",     valid & downtrend & ema_s & vol_ok),
        ("+VP VAH 아래",    valid & downtrend & ema_s & vol_ok & vp_short_ok),
        ("+변동성 돌파",     valid & downtrend & ema_s & vol_ok & vp_short_ok & short_break),
    ]

    print(f"\n  [롱 필터 단계별 잔존 봉 수]")
    for label, mask in steps_long:
        n = mask.sum()
        pct = n / total * 100 if total > 0 else 0
        print(f"    {label:<22}: {n:4d}봉 ({pct:5.1f}%)")

    print(f"\n  [숏 필터 단계별 잔존 봉 수]")
    for label, mask in steps_short:
        n = mask.sum()
        pct = n / total * 100 if total > 0 else 0
        print(f"    {label:<22}: {n:4d}봉 ({pct:5.1f}%)")


def run_scored_comparison(df: pd.DataFrame, market: str):
    """AND체인 vs 점수제 백테스트 비교"""
    print(f"\n{'='*65}")
    print(f"  AND체인 vs 점수제 비교: {market}")
    print(f"{'='*65}")
    print(f"  {'전략':<32} {'거래':>4} {'승률':>6} {'수익률':>8} {'샤프':>6} {'MDD':>7}")
    print(f"  {'─'*32} {'─'*4} {'─'*6} {'─'*8} {'─'*6} {'─'*7}")

    configs = [
        # (이름, 방식, k, use_short, min_long, min_short)
        ("AND체인 롱만 k=0.4",    "and",    0.4, False, 0, 0),
        ("AND체인 롱+숏 k=0.4",   "and",    0.4, True,  0, 0),
        ("점수제 min_L=5 min_S=4", "scored", 0.4, True,  5, 4),
        ("점수제 min_L=4 min_S=3", "scored", 0.4, True,  4, 3),
        ("점수제 min_L=4 min_S=2", "scored", 0.4, True,  4, 2),
        ("점수제 min_L=3 min_S=2", "scored", 0.4, True,  3, 2),
        ("점수제 k=0.3 L=4 S=3",  "scored", 0.3, True,  4, 3),
        ("점수제 k=0.3 L=3 S=2",  "scored", 0.3, True,  3, 2),
    ]

    best_sharpe = -99
    best_cfg = None

    for name, mode, k, use_short, min_l, min_s in configs:
        strat = VolatilityBreakoutStrategy(
            k=k,
            volume_multiplier=1.5,
            use_short=use_short,
            use_supertrend=True,
        )

        # 점수제는 generate_signals_scored 사���
        if mode == "scored":
            orig_gen = strat.generate_signals
            strat.generate_signals = lambda df_in, ml=min_l, ms=min_s: strat.generate_signals_scored(df_in, ml, ms)

        try:
            bt = Backtester(strat, CAPITAL, COMMISSION, SLIPPAGE, -0.03, 0.05)
            m = bt.run(df)
            tc = len(bt.trades) if bt.trades is not None else 0
            wr = float(m.get("승률 (%)", 0) or 0)
            tr = float(str(m.get("총 수익률 (%)", 0)).replace("%", "") or 0)
            sh = float(m.get("샤프 비율", 0) or 0)
            md = float(str(m.get("최대 낙폭 MDD (%)", 0)).replace("%", "") or 0)
            mark = " ★" if tc >= 5 and sh > best_sharpe else ""
            if tc >= 5 and sh > best_sharpe:
                best_sharpe = sh
                best_cfg = name
            print(f"  {name:<32} {tc:>4} {wr:>5.1f}% {tr:>+7.1f}% {sh:>6.3f} {md:>6.1f}%{mark}")
        except Exception as e:
            print(f"  {name:<32} 오류: {e}")

        # 원래 메서드 복원
        if mode == "scored":
            strat.generate_signals = orig_gen

    if best_cfg:
        print(f"\n  ★ 최고 샤프 전략: {best_cfg}")


def score_distribution(df: pd.DataFrame, market: str):
    """점수 분포 분석: 각 점수에 해당하는 봉 수"""
    print(f"\n{'─'*55}")
    print(f"  점수 분포 분석: {market}")
    print(f"{'─'*55}")

    strat = VolatilityBreakoutStrategy(k=0.4, use_short=True, use_supertrend=True)
    result = strat.generate_signals_scored(df, min_long_score=0, min_short_score=0)

    for direction, score_col in [("롱", "long_score"), ("숏", "short_score")]:
        print(f"\n  [{direction} 점수 분포]")
        for score in range(0, 10):
            n = (result[score_col] == score).sum()
            if n > 0:
                pct = n / len(result) * 100
                bar = "█" * min(50, int(pct * 2))
                print(f"    score={score}: {n:4d}봉 ({pct:5.1f}%) {bar}")


# ─�� 메인 ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("  전략 분석 v2 — AND체인 vs 점수제  (2022~2024 일봉)")
    print("=" * 65)

    for market in MARKETS:
        print(f"\n\n{'#'*65}")
        print(f"  {market}")
        print(f"{'#'*65}")
        df = fetch(market)
        if df.empty:
            print(f"  데이터 없음 — 스킵")
            continue
        print(f"  데이터: {len(df)}봉  ({df.index[0].date()} ~ {df.index[-1].date()})")
        filter_analysis(df, market)
        score_distribution(df, market)
        run_scored_comparison(df, market)

    print("\n" + "=" * 65 + "\n")
