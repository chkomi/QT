"""
전략 파라미터 분석 백테스트

목적:
  1) 현재 전략에서 신호가 0개인 이유 파악 (필터별 차단 분석)
  2) 파라미터 조합별 성과 비교 (거래 횟수 / 승률 / 샤프 / MDD)
  3) OKX 롱/숏 포함한 전체 분석

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
MARKETS     = ["KRW-BTC", "KRW-ETH"]
START_DATE  = "2022-01-01"
END_DATE    = "2024-12-31"
CAPITAL     = 1_000_000
COMMISSION  = 0.0005
SLIPPAGE    = 0.001


def fetch(market: str) -> pd.DataFrame:
    df = pyupbit.get_ohlcv(market, interval="day", count=1200)
    if df is None or df.empty:
        return pd.DataFrame()
    df.columns = [c.lower() for c in df.columns]
    # 기간 필터
    df = df[START_DATE:END_DATE]
    return df


def filter_analysis(df: pd.DataFrame, market: str):
    """필터 단계별 신호 차단 수 분석"""
    print(f"\n{'─'*55}")
    print(f"  필터 차단 분석: {market}  ({len(df)}봉)")
    print(f"{'─'*55}")

    d = df.copy()
    d["ma200"]      = d["close"].rolling(200).mean()
    d = calc_multi_ema(d, [20, 55, 100, 200])
    d["prev_range"] = d["high"].shift(1) - d["low"].shift(1)
    d["target_long"]  = d["open"] + d["prev_range"] * 0.5
    d["target_short"] = d["open"] - d["prev_range"] * 0.5
    d["vol_surge"]  = volume_surge(d, 20, 1.5)
    vp = calc_volume_profile(d, 20, 50)
    d["vp_vah"] = vp["vah"]
    d["vp_val"] = vp["val"]

    valid = d["prev_range"].notna() & (d["prev_range"] > 0)
    total = valid.sum()

    uptrend      = d["close"] > d["ma200"]
    ema_l        = ema_aligned_long(d)
    vol_ok       = d["vol_surge"]
    vp_long_ok   = d["close"] >= d["vp_val"].fillna(0)
    long_break   = (d["high"] >= d["target_long"]) & (d["open"] < d["target_long"])

    downtrend    = d["close"] < d["ma200"]
    ema_s        = ema_aligned_short(d)
    vp_short_ok  = d["close"] <= d["vp_vah"].fillna(float("inf"))
    short_break  = (d["low"] <= d["target_short"]) & (d["open"] > d["target_short"])

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
        ("+EMA 정렬",       valid & downtrend & ema_s),
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


def run_param_sweep(df: pd.DataFrame, market: str):
    """파라미터 조합 백테스트"""
    print(f"\n{'='*65}")
    print(f"  파라미터 스윕: {market}")
    print(f"{'='*65}")
    print(f"  {'전략':<38} {'거래':>4} {'승률':>6} {'수익률':>8} {'샤프':>6} {'MDD':>7}")
    print(f"  {'─'*38} {'─'*4} {'─'*6} {'─'*8} {'─'*6} {'─'*7}")

    configs = [
        # (이름,  k,   vol_mult, ema_on, vp_on,  use_short)
        ("현재설정 k=0.5 full",       0.5, 1.5, True,  True,  False),
        ("k=0.5 full L/S",            0.5, 1.5, True,  True,  True),
        ("k=0.4 full",                0.4, 1.5, True,  True,  False),
        ("k=0.3 full",                0.3, 1.5, True,  True,  False),
        ("k=0.5 vol완화(1.2x)",       0.5, 1.2, True,  True,  False),
        ("k=0.5 vol완화(1.2x) L/S",   0.5, 1.2, True,  True,  True),
        ("k=0.5 EMA제거",             0.5, 1.5, False, True,  False),
        ("k=0.5 VP제거",              0.5, 1.5, True,  False, False),
        ("k=0.5 EMA+VP제거",          0.5, 1.5, False, False, False),
        ("k=0.5 EMA+VP제거 L/S",      0.5, 1.5, False, False, True),
        ("k=0.4 vol=1.2 EMA+VP제거",  0.4, 1.2, False, False, False),
        ("MA필터만(no EMA/VP/vol)",    0.5, 0.0, False, False, False),
    ]

    best_sharpe = -99
    best_cfg    = None

    for name, k, vm, ema_on, vp_on, use_short in configs:
        # EMA/VP 제거는 volume_multiplier=0으로 vol 필터도 끔
        vol_mult = vm if vm > 0 else 9999  # 9999 → 거의 항상 False

        class CustomVB(VolatilityBreakoutStrategy):
            def generate_signals(self, df_in):
                import pandas as pd, numpy as np
                from strategies.indicators import (
                    calc_multi_ema, ema_aligned_long, ema_aligned_short,
                    calc_volume_profile, calc_fibonacci, volume_surge, near_fib_level,
                )
                d = df_in.copy()
                d["ma200"]      = d["close"].rolling(self.ma_period).mean()
                d = calc_multi_ema(d, [20, 55, 100, 200])
                d["prev_range"]   = d["high"].shift(1) - d["low"].shift(1)
                d["target_long"]  = d["open"] + d["prev_range"] * self.k
                d["target_short"] = d["open"] - d["prev_range"] * self.k
                d["vol_surge"] = volume_surge(d, self.volume_lookback, self.volume_multiplier)
                vp_df = calc_volume_profile(d, self.vp_lookback, self.vp_bins)
                d["vp_poc"] = vp_df["poc"]
                d["vp_vah"] = vp_df["vah"]
                d["vp_val"] = vp_df["val"]
                fib_df = calc_fibonacci(d, self.fib_lookback)
                for col in fib_df.columns:
                    d[col] = fib_df[col]

                valid     = d["prev_range"].notna() & (d["prev_range"] > 0)
                uptrend   = d["close"] > d["ma200"]
                downtrend = d["close"] < d["ma200"]
                vol_ok    = d["vol_surge"] if self.volume_multiplier < 9999 else pd.Series(True, index=d.index)
                ema_l     = ema_aligned_long(d)  if self._ema_on else pd.Series(True, index=d.index)
                ema_s     = ema_aligned_short(d) if self._ema_on else pd.Series(True, index=d.index)
                vp_long_ok  = (d["close"] >= d["vp_val"].fillna(0))  if self._vp_on else pd.Series(True, index=d.index)
                vp_short_ok = (d["close"] <= d["vp_vah"].fillna(float("inf"))) if self._vp_on else pd.Series(True, index=d.index)
                long_break  = (d["high"] >= d["target_long"])  & (d["open"] < d["target_long"])
                short_break = (d["low"]  <= d["target_short"]) & (d["open"] > d["target_short"])

                d["signal"] = 0
                d.loc[valid & uptrend   & ema_l & vol_ok & vp_long_ok  & long_break,  "signal"] = 1
                if self.use_short:
                    d.loc[valid & downtrend & ema_s & vol_ok & vp_short_ok & short_break, "signal"] = 2

                d["position"]    = d["signal"].apply(lambda s: 1 if s in (1,2) else 0)
                d["entry_price"] = np.where(d["signal"]==1, d["target_long"],
                                   np.where(d["signal"]==2, d["target_short"], np.nan))
                d["exit_price"]  = d["open"].shift(-1)
                long_ret  = (d["exit_price"] - d["target_long"])  / d["target_long"]
                short_ret = (d["target_short"] - d["exit_price"]) / d["target_short"]
                d["strategy_return"] = np.where(d["signal"]==1, long_ret,
                                       np.where(d["signal"]==2, short_ret, 0.0))
                d["confidence"] = 1
                return d.dropna(subset=["prev_range"])

        strat = CustomVB(k=k, volume_multiplier=vol_mult if vm > 0 else 1.5, use_short=use_short)
        strat._ema_on = ema_on
        strat._vp_on  = vp_on
        if vm == 0.0:
            strat.volume_multiplier = 9999

        try:
            bt = Backtester(strat, CAPITAL, COMMISSION, SLIPPAGE, -0.03, 0.054)
            m  = bt.run(df)
            tc = len(bt.trades) if bt.trades is not None else 0
            wr = float(m.get("승률 (%)", 0) or 0)
            tr = float(str(m.get("총 수익률 (%)", 0)).replace("%","") or 0)
            sh = float(m.get("샤프 비율", 0) or 0)
            md = float(str(m.get("최대 낙폭 MDD (%)", 0)).replace("%","") or 0)
            mark = " ★" if sh > best_sharpe and tc >= 5 else ""
            if tc >= 5 and sh > best_sharpe:
                best_sharpe = sh
                best_cfg = name
            print(f"  {name:<38} {tc:>4} {wr:>5.1f}% {tr:>+7.1f}% {sh:>6.3f} {md:>6.1f}%{mark}")
        except Exception as e:
            print(f"  {name:<38} 오류: {e}")

    if best_cfg:
        print(f"\n  ★ 최고 샤프 전략: {best_cfg}")


# ── 메인 ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "="*65)
    print("  전략 파라미터 분석 백테스트  (2022-01-01 ~ 2024-12-31)")
    print("="*65)

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
        run_param_sweep(df, market)

    print("\n" + "="*65 + "\n")
