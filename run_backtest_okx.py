#!/usr/bin/env python3
"""
OKX BTC 선물 전략 백테스트 — 기존 전략 vs 개선 전략 비교

사용법:
    python run_backtest_okx.py               # 기본 (2022-01-01 ~ 2024-12-31)
    python run_backtest_okx.py --market ETH  # ETH 백테스트
    python run_backtest_okx.py --start 2023-01-01 --end 2024-12-31

출력:
    - 콘솔: 성능 비교 테이블
    - data/processed/*.png: 자산곡선 + 신호 차트
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("backtest_okx")

import pandas as pd
import numpy as np

from strategies.volatility_breakout import VolatilityBreakoutStrategy
from backtest.backtester import Backtester, MultiStrategyBacktester


# ── OKX 데이터 다운로드 ──────────────────────────────────────────────────────

def fetch_okx_daily(symbol: str = "BTC/USDT", start: str = "2022-01-01", end: str = "2024-12-31") -> pd.DataFrame:
    """
    ccxt를 통해 OKX 현물 BTC/USDT 일봉 데이터 다운로드.
    (선물 역사 데이터와 현물은 거의 동일)
    """
    try:
        import ccxt
    except ImportError:
        raise RuntimeError("ccxt 패키지 필요: pip install ccxt")

    print(f"OKX {symbol} 일봉 데이터 다운로드 중... ({start} ~ {end})")

    exchange = ccxt.okx({"options": {"defaultType": "spot"}})

    start_ts = int(datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ts   = int(datetime.strptime(end,   "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)

    all_candles = []
    since = start_ts

    while since < end_ts:
        candles = exchange.fetch_ohlcv(symbol, "1d", since=since, limit=300)
        if not candles:
            break
        all_candles.extend(candles)
        since = candles[-1][0] + 86400_000  # 다음 날

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df.index = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    df = df.drop(columns=["timestamp"])
    df = df[df.index <= pd.Timestamp(end)]
    df = df[~df.index.duplicated(keep="first")]
    df = df.sort_index()

    print(f"  다운로드 완료: {len(df)}봉 ({df.index[0].date()} ~ {df.index[-1].date()})")
    return df


# ── 전략 정의 ────────────────────────────────────────────────────────────────

def build_strategies():
    """기존 전략 vs 개선 전략 목록"""
    # 공통 파라미터
    base_params = dict(
        k=0.4,
        ma_period=200,
        use_short=True,
        volume_lookback=20,
        volume_multiplier=1.5,
        vp_lookback=20,
        vp_bins=50,
        fib_lookback=50,
        short_consec=2,
    )

    strategies = [
        # 1. 기존 전략 (베이스라인)
        VolatilityBreakoutStrategy(
            **base_params,
            use_supertrend=False,
            use_macd_filter=False,
            use_atr_sl=False,
            use_rsi_div=False,
            use_bb_squeeze=False,
        ),

        # 2. Supertrend 필터 추가
        VolatilityBreakoutStrategy(
            **base_params,
            use_supertrend=True,
            supertrend_period=7,
            supertrend_mult=3.0,
            use_macd_filter=False,
            use_atr_sl=False,
        ),

        # 3. Supertrend + MACD 필터
        VolatilityBreakoutStrategy(
            **base_params,
            use_supertrend=True,
            use_macd_filter=True,
            use_atr_sl=False,
        ),

        # 4. Supertrend + ATR 동적 SL/TP
        VolatilityBreakoutStrategy(
            **base_params,
            use_supertrend=True,
            use_macd_filter=False,
            use_atr_sl=True,
            atr_period=14,
            atr_sl_mult=1.5,
            atr_tp_mult=3.0,
        ),

        # 5. 풀 개선 전략 (Supertrend + MACD + ATR SL/TP + RSI Div)
        VolatilityBreakoutStrategy(
            **base_params,
            use_supertrend=True,
            use_macd_filter=True,
            use_atr_sl=True,
            atr_period=14,
            atr_sl_mult=1.5,
            atr_tp_mult=3.0,
            use_rsi_div=True,
            use_bb_squeeze=False,
        ),
    ]
    return strategies


# ── 메인 ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="OKX BTC 선물 전략 백테스트")
    parser.add_argument("--market", default="BTC", choices=["BTC", "ETH", "SOL", "XRP"])
    parser.add_argument("--start",  default="2022-01-01")
    parser.add_argument("--end",    default="2024-12-31")
    parser.add_argument("--capital", type=float, default=1_000.0, help="초기 자본 (USDT)")
    parser.add_argument("--leverage", type=float, default=1.0)
    args = parser.parse_args()

    symbol = f"{args.market}/USDT"
    market_key = f"KRW-{args.market}"  # 차트 파일명용

    # 데이터 수집
    df = fetch_okx_daily(symbol, args.start, args.end)
    if df.empty:
        print("데이터 수집 실패")
        return

    strategies = build_strategies()

    print(f"\n{'='*65}")
    print(f"  OKX {symbol} 백테스트  |  {args.start} ~ {args.end}")
    print(f"  초기 자본: {args.capital:,.0f} USDT  |  레버리지: {args.leverage}x")
    print(f"{'='*65}\n")

    bt_kwargs = dict(
        initial_capital=args.capital,
        commission=0.0005,
        slippage=0.001,
        stop_loss_pct=-0.03,
        take_profit_pct=0.054,
        leverage=args.leverage,
    )

    multi_bt = MultiStrategyBacktester(strategies, **bt_kwargs)

    # ATR SL/TP 사용 전략은 개별로 실행 (use_atr_sl 옵션 전달)
    all_metrics = []
    for strat in strategies:
        use_atr = getattr(strat, "use_atr_sl", False)
        bt = Backtester(
            strat,
            use_atr_sl=use_atr,
            **bt_kwargs,
        )
        metrics = bt.run(df)
        metrics["전략명"] = strat.name
        all_metrics.append(metrics)
        try:
            bt.plot(market=market_key, save=True)
        except Exception as e:
            logger.warning(f"차트 저장 실패: {e}")

    comparison = pd.DataFrame(all_metrics).set_index("전략명")

    # 비교 테이블 출력
    print("\n" + "=" * 70)
    print("  전략 비교 요약")
    print("=" * 70)

    display_cols = [
        "총 수익률 (%)", "연 수익률 CAGR (%)", "샤프 비율",
        "최대 낙폭 (%)", "승률 (%)", "수익 팩터", "총 거래 수"
    ]
    display_cols = [c for c in display_cols if c in comparison.columns]
    print(comparison[display_cols].to_string())
    print("=" * 70)

    # 최우수 전략 선정 기준: 샤프 비율
    if "샤프 비율" in comparison.columns:
        best = comparison["샤프 비율"].idxmax()
        print(f"\n최우수 전략 (샤프 비율 기준): {best}")

    print("\n차트 저장 위치: data/processed/")


if __name__ == "__main__":
    main()
