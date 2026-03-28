"""
백테스트 실행 스크립트 (v2)

개선 사항:
  - 8종목 전체 (BTC/ETH/BNB/SOL/XRP/DOGE/LINK/SUI)
  - 일봉 + 1H 멀티 타임프레임
  - OKX USDT 데이터 (ccxt) + Upbit KRW 데이터 (pyupbit)
  - ATR 기반 SL/TP 지원
  - 숏 전략 검증 포함
  - 기간: 2022~현재

사용법:
  python backtest/run_backtest.py
  python backtest/run_backtest.py --tf 1h         # 1H 스캘프 백테스트
  python backtest/run_backtest.py --short          # 숏 포함
  python backtest/run_backtest.py --leverage 3     # 레버리지 3x
"""
import sys
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.data_collector import load_or_fetch, load_or_fetch_okx
from strategies import VolatilityBreakoutStrategy
from backtest.backtester import Backtester, MultiStrategyBacktester
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── 설정 ─────────────────────────────────────────────────
MARKETS_KRW = [
    "KRW-BTC", "KRW-ETH", "KRW-SOL", "KRW-XRP",
    "KRW-DOGE", "KRW-LINK", "KRW-AVAX", "KRW-SUI",
]
MARKETS_OKX = [
    "KRW-BTC", "KRW-ETH", "KRW-BNB", "KRW-SOL",
    "KRW-XRP", "KRW-DOGE", "KRW-LINK", "KRW-SUI",
]

START_DATE = "2022-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")
INITIAL_CAPITAL = 1_000_000  # 100만원 (KRW) 또는 1000 USDT

# ── 타임프레임별 기본 파라미터 ──────────────────────────────
TF_PARAMS = {
    "day": {
        "k": 0.4,
        "volume_multiplier": 1.5,
        "ma_period": 200,
        "sl": -0.04,
        "tp": 0.08,
        "atr_sl_mult": 2.0,
        "atr_tp_mult": 4.0,
        "commission": 0.0005,   # Upbit 현물 0.05%
        "slippage": 0.001,
    },
    "1h": {
        "k": 0.08,
        "volume_multiplier": 1.0,
        "ma_period": 50,
        "sl": -0.010,
        "tp": 0.015,
        "atr_sl_mult": 0.9,
        "atr_tp_mult": 1.5,
        "commission": 0.0005,   # OKX taker 0.05%
        "slippage": 0.001,
    },
    "4h": {
        "k": 0.06,
        "volume_multiplier": 1.0,
        "ma_period": 200,
        "sl": -0.02,
        "tp": 0.04,
        "atr_sl_mult": 1.2,
        "atr_tp_mult": 2.0,
        "commission": 0.0005,
        "slippage": 0.001,
    },
}


def run_single(market, tf, use_short, leverage, source, params,
               scored=False, min_long=4, min_short=3):
    """단일 종목+타임프레임 백테스트"""
    # 데이터 로드
    interval_map = {"day": "day", "1h": "minute60", "4h": "minute240"}
    interval = interval_map.get(tf, "day")

    if source == "okx":
        df = load_or_fetch_okx(market, interval, START_DATE, END_DATE)
    else:
        df = load_or_fetch(market, interval, START_DATE, END_DATE)

    if df.empty:
        return None

    # 전략 생성
    strat = VolatilityBreakoutStrategy(
        k=params["k"],
        volume_multiplier=params["volume_multiplier"],
        ma_period=params["ma_period"],
        use_short=use_short,
        use_supertrend=True,
    )

    # 점수제 모드: generate_signals를 scored 버전으로 교체
    if scored:
        _ml, _ms = min_long, min_short
        strat.generate_signals = lambda d: strat.generate_signals_scored(d, _ml, _ms)

    bt = Backtester(
        strategy=strat,
        initial_capital=INITIAL_CAPITAL,
        commission=params["commission"],
        slippage=params["slippage"],
        stop_loss_pct=params["sl"],
        take_profit_pct=params["tp"],
        leverage=leverage,
        use_atr_sl=True,
    )

    try:
        metrics = bt.run(df)
        metrics["종목"] = market
        metrics["TF"] = tf
        metrics["source"] = source
        metrics["use_short"] = use_short
        metrics["leverage"] = leverage
        bt.plot(market=f"{market}_{tf}_{source}".replace("/", "_"), save=True)
        return metrics
    except Exception as e:
        logger.error(f"{market}/{tf} 백테스트 오류: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="퀀트 백테스트 v2")
    parser.add_argument("--tf", default="day", choices=["day", "1h", "4h"],
                        help="타임프레임 (default: day)")
    parser.add_argument("--short", action="store_true", help="숏 전략 포함")
    parser.add_argument("--leverage", type=float, default=1.0, help="레버리지 (default: 1x)")
    parser.add_argument("--source", default="auto", choices=["upbit", "okx", "auto"],
                        help="데이터 소스 (default: auto)")
    parser.add_argument("--market", default=None, help="단일 종목만 테스트 (예: KRW-BTC)")
    parser.add_argument("--scored", action="store_true", help="점수제 신호 생성 ��용")
    parser.add_argument("--min-long", type=int, default=4, help="롱 최소 점수 (scored 모드)")
    parser.add_argument("--min-short", type=int, default=3, help="숏 최소 점수 (scored 모드)")
    args = parser.parse_args()

    tf = args.tf
    params = TF_PARAMS[tf]

    # 소스 자동 결정: 1H/4H → OKX, day → Upbit
    if args.source == "auto":
        source = "okx" if tf in ("1h", "4h") else "upbit"
    else:
        source = args.source

    markets = MARKETS_OKX if source == "okx" else MARKETS_KRW
    if args.market:
        markets = [args.market]

    mode_str = "점수제" if args.scored else "AND체인"
    print("\n" + "=" * 70)
    print(f"  퀀트 백테스트 v2 — {tf.upper()} | {'롱+숏' if args.short else '롱만'} | "
          f"{args.leverage}x | {source.upper()} | {mode_str}")
    print(f"  기간: {START_DATE} ~ {END_DATE} | 종목: {len(markets)}개")
    print("=" * 70)

    all_metrics = []
    for market in markets:
        print(f"\n{'─'*60}")
        print(f"  {market} ({source})")
        print(f"{'─'*60}")

        m = run_single(market, tf, args.short, args.leverage, source, params,
                       scored=args.scored, min_long=args.min_long, min_short=args.min_short)
        if m:
            all_metrics.append(m)

    if not all_metrics:
        print("\n  결과 없음")
        return

    # 전체 요약
    import pandas as pd
    summary = pd.DataFrame(all_metrics)
    summary = summary.set_index("종목")

    cols = ["TF", "총 수익률 (%)", "샤프 비율", "최대 낙폭 MDD (%)",
            "승률 (%)", "총 거래 횟수", "손익비 (Profit Factor)"]
    display_cols = [c for c in cols if c in summary.columns]

    print("\n" + "=" * 70)
    print("  전체 요약")
    print("=" * 70)
    print(summary[display_cols].to_string())

    # 통합 성과
    avg_return = summary["총 수익률 (%)"].mean()
    avg_sharpe = summary["샤프 비율"].mean()
    total_trades = summary["총 거래 횟수"].sum()
    avg_winrate = summary["승률 (%)"].mean()

    print(f"\n  평균 수익률: {avg_return:+.2f}% | 평균 샤프: {avg_sharpe:.3f} | "
          f"총 거래: {total_trades}건 | 평균 승률: {avg_winrate:.1f}%")
    print("=" * 70)
    print("  차트: data/processed/ 폴더 확인\n")


if __name__ == "__main__":
    main()
