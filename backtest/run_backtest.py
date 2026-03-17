"""
백테스트 실행 스크립트

사용법:
  python backtest/run_backtest.py

결과:
  - 콘솔에 성과 지표 출력
  - data/processed/ 에 차트 PNG 저장
"""
import sys
import logging
from pathlib import Path

# 프로젝트 루트를 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.data_collector import load_or_fetch
from strategies import VolatilityBreakoutStrategy, MovingAverageCrossStrategy, RSIStrategy
from backtest.backtester import Backtester, MultiStrategyBacktester

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ── 설정 ────────────────────────────────────────────────
MARKETS = ["KRW-BTC", "KRW-ETH"]
START_DATE = "2022-01-01"
END_DATE = "2024-12-31"
INITIAL_CAPITAL = 1_000_000  # 100만원

# ── 전략 목록 ────────────────────────────────────────────
STRATEGIES = [
    VolatilityBreakoutStrategy(k=0.5),
    MovingAverageCrossStrategy(short_window=5, long_window=20),
    RSIStrategy(period=14, oversold=30, overbought=70),
]


def main():
    print("\n" + "=" * 60)
    print("  퀀트 투자 백테스팅 시스템")
    print("=" * 60)

    for market in MARKETS:
        print(f"\n{'─'*60}")
        print(f"  마켓: {market}  |  기간: {START_DATE} ~ {END_DATE}")
        print(f"{'─'*60}")

        # 데이터 수집 (캐시 우선)
        df = load_or_fetch(
            market=market,
            interval="day",
            start_date=START_DATE,
            end_date=END_DATE,
        )

        if df.empty:
            print(f"  [경고] {market} 데이터 없음 — 스킵")
            continue

        print(f"  데이터 로드 완료: {len(df)}개 캔들 ({df.index[0].date()} ~ {df.index[-1].date()})")

        # 여러 전략 동시 백테스트
        multi_bt = MultiStrategyBacktester(
            strategies=STRATEGIES,
            initial_capital=INITIAL_CAPITAL,
            commission=0.0005,
            slippage=0.001,
            stop_loss_pct=-0.03,
            take_profit_pct=0.05,
        )
        comparison = multi_bt.run(df, market=market)

        # 최고 전략 출력
        best = comparison["총 수익률 (%)"].idxmax()
        print(f"\n  ★ {market} 최고 전략: {best}")
        print(f"     수익률 {comparison.loc[best, '총 수익률 (%)']:+.2f}%  |  "
              f"샤프 {comparison.loc[best, '샤프 비율']:.3f}  |  "
              f"MDD {comparison.loc[best, '최대 낙폭 MDD (%)']:.2f}%")

    print("\n  차트는 data/processed/ 폴더를 확인하세요.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
