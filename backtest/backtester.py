"""
백테스팅 엔진

기능:
  - 전략 신호 기반 가상 매매 시뮬레이션
  - 수수료 및 슬리피지 반영
  - 손절/익절 적용
  - 자산 곡선(equity curve) 생성
  - 성과 지표 계산 및 차트 시각화
"""
import logging
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from typing import Optional

from strategies.base_strategy import BaseStrategy
from backtest.performance import summarize

logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "data" / "processed"
RESULTS_DIR.mkdir(exist_ok=True)


class Backtester:
    """
    단일 전략 백테스터

    Parameters
    ----------
    strategy         : BaseStrategy 하위 클래스 인스턴스
    initial_capital  : 초기 투자금 (원, 기본 1,000,000)
    commission       : 수수료율 (기본 0.05% = 0.0005)
    slippage         : 슬리피지율 (기본 0.1% = 0.001)
    stop_loss_pct    : 손절 비율 (기본 -3% = -0.03 / None이면 미사용)
    take_profit_pct  : 익절 비율 (기본 5% = 0.05 / None이면 미사용)
    leverage         : 레버리지 배수 (기본 1.0 — OKX 선물 시 조정)
    use_atr_sl       : True이면 신호 df의 atr_sl/atr_tp 컬럼 우선 사용
    """

    def __init__(
        self,
        strategy: BaseStrategy,
        initial_capital: float = 1_000_000,
        commission: float = 0.0005,
        slippage: float = 0.001,
        stop_loss_pct: Optional[float] = -0.03,
        take_profit_pct: Optional[float] = 0.05,
        leverage: float = 1.0,
        use_atr_sl: bool = False,
    ):
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.commission = commission
        self.slippage = slippage
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.leverage = leverage
        self.use_atr_sl = use_atr_sl

        self.equity_curve: pd.Series = None
        self.returns: pd.Series = None
        self.trades: pd.DataFrame = None
        self.result_df: pd.DataFrame = None

    def run(self, df: pd.DataFrame) -> dict:
        """
        백테스트 실행

        Parameters
        ----------
        df : OHLCV DataFrame

        Returns
        -------
        성과 지표 딕셔너리
        """
        logger.info(f"백테스트 시작: {self.strategy.name}")

        signal_df = self.strategy.generate_signals(df)
        self.result_df = self._simulate(signal_df)

        self.equity_curve = self.result_df["equity"]
        self.returns = self.result_df["daily_return"]
        self.trades = self._extract_trades(self.result_df)

        metrics = summarize(self.equity_curve, self.returns, self.trades)

        logger.info(f"백테스트 완료: {self.strategy.name}")
        self._print_metrics(metrics)

        return metrics

    def _simulate(self, df: pd.DataFrame) -> pd.DataFrame:
        """포지션 시뮬레이션 — 자산곡선 계산 (롱/숏 양방향 + 레버리지 지원)"""
        df = df.copy()

        capital = self.initial_capital

        # 롱 상태
        long_pos    = 0
        long_entry  = 0.0
        long_sl     = None  # ATR 기반 절대 손절가
        long_tp     = None  # ATR 기반 절대 익절가

        # 숏 상태
        short_pos   = 0
        short_entry = 0.0
        short_sl    = None
        short_tp    = None

        equity_list = []
        daily_return_list = []

        for i, (idx, row) in enumerate(df.iterrows()):
            sig   = row.get("signal", 0)
            close = row["close"]

            # ATR 기반 SL/TP (있으면 사용)
            row_atr_sl_long  = row.get("atr_sl_long",  None) if self.use_atr_sl else None
            row_atr_tp_long  = row.get("atr_tp_long",  None) if self.use_atr_sl else None
            row_atr_sl_short = row.get("atr_sl_short", None) if self.use_atr_sl else None
            row_atr_tp_short = row.get("atr_tp_short", None) if self.use_atr_sl else None

            # ── 롱 손절/익절 ──────────────────────────────────
            if long_pos == 1 and long_entry > 0:
                sl_price = long_sl if long_sl else (
                    long_entry * (1 + self.stop_loss_pct) if self.stop_loss_pct else None
                )
                tp_price = long_tp if long_tp else (
                    long_entry * (1 + self.take_profit_pct) if self.take_profit_pct else None
                )
                if sl_price and close <= sl_price:
                    exit_p = close * (1 - self.slippage)
                    pnl = (exit_p - long_entry) / long_entry * self.leverage
                    capital *= (1 + pnl)
                    capital *= (1 - self.commission)
                    long_pos = 0; long_entry = 0.0; long_sl = None; long_tp = None
                    df.at[idx, "signal"] = -1
                elif tp_price and close >= tp_price:
                    exit_p = close * (1 - self.slippage)
                    pnl = (exit_p - long_entry) / long_entry * self.leverage
                    capital *= (1 + pnl)
                    capital *= (1 - self.commission)
                    long_pos = 0; long_entry = 0.0; long_sl = None; long_tp = None
                    df.at[idx, "signal"] = -1

            # ── 숏 손절/익절 ──────────────────────────────────
            if short_pos == 1 and short_entry > 0:
                sl_price = short_sl if short_sl else (
                    short_entry * (1 + abs(self.stop_loss_pct)) if self.stop_loss_pct else None
                )
                tp_price = short_tp if short_tp else (
                    short_entry * (1 - self.take_profit_pct) if self.take_profit_pct else None
                )
                if sl_price and close >= sl_price:
                    exit_p = close * (1 + self.slippage)
                    pnl = (short_entry - exit_p) / short_entry * self.leverage
                    capital *= (1 + pnl)
                    capital *= (1 - self.commission)
                    short_pos = 0; short_entry = 0.0; short_sl = None; short_tp = None
                    df.at[idx, "signal"] = -2
                elif tp_price and close <= tp_price:
                    exit_p = close * (1 + self.slippage)
                    pnl = (short_entry - exit_p) / short_entry * self.leverage
                    capital *= (1 + pnl)
                    capital *= (1 - self.commission)
                    short_pos = 0; short_entry = 0.0; short_sl = None; short_tp = None
                    df.at[idx, "signal"] = -2

            # ── 신호 처리 ─────────────────────────────────────
            # 롱 진입
            if sig == 1 and long_pos == 0 and short_pos == 0:
                long_entry = close * (1 + self.slippage)
                capital *= (1 - self.commission)
                long_pos = 1
                if self.use_atr_sl and row_atr_sl_long and not np.isnan(row_atr_sl_long):
                    long_sl = float(row_atr_sl_long)
                    long_tp = float(row_atr_tp_long) if row_atr_tp_long and not np.isnan(row_atr_tp_long) else None

            # 롱 청산
            elif sig == -1 and long_pos == 1:
                exit_p = close * (1 - self.slippage)
                pnl = (exit_p - long_entry) / long_entry * self.leverage
                capital *= (1 + pnl)
                capital *= (1 - self.commission)
                long_pos = 0; long_entry = 0.0; long_sl = None; long_tp = None

            # 숏 진입
            elif sig == 2 and short_pos == 0 and long_pos == 0:
                short_entry = close * (1 - self.slippage)
                capital *= (1 - self.commission)
                short_pos = 1
                if self.use_atr_sl and row_atr_sl_short and not np.isnan(row_atr_sl_short):
                    short_sl = float(row_atr_sl_short)
                    short_tp = float(row_atr_tp_short) if row_atr_tp_short and not np.isnan(row_atr_tp_short) else None

            # 숏 청산
            elif sig == -2 and short_pos == 1:
                exit_p = close * (1 + self.slippage)
                pnl = (short_entry - exit_p) / short_entry * self.leverage
                capital *= (1 + pnl)
                capital *= (1 - self.commission)
                short_pos = 0; short_entry = 0.0; short_sl = None; short_tp = None

            # ── 미실현 손익 반영 ──────────────────────────────
            if long_pos == 1 and long_entry > 0:
                unrealized = (close - long_entry) / long_entry * self.leverage
                current_equity = capital * (1 + unrealized)
            elif short_pos == 1 and short_entry > 0:
                unrealized = (short_entry - close) / short_entry * self.leverage
                current_equity = capital * (1 + unrealized)
            else:
                current_equity = capital

            equity_list.append(current_equity)

            if i == 0:
                daily_return_list.append(0.0)
            else:
                prev_eq = equity_list[i - 1]
                daily_return_list.append(
                    (current_equity - prev_eq) / prev_eq if prev_eq > 0 else 0.0
                )

        df["equity"] = equity_list
        df["daily_return"] = daily_return_list
        return df

    def _extract_trades(self, df: pd.DataFrame) -> pd.DataFrame:
        """매매 내역 추출 (롱/숏 양방향)"""
        trades = []

        long_entry_price = None
        long_entry_date  = None
        short_entry_price = None
        short_entry_date  = None

        for idx, row in df.iterrows():
            sig = row.get("signal", 0)

            # 롱 진입
            if sig == 1 and long_entry_price is None:
                long_entry_price = row["close"]
                long_entry_date  = idx
            # 롱 청산
            elif sig == -1 and long_entry_price is not None:
                exit_price = row["close"]
                pnl_pct = (exit_price - long_entry_price) / long_entry_price * 100 * self.leverage
                trades.append({
                    "entry_date": long_entry_date,
                    "exit_date":  idx,
                    "side":       "long",
                    "entry_price": long_entry_price,
                    "exit_price":  exit_price,
                    "pnl":         pnl_pct,
                })
                long_entry_price = None
                long_entry_date  = None

            # 숏 진입
            elif sig == 2 and short_entry_price is None:
                short_entry_price = row["close"]
                short_entry_date  = idx
            # 숏 청산
            elif sig == -2 and short_entry_price is not None:
                exit_price = row["close"]
                pnl_pct = (short_entry_price - exit_price) / short_entry_price * 100 * self.leverage
                trades.append({
                    "entry_date":  short_entry_date,
                    "exit_date":   idx,
                    "side":        "short",
                    "entry_price": short_entry_price,
                    "exit_price":  exit_price,
                    "pnl":         pnl_pct,
                })
                short_entry_price = None
                short_entry_date  = None

        return pd.DataFrame(trades)

    def _print_metrics(self, metrics: dict):
        """콘솔 출력"""
        print(f"\n{'='*50}")
        print(f"  전략: {self.strategy.name}  |  파라미터: {self.strategy.params}")
        print(f"{'='*50}")
        for k, v in metrics.items():
            print(f"  {k:<28}: {v}")
        print(f"{'='*50}\n")

    def plot(self, market: str = "", save: bool = True) -> str:
        """
        자산 곡선 + 매매 시그널 차트 생성

        Returns
        -------
        저장된 파일 경로
        """
        if self.result_df is None:
            raise RuntimeError("run() 을 먼저 호출하세요.")

        df = self.result_df
        fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
        fig.suptitle(
            f"{self.strategy.name} 백테스트 결과  |  {market}",
            fontsize=14, fontweight="bold"
        )

        # 1. 가격 차트 + 매매 시그널
        ax1 = axes[0]
        ax1.plot(df.index, df["close"], color="#2196F3", linewidth=1, label="종가")
        buy_signals = df[df["signal"] == 1]
        sell_signals = df[df["signal"] == -1]
        ax1.scatter(buy_signals.index, buy_signals["close"],
                    marker="^", color="#00C853", s=80, zorder=5, label="매수")
        ax1.scatter(sell_signals.index, sell_signals["close"],
                    marker="v", color="#FF1744", s=80, zorder=5, label="매도")

        # 이동평균 있으면 표시
        if "ma_short" in df.columns:
            ax1.plot(df.index, df["ma_short"], color="#FF9800", linewidth=1,
                     linestyle="--", label=f"MA{self.strategy.params.get('short_window','')}")
        if "ma_long" in df.columns:
            ax1.plot(df.index, df["ma_long"], color="#9C27B0", linewidth=1,
                     linestyle="--", label=f"MA{self.strategy.params.get('long_window','')}")

        ax1.set_ylabel("가격 (KRW)")
        ax1.legend(loc="upper left", fontsize=8)
        ax1.grid(alpha=0.3)

        # 2. 자산 곡선
        ax2 = axes[1]
        ax2.plot(df.index, df["equity"], color="#4CAF50", linewidth=1.5, label="자산")
        ax2.axhline(self.initial_capital, color="gray", linestyle="--",
                    linewidth=0.8, label=f"초기자본 {self.initial_capital:,}원")
        ax2.fill_between(df.index, self.initial_capital, df["equity"],
                         where=df["equity"] >= self.initial_capital,
                         alpha=0.15, color="#4CAF50")
        ax2.fill_between(df.index, self.initial_capital, df["equity"],
                         where=df["equity"] < self.initial_capital,
                         alpha=0.15, color="#FF1744")
        ax2.set_ylabel("자산 (KRW)")
        ax2.legend(loc="upper left", fontsize=8)
        ax2.grid(alpha=0.3)

        # 3. MDD (낙폭 차트)
        ax3 = axes[2]
        rolling_max = df["equity"].cummax()
        drawdown = (df["equity"] - rolling_max) / rolling_max * 100
        ax3.fill_between(df.index, drawdown, 0, color="#FF5722", alpha=0.4)
        ax3.plot(df.index, drawdown, color="#FF5722", linewidth=0.8)
        ax3.set_ylabel("낙폭 (%)")
        ax3.set_xlabel("날짜")
        ax3.grid(alpha=0.3)

        ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        plt.xticks(rotation=30)
        plt.tight_layout()

        if save:
            fname = f"{self.strategy.name}_{market.replace('-','_')}.png"
            fpath = RESULTS_DIR / fname
            plt.savefig(fpath, dpi=150, bbox_inches="tight")
            plt.close()
            logger.info(f"차트 저장: {fpath}")
            return str(fpath)
        else:
            plt.show()
            return ""


class MultiStrategyBacktester:
    """
    여러 전략을 동시에 백테스트하고 결과를 비교합니다.
    """

    def __init__(self, strategies: list, **backtester_kwargs):
        self.strategies = strategies
        self.backtester_kwargs = backtester_kwargs
        self.results = {}

    def run(self, df: pd.DataFrame, market: str = "") -> pd.DataFrame:
        """
        모든 전략 실행 후 성과 비교 테이블 반환
        """
        all_metrics = []
        for strategy in self.strategies:
            bt = Backtester(strategy, **self.backtester_kwargs)
            metrics = bt.run(df)
            metrics["전략명"] = strategy.name
            metrics["파라미터"] = str(strategy.params)
            all_metrics.append(metrics)
            self.results[strategy.name] = bt

            # 개별 차트 저장
            bt.plot(market=market, save=True)

        comparison = pd.DataFrame(all_metrics).set_index("전략명")

        print("\n" + "=" * 60)
        print("  전략 비교 요약")
        print("=" * 60)
        print(comparison.to_string())
        print("=" * 60 + "\n")

        self._plot_comparison(market)

        return comparison

    def _plot_comparison(self, market: str = ""):
        """자산 곡선 비교 차트"""
        if not self.results:
            return

        fig, ax = plt.subplots(figsize=(14, 6))
        colors = ["#2196F3", "#FF9800", "#4CAF50", "#9C27B0", "#FF1744"]

        for i, (name, bt) in enumerate(self.results.items()):
            equity = bt.equity_curve
            normalized = equity / equity.iloc[0] * 100
            ax.plot(equity.index, normalized, label=name,
                    color=colors[i % len(colors)], linewidth=1.5)

        ax.axhline(100, color="gray", linestyle="--", linewidth=0.8, label="기준 (100)")
        ax.set_title(f"전략 비교 — {market}", fontsize=13)
        ax.set_ylabel("정규화 자산 (시작=100)")
        ax.set_xlabel("날짜")
        ax.legend()
        ax.grid(alpha=0.3)

        fpath = RESULTS_DIR / f"strategy_comparison_{market.replace('-','_')}.png"
        plt.savefig(fpath, dpi=150, bbox_inches="tight")
        plt.close()
        logger.info(f"비교 차트 저장: {fpath}")
