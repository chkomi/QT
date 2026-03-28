"""
백테스팅 엔진 (v2)

개선 사항:
  - 롱/숏 동시 보유 (OKX 헤지모드)
  - ATR 기반 동적 SL/TP
  - confidence → leverage 매핑
  - 시간 기반 강제 청산 (max_holding_bars)
  - OKX 선물 수수료 분리 (maker/taker)
  - Break-even SL (수익 발생 시 진입가로 SL 이동)
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
    단일 전략 백테스터 (v2)

    Parameters
    ----------
    strategy         : BaseStrategy 인스턴스
    initial_capital  : 초기 투자금
    commission       : 수수료율 (taker 기본 0.05%)
    slippage         : 슬리피지율
    stop_loss_pct    : 고정 손절 비율 (ATR 미사용 시 폴백)
    take_profit_pct  : 고정 익절 비율 (ATR 미사용 시 폴백)
    leverage         : 레버리지 배수 (기본 1.0)
    use_atr_sl       : ATR 기반 SL/TP 사용 여부
    max_holding_bars : 최대 보유 봉 수 (None=무제한, 스캘프 시 설정)
    use_breakeven_sl : Break-even SL 사용 (수익 > |SL| 시 진입가로 SL 이동)
    hedge_mode       : True면 롱+숏 동시 보유 허용
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
        max_holding_bars: Optional[int] = None,
        use_breakeven_sl: bool = True,
        hedge_mode: bool = True,
    ):
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.commission = commission
        self.slippage = slippage
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.leverage = leverage
        self.use_atr_sl = use_atr_sl
        self.max_holding_bars = max_holding_bars
        self.use_breakeven_sl = use_breakeven_sl
        self.hedge_mode = hedge_mode

        self.equity_curve: pd.Series = None
        self.returns: pd.Series = None
        self.trades: pd.DataFrame = None
        self.result_df: pd.DataFrame = None

    def run(self, df: pd.DataFrame) -> dict:
        """백테스트 실행"""
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
        """포지션 시뮬레이션 — 헤지모드 + ATR SL/TP + 시간청산 + Break-even SL"""
        df = df.copy()

        capital = self.initial_capital

        # 롱 상태
        long_pos = 0
        long_entry = 0.0
        long_sl = None
        long_tp = None
        long_bars = 0
        long_capital = 0.0  # 롱에 투입된 자본

        # 숏 상태
        short_pos = 0
        short_entry = 0.0
        short_sl = None
        short_tp = None
        short_bars = 0
        short_capital = 0.0

        equity_list = []
        daily_return_list = []

        for i, (idx, row) in enumerate(df.iterrows()):
            sig = row.get("signal", 0)
            close = row["close"]
            confidence = row.get("confidence", 1)

            # confidence → 동적 레버리지
            if self.leverage > 1:
                dyn_leverage = min(confidence, self.leverage)
            else:
                dyn_leverage = self.leverage

            # ATR 기반 SL/TP 값
            row_atr_sl_long = row.get("atr_sl_long", None) if self.use_atr_sl else None
            row_atr_tp_long = row.get("atr_tp_long", None) if self.use_atr_sl else None
            row_atr_sl_short = row.get("atr_sl_short", None) if self.use_atr_sl else None
            row_atr_tp_short = row.get("atr_tp_short", None) if self.use_atr_sl else None

            # ── 롱 SL/TP + 시간청산 ─────────────────────────────
            if long_pos == 1 and long_entry > 0:
                long_bars += 1

                # Break-even SL: 수익이 SL 폭 이상이면 SL을 진입가로 이동
                if self.use_breakeven_sl and long_sl and long_sl < long_entry:
                    profit_pct = (close - long_entry) / long_entry
                    loss_pct = abs((long_sl - long_entry) / long_entry)
                    if profit_pct >= loss_pct:
                        long_sl = long_entry

                sl_price = long_sl if long_sl else (
                    long_entry * (1 + self.stop_loss_pct) if self.stop_loss_pct else None
                )
                tp_price = long_tp if long_tp else (
                    long_entry * (1 + self.take_profit_pct) if self.take_profit_pct else None
                )

                # 시간 기반 청산
                time_exit = (self.max_holding_bars and long_bars >= self.max_holding_bars)

                if (sl_price and close <= sl_price) or time_exit:
                    exit_p = close * (1 - self.slippage)
                    pnl = (exit_p - long_entry) / long_entry * dyn_leverage
                    capital += long_capital * pnl
                    capital -= long_capital * self.commission
                    long_pos = 0; long_entry = 0.0; long_sl = None; long_tp = None
                    long_bars = 0; long_capital = 0.0
                    df.at[idx, "signal"] = -1
                elif tp_price and close >= tp_price:
                    exit_p = close * (1 - self.slippage)
                    pnl = (exit_p - long_entry) / long_entry * dyn_leverage
                    capital += long_capital * pnl
                    capital -= long_capital * self.commission
                    long_pos = 0; long_entry = 0.0; long_sl = None; long_tp = None
                    long_bars = 0; long_capital = 0.0
                    df.at[idx, "signal"] = -1

            # ── 숏 SL/TP + 시간청산 ─────────────────────────────
            if short_pos == 1 and short_entry > 0:
                short_bars += 1

                # Break-even SL
                if self.use_breakeven_sl and short_sl and short_sl > short_entry:
                    profit_pct = (short_entry - close) / short_entry
                    loss_pct = abs((short_sl - short_entry) / short_entry)
                    if profit_pct >= loss_pct:
                        short_sl = short_entry

                sl_price = short_sl if short_sl else (
                    short_entry * (1 + abs(self.stop_loss_pct)) if self.stop_loss_pct else None
                )
                tp_price = short_tp if short_tp else (
                    short_entry * (1 - self.take_profit_pct) if self.take_profit_pct else None
                )

                time_exit = (self.max_holding_bars and short_bars >= self.max_holding_bars)

                if (sl_price and close >= sl_price) or time_exit:
                    exit_p = close * (1 + self.slippage)
                    pnl = (short_entry - exit_p) / short_entry * dyn_leverage
                    capital += short_capital * pnl
                    capital -= short_capital * self.commission
                    short_pos = 0; short_entry = 0.0; short_sl = None; short_tp = None
                    short_bars = 0; short_capital = 0.0
                    df.at[idx, "signal"] = -2
                elif tp_price and close <= tp_price:
                    exit_p = close * (1 + self.slippage)
                    pnl = (short_entry - exit_p) / short_entry * dyn_leverage
                    capital += short_capital * pnl
                    capital -= short_capital * self.commission
                    short_pos = 0; short_entry = 0.0; short_sl = None; short_tp = None
                    short_bars = 0; short_capital = 0.0
                    df.at[idx, "signal"] = -2

            # ── 신호 처리 ────────────────────────────────────────
            # 롱 진입 (헤지모드: 숏 보유 중에도 진입 가능)
            can_long = (long_pos == 0) and (self.hedge_mode or short_pos == 0)
            if sig == 1 and can_long and capital > 0:
                long_entry = close * (1 + self.slippage)
                long_capital = capital * 0.5 if (self.hedge_mode and short_pos == 1) else capital
                capital -= long_capital * self.commission
                long_pos = 1
                long_bars = 0
                if self.use_atr_sl and row_atr_sl_long and not np.isnan(row_atr_sl_long):
                    long_sl = float(row_atr_sl_long)
                    long_tp = float(row_atr_tp_long) if row_atr_tp_long and not np.isnan(row_atr_tp_long) else None

            # 롱 청산 (signal=-1)
            elif sig == -1 and long_pos == 1:
                exit_p = close * (1 - self.slippage)
                pnl = (exit_p - long_entry) / long_entry * dyn_leverage
                capital += long_capital * (1 + pnl)
                capital -= long_capital * self.commission
                long_pos = 0; long_entry = 0.0; long_sl = None; long_tp = None
                long_bars = 0; long_capital = 0.0

            # 숏 진입 (헤지모드: 롱 보유 중에도 진입 가능)
            can_short = (short_pos == 0) and (self.hedge_mode or long_pos == 0)
            if sig == 2 and can_short and capital > 0:
                short_entry = close * (1 - self.slippage)
                short_capital = capital * 0.5 if (self.hedge_mode and long_pos == 1) else capital
                capital -= short_capital * self.commission
                short_pos = 1
                short_bars = 0
                if self.use_atr_sl and row_atr_sl_short and not np.isnan(row_atr_sl_short):
                    short_sl = float(row_atr_sl_short)
                    short_tp = float(row_atr_tp_short) if row_atr_tp_short and not np.isnan(row_atr_tp_short) else None

            # 숏 청산 (signal=-2)
            elif sig == -2 and short_pos == 1:
                exit_p = close * (1 + self.slippage)
                pnl = (short_entry - exit_p) / short_entry * dyn_leverage
                capital += short_capital * (1 + pnl)
                capital -= short_capital * self.commission
                short_pos = 0; short_entry = 0.0; short_sl = None; short_tp = None
                short_bars = 0; short_capital = 0.0

            # ── 미실현 손익 반영 ─────────────────────────────────
            current_equity = capital
            if long_pos == 1 and long_entry > 0:
                unrealized = (close - long_entry) / long_entry * dyn_leverage
                current_equity += long_capital * unrealized
            if short_pos == 1 and short_entry > 0:
                unrealized = (short_entry - close) / short_entry * dyn_leverage
                current_equity += short_capital * unrealized

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
        long_entry_date = None
        short_entry_price = None
        short_entry_date = None

        for idx, row in df.iterrows():
            sig = row.get("signal", 0)

            if sig == 1 and long_entry_price is None:
                long_entry_price = row["close"]
                long_entry_date = idx
            elif sig == -1 and long_entry_price is not None:
                exit_price = row["close"]
                pnl_pct = (exit_price - long_entry_price) / long_entry_price * 100 * self.leverage
                trades.append({
                    "entry_date": long_entry_date,
                    "exit_date": idx,
                    "side": "long",
                    "entry_price": long_entry_price,
                    "exit_price": exit_price,
                    "pnl": pnl_pct,
                    "holding_bars": (idx - long_entry_date).days if hasattr(idx - long_entry_date, 'days') else 0,
                })
                long_entry_price = None
                long_entry_date = None

            elif sig == 2 and short_entry_price is None:
                short_entry_price = row["close"]
                short_entry_date = idx
            elif sig == -2 and short_entry_price is not None:
                exit_price = row["close"]
                pnl_pct = (short_entry_price - exit_price) / short_entry_price * 100 * self.leverage
                trades.append({
                    "entry_date": short_entry_date,
                    "exit_date": idx,
                    "side": "short",
                    "entry_price": short_entry_price,
                    "exit_price": exit_price,
                    "pnl": pnl_pct,
                    "holding_bars": (idx - short_entry_date).days if hasattr(idx - short_entry_date, 'days') else 0,
                })
                short_entry_price = None
                short_entry_date = None

        return pd.DataFrame(trades)

    def _print_metrics(self, metrics: dict):
        """콘솔 출력"""
        print(f"\n{'='*50}")
        print(f"  전략: {self.strategy.name}  |  파라미터: {self.strategy.params}")
        if self.leverage > 1:
            print(f"  레버리지: {self.leverage}x  |  헤지모드: {self.hedge_mode}")
        if self.max_holding_bars:
            print(f"  최대 보유: {self.max_holding_bars}봉")
        print(f"{'='*50}")
        for k, v in metrics.items():
            print(f"  {k:<28}: {v}")
        print(f"{'='*50}\n")

    def plot(self, market: str = "", save: bool = True) -> str:
        """자산 곡선 + 매매 시그널 차트"""
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
        short_signals = df[df["signal"] == 2]
        cover_signals = df[df["signal"] == -2]
        ax1.scatter(buy_signals.index, buy_signals["close"],
                    marker="^", color="#00C853", s=80, zorder=5, label="롱 진입")
        ax1.scatter(sell_signals.index, sell_signals["close"],
                    marker="v", color="#FF1744", s=80, zorder=5, label="롱 청산")
        ax1.scatter(short_signals.index, short_signals["close"],
                    marker="v", color="#FF9800", s=80, zorder=5, label="숏 진입")
        ax1.scatter(cover_signals.index, cover_signals["close"],
                    marker="^", color="#2196F3", s=60, zorder=5, label="숏 청산")

        if "ma200" in df.columns:
            ax1.plot(df.index, df["ma200"], color="white", linewidth=0.8,
                     alpha=0.5, linestyle="--", label="MA200")

        ax1.set_ylabel("가격")
        ax1.legend(loc="upper left", fontsize=8)
        ax1.grid(alpha=0.3)

        # 2. 자산 곡선
        ax2 = axes[1]
        ax2.plot(df.index, df["equity"], color="#4CAF50", linewidth=1.5, label="자산")
        ax2.axhline(self.initial_capital, color="gray", linestyle="--",
                    linewidth=0.8, label=f"초기자본 {self.initial_capital:,.0f}")
        ax2.fill_between(df.index, self.initial_capital, df["equity"],
                         where=df["equity"] >= self.initial_capital,
                         alpha=0.15, color="#4CAF50")
        ax2.fill_between(df.index, self.initial_capital, df["equity"],
                         where=df["equity"] < self.initial_capital,
                         alpha=0.15, color="#FF1744")
        ax2.set_ylabel("자산")
        ax2.legend(loc="upper left", fontsize=8)
        ax2.grid(alpha=0.3)

        # 3. MDD
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
            safe_name = self.strategy.name.replace("/", "_").replace("\\", "_")
            fname = f"{safe_name}_{market.replace('-', '_')}.png"
            fpath = RESULTS_DIR / fname
            plt.savefig(fpath, dpi=150, bbox_inches="tight")
            plt.close()
            logger.info(f"차트 저장: {fpath}")
            return str(fpath)
        else:
            plt.show()
            return ""


class MultiStrategyBacktester:
    """여러 전략을 동시에 백테스트하고 결과를 비교"""

    def __init__(self, strategies: list, **backtester_kwargs):
        self.strategies = strategies
        self.backtester_kwargs = backtester_kwargs
        self.results = {}

    def run(self, df: pd.DataFrame, market: str = "") -> pd.DataFrame:
        all_metrics = []
        for strategy in self.strategies:
            bt = Backtester(strategy, **self.backtester_kwargs)
            metrics = bt.run(df)
            metrics["전략명"] = strategy.name
            metrics["파라미터"] = str(strategy.params)
            all_metrics.append(metrics)
            self.results[strategy.name] = bt
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

        fpath = RESULTS_DIR / f"strategy_comparison_{market.replace('-', '_')}.png"
        plt.savefig(fpath, dpi=150, bbox_inches="tight")
        plt.close()
        logger.info(f"비교 차트 저장: {fpath}")
