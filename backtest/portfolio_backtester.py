"""
포트폴리오 백테스터 (v2)

기존 backtester.py 대비 추가:
  - 8종목 동시 시뮬레이션 (타임스탬프 동기 반복)
  - Confluence Score 기반 동적 포지션 크기
  - 코인당 최대 Tier 수 제한 (max_tiers_per_coin)
  - 공유 자본 풀 + 포지션 수 한도
  - 일일 손실 한도 (-20%) 적용
  - 종목별 기여도 분석

사용법:
  python backtest/portfolio_backtester.py
"""
import sys
import logging
import warnings
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from backtest.performance import summarize
from strategies.volatility_breakout import VolatilityBreakoutStrategy

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

# ── 설정 ─────────────────────────────────────────────────────────────────────

SYMBOLS = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "LINK", "SUI"]
TIERS   = ["daily", "4h", "1h"]

TIER_CFG = {
    "daily": dict(tf="1d",  k=0.08, ma=200, atr_sl=1.5, atr_tp=2.5, max_hold_bars=4,   lev_max=2,  size_pct=0.30),
    "4h":    dict(tf="4h",  k=0.06, ma=200, atr_sl=1.2, atr_tp=2.0, max_hold_bars=12,  lev_max=3,  size_pct=0.25),
    "1h":    dict(tf="1h",  k=0.04, ma=50,  atr_sl=0.9, atr_tp=1.5, max_hold_bars=24,  lev_max=5,  size_pct=0.25),
}

SCORE_TO_SIZE = {1: 0.40, 3: 0.70, 5: 1.00, 7: 1.00}
SCORE_TO_LEV  = {1: 1,    3: 2,    5: 4,    7: 5}

COMMISSION    = 0.0005   # 수수료 (taker 0.05%)
SLIPPAGE      = 0.001    # 슬리피지 0.1%
MIN_SCORE     = 3        # 최소 Confluence Score
MAX_TIERS_PER_COIN = 2   # 코인당 최대 동시 Tier 수
MAX_POSITIONS = 12       # 전체 최대 동시 포지션 수
DAILY_LOSS_LIMIT = -0.20 # 일일 손실 한도 -20%


def _score_mult(score: int, table: dict) -> float:
    """점수 → 배수 조회 (최근 낮은 임계값 기준 룩업)."""
    for threshold in sorted(table.keys(), reverse=True):
        if score >= threshold:
            return table[threshold]
    return table[min(table.keys())]


@dataclass
class SimPosition:
    symbol:     str
    tier:       str
    direction:  str          # "long" | "short"
    entry_bar:  int
    entry_price: float
    volume:     float        # 계약 수 or 코인 수
    sl_price:   float
    tp_price:   float
    leverage:   float
    score:      int
    cost:       float        # 투자원금 (증거금)


class PortfolioBacktester:
    """
    다중 종목 · 다중 Tier 포트폴리오 백테스터

    Parameters
    ----------
    initial_capital : 초기 자산 (USDT)
    use_short       : 숏 전략 활성화 여부
    verbose         : 상세 로그 출력
    """

    def __init__(
        self,
        initial_capital: float = 1_000.0,
        use_short: bool = True,
        verbose: bool = False,
    ):
        self.initial_capital = initial_capital
        self.use_short = use_short
        self.verbose = verbose

    # ── 데이터 로드 ─────────────────────────────────────────────────────────

    def _load_ohlcv(self, symbol: str, tf: str) -> Optional[pd.DataFrame]:
        """data/raw/{symbol}_{tf}.csv 캐시 로드. 없으면 ccxt로 다운로드."""
        cache_dir = ROOT / "data" / "raw"
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"{symbol}_{tf}.csv"

        if cache_file.exists():
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(df) > 50:
                return df

        logger.info(f"OHLCV 다운로드: {symbol}/USDT:USDT {tf}")
        try:
            import ccxt
            exchange = ccxt.okx({"enableRateLimit": True})
            tf_map = {"1d": "1D", "4h": "4H", "1h": "1H"}
            raw = exchange.fetch_ohlcv(
                f"{symbol}/USDT:USDT", tf_map.get(tf, tf), limit=1000
            )
            df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
            df.index = pd.to_datetime(df["ts"], unit="ms")
            df = df[["open", "high", "low", "close", "volume"]]
            df.to_csv(cache_file)
            return df
        except Exception as e:
            logger.error(f"  실패: {e}")
            return None

    # ── 지표 계산 ────────────────────────────────────────────────────────────

    @staticmethod
    def _add_indicators(df: pd.DataFrame, k: float, ma_period: int) -> pd.DataFrame:
        """ATR, EMA, Supertrend, VB target 추가."""
        df = df.copy()
        # ATR
        hl  = df["high"] - df["low"]
        hc  = (df["high"] - df["close"].shift(1)).abs()
        lc  = (df["low"]  - df["close"].shift(1)).abs()
        tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        df["atr"] = tr.rolling(14).mean()

        # EMA
        df["ema_20"]  = df["close"].ewm(span=20,  adjust=False).mean()
        df["ema_55"]  = df["close"].ewm(span=55,  adjust=False).mean()
        df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()
        df[f"ma{ma_period}"] = df["close"].rolling(ma_period).mean()

        # 변동성 돌파 타겟
        prev_range = df["high"].shift(1) - df["low"].shift(1)
        df["target_long"]  = df["open"] + prev_range * k
        df["target_short"] = df["open"] - prev_range * k
        df["prev_range"]   = prev_range

        # Supertrend (간소화: ATR 기반 밴드)
        factor = 2.0
        hl2 = (df["high"] + df["low"]) / 2
        upper = hl2 + factor * df["atr"]
        lower = hl2 - factor * df["atr"]
        st_dir = pd.Series(1, index=df.index)
        for i in range(1, len(df)):
            if df["close"].iloc[i] > upper.iloc[i - 1]:
                st_dir.iloc[i] = 1
            elif df["close"].iloc[i] < lower.iloc[i - 1]:
                st_dir.iloc[i] = -1
            else:
                st_dir.iloc[i] = st_dir.iloc[i - 1]
        df["supertrend_dir"] = st_dir

        # 거래량 서지
        df["vol_ma"] = df["volume"].rolling(20).mean()
        df["vol_surge"] = df["volume"] > df["vol_ma"] * 1.0

        return df

    # ── 신호 생성 ────────────────────────────────────────────────────────────

    @staticmethod
    def _generate_signals(df: pd.DataFrame, ma_period: int, use_short: bool) -> pd.DataFrame:
        """롱(1) / 숏(2) / 없음(0) 신호 + Confluence Score 계산."""
        ma_col = f"ma{ma_period}"
        close  = df["close"]
        ma     = df[ma_col]
        ema20  = df["ema_20"]
        ema55  = df["ema_55"]
        st_dir = df["supertrend_dir"]
        vol_ok = df["vol_surge"]

        # 롱 조건
        above_ma   = close > ma
        ema_long   = (ema20 > ema55) & above_ma
        st_long    = st_dir == 1
        bp_long    = (df["high"] >= df["target_long"]) & (df["open"] < df["target_long"])

        # 숏 조건
        below_ma   = close < ma
        ema_short  = (ema20 < ema55) & below_ma
        st_short   = st_dir == -1
        bp_short   = (df["low"] <= df["target_short"]) & (df["open"] > df["target_short"])
        bear_candle = close < close.shift(1)

        long_cond  = above_ma & ema_long & st_long & bp_long
        short_cond = below_ma & ema_short & st_short & bp_short & bear_candle if use_short else pd.Series(False, index=df.index)

        df["signal"] = 0
        df.loc[long_cond,  "signal"] = 1
        df.loc[short_cond, "signal"] = 2

        # Confluence Score (0~7)
        score = pd.Series(0, index=df.index)
        score += above_ma.astype(int)           # +1: MA 방향
        score += ema_long.astype(int)           # +1: EMA 정렬 (롱)
        score += (below_ma & ema_short).astype(int)  # +1: EMA 정렬 (숏)
        score += st_long.astype(int)            # +1: Supertrend (롱)
        score += st_short.astype(int)           # +1: Supertrend (숏)
        score += vol_ok.astype(int)             # +1: 거래량 서지
        score += bp_long.astype(int)            # +1: 돌파 (롱)
        score += bp_short.astype(int)           # +1: 돌파 (숏)
        df["confluence"] = score.clip(0, 7)

        return df

    # ── 메인 백테스트 ────────────────────────────────────────────────────────

    def run(
        self,
        symbols: Optional[list] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> dict:
        """
        포트폴리오 백테스트 실행.

        Returns
        -------
        dict  {
            "equity_curve": pd.Series,
            "trades": pd.DataFrame,
            "performance": dict,
            "symbol_contribution": pd.DataFrame,
        }
        """
        symbols = symbols or SYMBOLS

        # ── 데이터 준비 ──────────────────────────────────────────────────────
        data: dict[str, dict[str, pd.DataFrame]] = {}   # data[symbol][tier] = df
        for sym in symbols:
            data[sym] = {}
            for tier, cfg in TIER_CFG.items():
                df = self._load_ohlcv(sym, cfg["tf"])
                if df is None or len(df) < 50:
                    logger.warning(f"  데이터 부족: {sym} {tier}")
                    continue
                df = self._add_indicators(df, cfg["k"], cfg["ma"])
                df = self._generate_signals(df, cfg["ma"], self.use_short)
                if start:
                    df = df[df.index >= pd.Timestamp(start)]
                if end:
                    df = df[df.index <= pd.Timestamp(end)]
                data[sym][tier] = df

        if not data:
            raise ValueError("사용 가능한 데이터 없음")

        # ── 공통 날짜 인덱스 (daily 기준, 가장 짧은 공통 구간) ─────────────
        daily_indices = [
            data[s]["daily"].index
            for s in symbols if "daily" in data.get(s, {})
        ]
        if not daily_indices:
            raise ValueError("daily 데이터 없음")
        common_idx = daily_indices[0]
        for idx in daily_indices[1:]:
            common_idx = common_idx.intersection(idx)

        logger.info(f"백테스트 기간: {common_idx[0].date()} ~ {common_idx[-1].date()} ({len(common_idx)}봉)")

        # ── 시뮬레이션 ──────────────────────────────────────────────────────
        equity   = self.initial_capital
        positions: list[SimPosition] = []
        equity_history: list[tuple] = []
        all_trades: list[dict] = []
        daily_start_equity = equity

        for bar_i, date in enumerate(common_idx):
            # 일일 손실 한도 리셋 (날짜 변경 시)
            if bar_i == 0 or date.date() != common_idx[bar_i - 1].date():
                daily_start_equity = equity

            # ── 기존 포지션 청산 체크 ────────────────────────────────────
            remaining = []
            for pos in positions:
                sym_data = data.get(pos.symbol, {}).get(pos.tier)
                if sym_data is None or date not in sym_data.index:
                    remaining.append(pos)
                    continue

                row = sym_data.loc[date]
                price_now = float(row["close"])
                bars_held = bar_i - pos.entry_bar

                # SL/TP 체크 (당일 고가/저가 기준)
                hi, lo = float(row["high"]), float(row["low"])
                closed = False
                reason = ""

                if pos.direction == "long":
                    if lo <= pos.sl_price:
                        exit_price = pos.sl_price
                        reason = "SL"
                        closed = True
                    elif hi >= pos.tp_price:
                        exit_price = pos.tp_price
                        reason = "TP"
                        closed = True
                else:  # short
                    if hi >= pos.sl_price:
                        exit_price = pos.sl_price
                        reason = "SL"
                        closed = True
                    elif lo <= pos.tp_price:
                        exit_price = pos.tp_price
                        reason = "TP"
                        closed = True

                # 시간 손절
                max_bars = TIER_CFG[pos.tier]["max_hold_bars"]
                if not closed and bars_held >= max_bars:
                    exit_price = price_now
                    reason = "TIME"
                    closed = True

                if closed:
                    fee = exit_price * pos.volume * COMMISSION * pos.leverage
                    if pos.direction == "long":
                        raw_pnl = (exit_price - pos.entry_price) * pos.volume * pos.leverage
                    else:
                        raw_pnl = (pos.entry_price - exit_price) * pos.volume * pos.leverage
                    pnl = raw_pnl - fee
                    equity += pnl

                    all_trades.append({
                        "date":      date,
                        "symbol":    pos.symbol,
                        "tier":      pos.tier,
                        "direction": pos.direction,
                        "entry":     pos.entry_price,
                        "exit":      exit_price,
                        "volume":    pos.volume,
                        "leverage":  pos.leverage,
                        "score":     pos.score,
                        "pnl":       pnl,
                        "pnl_pct":   pnl / pos.cost * 100,
                        "reason":    reason,
                        "bars_held": bars_held,
                    })
                    if self.verbose:
                        logger.debug(
                            f"  CLOSE {pos.direction.upper()} {pos.symbol} {pos.tier} "
                            f"{reason} PnL={pnl:+.2f} USDT"
                        )
                else:
                    remaining.append(pos)

            positions = remaining

            # ── 일일 손실 한도 체크 ──────────────────────────────────────
            daily_pnl_pct = (equity - daily_start_equity) / daily_start_equity
            if daily_pnl_pct <= DAILY_LOSS_LIMIT:
                equity_history.append((date, equity))
                continue  # 이날 신규 진입 차단

            # ── 신규 진입 체크 ───────────────────────────────────────────
            for tier, cfg in TIER_CFG.items():
                if len(positions) >= MAX_POSITIONS:
                    break

                for sym in symbols:
                    if len(positions) >= MAX_POSITIONS:
                        break

                    sym_data = data.get(sym, {}).get(tier)
                    if sym_data is None or date not in sym_data.index:
                        continue

                    row    = sym_data.loc[date]
                    signal = int(row.get("signal", 0))
                    score  = int(row.get("confluence", 0))

                    if signal == 0 or score < MIN_SCORE:
                        continue

                    direction = "long" if signal == 1 else "short"
                    if not self.use_short and direction == "short":
                        continue

                    # 같은 Tier+종목+방향 이미 보유
                    if any(p.symbol == sym and p.tier == tier and p.direction == direction
                           for p in positions):
                        continue

                    # 코인당 최대 Tier 수 체크
                    coin_tiers = len({p.tier for p in positions if p.symbol == sym})
                    if coin_tiers >= MAX_TIERS_PER_COIN:
                        continue

                    # 포지션 크기 계산
                    weight    = 1.0 / len(symbols)
                    size_mult = _score_mult(score, SCORE_TO_SIZE)
                    lev_score = _score_mult(score, SCORE_TO_LEV)
                    leverage  = min(lev_score, cfg["lev_max"])
                    invest    = equity * weight * cfg["size_pct"] * size_mult
                    invest    = min(invest, equity * 0.25)  # 종목당 최대 25%

                    if invest < 5.0:
                        continue

                    atr_val    = float(row.get("atr", 0) or 0)
                    entry_price = float(row["close"]) * (1 + SLIPPAGE if direction == "long" else 1 - SLIPPAGE)
                    volume     = invest * leverage / entry_price if atr_val > 0 else invest / entry_price

                    if atr_val > 0:
                        sl_price = (entry_price - atr_val * cfg["atr_sl"]
                                    if direction == "long"
                                    else entry_price + atr_val * cfg["atr_sl"])
                        tp_price = (entry_price + atr_val * cfg["atr_tp"]
                                    if direction == "long"
                                    else entry_price - atr_val * cfg["atr_tp"])
                    else:
                        sl_price = entry_price * (0.97 if direction == "long" else 1.03)
                        tp_price = entry_price * (1.05 if direction == "long" else 0.95)

                    fee = entry_price * volume * COMMISSION * leverage
                    equity -= fee

                    pos = SimPosition(
                        symbol=sym, tier=tier, direction=direction,
                        entry_bar=bar_i, entry_price=entry_price,
                        volume=volume, sl_price=sl_price, tp_price=tp_price,
                        leverage=leverage, score=score, cost=invest,
                    )
                    positions.append(pos)

                    if self.verbose:
                        logger.debug(
                            f"  OPEN {direction.upper()} {sym} {tier} "
                            f"score={score} lev={leverage}x invest=${invest:.2f}"
                        )

            equity_history.append((date, equity))

        # ── 잔여 포지션 강제 청산 ────────────────────────────────────────
        last_date = common_idx[-1]
        for pos in positions:
            sym_data = data.get(pos.symbol, {}).get(pos.tier)
            if sym_data is None:
                continue
            if last_date in sym_data.index:
                exit_price = float(sym_data.loc[last_date, "close"])
            else:
                exit_price = pos.entry_price
            if pos.direction == "long":
                pnl = (exit_price - pos.entry_price) * pos.volume * pos.leverage
            else:
                pnl = (pos.entry_price - exit_price) * pos.volume * pos.leverage
            pnl -= exit_price * pos.volume * COMMISSION * pos.leverage
            equity += pnl
            all_trades.append({
                "date": last_date, "symbol": pos.symbol, "tier": pos.tier,
                "direction": pos.direction, "entry": pos.entry_price,
                "exit": exit_price, "volume": pos.volume, "leverage": pos.leverage,
                "score": pos.score, "pnl": pnl,
                "pnl_pct": pnl / pos.cost * 100, "reason": "END",
                "bars_held": len(common_idx) - pos.entry_bar,
            })

        # ── 결과 집계 ────────────────────────────────────────────────────
        dates, equities = zip(*equity_history) if equity_history else ([], [])
        equity_curve = pd.Series(list(equities), index=list(dates), name="equity")
        trades_df    = pd.DataFrame(all_trades)

        perf = summarize(equity_curve, trades_df) if not trades_df.empty else {}

        # 종목별 기여도
        symbol_contrib = pd.DataFrame()
        if not trades_df.empty:
            symbol_contrib = (
                trades_df.groupby("symbol")["pnl"]
                .agg(["sum", "count", lambda x: (x > 0).mean() * 100])
                .rename(columns={"sum": "총PnL", "count": "거래수", "<lambda_0>": "승률%"})
                .sort_values("총PnL", ascending=False)
            )

        return {
            "equity_curve":       equity_curve,
            "trades":             trades_df,
            "performance":        perf,
            "symbol_contribution": symbol_contrib,
        }


# ── CLI 실행 ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="포트폴리오 백테스트")
    parser.add_argument("--symbols", nargs="*", default=None, help="종목 리스트 (기본: 8종목)")
    parser.add_argument("--start",   default="2023-01-01",   help="시작일 YYYY-MM-DD")
    parser.add_argument("--end",     default="2026-03-28",   help="종료일 YYYY-MM-DD")
    parser.add_argument("--capital", type=float, default=1000.0, help="초기 자산 USDT")
    parser.add_argument("--no-short", action="store_true",   help="숏 전략 비활성화")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    bt = PortfolioBacktester(
        initial_capital=args.capital,
        use_short=not args.no_short,
        verbose=args.verbose,
    )

    result = bt.run(symbols=args.symbols, start=args.start, end=args.end)

    print("\n" + "=" * 60)
    print("  포트폴리오 백테스트 결과")
    print("=" * 60)
    perf = result["performance"]
    for k, v in perf.items():
        if isinstance(v, float):
            print(f"  {k:<25}: {v:>10.2f}")
        else:
            print(f"  {k:<25}: {v}")

    print("\n── 종목별 기여도 ──")
    print(result["symbol_contribution"].to_string())

    # 거래 건수
    trades = result["trades"]
    if not trades.empty:
        print(f"\n  총 거래: {len(trades)}건 | 롱: {(trades.direction=='long').sum()}건 | 숏: {(trades.direction=='short').sum()}건")
        print(f"  Tier별: " + " | ".join(
            f"{t}: {(trades.tier==t).sum()}건"
            for t in ["daily", "4h", "1h"]
        ))
