"""
거래소 API 및 전략 신호 집계

봇(main.py)과 독립적인 별도 프로세스에서 실행.
read-only API 호출만 수행.
"""
import sys
import os
import logging
import threading
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / "config" / ".env")

import pyupbit
import yaml
import pandas as pd

logger = logging.getLogger(__name__)

with open(ROOT / "config" / "config.yaml", encoding="utf-8") as f:
    _config = yaml.safe_load(f)

MARKETS        = _config["markets"]
ASSET_WEIGHTS  = _config["asset_weights"]
EX_CFG         = _config["exchanges"]
OKX_CFG        = _config.get("okx_futures", {})
TRADING_CFG    = _config["trading"]
_EX_MARKETS    = _config.get("exchange_markets", {})
OKX_MARKETS    = _EX_MARKETS.get("okx", MARKETS)
UPBIT_MARKETS  = _EX_MARKETS.get("upbit", MARKETS)


class DataAggregator:
    def __init__(self):
        self._lock      = threading.Lock()
        self._exchanges = {}
        self._strategy_long       = None
        self._strategy_longshort  = None
        self._init_exchanges()
        self._init_strategies()

    # ── 초기화 ──────────────────────────────────────────────────

    def _init_exchanges(self):
        from engine.upbit_exchange   import UpbitExchange
        from engine.bithumb_exchange import BithumbExchange
        from engine.okx_exchange     import OKXExchange

        if EX_CFG["upbit"]["enabled"]:
            try:
                self._exchanges["upbit"] = UpbitExchange(
                    paper_trading=EX_CFG["upbit"]["paper_trading"]
                )
            except Exception as e:
                logger.warning(f"Upbit 초기화 실패: {e}")

        if EX_CFG["bithumb"]["enabled"]:
            try:
                self._exchanges["bithumb"] = BithumbExchange(
                    paper_trading=EX_CFG["bithumb"]["paper_trading"]
                )
            except Exception as e:
                logger.warning(f"Bithumb 초기화 실패: {e}")

        if EX_CFG["okx"]["enabled"]:
            try:
                self._exchanges["okx"] = OKXExchange(
                    paper_trading=EX_CFG["okx"]["paper_trading"],
                    leverage=OKX_CFG.get("leverage", 1),
                    use_short=OKX_CFG.get("use_short", True),
                )
            except Exception as e:
                logger.warning(f"OKX 초기화 실패: {e}")

    def _init_strategies(self):
        from strategies import VolatilityBreakoutStrategy
        _vb = _config["strategies"]["volatility_breakout"]
        kwargs = dict(
            k=_vb["k"],
            ma_period=200,
            volume_lookback=_vb.get("volume_lookback", 20),
            volume_multiplier=_vb.get("volume_multiplier", 1.5),
            vp_lookback=_vb.get("vp_lookback", 20),
            vp_bins=_vb.get("vp_bins", 50),
            fib_lookback=_vb.get("fib_lookback", 50),
        )
        self._strategy_long      = VolatilityBreakoutStrategy(use_short=False, **kwargs)
        self._strategy_longshort = VolatilityBreakoutStrategy(use_short=True,  **kwargs)

    # ── OHLCV 수집 ──────────────────────────────────────────────

    def _get_usdt_krw_rate(self) -> float:
        try:
            rate = pyupbit.get_current_price("KRW-USDT")
            return float(rate) if rate else 1380.0
        except Exception:
            return 1380.0

    def _fetch_upbit_ohlcv(self, market: str, interval: str = "day", count: int = 200) -> pd.DataFrame:
        try:
            df = pyupbit.get_ohlcv(market, interval=interval, count=count)
            if df is None or df.empty:
                return pd.DataFrame()
            df.columns = [c.lower() for c in df.columns]
            return df
        except Exception as e:
            logger.error(f"Upbit OHLCV 실패 ({market}): {e}")
            return pd.DataFrame()

    def _fetch_okx_ohlcv(self, market: str, interval: str = "day", count: int = 200) -> pd.DataFrame:
        """OKX ccxt 직접 호출 (ccxt timeframe key 소문자 사용)"""
        try:
            import ccxt
            ex_okx = self._exchanges.get("okx")
            if ex_okx and hasattr(ex_okx, "_spot") and ex_okx._spot:
                spot = ex_okx._spot
            else:
                spot = ccxt.okx({"options": {"defaultType": "spot"}})

            tf_map = {
                "day": "1d", "week": "1w", "month": "1M",
                "minute60": "1h", "minute240": "4h",
                "minute30": "30m", "minute15": "15m",
                "minute5": "5m", "minute1": "1m",
            }
            tf     = tf_map.get(interval, "1d")
            symbol  = ex_okx._spot_symbol(market) if ex_okx else f"{market.replace('KRW-', '')}/USDT"

            raw = spot.fetch_ohlcv(symbol, tf, limit=count)
            df  = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df.index = pd.to_datetime(df["timestamp"], unit="ms")
            return df[["open", "high", "low", "close", "volume"]]
        except Exception as e:
            logger.error(f"OKX OHLCV 실패 ({market}): {e}")
            return pd.DataFrame()

    def _df_to_ts(self, dt_idx) -> int:
        """DatetimeIndex 항목 → UTC Unix 초 변환"""
        try:
            if hasattr(dt_idx, "tzinfo") and dt_idx.tzinfo is not None:
                return int(dt_idx.timestamp())
            else:
                import pytz
                kst = pytz.timezone("Asia/Seoul")
                return int(kst.localize(dt_idx.to_pydatetime()).timestamp())
        except Exception:
            return int(dt_idx.timestamp()) if hasattr(dt_idx, "timestamp") else 0

    # ── API 응답 메서드 ──────────────────────────────────────────

    def get_candles(self, exchange: str, market: str, interval: str = "day", count: int = 200) -> dict:
        with self._lock:
            # MA200 계산을 위해 count+210 개 가져온 후 마지막 count 개만 반환
            fetch_count = count + 210
            df = self._fetch_okx_ohlcv(market, interval, fetch_count) if exchange == "okx" \
                 else self._fetch_upbit_ohlcv(market, interval, fetch_count)

            if df.empty:
                return {"exchange": exchange, "market": market, "candles": [], "indicators": {}}

            strat = self._strategy_longshort if exchange == "okx" else self._strategy_long
            try:
                sig_df = strat.generate_signals(df)
            except Exception as e:
                logger.error(f"신호 생성 실패: {e}")
                sig_df = df

            # 마지막 count 개 행만 차트에 표시
            if len(sig_df) > count:
                sig_df = sig_df.iloc[-count:]

            candles    = []
            indicators = {k: [] for k in ["ma200", "target_long", "target_short",
                                           "vp_poc", "vp_vah", "vp_val"]}

            for dt_idx, row in sig_df.iterrows():
                ts = self._df_to_ts(dt_idx)
                candles.append({
                    "time":   ts,
                    "open":   float(row.get("open",   0) or 0),
                    "high":   float(row.get("high",   0) or 0),
                    "low":    float(row.get("low",    0) or 0),
                    "close":  float(row.get("close",  0) or 0),
                    "volume": float(row.get("volume", 0) or 0),
                })
                for key in indicators:
                    val = row.get(key)
                    if val is not None and pd.notna(val) and float(val) > 0:
                        indicators[key].append({"time": ts, "value": float(val)})

            return {
                "exchange":   exchange,
                "market":     market,
                "interval":   interval,
                "candles":    candles,
                "indicators": indicators,
            }

    def get_portfolio(self) -> dict:
        with self._lock:
            usdt_krw = self._get_usdt_krw_rate()
            result = {
                "timestamp":      __import__("datetime").datetime.now().isoformat(),
                "usdt_krw_rate":  usdt_krw,
                "total_krw_equiv": 0.0,
                "exchanges":      {},
            }

            for ex_name, ex in self._exchanges.items():
                try:
                    quote_bal = ex.get_balance_quote()
                    equity    = quote_bal
                    positions = {}

                    # OKX는 선물 전용 — 스팟 코인 잔고 조회 생략 (항상 0, API 낭비)
                    if ex_name != "okx":
                        spot_markets = UPBIT_MARKETS if ex_name == "upbit" else MARKETS
                        for market in spot_markets:
                            try:
                                vol   = ex.get_balance_coin(market)
                                avg   = ex.get_avg_buy_price(market) if vol > 0 else 0.0
                                price = ex.get_current_price(market) or 0.0
                                coin_val   = vol * price
                                equity    += coin_val
                                unr_pct    = ((price - avg) / avg * 100) if avg > 0 else 0.0
                                unr_quote  = (price - avg) * vol if avg > 0 else 0.0
                                cur        = ex.quote_currency.lower()
                                if vol > 0:   # 보유 중인 포지션만 포함
                                    positions[market] = {
                                        "held":            True,
                                        "volume":          vol,
                                        "entry_price":     avg,
                                        "current_price":   price,
                                        "unrealized_pnl_pct":                round(unr_pct,   4),
                                        f"unrealized_pnl_{cur}":             round(unr_quote, 2),
                                        "coin_value":      round(coin_val, 2),
                                    }
                            except Exception as e:
                                logger.error(f"포지션 오류 {ex_name}/{market}: {e}")

                    ex_data = {
                        "enabled":        True,
                        "paper_trading":  ex.paper_trading,
                        "quote_currency": ex.quote_currency,
                        "quote_balance":  round(quote_bal, 2),
                        "total_equity":   round(equity,    2),
                        "positions":      positions,
                    }

                    # OKX 선물 포지션 — 보유 중인 것만 포함, 미실현 PnL을 equity에 반영
                    if ex_name == "okx":
                        from engine.okx_exchange import OKXExchange
                        if isinstance(ex, OKXExchange) and ex.use_short:
                            futures = {}
                            futures_unrealized = 0.0
                            for market in OKX_MARKETS:
                                try:
                                    p     = ex.get_futures_position(market)
                                    vol   = p.get("volume", 0.0) or 0.0
                                    side  = p.get("side")
                                    if not side or vol <= 0:
                                        continue   # 미보유 종목 스킵
                                    price = ex.get_current_price(market) or 0.0
                                    entry = p.get("entry_price", 0.0) or 0.0
                                    if side == "short" and entry > 0:
                                        pnl_pct  = (entry - price) / entry * 100
                                        pnl_usdt = (entry - price) * vol
                                    elif side == "long" and entry > 0:
                                        pnl_pct  = (price - entry) / entry * 100
                                        pnl_usdt = (price - entry) * vol
                                    else:
                                        pnl_pct  = 0.0
                                        pnl_usdt = 0.0
                                    futures_unrealized += pnl_usdt
                                    futures[market] = {
                                        "side":                side,
                                        "volume":              vol,
                                        "entry_price":         entry,
                                        "current_price":       price,
                                        "unrealized_pnl_pct":  round(pnl_pct, 4),
                                        "unrealized_pnl_usdt": round(pnl_usdt, 2),
                                    }
                                except Exception as e:
                                    logger.error(f"OKX 선물 포지션 오류 {market}: {e}")
                            # 미실현 PnL을 OKX 총자산에 반영
                            equity += futures_unrealized
                            ex_data["futures"] = futures
                            ex_data["total_equity"] = round(equity, 2)

                    krw_equiv = equity * usdt_krw if ex.quote_currency == "USDT" else equity
                    result["total_krw_equiv"] += krw_equiv
                    result["exchanges"][ex_name] = ex_data

                except Exception as e:
                    logger.error(f"{ex_name} 포트폴리오 오류: {e}")
                    result["exchanges"][ex_name] = {"enabled": False, "error": str(e)}

            result["total_krw_equiv"] = round(result["total_krw_equiv"], 0)
            return result

    def get_risk_status(self) -> dict:
        with self._lock:
            result = {}
            for ex_name, ex in self._exchanges.items():
                try:
                    equity = ex.get_balance_quote()
                    sl_prices = {}
                    tp_prices = {}
                    for market in MARKETS:
                        vol = ex.get_balance_coin(market)
                        price = ex.get_current_price(market) or 0.0
                        equity += vol * price
                        avg = ex.get_avg_buy_price(market) if vol > 0 else 0.0
                        if avg > 0:
                            sl_prices[market] = round(avg * (1 + TRADING_CFG["stop_loss_pct"]),  0)
                            tp_prices[market] = round(avg * (1 + TRADING_CFG["take_profit_pct"]), 0)
                        else:
                            sl_prices[market] = 0.0
                            tp_prices[market] = 0.0

                    result[ex_name] = {
                        "current_equity":       round(equity, 2),
                        "quote_currency":       ex.quote_currency,
                        "stop_loss_pct":        TRADING_CFG["stop_loss_pct"],
                        "take_profit_pct":      TRADING_CFG["take_profit_pct"],
                        "daily_loss_limit_pct": TRADING_CFG["daily_loss_limit_pct"],
                        "stop_loss_prices":     sl_prices,
                        "take_profit_prices":   tp_prices,
                    }
                except Exception as e:
                    logger.error(f"{ex_name} 리스크 상태 오류: {e}")
                    result[ex_name] = {"error": str(e)}
            return result

    def get_signals(self) -> dict:
        with self._lock:
            result = {}
            for ex_name, ex in self._exchanges.items():
                result[ex_name] = {}
                strat = self._strategy_longshort if ex_name == "okx" else self._strategy_long
                markets_to_check = OKX_MARKETS if ex_name == "okx" else (UPBIT_MARKETS if ex_name == "upbit" else MARKETS)
                for market in markets_to_check:
                    try:
                        df = self._fetch_okx_ohlcv(market, "day", 210) if ex_name == "okx" \
                             else self._fetch_upbit_ohlcv(market, "day", 210)
                        if df.empty:
                            continue
                        sig_df = strat.generate_signals(df)
                        if sig_df.empty:
                            continue
                        latest = sig_df.iloc[-1]
                        price  = float(latest.get("close",  0) or 0)
                        ma200  = float(latest.get("ma200",  0) or 0)
                        result[ex_name][market] = {
                            "signal":       int(latest.get("signal", 0)),
                            "current_price": price,
                            "ma200":        ma200,
                            "trend":        "uptrend" if price > ma200 else "downtrend",
                            "target_long":  float(latest.get("target_long",  0) or 0),
                            "target_short": float(latest.get("target_short", 0) or 0),
                            "vol_surge":    bool(latest.get("vol_surge", False)),
                            "vp_poc":       float(latest.get("vp_poc", 0) or 0),
                            "vp_vah":       float(latest.get("vp_vah", 0) or 0),
                            "vp_val":       float(latest.get("vp_val", 0) or 0),
                        }
                    except Exception as e:
                        logger.error(f"신호 조회 실패 {ex_name}/{market}: {e}")
            return result
