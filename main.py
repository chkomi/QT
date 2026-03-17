"""
퀀트 자동매매 봇 — 멀티 거래소 + 스윙/단타 복합 전략
─────────────────────────────────────────────────────────
업비트 / 빗썸 : 현물 롱 스윙 (MA200 위에서만 매수)
OKX 스윙      : 현물 롱 + 선물 숏 (MA200 기준 양방향, 일봉)
OKX 단타      : 현물 롱 + 선물 숏 (일봉 추세 확인 후 1시간봉 진입)
자산 배분     : BTC 70% / ETH 30%
단타 자본     : OKX 잔고의 30% 별도 운용
"""
import os
import sys
import time
import logging
import schedule
import yaml
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
load_dotenv(dotenv_path=ROOT / "config" / ".env")

from data.data_collector import fetch_ohlcv
from strategies import VolatilityBreakoutStrategy
from engine.upbit_exchange import UpbitExchange
from engine.bithumb_exchange import BithumbExchange
from engine.okx_exchange import OKXExchange
from engine.risk_manager import RiskManager
from monitor.telegram_bot import TelegramNotifier

# ── 로그 설정 ──────────────────────────────────────────
LOG_FILE = ROOT / "logs" / "trades.log"
LOG_FILE.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")

# ── 설정 로드 ──────────────────────────────────────────
with open(ROOT / "config" / "config.yaml", encoding="utf-8") as f:
    config = yaml.safe_load(f)

MARKETS      = config["markets"]
ASSET_WEIGHTS = config["asset_weights"]
EX_CFG       = config["exchanges"]
OKX_CFG      = config.get("okx_futures", {})
SC_CFG       = config.get("scalp_trading", {})

# ── 거래소 초기화 ──────────────────────────────────────
EXCHANGES = {}
if EX_CFG["upbit"]["enabled"]:
    EXCHANGES["upbit"] = UpbitExchange(
        paper_trading=EX_CFG["upbit"]["paper_trading"]
    )
if EX_CFG["bithumb"]["enabled"]:
    EXCHANGES["bithumb"] = BithumbExchange(
        paper_trading=EX_CFG["bithumb"]["paper_trading"]
    )
if EX_CFG["okx"]["enabled"]:
    EXCHANGES["okx"] = OKXExchange(
        paper_trading=EX_CFG["okx"]["paper_trading"],
        leverage=OKX_CFG.get("leverage", 1),
        use_short=OKX_CFG.get("use_short", True),
    )

if not EXCHANGES:
    raise RuntimeError("활성화된 거래소가 없습니다. config.yaml을 확인하세요.")

# ── 전략 ───────────────────────────────────────────────
# KRW 거래소 (업비트/빗썸): 롱 전용
_vb = config["strategies"]["volatility_breakout"]
strategy_long = VolatilityBreakoutStrategy(
    k=_vb["k"], ma_period=200, use_short=False,
    volume_lookback=_vb.get("volume_lookback", 20),
    volume_multiplier=_vb.get("volume_multiplier", 1.5),
    vp_lookback=_vb.get("vp_lookback", 20),
    vp_bins=_vb.get("vp_bins", 50),
    fib_lookback=_vb.get("fib_lookback", 50),
)
# OKX: 롱 + 숏
strategy_longshort = VolatilityBreakoutStrategy(
    k=_vb["k"], ma_period=200, use_short=True,
    volume_lookback=_vb.get("volume_lookback", 20),
    volume_multiplier=_vb.get("volume_multiplier", 1.5),
    vp_lookback=_vb.get("vp_lookback", 20),
    vp_bins=_vb.get("vp_bins", 50),
    fib_lookback=_vb.get("fib_lookback", 50),
)
# OKX 단타: 1시간봉 기반, 타이트한 파라미터
strategy_scalp = VolatilityBreakoutStrategy(
    k=SC_CFG.get("k", 0.4),
    ma_period=SC_CFG.get("ma_period", 50),
    use_short=True,
    volume_lookback=SC_CFG.get("volume_lookback", 20),
    volume_multiplier=SC_CFG.get("volume_multiplier", 2.0),
    vp_lookback=SC_CFG.get("vp_lookback", 20),
    vp_bins=SC_CFG.get("vp_bins", 30),
    fib_lookback=SC_CFG.get("fib_lookback", 30),
)

notifier = TelegramNotifier()

# ── 포지션 상태 ────────────────────────────────────────
# long_pos        : 현물 롱 포지션 {held, entry_price, volume}
# short_pos       : 선물 숏 포지션 {held, entry_price, volume}  ← OKX 전용
# scalp_long_pos  : 단타 현물 롱   {held, entry_price, volume, entry_time}
# scalp_short_pos : 단타 선물 숏   {held, entry_price, volume, entry_time}
long_positions       = {}
short_positions      = {}
scalp_long_positions = {}
scalp_short_positions= {}
risk_managers        = {}
scalp_risk_managers  = {}

_SCALP_RATIO = SC_CFG.get("capital_ratio", 0.30)

for ex_name, ex in EXCHANGES.items():
    initial_bal = ex.get_balance_quote()
    min_ord = 5.0 if ex.quote_currency == "USDT" else config["trading"]["min_order_amount"]

    risk_managers[ex_name] = RiskManager(
        initial_capital=max(initial_bal, 1),
        max_position_ratio=config["trading"]["max_position_ratio"],
        stop_loss_pct=config["trading"]["stop_loss_pct"],
        take_profit_pct=config["trading"]["take_profit_pct"],
        daily_loss_limit_pct=config["trading"]["daily_loss_limit_pct"],
        min_order_amount=min_ord,
    )
    long_positions[ex_name]  = {m: {"held": False, "entry_price": 0.0, "volume": 0.0}
                                 for m in MARKETS}
    short_positions[ex_name] = {m: {"held": False, "entry_price": 0.0, "volume": 0.0}
                                 for m in MARKETS}

    # 단타는 OKX만
    if ex_name == "okx" and SC_CFG.get("enabled", False):
        scalp_cap = max(initial_bal * _SCALP_RATIO, 1)
        scalp_risk_managers[ex_name] = RiskManager(
            initial_capital=scalp_cap,
            max_position_ratio=config["trading"]["max_position_ratio"],
            stop_loss_pct=SC_CFG.get("stop_loss_pct", -0.012),
            take_profit_pct=SC_CFG.get("take_profit_pct", 0.020),
            daily_loss_limit_pct=SC_CFG.get("daily_loss_limit_pct", -0.10),
            min_order_amount=5.0,
        )
        scalp_long_positions[ex_name]  = {
            m: {"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None}
            for m in MARKETS
        }
        scalp_short_positions[ex_name] = {
            m: {"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None}
            for m in MARKETS
        }


# ── 유틸 ──────────────────────────────────────────────

def calc_total_equity(ex_name: str) -> float:
    ex = EXCHANGES[ex_name]
    total = ex.get_balance_quote()
    for market in MARKETS:
        vol = ex.get_balance_coin(market)
        if vol > 0:
            price = ex.get_current_price(market)
            if price:
                total += vol * price
    return total


def sync_positions(ex_name: str):
    """시작 시 실제 잔고 → 포지션 동기화"""
    ex = EXCHANGES[ex_name]

    # 현물 롱 동기화
    for market in MARKETS:
        vol = ex.get_balance_coin(market)
        if vol > 0.000001:
            avg = ex.get_avg_buy_price(market)
            if avg == 0:
                avg = ex.get_current_price(market) or 0
            long_positions[ex_name][market].update(
                {"held": True, "entry_price": avg, "volume": vol}
            )
            logger.info(f"[{ex_name}] 롱 동기화 | {market} {vol:.6f}개 @ {avg:,.2f}")

    # OKX 숏 동기화
    if ex_name == "okx" and isinstance(ex, OKXExchange) and ex.use_short:
        for market in MARKETS:
            p = ex.get_futures_position(market)
            if p["side"] == "short" and p["volume"] > 0:
                short_positions[ex_name][market].update(
                    {"held": True, "entry_price": p["entry_price"], "volume": p["volume"]}
                )
                logger.info(f"[{ex_name}] 숏 동기화 | {market} {p['volume']:.6f}개 @ {p['entry_price']:,.2f}")


# ── 메인 전략 루프 ─────────────────────────────────────

def run_strategy():
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    logger.info(f"{'='*55}")
    logger.info(f"전략 실행 | {now}")

    # 거래소별 OHLCV 수집 (OKX는 USDT 기준, 나머지는 KRW 업비트 기준)
    upbit_ohlcv = {}
    for market in MARKETS:
        df = fetch_ohlcv(market, interval="day", count=210)
        if df is not None and not df.empty:
            upbit_ohlcv[market] = df

    for ex_name, ex in EXCHANGES.items():
        equity = calc_total_equity(ex_name)
        risk_managers[ex_name].reset_daily(equity)
        logger.info(f"[{ex_name}] 총 자산: {equity:,.2f} {ex.quote_currency}")

        strat = strategy_longshort if ex_name == "okx" else strategy_long

        # OKX: ccxt로 USDT 가격 OHLCV 수집
        if ex_name == "okx" and hasattr(ex, "fetch_ohlcv"):
            ohlcv_cache = {}
            for market in MARKETS:
                df = ex.fetch_ohlcv(market, interval="day", count=210)
                if df is not None and not df.empty:
                    ohlcv_cache[market] = df
        else:
            ohlcv_cache = upbit_ohlcv

        for market in MARKETS:
            if market not in ohlcv_cache:
                continue
            _process(ex_name, market, ohlcv_cache[market], strat)


def _process(ex_name: str, market: str, df, strat: VolatilityBreakoutStrategy):
    ex  = EXCHANGES[ex_name]
    rm  = risk_managers[ex_name]
    lp  = long_positions[ex_name][market]   # 롱 포지션
    sp  = short_positions[ex_name][market]  # 숏 포지션

    equity = calc_total_equity(ex_name)
    if not rm.is_trading_allowed(equity):
        logger.warning(f"[{ex_name}][{market}] 거래 중단 상태")
        return

    price = ex.get_current_price(market)
    if not price:
        return

    # ── 롱 손절/익절 ──────────────────────────────────
    if lp["held"] and lp["entry_price"] > 0:
        if rm.should_stop_loss(lp["entry_price"], price):
            if ex.sell_market_order(market, lp["volume"]):
                notifier.notify_stop_loss(f"{ex_name}/{market}", lp["entry_price"], price)
                lp.update({"held": False, "entry_price": 0.0, "volume": 0.0})
                logger.warning(f"[{ex_name}][{market}] 롱 손절")
            return
        if rm.should_take_profit(lp["entry_price"], price):
            if ex.sell_market_order(market, lp["volume"]):
                notifier.notify_take_profit(f"{ex_name}/{market}", lp["entry_price"], price)
                lp.update({"held": False, "entry_price": 0.0, "volume": 0.0})
                logger.info(f"[{ex_name}][{market}] 롱 익절")
            return

    # ── 숏 손절/익절 (OKX 전용) ───────────────────────
    if ex_name == "okx" and sp["held"] and sp["entry_price"] > 0:
        # 숏은 가격이 올라가면 손실 → 손절 반대 방향
        short_stop   = sp["entry_price"] * (1 + abs(rm.stop_loss_pct))
        short_profit = sp["entry_price"] * (1 - rm.take_profit_pct)

        if price >= short_stop:
            if ex.close_short(market, sp["volume"]):
                loss_pct = (price - sp["entry_price"]) / sp["entry_price"] * 100
                notifier.notify_stop_loss(f"{ex_name}/{market} 숏", sp["entry_price"], price)
                sp.update({"held": False, "entry_price": 0.0, "volume": 0.0})
                logger.warning(f"[{ex_name}][{market}] 숏 손절 ({loss_pct:+.2f}%)")
            return

        if price <= short_profit:
            if ex.close_short(market, sp["volume"]):
                gain_pct = (sp["entry_price"] - price) / sp["entry_price"] * 100
                notifier.notify_take_profit(f"{ex_name}/{market} 숏", sp["entry_price"], price)
                sp.update({"held": False, "entry_price": 0.0, "volume": 0.0})
                logger.info(f"[{ex_name}][{market}] 숏 익절 (+{gain_pct:.2f}%)")
            return

    # ── 전략 신호 생성 ────────────────────────────────
    signal_df = strat.generate_signals(df)
    if signal_df.empty:
        return

    latest  = signal_df.iloc[-1]
    signal  = latest.get("signal", 0)
    ma200   = latest.get("ma200", 0)
    trend   = "▲상승" if price > ma200 else "▼하락"

    logger.info(
        f"[{ex_name}][{market}] 현재가: {price:,.2f} | MA200: {ma200:,.2f} "
        f"| {trend} | 신호: {int(signal)}"
    )

    cash   = ex.get_balance_quote()
    weight = ASSET_WEIGHTS.get(market, 0.5)
    invest = rm.calc_position_size(cash, price) * weight

    # ── 롱 진입 (signal=1) ────────────────────────────
    if signal == 1 and not lp["held"]:
        if invest > 0 and ex.buy_market_order(market, invest):
            vol = invest / price
            lp.update({"held": True, "entry_price": price, "volume": vol})
            notifier.notify_buy(f"{ex_name}/{market} 롱", price, invest)
            logger.info(f"[{ex_name}][{market}] 롱 진입 | {invest:,.2f} {ex.quote_currency}")

    # ── 숏 진입 (signal=2, OKX 전용) ─────────────────
    elif signal == 2 and ex_name == "okx" and not sp["held"]:
        if invest > 0 and isinstance(ex, OKXExchange):
            if ex.open_short(market, invest):
                vol = invest / price
                sp.update({"held": True, "entry_price": price, "volume": vol})
                notifier.notify_buy(f"{ex_name}/{market} 숏↓", price, invest)
                logger.info(f"[{ex_name}][{market}] 숏 진입 | {invest:,.2f} USDT")

    # ── 롱 청산 (signal=-1) ───────────────────────────
    elif signal == -1 and lp["held"]:
        if ex.sell_market_order(market, lp["volume"]):
            pnl = (price - lp["entry_price"]) / lp["entry_price"] * 100
            notifier.notify_sell(f"{ex_name}/{market}", price, lp["volume"], pnl)
            lp.update({"held": False, "entry_price": 0.0, "volume": 0.0})
            logger.info(f"[{ex_name}][{market}] 롱 청산 | {pnl:+.2f}%")


# ── 단타 전략 루프 (OKX 전용, 매시 :30) ───────────────

def run_scalp_strategy():
    if "okx" not in EXCHANGES or not SC_CFG.get("enabled", False):
        return
    if "okx" not in scalp_risk_managers:
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    logger.info(f"{'─'*55}")
    logger.info(f"[단타] 전략 실행 | {now}")

    ex_name = "okx"
    ex = EXCHANGES[ex_name]
    rm = scalp_risk_managers[ex_name]

    # 1H OHLCV + 일봉 OHLCV 수집
    ohlcv_1h = {}
    ohlcv_1d = {}
    for market in MARKETS:
        df_1h = ex.fetch_ohlcv(market, interval=SC_CFG.get("timeframe", "minute60"), count=210)
        if df_1h is not None and not df_1h.empty:
            ohlcv_1h[market] = df_1h
        df_1d = ex.fetch_ohlcv(market, interval="day", count=210)
        if df_1d is not None and not df_1d.empty:
            ohlcv_1d[market] = df_1d

    equity = calc_total_equity(ex_name)
    rm.reset_daily(equity * _SCALP_RATIO)

    for market in MARKETS:
        if market not in ohlcv_1h or market not in ohlcv_1d:
            continue
        _process_scalp(ex_name, market, ohlcv_1h[market], ohlcv_1d[market])


def _process_scalp(ex_name: str, market: str, df_1h, df_1d):
    ex  = EXCHANGES[ex_name]
    rm  = scalp_risk_managers[ex_name]
    lp  = scalp_long_positions[ex_name][market]
    sp  = scalp_short_positions[ex_name][market]

    equity = calc_total_equity(ex_name)
    if not rm.is_trading_allowed(equity * _SCALP_RATIO):
        logger.warning(f"[{ex_name}][{market}] 단타 거래 중단 상태")
        return

    price = ex.get_current_price(market)
    if not price:
        return

    MAX_H = SC_CFG.get("max_holding_hours", 6)

    # ── 시간 손절 (강제 청산) ─────────────────────────
    if lp["held"] and lp["entry_time"]:
        held_h = (datetime.now() - lp["entry_time"]).total_seconds() / 3600
        if held_h >= MAX_H:
            if ex.sell_market_order(market, lp["volume"]):
                pnl = (price - lp["entry_price"]) / lp["entry_price"] * 100
                notifier.notify_sell(f"{ex_name}/{market} 단타롱(시간)", price, lp["volume"], pnl)
                lp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None})
                logger.info(f"[{ex_name}][{market}] 단타 롱 시간청산 {MAX_H}H | {pnl:+.2f}%")
            return

    if sp["held"] and sp["entry_time"]:
        held_h = (datetime.now() - sp["entry_time"]).total_seconds() / 3600
        if held_h >= MAX_H:
            if ex.close_short(market, sp["volume"]):
                pnl = (sp["entry_price"] - price) / sp["entry_price"] * 100
                notifier.notify_sell(f"{ex_name}/{market} 단타숏(시간)", price, sp["volume"], pnl)
                sp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None})
                logger.info(f"[{ex_name}][{market}] 단타 숏 시간청산 {MAX_H}H | {pnl:+.2f}%")
            return

    # ── 롱 손절/익절 ──────────────────────────────────
    if lp["held"] and lp["entry_price"] > 0:
        if rm.should_stop_loss(lp["entry_price"], price):
            if ex.sell_market_order(market, lp["volume"]):
                notifier.notify_stop_loss(f"{ex_name}/{market} 단타롱", lp["entry_price"], price)
                lp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None})
                logger.warning(f"[{ex_name}][{market}] 단타 롱 손절")
            return
        if rm.should_take_profit(lp["entry_price"], price):
            if ex.sell_market_order(market, lp["volume"]):
                pnl = (price - lp["entry_price"]) / lp["entry_price"] * 100
                notifier.notify_take_profit(f"{ex_name}/{market} 단타롱", lp["entry_price"], price)
                lp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None})
                logger.info(f"[{ex_name}][{market}] 단타 롱 익절 (+{pnl:.2f}%)")
            return

    # ── 숏 손절/익절 ──────────────────────────────────
    if sp["held"] and sp["entry_price"] > 0:
        short_stop   = sp["entry_price"] * (1 + abs(rm.stop_loss_pct))
        short_profit = sp["entry_price"] * (1 - rm.take_profit_pct)
        if price >= short_stop:
            if ex.close_short(market, sp["volume"]):
                notifier.notify_stop_loss(f"{ex_name}/{market} 단타숏", sp["entry_price"], price)
                sp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None})
                logger.warning(f"[{ex_name}][{market}] 단타 숏 손절")
            return
        if price <= short_profit:
            if ex.close_short(market, sp["volume"]):
                pnl = (sp["entry_price"] - price) / sp["entry_price"] * 100
                notifier.notify_take_profit(f"{ex_name}/{market} 단타숏", sp["entry_price"], price)
                sp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None})
                logger.info(f"[{ex_name}][{market}] 단타 숏 익절 (+{pnl:.2f}%)")
            return

    # ── 신호 생성 ─────────────────────────────────────
    signal_df = strategy_scalp.generate_signals(df_1h)
    if signal_df.empty:
        return

    latest = signal_df.iloc[-1]
    signal = latest.get("signal", 0)

    # 일봉 MA200 기준 추세 판단 (단타 방향 게이트)
    daily_ma200   = df_1d["close"].rolling(200).mean().iloc[-1]
    daily_uptrend = price > daily_ma200
    daily_downtrend = price < daily_ma200
    # MA200 ±2% 이내 (횡보 구간) → 단타 스킵
    if abs(price - daily_ma200) / daily_ma200 < 0.02:
        logger.info(f"[{ex_name}][{market}] 단타 스킵: MA200 횡보 구간")
        return

    trend_str = "▲상승" if daily_uptrend else "▼하락"
    logger.info(
        f"[{ex_name}][{market}] 단타 | 현재가: {price:,.2f} | 일봉추세: {trend_str} | 신호: {int(signal)}"
    )

    cash        = ex.get_balance_quote()
    scalp_cash  = cash * _SCALP_RATIO
    weight      = ASSET_WEIGHTS.get(market, 0.5)
    invest      = rm.calc_position_size(scalp_cash, price) * weight

    # ── 단타 롱 진입 (현물, 상승장 전용) ─────────────
    if signal == 1 and daily_uptrend and not lp["held"]:
        if invest > 0 and ex.buy_market_order(market, invest):
            vol = invest / price
            lp.update({"held": True, "entry_price": price, "volume": vol,
                        "entry_time": datetime.now()})
            notifier.notify_buy(f"{ex_name}/{market} 단타롱", price, invest)
            logger.info(f"[{ex_name}][{market}] 단타 롱 진입 | {invest:,.2f} USDT")

    # ── 단타 숏 진입 (선물, 하락장 전용 + 스윙숏 미보유) ──
    elif signal == 2 and daily_downtrend and not sp["held"]:
        swing_sp = short_positions[ex_name][market]
        if swing_sp["held"]:
            logger.info(f"[{ex_name}][{market}] 단타 숏 스킵 — 스윙 숏 보유 중")
            return
        if invest > 0 and isinstance(ex, OKXExchange):
            if ex.open_short(market, invest):
                vol = invest / price
                sp.update({"held": True, "entry_price": price, "volume": vol,
                            "entry_time": datetime.now()})
                notifier.notify_buy(f"{ex_name}/{market} 단타숏↓", price, invest)
                logger.info(f"[{ex_name}][{market}] 단타 숏 진입 | {invest:,.2f} USDT")


def send_daily_report():
    lines = [f"📊 일일 리포트  {datetime.now().strftime('%Y-%m-%d')}"]
    for ex_name, ex in EXCHANGES.items():
        equity = calc_total_equity(ex_name)
        lines.append(f"\n[{ex_name}] 총 자산: {equity:,.2f} {ex.quote_currency}")
        for market in MARKETS:
            p = ex.get_current_price(market)
            lp = long_positions[ex_name][market]
            sp = short_positions[ex_name][market]
            if lp["held"] and p:
                pnl = (p - lp["entry_price"]) / lp["entry_price"] * 100
                lines.append(f"  {market} 스윙롱: {pnl:+.2f}%")
            if sp["held"] and p:
                pnl = (sp["entry_price"] - p) / sp["entry_price"] * 100
                lines.append(f"  {market} 스윙숏: {pnl:+.2f}%")
            # 단타 포지션
            slp = scalp_long_positions.get(ex_name, {}).get(market, {})
            ssp = scalp_short_positions.get(ex_name, {}).get(market, {})
            if slp.get("held") and p:
                pnl = (p - slp["entry_price"]) / slp["entry_price"] * 100
                elapsed = ""
                if slp.get("entry_time"):
                    h = (datetime.now() - slp["entry_time"]).total_seconds() / 3600
                    elapsed = f" {h:.1f}H"
                lines.append(f"  {market} 단타롱{elapsed}: {pnl:+.2f}%")
            if ssp.get("held") and p:
                pnl = (ssp["entry_price"] - p) / ssp["entry_price"] * 100
                elapsed = ""
                if ssp.get("entry_time"):
                    h = (datetime.now() - ssp["entry_time"]).total_seconds() / 3600
                    elapsed = f" {h:.1f}H"
                lines.append(f"  {market} 단타숏{elapsed}: {pnl:+.2f}%")
    notifier.send("\n".join(lines))


# ── 스케줄 ────────────────────────────────────────────
schedule.every().hour.at(":00").do(run_strategy)           # 스윙: 매시 정각 (일봉)
schedule.every().hour.at(":30").do(run_scalp_strategy)     # 단타: 매시 30분 (1시간봉)
schedule.every().day.at("23:00").do(send_daily_report)


def main():
    ex_list = ", ".join(
        f"{n}({'실전' if not e.paper_trading else '페이퍼'})"
        for n, e in EXCHANGES.items()
    )
    logger.info("=" * 60)
    logger.info(f"  퀀트 자동매매 봇 시작")
    logger.info(f"  거래소: {ex_list}")
    logger.info(f"  전략 (KRW 스윙): {strategy_long.name}")
    logger.info(f"  전략 (OKX 스윙): {strategy_longshort.name}")
    logger.info(f"  전략 (OKX 단타): {strategy_scalp.name} | 1H | SL:{SC_CFG.get('stop_loss_pct',0)*100:.1f}% TP:{SC_CFG.get('take_profit_pct',0)*100:.1f}%")
    logger.info(f"  단타 자본 비중: OKX 잔고의 {int(_SCALP_RATIO*100)}%")
    logger.info(f"  종목: BTC 70% / ETH 30%")
    logger.info("=" * 60)

    scalp_status = f"단타({'ON' if SC_CFG.get('enabled') else 'OFF'})"
    notifier.send(f"🤖 봇 시작\n거래소: {ex_list}\n스윙: 변동성 돌파+MA200\n{scalp_status}: 1H 변동성 돌파+일봉추세")

    for ex_name in EXCHANGES:
        sync_positions(ex_name)

    run_strategy()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
