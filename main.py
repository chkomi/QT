"""
퀀트 자동매매 봇 — 멀티 거래소 + 롱/숏 전략
─────────────────────────────────────────────────
업비트 / 빗썸 : 현물 롱 전략 (MA200 위에서만 매수)
OKX          : 현물 롱 + 선물 숏 전략 (MA200 기준 양방향)
자산 배분     : BTC 70% / ETH 30%
전략          : 변동성 돌파 + MA200 추세 필터
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

notifier = TelegramNotifier()

# ── 포지션 상태 ────────────────────────────────────────
# long_pos  : 현물 롱 포지션 {held, entry_price, volume}
# short_pos : 선물 숏 포지션 {held, entry_price, volume}
long_positions  = {}   # {ex_name: {market: {...}}}
short_positions = {}   # {ex_name: {market: {...}}}  ← OKX 전용
risk_managers   = {}

for ex_name, ex in EXCHANGES.items():
    initial_bal = ex.get_balance_quote()
    risk_managers[ex_name] = RiskManager(
        initial_capital=max(initial_bal, 1),
        max_position_ratio=config["trading"]["max_position_ratio"],
        stop_loss_pct=config["trading"]["stop_loss_pct"],
        take_profit_pct=config["trading"]["take_profit_pct"],
        daily_loss_limit_pct=config["trading"]["daily_loss_limit_pct"],
    )
    long_positions[ex_name]  = {m: {"held": False, "entry_price": 0.0, "volume": 0.0}
                                 for m in MARKETS}
    short_positions[ex_name] = {m: {"held": False, "entry_price": 0.0, "volume": 0.0}
                                 for m in MARKETS}


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

    # MA200을 위해 210개 캔들 수집 (캐시 활용)
    ohlcv_cache = {}
    for market in MARKETS:
        df = fetch_ohlcv(market, interval="day", count=210)
        if df is not None and not df.empty:
            ohlcv_cache[market] = df

    for ex_name, ex in EXCHANGES.items():
        equity = calc_total_equity(ex_name)
        risk_managers[ex_name].reset_daily(equity)
        logger.info(f"[{ex_name}] 총 자산: {equity:,.2f} {ex.quote_currency}")

        strat = strategy_longshort if ex_name == "okx" else strategy_long

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


def send_daily_report():
    lines = [f"📊 일일 리포트  {datetime.now().strftime('%Y-%m-%d')}"]
    for ex_name, ex in EXCHANGES.items():
        equity = calc_total_equity(ex_name)
        lines.append(f"\n[{ex_name}] 총 자산: {equity:,.2f} {ex.quote_currency}")
        for market in MARKETS:
            lp = long_positions[ex_name][market]
            sp = short_positions[ex_name][market]
            p  = ex.get_current_price(market)
            if lp["held"] and p:
                pnl = (p - lp["entry_price"]) / lp["entry_price"] * 100
                lines.append(f"  {market} 롱: {pnl:+.2f}%")
            if sp["held"] and p:
                pnl = (sp["entry_price"] - p) / sp["entry_price"] * 100
                lines.append(f"  {market} 숏: {pnl:+.2f}%")
    notifier.send("\n".join(lines))


# ── 스케줄 ────────────────────────────────────────────
schedule.every().hour.at(":00").do(run_strategy)
schedule.every().day.at("23:00").do(send_daily_report)


def main():
    ex_list = ", ".join(
        f"{n}({'실전' if not e.paper_trading else '페이퍼'})"
        for n, e in EXCHANGES.items()
    )
    logger.info("=" * 60)
    logger.info(f"  퀀트 자동매매 봇 시작")
    logger.info(f"  거래소: {ex_list}")
    logger.info(f"  전략 (KRW): {strategy_long.name}")
    logger.info(f"  전략 (OKX): {strategy_longshort.name}")
    logger.info(f"  종목: BTC 70% / ETH 30%")
    logger.info("=" * 60)

    notifier.send(f"🤖 봇 시작\n거래소: {ex_list}\n전략: 변동성 돌파+MA200 (롱/숏)")

    for ex_name in EXCHANGES:
        sync_positions(ex_name)

    run_strategy()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
