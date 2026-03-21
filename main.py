"""
퀀트 자동매매 봇 — 멀티 거래소 + 스윙/단타 복합 전략
─────────────────────────────────────────────────────────
업비트 / 빗썸 : 현물 롱 스윙 (MA200 위에서만 매수)
OKX 스윙      : 선물 롱 + 선물 숏 (MA200 기준 양방향, 일봉) ← 선물 전용
OKX 단타      : 선물 롱 + 선물 숏 (일봉 추세 + 1H 신호 + Top Trader 혼합)
자산 배분     : BTC 70% / ETH 30%
단타 자본     : OKX 잔고의 30% 별도 운용
단타 레버리지 : 확신도 1~5 → 1x~5x 동적 적용
Top Trader    : 순추종(55~75% 쏠림) + 역추종(>75% 극단 쏠림) 혼합
"""
import os
import sys
import time
import logging
import threading
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
from macro.indicators import calc_macro_signal
from macro.fetchers import fetch_fear_greed, fetch_btc_dominance, fetch_market_caps

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

MARKETS       = config["markets"]
ASSET_WEIGHTS = config.get("asset_weights", {})   # 동적 갱신 — 초기 빈 dict
EX_CFG        = config["exchanges"]
OKX_CFG       = config.get("okx_futures", {})
SC_CFG        = config.get("scalp_trading", {})
_EX_MARKETS   = config.get("exchange_markets", {})  # 거래소별 종목 오버라이드
_MACRO_CFG    = config.get("macro_factors", {})
_MACRO_ENABLED   = _MACRO_CFG.get("enabled", False)
_FINNHUB_TOKEN   = _MACRO_CFG.get("finnhub_token", "")
_AV_KEY          = _MACRO_CFG.get("alpha_vantage_key", "")
_CGECKO_IDS      = _MACRO_CFG.get("coingecko_ids", {})  # ticker → CoinGecko ID
_SCALP_TOTAL_RATIO = SC_CFG.get("total_capital_ratio", SC_CFG.get("capital_ratio", 0.075) * 4)

# 시가총액 가중치 상한 (BTC 최대 30%, ETH 최대 20%, 알트 각 8%)
_WEIGHT_CAP = {
    "BTC": OKX_CFG.get("weight_cap_btc", 0.30),
    "ETH": OKX_CFG.get("weight_cap_eth", 0.20),
    "_alt": OKX_CFG.get("weight_cap_alt", 0.08),
}

def get_markets(ex_name: str) -> list:
    """거래소별 거래 종목 반환 (오버라이드 없으면 전체 MARKETS)"""
    return _EX_MARKETS.get(ex_name, MARKETS)

def refresh_asset_weights():
    """CoinGecko 시가총액 기반 ASSET_WEIGHTS 동적 갱신 (매시 run_strategy 시작 시 호출)."""
    global ASSET_WEIGHTS
    okx_markets = get_markets("okx")
    coins = [m.replace("KRW-", "") for m in okx_markets]
    ids   = [_CGECKO_IDS.get(c, c.lower()) for c in coins]

    caps = fetch_market_caps(ids)
    if not caps:
        if not ASSET_WEIGHTS:
            n = len(okx_markets)
            ASSET_WEIGHTS = {m: round(1.0 / n, 4) for m in okx_markets}
            logger.info(f"[자본배분] CoinGecko 실패 → 동일 비중 {1/n:.1%} 적용")
        return

    total = sum(caps.get(c, 0) for c in coins)
    if total == 0:
        return

    # 1차: 시가총액 비율 계산
    weights = {}
    for market in okx_markets:
        coin = market.replace("KRW-", "")
        weights[market] = caps.get(coin, 0) / total

    # 반복적 상한 적용: 정규화 → 캡 → 정규화 반복 (수렴 보장)
    for _ in range(20):
        total_w = sum(weights.values())
        if total_w == 0:
            break
        weights = {m: v / total_w for m, v in weights.items()}  # 정규화
        violated = False
        for market in list(weights.keys()):
            coin = market.replace("KRW-", "")
            cap = _WEIGHT_CAP.get(coin, _WEIGHT_CAP["_alt"])
            if weights[market] > cap:
                weights[market] = cap
                violated = True
        if not violated:
            break

    # 최종 정규화 (부동소수점 오차 보정)
    total2 = sum(weights.values())
    ASSET_WEIGHTS = {m: round(v / total2, 4) for m, v in weights.items()}

    top5 = sorted(ASSET_WEIGHTS.items(), key=lambda x: -x[1])[:5]
    logger.info("[자본배분] ASSET_WEIGHTS 갱신: " +
                ", ".join(f"{m.replace('KRW-','')}:{v:.1%}" for m, v in top5) + " ...")

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
        markets=_EX_MARKETS.get("okx", MARKETS),   # 20종목 레버리지 초기 설정
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
# OKX: 롱 + 숏 (Supertrend/ATR SL/TP 등 신규 지표 포함)
strategy_longshort = VolatilityBreakoutStrategy(
    k=_vb["k"], ma_period=200, use_short=True,
    volume_lookback=_vb.get("volume_lookback", 20),
    volume_multiplier=_vb.get("volume_multiplier", 1.5),
    vp_lookback=_vb.get("vp_lookback", 20),
    vp_bins=_vb.get("vp_bins", 50),
    fib_lookback=_vb.get("fib_lookback", 50),
    short_consec=_vb.get("short_consec", 2),
    use_supertrend=_vb.get("use_supertrend", False),
    supertrend_period=_vb.get("supertrend_period", 7),
    supertrend_mult=_vb.get("supertrend_multiplier", 3.0),
    use_macd_filter=_vb.get("use_macd_filter", False),
    use_atr_sl=_vb.get("use_atr_sl", False),
    atr_period=_vb.get("atr_period", 14),
    atr_sl_mult=_vb.get("atr_sl_multiplier", 1.5),
    atr_tp_mult=_vb.get("atr_tp_multiplier", 3.0),
    use_rsi_div=_vb.get("use_rsi_divergence", False),
    use_bb_squeeze=_vb.get("use_bb_squeeze", False),
)
# OKX 단타: 1시간봉 기반, 타이트한 파라미터
strategy_scalp = VolatilityBreakoutStrategy(
    k=SC_CFG.get("k", 0.35),
    ma_period=SC_CFG.get("ma_period", 50),
    use_short=True,
    volume_lookback=SC_CFG.get("volume_lookback", 20),
    volume_multiplier=SC_CFG.get("volume_multiplier", 2.0),
    vp_lookback=SC_CFG.get("vp_lookback", 20),
    vp_bins=SC_CFG.get("vp_bins", 30),
    fib_lookback=SC_CFG.get("fib_lookback", 30),
    short_consec=SC_CFG.get("short_consec", 1),
    use_supertrend=SC_CFG.get("use_supertrend", True),
    supertrend_period=SC_CFG.get("supertrend_period", 7),
    supertrend_mult=SC_CFG.get("supertrend_multiplier", 3.0),
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

_SCALP_RATIO = _SCALP_TOTAL_RATIO  # 전체 단타 자본 비중 (기본 30%)

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
    ex_mkts = get_markets(ex_name)
    long_positions[ex_name]  = {
        m: {"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None}
        for m in ex_mkts
    }
    short_positions[ex_name] = {
        m: {"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None}
        for m in ex_mkts
    }

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
            m: {"held": False, "entry_price": 0.0, "volume": 0.0,
                "entry_time": None, "leverage": 1}
            for m in ex_mkts
        }
        scalp_short_positions[ex_name] = {
            m: {"held": False, "entry_price": 0.0, "volume": 0.0,
                "entry_time": None, "leverage": 1}
            for m in ex_mkts
        }


# ── 유틸 ──────────────────────────────────────────────

def calc_total_equity(ex_name: str) -> float:
    ex = EXCHANGES[ex_name]
    total = ex.get_balance_quote()
    if ex_name != "okx":   # OKX 선물 계정은 코인 잔고 없음 (불필요한 API 호출 제거)
        for market in MARKETS:
            vol = ex.get_balance_coin(market)
            if vol > 0:
                price = ex.get_current_price(market)
                if price:
                    total += vol * price
    return total


def sync_positions(ex_name: str):
    """실제 OKX 포지션 → 봇 메모리 동기화 (시작 시 및 매 사이클 호출)"""
    ex = EXCHANGES[ex_name]

    if ex_name == "okx":
        # OKX: 선물 롱/숏 모두 선물 포지션에서 동기화 (전체 OKX 종목 대상)
        if isinstance(ex, OKXExchange):
            for market in get_markets(ex_name):
                p = ex.get_futures_position(market)
                if p["side"] == "long" and p["volume"] > 0:
                    long_positions[ex_name][market].update(
                        {"held": True, "entry_price": p["entry_price"], "volume": p["volume"]}
                    )
                    logger.info(f"[{ex_name}] 선물롱 동기화 | {market} {p['volume']:.6f}개 @ {p['entry_price']:,.2f}")
                elif p["side"] == "short" and p["volume"] > 0:
                    short_positions[ex_name][market].update(
                        {"held": True, "entry_price": p["entry_price"], "volume": p["volume"]}
                    )
                    logger.info(f"[{ex_name}] 선물숏 동기화 | {market} {p['volume']:.6f}개 @ {p['entry_price']:,.2f}")
                else:
                    # 포지션 없으면 메모리도 리셋 (불일치 방지)
                    prev_long  = long_positions[ex_name][market]["held"]
                    prev_short = short_positions[ex_name][market]["held"]
                    if prev_long or prev_short:
                        logger.info(f"[{ex_name}] 포지션 없음 확인, 메모리 리셋 | {market}")
                    long_positions[ex_name][market].update(
                        {"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None}
                    )
                    short_positions[ex_name][market].update(
                        {"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None}
                    )
    else:
        # 업비트/빗썸: 현물 롱 동기화 (거래소별 설정 종목)
        for market in get_markets(ex_name):
            vol = ex.get_balance_coin(market)
            if vol > 0.000001:
                avg = ex.get_avg_buy_price(market)
                if avg == 0:
                    avg = ex.get_current_price(market) or 0
                long_positions[ex_name][market].update(
                    {"held": True, "entry_price": avg, "volume": vol}
                )
                logger.info(f"[{ex_name}] 현물롱 동기화 | {market} {vol:.6f}개 @ {avg:,.2f}")


# ── 메인 전략 루프 ─────────────────────────────────────

def run_strategy():
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    logger.info(f"{'='*55}")
    logger.info(f"전략 실행 | {now}")
    refresh_asset_weights()   # 매시 정각 시가총액 기반 자본배분 갱신

    # 거래소별 OHLCV 수집 (OKX는 USDT 기준, 나머지는 KRW 업비트 기준)
    # 업비트/빗썸 전체 종목 합집합으로 캔들 수집
    krw_markets = list(dict.fromkeys(
        m for ex_name in EXCHANGES if ex_name != "okx"
        for m in get_markets(ex_name)
    ))
    upbit_ohlcv = {}
    for market in krw_markets:
        df = fetch_ohlcv(market, interval="day", count=210)
        if df is not None and not df.empty:
            upbit_ohlcv[market] = df
        time.sleep(0.15)   # Upbit API rate limit 준수 (초당 10건)

    for ex_name, ex in EXCHANGES.items():
        equity = calc_total_equity(ex_name)
        risk_managers[ex_name].reset_daily(equity)
        logger.info(f"[{ex_name}] 총 자산: {equity:,.2f} {ex.quote_currency}")

        # OKX: 매 사이클 포지션 재동기화 (메모리-실제 불일치 방지)
        if ex_name == "okx" and isinstance(ex, OKXExchange):
            sync_positions(ex_name)

        strat    = strategy_longshort if ex_name == "okx" else strategy_long
        ex_mkts  = get_markets(ex_name)

        # OKX: ccxt로 USDT 가격 OHLCV 수집
        if ex_name == "okx" and hasattr(ex, "fetch_ohlcv"):
            ohlcv_cache = {}
            for market in ex_mkts:
                df = ex.fetch_ohlcv(market, interval="day", count=210)
                if df is not None and not df.empty:
                    ohlcv_cache[market] = df
        else:
            ohlcv_cache = upbit_ohlcv

        for market in ex_mkts:
            if market not in ohlcv_cache:
                continue
            _process(ex_name, market, ohlcv_cache[market], strat, equity)


# ── 동시 실행 방지 Lock ────────────────────────────────
_monitor_lock = threading.Lock()   # run_price_monitor 전용
_scalp_lock   = threading.Lock()   # run_scalp_strategy 전용


# ── SL/TP 공통 헬퍼 함수 ───────────────────────────────
# run_price_monitor()와 _process()/_process_scalp() 양쪽에서 재사용.
# True 반환 = 청산 실행됨 (호출 측에서 return 처리)

def _check_and_close_long(ex_name: str, ex, rm, market: str, price: float, lp: dict) -> bool:
    """스윙 롱 Break-even SL + SL/TP 체크. 청산 시 True."""
    _close = (ex.close_long_futures if ex_name == "okx" and isinstance(ex, OKXExchange)
              else ex.sell_market_order)
    # Break-even SL
    lp_profit_pct = (price - lp["entry_price"]) / lp["entry_price"]
    lp_sl_now = lp.get("atr_sl")
    if lp_profit_pct >= abs(rm.stop_loss_pct) and (lp_sl_now is None or lp_sl_now < lp["entry_price"]):
        lp["atr_sl"] = lp["entry_price"]
        logger.info(f"[{ex_name}][{market}] 스윙 롱 Break-even SL 적용 | 진입가: {lp['entry_price']:.2f} (+{lp_profit_pct*100:.1f}%)")
    # SL
    if rm.should_stop_loss(lp["entry_price"], price, stop_price=lp.get("atr_sl")):
        if _close(market, lp["volume"]):
            entry = lp["entry_price"]
            lp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None})
            notifier.notify_stop_loss(f"{ex_name}/{market}", entry, price)
            logger.warning(f"[{ex_name}][{market}] 롱 손절")
        return True
    # TP
    if rm.should_take_profit(lp["entry_price"], price, take_price=lp.get("atr_tp")):
        if _close(market, lp["volume"]):
            entry = lp["entry_price"]
            lp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None})
            notifier.notify_take_profit(f"{ex_name}/{market}", entry, price)
            logger.info(f"[{ex_name}][{market}] 롱 익절")
        return True
    return False


def _check_and_close_short(ex_name: str, ex, rm, market: str, price: float, sp: dict) -> bool:
    """스윙 숏 Break-even SL + SL/TP 체크. 청산 시 True."""
    short_stop   = sp.get("atr_sl") or sp["entry_price"] * (1 + abs(rm.stop_loss_pct))
    short_profit = sp.get("atr_tp") or sp["entry_price"] * (1 - rm.take_profit_pct)
    # Break-even SL
    sp_profit_pct = (sp["entry_price"] - price) / sp["entry_price"]
    if sp_profit_pct >= abs(rm.stop_loss_pct) and short_stop > sp["entry_price"]:
        sp["atr_sl"] = sp["entry_price"]
        short_stop = sp["entry_price"]
        logger.info(f"[{ex_name}][{market}] 스윙 숏 Break-even SL 적용 | 진입가: {sp['entry_price']:.2f} (+{sp_profit_pct*100:.1f}%)")
    # SL
    if price >= short_stop:
        if ex.close_short(market, sp["volume"]):
            loss_pct = (price - sp["entry_price"]) / sp["entry_price"] * 100
            entry = sp["entry_price"]
            sp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None})
            notifier.notify_stop_loss(f"{ex_name}/{market} 숏", entry, price)
            logger.warning(f"[{ex_name}][{market}] 숏 손절 ({loss_pct:+.2f}%)")
        return True
    # TP
    if price <= short_profit:
        if ex.close_short(market, sp["volume"]):
            gain_pct = (sp["entry_price"] - price) / sp["entry_price"] * 100
            entry = sp["entry_price"]
            sp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None})
            notifier.notify_take_profit(f"{ex_name}/{market} 숏", entry, price)
            logger.info(f"[{ex_name}][{market}] 숏 익절 (+{gain_pct:.2f}%)")
        return True
    return False


def _check_and_close_scalp_long(ex_name: str, ex, rm, market: str, price: float, lp: dict) -> bool:
    """단타 롱 시간 손절 + Break-even SL + SL/TP 체크. 청산 시 True."""
    MAX_H = SC_CFG.get("max_holding_hours", 6)
    # 시간 손절
    if lp["entry_time"]:
        held_h = (datetime.now() - lp["entry_time"]).total_seconds() / 3600
        if held_h >= MAX_H:
            if isinstance(ex, OKXExchange) and ex.close_long_futures(market, lp["volume"]):
                pnl = (price - lp["entry_price"]) / lp["entry_price"] * 100
                lp.update({"held": False, "entry_price": 0.0, "volume": 0.0,
                            "entry_time": None, "leverage": 1, "breakeven_sl": None})
                notifier.notify_sell(f"{ex_name}/{market} 단타롱(시간)", price, lp["volume"], pnl)
                logger.info(f"[{ex_name}][{market}] 단타 롱 시간청산 {MAX_H}H | {pnl:+.2f}%")
            return True
    # Break-even SL
    scalp_lp_profit_pct = (price - lp["entry_price"]) / lp["entry_price"]
    if scalp_lp_profit_pct >= abs(rm.stop_loss_pct) and lp.get("breakeven_sl") is None:
        lp["breakeven_sl"] = lp["entry_price"]
        logger.info(f"[{ex_name}][{market}] 단타 롱 Break-even SL 적용 | 진입가: {lp['entry_price']:.2f} (+{scalp_lp_profit_pct*100:.1f}%)")
    # SL
    if rm.should_stop_loss(lp["entry_price"], price, stop_price=lp.get("breakeven_sl")):
        if isinstance(ex, OKXExchange) and ex.close_long_futures(market, lp["volume"]):
            entry = lp["entry_price"]
            lp.update({"held": False, "entry_price": 0.0, "volume": 0.0,
                        "entry_time": None, "leverage": 1, "breakeven_sl": None})
            notifier.notify_stop_loss(f"{ex_name}/{market} 단타롱", entry, price)
            logger.warning(f"[{ex_name}][{market}] 단타 롱 손절")
        return True
    # TP
    if rm.should_take_profit(lp["entry_price"], price):
        if isinstance(ex, OKXExchange) and ex.close_long_futures(market, lp["volume"]):
            pnl = (price - lp["entry_price"]) / lp["entry_price"] * 100
            entry = lp["entry_price"]
            lp.update({"held": False, "entry_price": 0.0, "volume": 0.0,
                        "entry_time": None, "leverage": 1, "breakeven_sl": None})
            notifier.notify_take_profit(f"{ex_name}/{market} 단타롱", entry, price)
            logger.info(f"[{ex_name}][{market}] 단타 롱 익절 (+{pnl:.2f}%)")
        return True
    return False


def _check_and_close_scalp_short(ex_name: str, ex, rm, market: str, price: float, sp: dict) -> bool:
    """단타 숏 시간 손절 + Break-even SL + SL/TP 체크. 청산 시 True."""
    MAX_H = SC_CFG.get("max_holding_hours", 6)
    # 시간 손절
    if sp["entry_time"]:
        held_h = (datetime.now() - sp["entry_time"]).total_seconds() / 3600
        if held_h >= MAX_H:
            if ex.close_short(market, sp["volume"]):
                pnl = (sp["entry_price"] - price) / sp["entry_price"] * 100
                sp.update({"held": False, "entry_price": 0.0, "volume": 0.0,
                            "entry_time": None, "leverage": 1})
                notifier.notify_sell(f"{ex_name}/{market} 단타숏(시간)", price, sp["volume"], pnl)
                logger.info(f"[{ex_name}][{market}] 단타 숏 시간청산 {MAX_H}H | {pnl:+.2f}%")
            return True
    # Break-even SL
    short_stop   = sp["entry_price"] * (1 + abs(rm.stop_loss_pct))
    short_profit = sp["entry_price"] * (1 - rm.take_profit_pct)
    scalp_sp_profit_pct = (sp["entry_price"] - price) / sp["entry_price"]
    if scalp_sp_profit_pct >= abs(rm.stop_loss_pct) and short_stop > sp["entry_price"]:
        short_stop = sp["entry_price"]
        logger.info(f"[{ex_name}][{market}] 단타 숏 Break-even SL 적용 | 진입가: {sp['entry_price']:.2f} (+{scalp_sp_profit_pct*100:.1f}%)")
    # SL
    if price >= short_stop:
        if ex.close_short(market, sp["volume"]):
            entry = sp["entry_price"]
            sp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None})
            notifier.notify_stop_loss(f"{ex_name}/{market} 단타숏", entry, price)
            logger.warning(f"[{ex_name}][{market}] 단타 숏 손절")
        return True
    # TP
    if price <= short_profit:
        if ex.close_short(market, sp["volume"]):
            pnl = (sp["entry_price"] - price) / sp["entry_price"] * 100
            entry = sp["entry_price"]
            sp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None})
            notifier.notify_take_profit(f"{ex_name}/{market} 단타숏", entry, price)
            logger.info(f"[{ex_name}][{market}] 단타 숏 익절 (+{pnl:.2f}%)")
        return True
    return False


def _check_volume_exit_warning(df) -> bool:
    """
    EmperorBTC Volume Exit: 진입 후 거래량이 14봉 평균 대비 30% 이하로 지속되면
    청산 대기 경고 신호. 자동 청산 아님 — 로그 경고만 발생.
    """
    try:
        avg_vol = df["volume"].rolling(14).mean().iloc[-1]
        recent_avg = df["volume"].tail(3).mean()
        return recent_avg < avg_vol * 0.7
    except Exception:
        return False


def _process(ex_name: str, market: str, df, strat: VolatilityBreakoutStrategy, equity: float = None):
    ex  = EXCHANGES[ex_name]
    rm  = risk_managers[ex_name]
    lp  = long_positions[ex_name][market]   # 롱 포지션
    sp  = short_positions[ex_name][market]  # 숏 포지션

    if equity is None:
        equity = calc_total_equity(ex_name)
    if not rm.is_trading_allowed(equity):
        logger.warning(f"[{ex_name}][{market}] 거래 중단 상태")
        return

    price = ex.get_current_price(market)
    if not price:
        return

    # ── 롱 손절/익절 ──────────────────────────────────
    if lp["held"] and lp["entry_price"] > 0:
        if _check_and_close_long(ex_name, ex, rm, market, price, lp):
            return

    # ── 숏 손절/익절 (OKX 전용) ───────────────────────
    if ex_name == "okx" and sp["held"] and sp["entry_price"] > 0:
        if _check_and_close_short(ex_name, ex, rm, market, price, sp):
            return

    # ── 전략 신호 생성 ────────────────────────────────
    signal_df = strat.generate_signals(df)
    if signal_df.empty:
        return

    # Volume Exit Warning (EmperorBTC: 보유 중 거래량 저하 경고)
    if lp["held"] or sp["held"]:
        if _check_volume_exit_warning(signal_df):
            logger.warning(f"[{ex_name}][{market}] [VolumeExit] 거래량 저하 지속, 청산 고려")

    latest  = signal_df.iloc[-1]
    signal  = latest.get("signal", 0)
    ma200   = latest.get("ma200", 0)
    trend   = "▲상승" if price > ma200 else "▼하락"

    # ATR 기반 동적 SL/TP 추출 (없으면 None → RiskManager 비율 기준 사용)
    import math as _math
    def _safe_float(v):
        try:
            f = float(v)
            return None if _math.isnan(f) or f <= 0 else f
        except (TypeError, ValueError):
            return None

    atr_sl_long  = _safe_float(latest.get("atr_sl_long"))
    atr_tp_long  = _safe_float(latest.get("atr_tp_long"))
    atr_sl_short = _safe_float(latest.get("atr_sl_short"))
    atr_tp_short = _safe_float(latest.get("atr_tp_short"))

    logger.info(
        f"[{ex_name}][{market}] 현재가: {price:,.2f} | MA200: {ma200:,.2f} "
        f"| {trend} | 신호: {int(signal)}"
    )

    # ── 스윙 숏 추세 반전 청산 ────────────────────────
    # 진입 조건(MA200 하향 + EMA 하향)이 무너지면 즉시 청산
    if ex_name == "okx" and sp["held"] and sp["entry_price"] > 0:
        ema_20 = float(latest.get("ema_20", 0) or 0)
        ema_55 = float(latest.get("ema_55", 0) or 0)
        ema_reversed = ema_20 > 0 and ema_55 > 0 and ema_20 > ema_55   # EMA 상향 재정렬
        ma_reversed  = ma200 > 0 and price > ma200                       # MA200 위 복귀
        if ema_reversed or ma_reversed:
            reason = "EMA상향재정렬" if ema_reversed else "MA200위복귀"
            if ex.close_short(market, sp["volume"]):
                pnl = (sp["entry_price"] - price) / sp["entry_price"] * 100
                sp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None})
                notifier.notify_sell(f"{ex_name}/{market} 스윙숏({reason})", price, sp["volume"], pnl)
                logger.info(f"[{ex_name}][{market}] 스윙 숏 추세반전 청산 [{reason}] | {pnl:+.2f}%")
            return

    cash   = ex.get_balance_quote()
    n_mkts = len(get_markets(ex_name))
    weight = ASSET_WEIGHTS.get(market, round(1.0 / n_mkts, 4))
    invest = rm.calc_position_size(cash * weight, price)

    # ── 롱 진입 (signal=1) ────────────────────────────
    if signal == 1 and not lp["held"]:
        # OKX: 선물 롱 / 나머지: 현물 매수
        ok = (ex.open_long_futures(market, invest, leverage=1)
              if ex_name == "okx" and isinstance(ex, OKXExchange)
              else ex.buy_market_order(market, invest))
        if invest > 0 and ok:
            vol = invest / price
            lp.update({
                "held": True, "entry_price": price, "volume": vol,
                "atr_sl": atr_sl_long, "atr_tp": atr_tp_long,
            })
            tag = "선물롱" if ex_name == "okx" else "현물롱"
            notifier.notify_buy(f"{ex_name}/{market} {tag}", price, invest)
            logger.info(f"[{ex_name}][{market}] {tag} 진입 | {invest:,.2f} {ex.quote_currency}")

    # ── 숏 진입 (signal=2, OKX 전용) ─────────────────
    elif signal == 2 and ex_name == "okx" and not sp["held"]:
        if invest > 0 and isinstance(ex, OKXExchange):
            if ex.open_short(market, invest, leverage=1):
                vol = invest / price
                sp.update({
                    "held": True, "entry_price": price, "volume": vol,
                    "atr_sl": atr_sl_short, "atr_tp": atr_tp_short,
                })
                notifier.notify_buy(f"{ex_name}/{market} 스윙숏↓", price, invest)
                logger.info(f"[{ex_name}][{market}] 스윙 숏 진입 | {invest:,.2f} USDT")

    # ── 롱 청산 (signal=-1) ───────────────────────────
    elif signal == -1 and lp["held"]:
        _close = (ex.close_long_futures if ex_name == "okx" and isinstance(ex, OKXExchange)
                  else ex.sell_market_order)
        if _close(market, lp["volume"]):
            pnl = (price - lp["entry_price"]) / lp["entry_price"] * 100
            notifier.notify_sell(f"{ex_name}/{market}", price, lp["volume"], pnl)
            lp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "atr_sl": None, "atr_tp": None})
            logger.info(f"[{ex_name}][{market}] 롱 청산 | {pnl:+.2f}%")


# ── Top Trader 혼합 신호 (순추종 + 역추종) ────────────

def calc_top_trader_signal(long_ratio: float, direction: str) -> tuple:
    """
    Top Traders 롱 비율에 따른 확신도 조정 및 차단 여부.

    비율 구간    │ 롱 신호       │ 숏 신호
    ────────────┼─────────────┼────────────
    > 75%       │ 차단(역추종) │ +2 (역추종)
    55 ~ 75%    │ +1 (순추종)  │  0
    45 ~ 55%    │  0 (중립)    │  0
    25 ~ 45%    │  0           │ +1 (순추종)
    < 25%       │ +2 (역추종)  │ 차단(역추종)

    Returns: (confidence_delta, blocked, reason_str)
    """
    short_ratio = 1.0 - long_ratio

    if direction == "long":
        if long_ratio > 0.75:
            return (-1, True,  f"롱 극단쏠림({long_ratio:.0%}) → 역추종 차단")
        elif long_ratio >= 0.55:
            return (+1, False, f"롱 완만우위({long_ratio:.0%}) → 순추종 +1")
        elif long_ratio < 0.25:
            return (+2, False, f"숏 극단쏠림({long_ratio:.0%}) → 역추종 롱강화 +2")
        else:
            return (0,  False, f"중립({long_ratio:.0%})")
    else:
        if short_ratio > 0.75:
            return (-1, True,  f"숏 극단쏠림({short_ratio:.0%}) → 역추종 차단")
        elif short_ratio >= 0.55:
            return (+1, False, f"숏 완만우위({short_ratio:.0%}) → 순추종 +1")
        elif short_ratio < 0.25:
            return (+2, False, f"롱 극단쏠림({short_ratio:.0%}) → 역추종 숏강화 +2")
        else:
            return (0,  False, f"중립({short_ratio:.0%})")


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
    ex_mkts  = get_markets(ex_name)
    ohlcv_1h = {}
    ohlcv_1d = {}
    for market in ex_mkts:
        df_1h = ex.fetch_ohlcv(market, interval=SC_CFG.get("timeframe", "minute60"), count=210)
        if df_1h is not None and not df_1h.empty:
            ohlcv_1h[market] = df_1h
        df_1d = ex.fetch_ohlcv(market, interval="day", count=210)
        if df_1d is not None and not df_1d.empty:
            ohlcv_1d[market] = df_1d

    equity = calc_total_equity(ex_name)
    rm.reset_daily(equity * _SCALP_RATIO)

    for market in ex_mkts:
        if market not in ohlcv_1h or market not in ohlcv_1d:
            continue
        _process_scalp(ex_name, market, ohlcv_1h[market], ohlcv_1d[market], equity)


def _process_scalp(ex_name: str, market: str, df_1h, df_1d, equity: float = None):
    ex  = EXCHANGES[ex_name]
    rm  = scalp_risk_managers[ex_name]
    lp  = scalp_long_positions[ex_name][market]
    sp  = scalp_short_positions[ex_name][market]

    if equity is None:
        equity = calc_total_equity(ex_name)
    if not rm.is_trading_allowed(equity * _SCALP_RATIO):
        logger.warning(f"[{ex_name}][{market}] 단타 거래 중단 상태")
        return

    price = ex.get_current_price(market)
    if not price:
        return

    MAX_LEV   = SC_CFG.get("max_leverage", 5)

    # ── 시간/SL/TP 손절·익절 (헬퍼 위임) ─────────────
    if lp["held"] and lp["entry_price"] > 0:
        if _check_and_close_scalp_long(ex_name, ex, rm, market, price, lp):
            return

    if sp["held"] and sp["entry_price"] > 0:
        if _check_and_close_scalp_short(ex_name, ex, rm, market, price, sp):
            return

    # ── 신호 생성 ─────────────────────────────────────
    signal_df = strategy_scalp.generate_signals(df_1h)
    if signal_df.empty:
        return

    # Volume Exit Warning (EmperorBTC: 단타 보유 중 거래량 저하 경고)
    if lp["held"] or sp["held"]:
        if _check_volume_exit_warning(signal_df):
            logger.warning(f"[{ex_name}][{market}] [VolumeExit] 단타 거래량 저하, 청산 고려")

    latest     = signal_df.iloc[-1]
    signal     = latest.get("signal", 0)
    confidence = int(latest.get("confidence", 1))

    # ── 단타 신호반전 청산 ────────────────────────────
    # 보유 중 반대 신호가 나오면 즉시 청산 (기회비용 최소화)
    if lp["held"] and lp["entry_price"] > 0 and signal == 2:
        if isinstance(ex, OKXExchange) and ex.close_long_futures(market, lp["volume"]):
            pnl = (price - lp["entry_price"]) / lp["entry_price"] * 100
            lp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None, "leverage": 1, "breakeven_sl": None})
            notifier.notify_sell(f"{ex_name}/{market} 단타롱(신호반전↓)", price, lp["volume"], pnl)
            logger.info(f"[{ex_name}][{market}] 단타 롱 신호반전 청산 | {pnl:+.2f}%")
        return

    if sp["held"] and sp["entry_price"] > 0 and signal == 1:
        if ex.close_short(market, sp["volume"]):
            pnl = (sp["entry_price"] - price) / sp["entry_price"] * 100
            sp.update({"held": False, "entry_price": 0.0, "volume": 0.0, "entry_time": None, "leverage": 1})
            notifier.notify_sell(f"{ex_name}/{market} 단타숏(신호반전↑)", price, sp["volume"], pnl)
            logger.info(f"[{ex_name}][{market}] 단타 숏 신호반전 청산 | {pnl:+.2f}%")
        return

    # 일봉 MA200 기준 추세 판단 (단타 방향 게이트)
    daily_ma200     = df_1d["close"].rolling(200).mean().iloc[-1]
    daily_uptrend   = price > daily_ma200
    daily_downtrend = price < daily_ma200
    if abs(price - daily_ma200) / daily_ma200 < 0.02:
        logger.info(f"[{ex_name}][{market}] 단타 스킵: MA200 횡보 구간")
        return

    # Top Trader 혼합 신호 조회 및 확신도 보정
    direction   = "long" if signal == 1 else "short" if signal == 2 else "neutral"
    top_delta, top_blocked, top_reason = (0, False, "") if direction == "neutral" else \
        calc_top_trader_signal(
            ex.fetch_top_trader_ratio(market) if isinstance(ex, OKXExchange) else 0.5,
            direction
        )

    if top_blocked:
        logger.info(f"[{ex_name}][{market}] 단타 차단 — Top Trader: {top_reason}")
        return

    # 매크로 환경 신호 (FGI, 펀딩비, L/S비율, 도미넌스, VIX, DXY)
    macro_delta, macro_blocked, macro_reason = 0, False, ""
    if _MACRO_ENABLED:
        macro_delta, macro_blocked, macro_reason = calc_macro_signal(
            direction, market,
            finnhub_token=_FINNHUB_TOKEN,
            av_key=_AV_KEY,
        )

    if macro_blocked:
        logger.info(f"[{ex_name}][{market}] 단타 차단 — Macro: {macro_reason}")
        return

    # 최종 확신도: 전략 기반 + Top Trader 보정 + 매크로 보정, 1~5 클리핑
    confidence = max(1, min(5, confidence + top_delta + macro_delta))
    leverage   = min(confidence, MAX_LEV)

    trend_str = "▲상승" if daily_uptrend else "▼하락"
    logger.info(
        f"[{ex_name}][{market}] 단타 | 현재가: {price:,.2f} | 일봉추세: {trend_str} "
        f"| 신호: {int(signal)} | TopTrader: {top_reason} | Macro: {macro_reason} "
        f"| 확신도: {confidence}/5 → {leverage}x"
    )

    cash       = ex.get_balance_quote()
    n_mkts     = len(get_markets(ex_name))
    weight     = ASSET_WEIGHTS.get(market, round(1.0 / n_mkts, 4))
    # max_position_ratio 이중 곱셈 제거: 종목별 배분액을 그대로 투자금으로 사용
    alloc      = cash * _SCALP_RATIO * weight
    invest     = alloc if alloc >= rm.min_order_amount else 0.0
    if alloc > 0 and invest == 0.0:
        logger.warning(f"[RiskManager] 투자 가능 금액 부족 (최소 {rm.min_order_amount:.0f}) | {market}: {alloc:.2f} USDT")

    # ── 단타 롱 진입 (선물, 상승장 전용) ─────────────
    if signal == 1 and daily_uptrend and not lp["held"]:
        # 스윙 숏이 열려 있으면 선물 롱 불가 (포지션 충돌)
        if short_positions[ex_name][market]["held"]:
            logger.info(f"[{ex_name}][{market}] 단타 롱 스킵 — 스윙 숏 보유 중")
            return
        if invest > 0 and isinstance(ex, OKXExchange):
            if ex.open_long_futures(market, invest, leverage=leverage):
                vol = invest / price
                lp.update({"held": True, "entry_price": price, "volume": vol,
                            "entry_time": datetime.now(), "leverage": leverage})
                notifier.notify_buy(f"{ex_name}/{market} 단타롱 {leverage}x", price, invest)
                logger.info(f"[{ex_name}][{market}] 단타 롱 진입 | {invest:,.2f} USDT | {leverage}x (확신도 {confidence}/5)")

    # ── 단타 숏 진입 (선물, 하락장 전용 + 스윙숏 미보유) ──
    elif signal == 2 and daily_downtrend and not sp["held"]:
        if short_positions[ex_name][market]["held"]:
            logger.info(f"[{ex_name}][{market}] 단타 숏 스킵 — 스윙 숏 보유 중")
            return
        if invest > 0 and isinstance(ex, OKXExchange):
            if ex.open_short(market, invest, leverage=leverage):
                vol = invest / price
                sp.update({"held": True, "entry_price": price, "volume": vol,
                            "entry_time": datetime.now(), "leverage": leverage})
                notifier.notify_buy(f"{ex_name}/{market} 단타숏↓ {leverage}x", price, invest)
                logger.info(f"[{ex_name}][{market}] 단타 숏 진입 | {invest:,.2f} USDT | {leverage}x (확신도 {confidence}/5)")


# ── Tier 1: 매분 가격 모니터링 (SL/TP 전용) ────────────

def run_price_monitor():
    """
    매 1분 실행. OHLCV 수집 없이 현재가만 조회해 SL/TP 즉시 체크.
    20종목 처리 시간 ~2초.
    """
    if not _monitor_lock.acquire(blocking=False):
        return  # 이전 실행 진행 중이면 스킵
    try:
        for ex_name, ex in EXCHANGES.items():
            rm     = risk_managers[ex_name]
            scl_rm = scalp_risk_managers.get(ex_name)
            equity = calc_total_equity(ex_name)
            if not rm.is_trading_allowed(equity):
                continue
            for market in get_markets(ex_name):
                try:
                    lp  = long_positions[ex_name][market]
                    sp  = short_positions[ex_name][market]
                    slp = scalp_long_positions[ex_name][market]  if scl_rm else None
                    ssp = scalp_short_positions[ex_name][market] if scl_rm else None

                    # 보유 포지션이 하나도 없으면 현재가 조회 자체를 스킵
                    has_pos = (lp["held"] or sp["held"] or
                               (slp and slp["held"]) or (ssp and ssp["held"]))
                    if not has_pos:
                        continue

                    price = ex.get_current_price(market)
                    if not price:
                        continue

                    if lp["held"] and lp["entry_price"] > 0:
                        if _check_and_close_long(ex_name, ex, rm, market, price, lp):
                            continue
                    if ex_name == "okx" and sp["held"] and sp["entry_price"] > 0:
                        if _check_and_close_short(ex_name, ex, rm, market, price, sp):
                            continue
                    if scl_rm:
                        if slp["held"] and slp["entry_price"] > 0:
                            _check_and_close_scalp_long(ex_name, ex, scl_rm, market, price, slp)
                        if ssp["held"] and ssp["entry_price"] > 0:
                            _check_and_close_scalp_short(ex_name, ex, scl_rm, market, price, ssp)
                except Exception as e:
                    logger.error(f"[PriceMonitor] {ex_name}/{market} 오류: {e}")
    except Exception as e:
        logger.error(f"[PriceMonitor] 오류: {e}")
    finally:
        _monitor_lock.release()


def run_scalp_strategy_safe():
    """단타 신호 생성 + 진입. 동시 실행 방지."""
    if not _scalp_lock.acquire(blocking=False):
        logger.debug("[단타] 이전 실행 중, 스킵")
        return
    try:
        run_scalp_strategy()
    finally:
        _scalp_lock.release()


def send_daily_report():
    lines = [f"📊 일일 리포트  {datetime.now().strftime('%Y-%m-%d')}"]
    if _MACRO_ENABLED:
        try:
            fgi = fetch_fear_greed()
            dom = fetch_btc_dominance()
            macro_lines = []
            if fgi:
                macro_lines.append(f"공포탐욕지수: {fgi['value']} ({fgi['classification']})")
            if dom:
                macro_lines.append(f"BTC 도미넌스: {dom:.1f}%")
            if macro_lines:
                lines.append("\n[매크로]\n" + "\n".join(macro_lines))
        except Exception:
            pass
    for ex_name, ex in EXCHANGES.items():
        equity = calc_total_equity(ex_name)
        lines.append(f"\n[{ex_name}] 총 자산: {equity:,.2f} {ex.quote_currency}")
        for market in get_markets(ex_name):
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
schedule.every().hour.at(":00").do(run_strategy)            # 스윙: 매시 정각 (일봉)
schedule.every().minute.do(run_price_monitor)               # Tier1: 매분 SL/TP 체크
schedule.every(5).minutes.do(run_scalp_strategy_safe)       # 단타: 5분마다 신호+진입
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
    okx_n = len(get_markets("okx")) if "okx" in EXCHANGES else 0
    logger.info(f"  OKX 종목: {okx_n}개 (시가총액 가중 동적 배분)")
    logger.info("=" * 60)

    scalp_status = f"단타({'ON' if SC_CFG.get('enabled') else 'OFF'})"
    notifier.send(f"🤖 봇 시작\n거래소: {ex_list}\n스윙: 변동성 돌파+MA200\n{scalp_status}: 1H 변동성 돌파+일봉추세")

    for ex_name in EXCHANGES:
        sync_positions(ex_name)

    run_strategy()

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
