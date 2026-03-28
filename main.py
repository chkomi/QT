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
from strategies.tf_coordinator import TimeframeCoordinator, get_cached_ohlcv, TIER_INTERVAL
from strategies.mean_reversion import MeanReversionStrategy
from strategies.entry_engine import EntryEngine
from engine.upbit_exchange import UpbitExchange
from engine.bithumb_exchange import BithumbExchange
from engine.okx_exchange import OKXExchange
from engine.risk_manager import RiskManager
from engine.position_manager import PositionManager, Position
from engine.capital_allocator import CapitalAllocator
from monitor.telegram_bot import TelegramNotifier
from macro.indicators import calc_macro_signal
from macro.fetchers import fetch_fear_greed, fetch_btc_dominance, fetch_market_caps
from analysis.performance_analyzer import run_daily_report
from optimization.param_optimizer import run_weekly_optimization

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

# ── Multi-Timeframe v2 초기화 ─────────────────────────
tf_coordinator = TimeframeCoordinator(config)
pos_manager = PositionManager()
cap_allocator = CapitalAllocator(config)
entry_engine = EntryEngine(config)
_TIER_MARKETS = config.get("tier_markets", {})
_RISK_TIERS = config.get("risk_tiers", {})
_CONFLUENCE_CFG = config.get("confluence", {})
_TF_LOCK = threading.Lock()

# 7TF interval 매핑 확장
TIER_INTERVAL.update({
    "5m": "minute5",
    "30m": "minute30",
})

def get_tier_markets(ex_name: str, tier: str) -> list:
    """Tier별 거래 종목 반환."""
    return _TIER_MARKETS.get(ex_name, {}).get(tier, get_markets(ex_name))

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
                if p.get("error"):
                    # API 오류 → 현재 메모리 상태 유지 (잘못된 리셋 방지)
                    logger.warning(f"[{ex_name}] 포지션 조회 실패 — 메모리 유지 | {market}")
                    continue
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
                    # 포지션 없음이 확인된 경우만 메모리 리셋
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
                if ex_name == "okx" and hasattr(ex, "fetch_ohlcv"):
                    df = ex.fetch_ohlcv(market, interval="day", count=210)
                else:
                    df = fetch_ohlcv(market, interval="day", count=210)
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
    """단타 숏 시간 손절 + Break-even SL + SL/TP 체크. 청산 시 True.
    숏은 단타 원칙: 타이트 SL(급등 즉시 손절), 빠른 TP, 최대 2H 보유."""
    # 숏 전용 파라미터 (config: short_* 우선, 없으면 기본값)
    SHORT_SL  = abs(SC_CFG.get("short_stop_loss_pct",  -0.008))  # 0.8% 타이트
    SHORT_TP  =     SC_CFG.get("short_take_profit_pct",  0.012)  # 1.2% 빠른 익절
    MAX_H     =     SC_CFG.get("short_max_holding_hours", 2)     # 2H 최대 보유
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
    short_stop   = sp["entry_price"] * (1 + SHORT_SL)
    short_profit = sp["entry_price"] * (1 - SHORT_TP)
    scalp_sp_profit_pct = (sp["entry_price"] - price) / sp["entry_price"]
    if scalp_sp_profit_pct >= SHORT_SL and short_stop > sp["entry_price"]:
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

    latest     = signal_df.iloc[-1]
    signal     = latest.get("signal", 0)
    ma200      = latest.get("ma200", 0)
    trend      = "▲상승" if price > ma200 else "▼하락"
    sw_confidence = int(latest.get("confidence", 1))   # 스윙 전략 확신도 (1~5)

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
    # 스윙 숏: short_atr_sl_multiplier 적용 — 전략 기본값보다 타이트하게 재계산
    _strat_cfg    = config.get("strategies", {}).get("volatility_breakout", {})
    _atr_raw      = _safe_float(latest.get("atr"))
    _short_sl_mult = _strat_cfg.get("short_atr_sl_multiplier", 1.0)
    _short_tp_mult = _strat_cfg.get("short_atr_tp_multiplier", 2.0)
    if _atr_raw:
        atr_sl_short = price + _atr_raw * _short_sl_mult   # 숏 SL: 진입가 + ATR×1.0 (타이트)
        atr_tp_short = price - _atr_raw * _short_tp_mult   # 숏 TP: 진입가 - ATR×2.0 (빠른 익절)
    else:
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
            cur = ex.quote_currency
            notifier.notify_buy(f"{ex_name}/{market} {tag}", price, invest, currency=cur)
            logger.info(f"[{ex_name}][{market}] {tag} 진입 | {invest:,.2f} {cur}")

    # ── 숏 진입 (signal=2, OKX 전용) ─────────────────
    elif signal == 2 and ex_name == "okx" and not sp["held"]:
        if invest > 0 and isinstance(ex, OKXExchange):
            # 확신도에 따라 레버리지 동적화: 1→1x, 2~3→2x, 4~5→3x (최대 3x)
            sw_lev = 1 if sw_confidence <= 1 else (2 if sw_confidence <= 3 else 3)
            if ex.open_short(market, invest, leverage=sw_lev):
                vol = invest / price
                sp.update({
                    "held": True, "entry_price": price, "volume": vol,
                    "atr_sl": atr_sl_short, "atr_tp": atr_tp_short,
                })
                notifier.notify_buy(f"{ex_name}/{market} 스윙숏↓ {sw_lev}x", price, invest, currency="USDT")
                logger.info(f"[{ex_name}][{market}] 스윙 숏 진입 | {invest:,.2f} USDT | {sw_lev}x (확신도 {sw_confidence}/5)")

    # ── 업비트 RSI 과매도 반등 진입 (signal=0 이어도 허용) ─────────────
    # 하락장(MA200 하향)에서도 RSI<30 + 양봉 확인 시 소규모(50%) 반등 롱
    elif ex_name != "okx" and not lp["held"] and signal == 0:
        rsi_val    = float(latest.get("rsi", 50) or 50)
        last_close = float(df.iloc[-1]["close"])
        last_open  = float(df.iloc[-1]["open"])
        if rsi_val < 30 and last_close > last_open:
            invest_rsi = rm.calc_position_size(cash * weight * 0.5, price)
            if invest_rsi > 0 and ex.buy_market_order(market, invest_rsi):
                vol = invest_rsi / price
                lp.update({
                    "held": True, "entry_price": price, "volume": vol,
                    "atr_sl": atr_sl_long, "atr_tp": atr_tp_long,
                })
                cur = ex.quote_currency
                notifier.notify_buy(f"{ex_name}/{market} RSI반등롱↑", price, invest_rsi, currency=cur)
                logger.info(
                    f"[{ex_name}][{market}] RSI반등롱 진입 | RSI={rsi_val:.1f} | "
                    f"{invest_rsi:,.0f} {cur}"
                )

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
    rsi_val    = float(latest.get("rsi", 50) or 50)

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
    if abs(price - daily_ma200) / daily_ma200 < 0.003:
        logger.info(f"[{ex_name}][{market}] 단타 스킵: MA200 극횡보 구간(0.3% 이내)")
        return

    # ── RSI 반등/되돌림 보조 신호 (VB signal=0일 때 보완) ────────
    rsi_signal = False  # RSI 기반 신호 여부 (롱 진입 추세 게이트 면제용)
    if signal == 0:
        RSI_LONG_THRESH  = SC_CFG.get("rsi_oversold_long",  28)
        RSI_SHORT_THRESH = SC_CFG.get("rsi_overbought_short", 68)
        prev_close = df_1h["close"].iloc[-2] if len(df_1h) >= 2 else price
        bullish_candle = price > prev_close
        if rsi_val < RSI_LONG_THRESH and bullish_candle:
            signal     = 1
            confidence = 2
            rsi_signal = True
            logger.info(f"[{ex_name}][{market}] RSI반등롱신호: RSI={rsi_val:.1f} < {RSI_LONG_THRESH} + 양봉")
        elif rsi_val > RSI_SHORT_THRESH and daily_downtrend:
            signal     = 2
            confidence = 2
            rsi_signal = True
            logger.info(f"[{ex_name}][{market}] RSI되돌림숏신호: RSI={rsi_val:.1f} > {RSI_SHORT_THRESH} + 하락추세")

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

    # ── 단타 롱 진입 (VB: 상승장 전용 / RSI반등: 추세 무관) ───
    if signal == 1 and (daily_uptrend or rsi_signal) and not lp["held"]:
        # 스윙 숏이 열려 있으면 선물 롱 불가 (포지션 충돌)
        if short_positions[ex_name][market]["held"]:
            logger.info(f"[{ex_name}][{market}] 단타 롱 스킵 — 스윙 숏 보유 중")
            return
        if invest > 0 and isinstance(ex, OKXExchange):
            if ex.open_long_futures(market, invest, leverage=leverage):
                vol = invest / price
                lp.update({"held": True, "entry_price": price, "volume": vol,
                            "entry_time": datetime.now(), "leverage": leverage})
                notifier.notify_buy(f"{ex_name}/{market} 단타롱 {leverage}x", price, invest, currency="USDT")
                logger.info(f"[{ex_name}][{market}] 단타 롱 진입 | {invest:,.2f} USDT | {leverage}x (확신도 {confidence}/5)")

    # ── 단타 숏 진입 (선물, 방향 무관 — 숏 신호만으로 진입) ──
    elif signal == 2 and not sp["held"]:
        if short_positions[ex_name][market]["held"]:
            logger.info(f"[{ex_name}][{market}] 단타 숏 스킵 — 스윙 숏 보유 중")
            return
        if invest > 0 and isinstance(ex, OKXExchange):
            if ex.open_short(market, invest, leverage=leverage):
                vol = invest / price
                sp.update({"held": True, "entry_price": price, "volume": vol,
                            "entry_time": datetime.now(), "leverage": leverage})
                notifier.notify_buy(f"{ex_name}/{market} 단타숏↓ {leverage}x", price, invest, currency="USDT")
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


def _scan_entry_status(fgi_val) -> dict:  # fgi_val: Optional[int]
    """각 종목별 롱/숏 진입 상태 및 대기 사유 분석 (send_periodic_report 용)"""
    result = {}
    for ex_name, ex in EXCHANGES.items():
        result[ex_name] = {}
        strat = strategy_longshort if ex_name == "okx" else strategy_long
        for market in get_markets(ex_name):
            try:
                price = ex.get_current_price(market)
                if not price:
                    continue
                if ex_name == "okx" and hasattr(ex, "fetch_ohlcv"):
                    df = ex.fetch_ohlcv(market, interval="day", count=210)
                else:
                    df = fetch_ohlcv(market, interval="day", count=210)
                if df is None or len(df) < 200:
                    continue
                sig_df = strat.generate_signals(df)
                if sig_df.empty:
                    continue
                latest   = sig_df.iloc[-1]
                signal   = int(latest.get("signal", 0))
                rsi      = float(latest.get("rsi", 50) or 50)
                ma200    = float(latest.get("ma200", 0) or 0)
                conf     = int(latest.get("confidence", 1) or 1)
                trend_up = (price > ma200) if ma200 > 0 else None
                trend    = "▲상승" if trend_up else "▼하락" if trend_up is False else "횡보"

                lp = long_positions[ex_name][market]
                sp = short_positions[ex_name][market]

                # ── 롱 상태 ──────────────────────────────
                if lp["held"] and lp["entry_price"] > 0:
                    pnl = (price - lp["entry_price"]) / lp["entry_price"] * 100
                    long_s = f"✅ 보유 중 ({pnl:+.1f}%)"
                elif signal == 1:
                    long_s = f"🔵 신호 발생 — 확신도 {conf}/5"
                elif signal == -1:
                    long_s = "⏸ 대기 — 청산 신호 발생"
                elif signal == 2:
                    long_s = "⏸ 대기 — 숏 신호 상태 (signal=2)"
                elif trend_up is False:
                    long_s = f"⏸ 대기 — MA200 하락추세, RSI={rsi:.0f}" if rsi >= 35 else \
                             f"🟡 대기 — 하락추세+RSI과매도({rsi:.0f}), 반등 가능성"
                elif trend_up is True:
                    long_s = f"⏸ 대기 — 상승추세이나 롱 신호 미발생"
                else:
                    long_s = "⏸ 대기 — 횡보 구간"

                # ── 숏 상태 (OKX 전용) ────────────────────
                short_s = None
                if ex_name == "okx":
                    if sp["held"] and sp["entry_price"] > 0:
                        pnl = (sp["entry_price"] - price) / sp["entry_price"] * 100
                        short_s = f"✅ 보유 중 ({pnl:+.1f}%)"
                    elif fgi_val is not None and fgi_val <= 10:
                        short_s = f"🚫 차단 — FGI 극도공포({fgi_val})"
                    elif signal == 2:
                        short_s = f"🔵 신호 발생 — 확신도 {conf}/5"
                    elif signal == 1:
                        short_s = "⏸ 대기 — 롱 신호 상태 (signal=1)"
                    elif trend_up is True:
                        short_s = "⏸ 대기 — 상승추세(MA200 상향), 숏 불리"
                    else:
                        short_s = f"⏸ 대기 — 하락추세 지속, 숏 신호 미발생"

                result[ex_name][market] = {
                    "coin":  market.replace("KRW-", ""),
                    "price": price, "trend": trend, "rsi": rsi,
                    "signal": signal, "long": long_s, "short": short_s,
                }
            except Exception as e:
                logger.warning(f"[리포트 스캔] {ex_name}/{market}: {e}")
    return result


def send_upbit_report():
    """
    업비트 진입현황 리포트.
    - v2 포지션(pos_manager)
    - 실제 보유 코인 + 평균단가 + 현재 손익
    - OKX 롱과 동일 조건 (v2 Multi-Timeframe, 롱 전용)
    매일 09:00 + 사용자 요청 시 호출.
    """
    ex = EXCHANGES.get("upbit")
    if not ex:
        return

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"🏦 <b>업비트 현황</b>  {now_str}"]

    # ── KRW 잔고 ─────────────────────────────────────
    krw = ex.get_balance_quote()
    lines.append(f"\nKRW 잔고: {krw:,.0f}원")

    # ── 실제 보유 코인 조회 ───────────────────────────
    markets = get_markets("upbit")
    holdings = []
    total_coin_value = 0.0
    for market in markets:
        try:
            vol = ex.get_balance_coin(market)
            if vol <= 0:
                continue
            avg   = ex.get_avg_buy_price(market)
            curr  = ex.get_current_price(market) or 0
            if avg and curr:
                pnl_pct = (curr - avg) / avg * 100
                value   = vol * curr
                total_coin_value += value
                holdings.append({
                    "market": market, "volume": vol,
                    "avg": avg, "curr": curr,
                    "pnl_pct": pnl_pct, "value": value,
                })
        except Exception:
            pass

    if holdings:
        lines.append("\n보유 코인:")
        for h in sorted(holdings, key=lambda x: -x["value"]):
            em = "🟢" if h["pnl_pct"] >= 0 else "🔴"
            coin = h["market"].replace("KRW-", "")
            lines.append(
                f"  {em} {coin:6s} {h['volume']:.4f}개 | "
                f"평단 {h['avg']:,.0f} | 현재 {h['curr']:,.0f} | "
                f"{h['pnl_pct']:+.2f}% | {h['value']:,.0f}원"
            )
    else:
        lines.append("\n보유 코인 없음")

    # ── v2 포지션 (pos_manager에 기록된 upbit 포지션) ─
    v2_upbit = [p for p in pos_manager.all_positions() if p.exchange == "upbit"]
    if v2_upbit:
        lines.append("\nv2 진입 포지션:")
        for pos in v2_upbit:
            try:
                curr = ex.get_current_price(pos.market) or 0
                pnl_pct = (curr - pos.entry_price) / pos.entry_price * 100 if pos.entry_price else 0
                em = "🟢" if pnl_pct >= 0 else "🔴"
                sl_str = f"SL {pos.atr_sl:,.0f}" if pos.atr_sl else ""
                tp_str = f"TP {pos.atr_tp:,.0f}" if pos.atr_tp else ""
                lines.append(
                    f"  {em} {pos.market} {pos.tier} | "
                    f"진입 {pos.entry_price:,.0f} | 현재 {curr:,.0f} | "
                    f"{pnl_pct:+.2f}% | {pos.holding_hours:.1f}H | {sl_str} {tp_str}"
                )
            except Exception:
                pass

    # ── 총 평가금액 ──────────────────────────────────
    total = krw + total_coin_value
    lines.append(f"\n총 평가금액: {total:,.0f}원")

    # ── 진입 대기 종목 (신호 없는 이유) ─────────────
    waiting = []
    for market in markets:
        v2_held = any(p.market == market and p.exchange == "upbit" for p in v2_upbit)
        coin_held = any(h["market"] == market for h in holdings)
        if not v2_held and not coin_held:
            waiting.append(market.replace("KRW-", ""))
    if waiting:
        lines.append(f"\n대기 중 종목: {', '.join(waiting)}")

    notifier.send("\n".join(lines))
    logger.info(f"[업비트리포트] 전송 완료 | 총 {total:,.0f}원")


def send_periodic_report():
    """10분마다 전송하는 현황 리포트 + 종목별 진입 현황"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── 1. 매크로 ──────────────────────────────────────
    fgi_val, fgi_cls, dom = None, None, None
    if _MACRO_ENABLED:
        try:
            fgi_data = fetch_fear_greed()
            if fgi_data:
                fgi_val = int(fgi_data["value"])
                fgi_cls = fgi_data["classification"]
            dom_data = fetch_btc_dominance()
            if dom_data:
                dom = float(dom_data)
        except Exception:
            pass

    # ── 2. 포지션 수집 ─────────────────────────────────
    pos_lines = []
    total_open = 0
    for ex_name, ex in EXCHANGES.items():
        for market in get_markets(ex_name):
            p = ex.get_current_price(market)
            if not p:
                continue
            lp  = long_positions[ex_name][market]
            sp  = short_positions[ex_name][market]
            slp = scalp_long_positions.get(ex_name, {}).get(market, {})
            ssp = scalp_short_positions.get(ex_name, {}).get(market, {})

            if lp["held"] and lp["entry_price"] > 0:
                pnl = (p - lp["entry_price"]) / lp["entry_price"] * 100
                em  = "🟢" if pnl >= 0 else "🔴"
                pos_lines.append(f"  {em} {market} 스윙롱 {pnl:+.2f}% (진입 {lp['entry_price']:,.2f})")
                total_open += 1
            if sp["held"] and sp["entry_price"] > 0:
                pnl = (sp["entry_price"] - p) / sp["entry_price"] * 100
                em  = "🟢" if pnl >= 0 else "🔴"
                pos_lines.append(f"  {em} {market} 스윙숏 {pnl:+.2f}% (진입 {sp['entry_price']:,.2f})")
                total_open += 1
            if slp.get("held") and slp.get("entry_price", 0) > 0:
                pnl = (p - slp["entry_price"]) / slp["entry_price"] * 100
                h_str = ""
                if slp.get("entry_time"):
                    h = (datetime.now() - slp["entry_time"]).total_seconds() / 3600
                    h_str = f" {h:.1f}H"
                em = "🟢" if pnl >= 0 else "🔴"
                pos_lines.append(f"  {em} {market} 단타롱{h_str} {pnl:+.2f}%")
                total_open += 1
            if ssp.get("held") and ssp.get("entry_price", 0) > 0:
                pnl = (ssp["entry_price"] - p) / ssp["entry_price"] * 100
                h_str = ""
                if ssp.get("entry_time"):
                    h = (datetime.now() - ssp["entry_time"]).total_seconds() / 3600
                    h_str = f" {h:.1f}H"
                em = "🟢" if pnl >= 0 else "🔴"
                pos_lines.append(f"  {em} {market} 단타숏{h_str} {pnl:+.2f}%")
                total_open += 1

    # ── 3. 자산 ────────────────────────────────────────
    equity_lines = []
    for ex_name, ex in EXCHANGES.items():
        eq = calc_total_equity(ex_name)
        equity_lines.append(f"  {ex_name}: {eq:,.2f} {ex.quote_currency}")

    # ── 4. 개선 제안 (현황 기반 동적 생성) ──────────────
    suggestions = []
    if fgi_val is not None:
        if fgi_val <= 15:
            suggestions.append("📌 FGI 극단공포 — 반등 포지션 기회 주시")
        elif fgi_val <= 30:
            suggestions.append("📌 FGI 공포 — 스윙숏/RSI반등 전략 유효")
        elif fgi_val >= 75:
            suggestions.append("📌 FGI 탐욕 과열 — 신규 롱 비중 축소 고려")

    if dom is not None:
        if dom >= 58:
            suggestions.append("📌 BTC도미넌스 높음 — 알트 진입 신중 필요")
        elif dom <= 42:
            suggestions.append("📌 BTC도미넌스 낮음 — 알트코인 모멘텀 활용 가능")

    if total_open == 0:
        suggestions.append("📌 보유 포지션 없음 — 신호 대기 중 (정상)")
    elif total_open >= 5:
        suggestions.append(f"📌 동시 포지션 {total_open}개 — 리스크 분산 점검 권장")

    # 하락장 지속 여부 판단
    downtrend_count = 0
    for ex_name, ex in EXCHANGES.items():
        for market in get_markets(ex_name):
            try:
                p = ex.get_current_price(market)
                df_sw = ex.fetch_ohlcv(market, interval="day", count=205)
                if df_sw is not None and len(df_sw) >= 200 and p:
                    ma200 = df_sw["close"].rolling(200).mean().iloc[-1]
                    if p < ma200:
                        downtrend_count += 1
            except Exception:
                pass
        break  # upbit만 빠르게 체크

    total_mkts = len(get_markets(list(EXCHANGES.keys())[0]))
    if total_mkts > 0:
        down_ratio = downtrend_count / total_mkts
        if down_ratio >= 0.8:
            suggestions.append(f"📌 전체 {int(down_ratio*100)}% 하락장 — 현금 비중 유지 전략 권장")
        elif down_ratio <= 0.3:
            suggestions.append(f"📌 전체 {int((1-down_ratio)*100)}% 상승 추세 — 롱 진입 기회 확대")

    if not suggestions:
        suggestions.append("📌 현재 전략 정상 작동 중")

    # ── 5. 메시지 조립 ─────────────────────────────────
    lines = [f"📡 <b>정기 현황 리포트</b>  {now_str}"]

    if fgi_val is not None or dom is not None:
        macro_info = []
        if fgi_val is not None:
            macro_info.append(f"공포탐욕: {fgi_val} ({fgi_cls})")
        if dom is not None:
            macro_info.append(f"BTC도미: {dom:.1f}%")
        lines.append("\n🌐 <b>매크로</b>\n  " + " | ".join(macro_info))

    if equity_lines:
        lines.append("\n💰 <b>자산</b>\n" + "\n".join(equity_lines))

    # ── v2 포지션 (pos_manager) ────────────────────────
    v2_lines = []
    for pos in pos_manager.all_positions():
        ex = EXCHANGES.get(pos.exchange)
        if not ex:
            continue
        try:
            curr = ex.get_current_price(pos.market)
            if curr and pos.entry_price:
                if pos.direction == "long":
                    pnl_pct = (curr - pos.entry_price) / pos.entry_price * 100 * pos.leverage
                else:
                    pnl_pct = (pos.entry_price - curr) / pos.entry_price * 100 * pos.leverage
                em = "🟢" if pnl_pct >= 0 else "🔴"
                held_h = pos.holding_hours
                v2_lines.append(
                    f"  {em} [{pos.exchange}] {pos.market} {pos.direction} {pos.tier} "
                    f"{pnl_pct:+.2f}% {held_h:.1f}H | conf={pos.confluence_score}"
                )
                total_open += 1
        except Exception:
            pass

    all_pos_lines = pos_lines + v2_lines
    if all_pos_lines:
        lines.append(f"\n📊 <b>포지션 ({total_open}개)</b>\n" + "\n".join(all_pos_lines))
    else:
        lines.append("\n📊 <b>포지션</b>  없음 (현금 대기)")

    lines.append("\n💡 <b>개선 제안</b>\n" + "\n".join(suggestions))

    notifier.send("\n".join(lines))

    # ── 6. 종목별 진입 현황 스캔 (거래소별 별도 메시지) ─────
    try:
        scan = _scan_entry_status(fgi_val)
        for ex_name, markets in scan.items():
            if not markets:
                continue
            ex_label = "OKX 선물" if ex_name == "okx" else ex_name.upper()
            scan_lines = [f"🔍 <b>{ex_label} 종목별 진입 현황</b>  {now_str}\n"]
            for market, info in markets.items():
                coin  = info["coin"]
                trend = info["trend"]
                rsi   = info["rsi"]
                price = info["price"]
                scan_lines.append(
                    f"<b>{coin}</b>  {price:,.2f} | {trend} | RSI {rsi:.0f}"
                )
                scan_lines.append(f"  롱: {info['long']}")
                if info["short"] is not None:
                    scan_lines.append(f"  숏: {info['short']}")
                scan_lines.append("")   # 빈 줄 구분

            # 4000자 초과 시 분할 전송
            chunk, char_count = [], 0
            for line in scan_lines:
                if char_count + len(line) > 3800:
                    notifier.send("\n".join(chunk))
                    chunk, char_count = [], 0
                chunk.append(line)
                char_count += len(line)
            if chunk:
                notifier.send("\n".join(chunk))
    except Exception as e:
        logger.error(f"[정기 리포트] 종목 스캔 오류: {e}")

    logger.info(f"[정기 리포트] 전송 완료 | 포지션 {total_open}개")


# ══════════════════════════════════════════════════════════
# Multi-Timeframe v2 — Tier별 실행 루프
# ══════════════════════════════════════════════════════════

def _run_tier(tier: str):
    """
    단일 Tier의 전략 실행 루프.
    모든 활성 거래소 × Tier 종목에 대해 신호 생성 → Confluence 평가 → 진입/청산.
    """
    with _TF_LOCK:
        interval = TIER_INTERVAL.get(tier, "day")
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        logger.info(f"[v2][{tier}] ── 전략 실행 | {now}")

        strat = tf_coordinator.get_strategy(tier)
        if strat is None:
            logger.warning(f"[v2][{tier}] 전략 없음, 건너뜀")
            return

        # 만료 포지션 청산 (보유시간 초과)
        for pos in pos_manager.expired_positions():
            if pos.tier != tier:
                continue
            ex = EXCHANGES.get(pos.exchange)
            if ex is None:
                continue
            _close_position_v2(pos, ex, reason=f"보유시간초과({pos.holding_hours:.1f}h)")

        for ex_name, ex in EXCHANGES.items():
            equity = calc_total_equity(ex_name)
            tier_markets = get_tier_markets(ex_name, tier)
            is_upbit = (ex_name != "okx")

            for market in tier_markets:
                try:
                    # OHLCV 가져오기 (캐시 활용)
                    if ex_name == "okx" and hasattr(ex, "fetch_ohlcv"):
                        fetch_fn = ex.fetch_ohlcv
                    else:
                        fetch_fn = fetch_ohlcv
                    df = get_cached_ohlcv(fetch_fn, ex_name, market, interval, count=210)
                    if df is None or df.empty:
                        continue

                    # 전략 신호 생성 (v2: MA200 gate 없음)
                    if tier == "15m":
                        sig_df = strat.generate_signals(df)
                    else:
                        sig_df = strat.generate_signals_v2(df)
                    if sig_df.empty:
                        continue

                    latest = sig_df.iloc[-1]
                    signal = int(latest.get("signal", 0))
                    confidence = int(latest.get("confidence", 1))
                    price = float(latest.get("close", 0) or 0)

                    if signal == 0:
                        continue

                    # Upbit: 숏 불가
                    if is_upbit and signal == 2:
                        continue

                    direction = "long" if signal == 1 else "short"

                    # 이미 같은 Tier+종목+방향에 포지션 있으면 스킵
                    if pos_manager.has_position(ex_name, market, tier, direction):
                        continue

                    # 상위 TF bias 계산
                    df_daily = get_cached_ohlcv(fetch_fn, ex_name, market, "day", 210)
                    daily_bias = tf_coordinator.calc_daily_bias(df_daily) if df_daily is not None else ("neutral", 0)

                    df_4h = get_cached_ohlcv(fetch_fn, ex_name, market, "minute240", 210) if tier in ("1h", "15m") else None
                    h4_bias = tf_coordinator.calc_4h_bias(df_4h) if df_4h is not None else ("neutral", 0)

                    # 15m 평균회귀 안전장치: 강추세 시 역추세 MR 비활성화
                    if tier == "15m":
                        is_strong, strong_dir = tf_coordinator.is_strong_trend(daily_bias, h4_bias)
                        if is_strong:
                            if (direction == "long" and strong_dir == "bear") or \
                               (direction == "short" and strong_dir == "bull"):
                                logger.debug(f"[v2][15m] {market} 역추세 MR 차단 (강추세 {strong_dir})")
                                continue

                    # 매크로 보정 (v2: 차단 없음, delta만)
                    macro_delta = 0
                    macro_reason = ""
                    if _MACRO_ENABLED:
                        macro_delta, macro_reason = calc_macro_signal(
                            direction, market,
                            finnhub_token=_FINNHUB_TOKEN,
                            av_key=_AV_KEY,
                            tier=tier,
                        )

                    # Top Trader 보정 (OKX만, 1h/15m만)
                    top_delta = 0
                    if ex_name == "okx" and tier in ("1h", "15m") and hasattr(ex, "fetch_top_trader_ratio"):
                        try:
                            long_ratio = ex.fetch_top_trader_ratio(market)
                            if long_ratio is not None:
                                top_delta, _, _ = calc_top_trader_signal(long_ratio, direction)
                        except Exception:
                            pass

                    # 거래량 서지 여부
                    has_vol = bool(latest.get("vol_surge", False))

                    # Confluence Score 계산
                    confluence = tf_coordinator.calc_confluence(
                        signal=signal,
                        entry_confidence=confidence,
                        daily_bias=daily_bias,
                        h4_bias=h4_bias,
                        has_vol_surge=has_vol,
                        macro_delta=macro_delta,
                        top_trader_delta=top_delta,
                    )

                    if not tf_coordinator.should_trade(confluence):
                        logger.debug(
                            f"[v2][{tier}][{ex_name}][{market}] {direction} "
                            f"confluence={confluence} < min={tf_coordinator.min_score} | {macro_reason}"
                        )
                        continue

                    # SL 쿨다운 체크 (동일 종목/방향 재진입 방지)
                    pos_key = f"{ex_name}:{market}:{tier}:{direction}"
                    if pos_manager.is_in_sl_cooldown(pos_key):
                        logger.debug(f"[v2][{tier}][{ex_name}][{market}] SL 쿨다운 중, 재진입 차단")
                        continue

                    # 포지션 개시 가능 여부
                    open_list = pos_manager.all_as_dicts()
                    can_open, deny_reason = cap_allocator.can_open_position(
                        tier, direction, market, open_list, exchange=ex_name
                    )
                    if not can_open:
                        logger.info(f"[v2][{tier}][{ex_name}][{market}] 진입 거부: {deny_reason}")
                        continue

                    # 포지션 크기 + 레버리지
                    # OKX 선물: 계약 최소단위 확보를 위해 asset_weight 미적용 (종목별 소분산 금지)
                    # Upbit 현물: asset_weight 적용 (분할매수 가능)
                    if is_upbit:
                        weight = ASSET_WEIGHTS.get(market.replace("KRW-", ""), 1.0 / max(len(tier_markets), 1))
                    else:
                        weight = 1.0  # OKX 선물: tier_size_pct × equity 전액
                    min_order = 5000 if is_upbit else 100.0  # OKX: 최소 100 USDT (계약단위 확보)
                    invest = cap_allocator.calc_position_size(equity, weight, tier, confluence, min_order)
                    if invest <= 0:
                        continue

                    tier_params = _RISK_TIERS.get(tier, {})
                    max_lev = tier_params.get("max_leverage", 1)
                    leverage = cap_allocator.calc_leverage(tier, confluence, tier_max_lev=max_lev)

                    # ATR SL/TP
                    atr_val = float(latest.get("atr", 0) or 0)
                    rm = risk_managers.get(ex_name)
                    if rm and atr_val > 0:
                        sl_price, tp_price = rm.calc_atr_sl_tp(price, atr_val, tier, direction)
                    else:
                        sl_price, tp_price = None, None

                    # 최대 보유 시간
                    max_hold_sec = 0
                    if "max_holding_hours" in tier_params:
                        max_hold_sec = tier_params["max_holding_hours"] * 3600
                    elif "max_holding_days" in tier_params:
                        max_hold_sec = tier_params["max_holding_days"] * 86400

                    # ── 주문 실행 ──────────────────────────────────
                    order_ok = False
                    volume = 0.0

                    if direction == "long":
                        if is_upbit:
                            order = ex.buy_market_order(market, invest)
                            order_ok = order is not None
                            if order_ok:
                                volume = invest / price if price > 0 else 0
                        elif hasattr(ex, "open_long"):
                            ex.set_leverage(market, leverage)
                            order_ok = ex.open_long(market, invest, leverage=leverage)
                            if order_ok:
                                volume = invest / price if price > 0 else 0
                    else:  # short (OKX only)
                        if hasattr(ex, "open_short"):
                            ex.set_leverage(market, leverage)
                            order_ok = ex.open_short(market, invest, leverage=leverage)
                            if order_ok:
                                volume = invest / price if price > 0 else 0

                    if order_ok:
                        pos = Position(
                            tier=tier,
                            exchange=ex_name,
                            market=market,
                            direction=direction,
                            entry_price=price,
                            volume=volume,
                            leverage=leverage,
                            atr_sl=sl_price,
                            atr_tp=tp_price,
                            confluence_score=confluence,
                            max_holding_seconds=max_hold_sec,
                        )
                        pos_manager.open(pos)
                        logger.info(
                            f"[v2][{tier}][{ex_name}][{market}] "
                            f"{'롱' if direction == 'long' else '숏'} 진입 | "
                            f"conf={confluence} lev={leverage}x "
                            f"size={invest:,.0f} @ {price:,.2f} | "
                            f"SL={sl_price or '-'} TP={tp_price or '-'} | "
                            f"{macro_reason}"
                        )

                except Exception as e:
                    logger.error(f"[v2][{tier}][{ex_name}][{market}] 처리 오류: {e}")

        time.sleep(0.1)  # API 호출 간격


def _close_position_v2(pos: Position, ex, reason: str = ""):
    """v2 포지션 청산."""
    try:
        ok = False
        if pos.direction == "long":
            if pos.exchange != "okx":
                ok = ex.sell_market_order(pos.market, pos.volume) is not None
            elif hasattr(ex, "close_long"):
                ok = ex.close_long(pos.market, pos.volume)
        else:
            if hasattr(ex, "close_short"):
                # OKX: 실제 포지션 볼륨으로 청산 (내부 추적 볼륨과 불일치 방지)
                close_vol = pos.volume
                if pos.exchange == "okx" and hasattr(ex, "get_futures_position"):
                    try:
                        real = ex.get_futures_position(pos.market)
                        if not real.get("error") and real.get("side") == "short" and real.get("volume", 0) > 0:
                            close_vol = real["volume"]
                    except Exception:
                        pass
                ok = ex.close_short(pos.market, close_vol)

        if ok:
            pos_manager.close(pos.key, reason=reason)
            # SL 손절 시 재진입 쿨다운 등록
            if reason.startswith("SL("):
                pos_manager.record_sl_hit(pos.key, pos.tier)
            # ── 거래 결과 기록 (성과 분석용) ────────────────────────────
            try:
                exit_price = pos.exchange and EXCHANGES.get(pos.exchange)
                if exit_price:
                    exit_price = EXCHANGES[pos.exchange].get_current_price(pos.market) or 0.0
                else:
                    exit_price = 0.0
                if exit_price and pos.entry_price:
                    if pos.direction == "long":
                        pnl_pct = (exit_price - pos.entry_price) / pos.entry_price * 100 * pos.leverage
                    else:
                        pnl_pct = (pos.entry_price - exit_price) / pos.entry_price * 100 * pos.leverage
                    pnl = getattr(pos, "invest_usdt", pos.volume) * pnl_pct / 100
                    logger.info(
                        f"[TRADE] {pos.direction.upper()} {pos.market} tier={pos.tier} | "
                        f"entry={pos.entry_price} exit={exit_price:.6f} "
                        f"pnl={pnl:+.4f} pnl_pct={pnl_pct:+.2f}% reason={reason}"
                    )
            except Exception:
                pass
    except Exception as e:
        logger.error(f"[v2] 청산 실패 {pos.key}: {e}")


def run_price_monitor_v2():
    """v2 가격 모니터: 모든 오픈 포지션의 SL/TP 체크."""
    positions = pos_manager.all_positions()
    if not positions:
        return

    for pos in positions:
        ex = EXCHANGES.get(pos.exchange)
        if ex is None:
            continue
        try:
            price = ex.get_current_price(pos.market)
            if price is None or price <= 0:
                continue

            # Break-even trailing SL: TP 절반 이상 달성 시 진입가로 SL 이동
            if pos.entry_price and pos.atr_sl and pos.atr_tp:
                if pos.direction == "long" and pos.atr_sl < pos.entry_price:
                    half_tp = pos.entry_price + (pos.atr_tp - pos.entry_price) * 0.5
                    if price >= half_tp:
                        pos.atr_sl = pos.entry_price
                        pos_manager._save()
                        logger.info(f"[v2 Monitor] {pos.market} 롱 Break-even SL 적용 @ {pos.entry_price:,.2f}")
                elif pos.direction == "short" and pos.atr_sl > pos.entry_price:
                    half_tp = pos.entry_price - (pos.entry_price - pos.atr_tp) * 0.5
                    if price <= half_tp:
                        pos.atr_sl = pos.entry_price
                        pos_manager._save()
                        logger.info(f"[v2 Monitor] {pos.market} 숏 Break-even SL 적용 @ {pos.entry_price:,.2f}")

            # SL/TP 체크
            if pos.direction == "long":
                if pos.atr_sl and price <= pos.atr_sl:
                    _close_position_v2(pos, ex, reason=f"SL({pos.atr_sl:,.2f})")
                elif pos.atr_tp and price >= pos.atr_tp:
                    _close_position_v2(pos, ex, reason=f"TP({pos.atr_tp:,.2f})")
            else:  # short
                if pos.atr_sl and price >= pos.atr_sl:
                    _close_position_v2(pos, ex, reason=f"SL({pos.atr_sl:,.2f})")
                elif pos.atr_tp and price <= pos.atr_tp:
                    _close_position_v2(pos, ex, reason=f"TP({pos.atr_tp:,.2f})")

            # 만료 체크
            if pos.is_expired:
                _close_position_v2(pos, ex, reason=f"보유시간초과({pos.holding_hours:.1f}h)")

        except Exception as e:
            logger.error(f"[v2 Monitor] {pos.key}: {e}")


def run_tf_daily():
    """Tier 1: Daily 추세 추종."""
    refresh_asset_weights()
    _run_tier("daily")

def run_tf_4h():
    """Tier 2: 4H 스윙."""
    _run_tier("4h")

def run_tf_1h():
    """Tier 3: 1H 모멘텀."""
    _run_tier("1h")

def run_tf_15m():
    """Tier 4: 15m 평균회귀."""
    _run_tier("15m")

def run_tf_safe(fn, name):
    """안전 래퍼 (예외 방지)."""
    try:
        fn()
    except Exception as e:
        logger.error(f"[v2][{name}] 루프 오류: {e}", exc_info=True)


# ══════════════════════════════════════════════════════════
# Advanced Entry Engine — 7TF 분석 기반 진입
# ══════════════════════════════════════════════════════════

def run_advanced_analysis():
    """
    Advanced Entry Engine: 모든 OKX 종목에 대해 7TF OHLCV를 수집하고
    Regime + MTF Alignment + Entry Trigger를 평가하여 진입/관망 판단.
    """
    with _TF_LOCK:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        logger.info(f"[ADV] ── Advanced Analysis | {now_str}")

        ex_name = "okx"
        ex = EXCHANGES.get(ex_name)
        if ex is None:
            return

        equity = calc_total_equity(ex_name)
        if equity <= 0:
            return

        # 만료 포지션 청산
        for pos in pos_manager.expired_positions():
            _close_position_v2(pos, ex, reason=f"보유시간초과({pos.holding_hours:.1f}h)")

        # 분석 대상 종목 (전 Tier 합집합)
        all_markets = set()
        for tier_key in ["daily", "4h", "1h", "15m", "5m", "30m"]:
            all_markets.update(get_tier_markets(ex_name, tier_key))

        for market in all_markets:
            try:
                # 7TF OHLCV 수집 (캐시 활용)
                fetch_fn = ex.fetch_ohlcv if hasattr(ex, "fetch_ohlcv") else fetch_ohlcv
                ohlcv_dict = {}
                for tf_name in ["day", "minute240", "minute60", "minute30", "minute15", "minute5"]:
                    df = get_cached_ohlcv(fetch_fn, ex_name, market, tf_name, count=210)
                    if df is not None and not df.empty:
                        ohlcv_dict[tf_name] = df.copy()  # 캐시 원본 보호

                if not ohlcv_dict:
                    continue

                # Entry Engine 평가
                decision = entry_engine.evaluate(market, ohlcv_dict)

                if not decision.should_enter:
                    if decision.reasons and any("MTF" in r for r in decision.reasons):
                        logger.debug(f"[ADV][{market}] 관망 | {' | '.join(decision.reasons[:2])}")
                    continue

                direction = decision.direction
                coin = market.replace("KRW-", "")

                # 이미 같은 종목+방향에 포지션 있으면 스킵
                if pos_manager.has_any_position(ex_name, market, direction):
                    continue

                # SL 쿨다운 체크
                cool_key = f"{ex_name}:{market}:adv:{direction}"
                if pos_manager.is_in_sl_cooldown(cool_key):
                    continue

                # 포지션 개시 가능 여부
                open_list = pos_manager.all_as_dicts()
                can_open, deny_reason = cap_allocator.can_open_position(
                    "1h", direction, market, open_list, exchange=ex_name
                )
                if not can_open:
                    continue

                # 포지션 크기 + 레버리지
                weight = ASSET_WEIGHTS.get(coin, 1.0 / 8)
                min_order = 5.0
                invest = cap_allocator.calc_position_size(
                    equity, weight, "1h", decision.confidence, min_order
                )
                invest *= decision.size_mult
                if invest < min_order:
                    continue

                leverage = cap_allocator.calc_leverage("1h", decision.confidence, tier_max_lev=5)

                # 현재가 + ATR SL/TP
                price = ex.get_current_price(market) or 0
                if price <= 0:
                    continue

                atr_val = 0
                df_entry = ohlcv_dict.get("minute15") if ohlcv_dict.get("minute15") is not None else ohlcv_dict.get("minute60")
                if df_entry is not None and "atr" in df_entry.columns:
                    atr_val = float(df_entry["atr"].iloc[-1] or 0)

                if atr_val > 0:
                    if direction == "long":
                        sl_price = price - atr_val * decision.sl_mult
                        tp_price = price + atr_val * decision.tp_mult
                    else:
                        sl_price = price + atr_val * decision.sl_mult
                        tp_price = price - atr_val * decision.tp_mult
                else:
                    sl_price, tp_price = None, None

                # 보유시간
                tier_params = _RISK_TIERS.get("1h", {})
                max_hold_sec = tier_params.get("max_holding_hours", 12) * 3600

                # 주문 실행
                order_ok = False
                volume = invest / price if price > 0 else 0

                if direction == "long":
                    ex.set_leverage(market, leverage)
                    order_ok = ex.open_long(market, invest, leverage=leverage)
                else:
                    ex.set_leverage(market, leverage)
                    order_ok = ex.open_short(market, invest, leverage=leverage)

                if order_ok:
                    pos = Position(
                        tier="adv",
                        exchange=ex_name,
                        market=market,
                        direction=direction,
                        entry_price=price,
                        volume=volume,
                        leverage=leverage,
                        atr_sl=sl_price,
                        atr_tp=tp_price,
                        confluence_score=decision.confidence,
                        max_holding_seconds=max_hold_sec,
                    )
                    pos_manager.open(pos)
                    logger.info(
                        f"[ADV][{market}] {'롱' if direction == 'long' else '숏'} 진입 | "
                        f"regime={decision.regime} align={decision.alignment:+.1f} "
                        f"conf={decision.confidence} lev={leverage}x "
                        f"size=${invest:.0f} @ {price:,.2f} | "
                        f"trigger={decision.trigger_reason} | "
                        f"SL={sl_price or '-'} TP={tp_price or '-'}"
                    )

            except Exception as e:
                logger.error(f"[ADV][{market}] 처리 오류: {e}", exc_info=True)

        time.sleep(0.1)


def run_advanced_safe():
    run_tf_safe(run_advanced_analysis, "ADV")


# ── 스케줄 (v3: 7-Timeframe Advanced) ────────────────
schedule.every(3).minutes.do(run_advanced_safe)                         # Advanced: 매 3분
schedule.every().minute.do(run_price_monitor_v2)                        # SL/TP: 매분
schedule.every().day.at("23:00").do(send_daily_report)
schedule.every(10).minutes.do(send_periodic_report)

# ── 자동 성과 분석 + 파라미터 최적화 ────────────────────
def run_perf_analysis():
    """매일 03:00 — 최근 7일 성과 분석 + tier_size_pct 자동 조정."""
    try:
        summary = run_daily_report(days=7, update_config=True)
        notifier = globals().get("notifier")
        if notifier:
            notifier.send(f"[자동분석]\n{summary}")
        logger.info("[자동분석] 성과 분석 완료")
    except Exception as e:
        logger.error(f"[자동분석] 실패: {e}")

def run_param_opt():
    """매주 일요일 04:00 — 백테스트 기반 파라미터 재최적화."""
    try:
        okx_markets = get_markets("okx")
        summary = run_weekly_optimization(markets=okx_markets[:3])
        notifier = globals().get("notifier")
        if notifier:
            notifier.send(f"[자동최적화]\n{summary}")
        logger.info("[자동최적화] 파라미터 최적화 완료")
    except Exception as e:
        logger.error(f"[자동최적화] 실패: {e}")

schedule.every().day.at("03:00").do(run_perf_analysis)      # 일일 성과 분석
schedule.every().sunday.at("04:00").do(run_param_opt)       # 주간 파라미터 최적화
schedule.every().day.at("09:00").do(send_upbit_report)      # 업비트 진입현황 (매일 9시)


def main():
    ex_list = ", ".join(
        f"{n}({'실전' if not e.paper_trading else '페이퍼'})"
        for n, e in EXCHANGES.items()
    )
    logger.info("=" * 60)
    logger.info(f"  퀀트 자동매매 봇 v3 시작 (Advanced 7TF Analysis)")
    logger.info(f"  거래소: {ex_list}")
    logger.info(f"  3-Layer Engine: Regime → MTF Alignment → Precision Entry")
    logger.info(f"  7TF: 1D / 4H / 1H / 30m / 15m / 5m (+ 1m monitor)")
    logger.info(f"  Advanced Analysis: 매 3분 | SL/TP Monitor: 매분")
    logger.info(f"  MTF Alignment 임계값: long>={entry_engine.mtf_analyzer.long_threshold} short<={entry_engine.mtf_analyzer.short_threshold}")
    logger.info(f"  최대 동시 포지션: {cap_allocator.max_total_positions}")
    logger.info("=" * 60)

    notifier.send(
        f"🤖 봇 v3 시작 (Advanced 7TF)\n"
        f"거래소: {ex_list}\n"
        f"Engine: Regime+MTF+Entry\n"
        f"Analysis: �� 3분 | OKX 전용"
    )

    # 포지션 동기화 + 자본배분 초기화
    for ex_name in EXCHANGES:
        sync_positions(ex_name)
    refresh_asset_weights()

    # 즉시 첫 실행
    run_advanced_safe()

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
