"""
매크로 지표 실시간 데이터 수집 모듈

각 fetcher는 성공 시 값 반환, 실패 시 None 반환.
API 실패가 매매 중단으로 이어지지 않도록 모든 함수는 예외를 내부에서 처리.
TTL 캐시로 불필요한 반복 API 호출 방지.
"""
import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

# ── 단순 TTL 캐시 ─────────────────────────────────────────────────────────────
_cache: dict = {}   # {key: (value, expire_ts)}
_DEFAULT_TTL = 300  # 기본 5분


def _get_cache(key: str):
    entry = _cache.get(key)
    if entry and time.time() < entry[1]:
        return entry[0]
    return None


def _set_cache(key: str, value, ttl: int = _DEFAULT_TTL):
    _cache[key] = (value, time.time() + ttl)


# ── Fear & Greed Index (alternative.me, 무료, 인증 불필요) ───────────────────

def fetch_fear_greed(ttl: int = 3600) -> Optional[dict]:
    """
    크립토 공포탐욕지수 반환.
    value: 0(극도공포) ~ 100(극도탐욕)
    classification: 'Extreme Fear' | 'Fear' | 'Neutral' | 'Greed' | 'Extreme Greed'
    캐시 1시간 (일 1회 업데이트이므로 충분)
    """
    cached = _get_cache("fgi")
    if cached is not None:
        return cached
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        r.raise_for_status()
        data = r.json()["data"][0]
        result = {
            "value": int(data["value"]),
            "classification": data["value_classification"],
        }
        _set_cache("fgi", result, ttl)
        logger.debug(f"[Macro] FGI: {result['value']} ({result['classification']})")
        return result
    except Exception as e:
        logger.warning(f"[Macro] FGI 수집 실패: {e}")
        return None


# ── BTC 도미넌스 (CoinGecko, 무료, 인증 불필요) ──────────────────────────────

def fetch_btc_dominance(ttl: int = 300) -> Optional[float]:
    """BTC 시가총액 도미넌스 (%) 반환. 5분 캐시."""
    cached = _get_cache("btc_dom")
    if cached is not None:
        return cached
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/global",
            headers={"Accept": "application/json"},
            timeout=8,
        )
        r.raise_for_status()
        dom = r.json()["data"]["market_cap_percentage"]["btc"]
        result = round(float(dom), 2)
        _set_cache("btc_dom", result, ttl)
        logger.debug(f"[Macro] BTC 도미넌스: {result:.1f}%")
        return result
    except Exception as e:
        logger.warning(f"[Macro] BTC 도미넌스 수집 실패: {e}")
        return None


# ── OKX 펀딩비 (공개 REST API, 인증 불필요) ──────────────────────────────────

def fetch_okx_funding_rate(inst_id: str, ttl: int = 300) -> Optional[float]:
    """
    OKX 무기한 선물 현재 펀딩비 반환.
    inst_id 예: 'BTC-USDT-SWAP', 'ETH-USDT-SWAP'
    양수 = 롱 비용, 음수 = 숏 비용
    """
    cache_key = f"fr_{inst_id}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/public/funding-rate",
            params={"instId": inst_id},
            timeout=5,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        if data:
            result = float(data[0]["fundingRate"])
            _set_cache(cache_key, result, ttl)
            logger.debug(f"[Macro] 펀딩비 {inst_id}: {result*100:.4f}%")
            return result
    except Exception as e:
        logger.warning(f"[Macro] 펀딩비 수집 실패 ({inst_id}): {e}")
    return None


# ── OKX Long/Short 비율 (공개 REST API, 인증 불필요) ─────────────────────────

def fetch_okx_long_short_ratio(inst_id: str, period: str = "1H", ttl: int = 300) -> Optional[float]:
    """
    OKX 전체 계좌 롱/숏 비율 반환 (롱계좌수 / 숏계좌수 비율).
    1.0 = 균형, > 1.5 = 롱 과열, < 0.7 = 숏 과열
    inst_id 예: 'BTC-USDT-SWAP' → ccy='BTC' 로 변환
    """
    # OKX /rubik/stat/contracts/long-short-account-ratio 는 ccy 파라미터 사용
    ccy = inst_id.split("-")[0]  # 'BTC-USDT-SWAP' → 'BTC'
    cache_key = f"ls_{ccy}_{period}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio",
            params={"ccy": ccy, "period": period},
            timeout=5,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        if data:
            result = float(data[0][1])  # [timestamp, longRatio, shortRatio]
            _set_cache(cache_key, result, ttl)
            logger.debug(f"[Macro] L/S 비율 {ccy}: {result:.1%}")
            return result
    except Exception as e:
        logger.warning(f"[Macro] L/S 비율 수집 실패 ({ccy}): {e}")
    return None


# ── VIX (Finnhub, 무료 API 키 필요) ──────────────────────────────────────────

def fetch_vix(finnhub_token: str, ttl: int = 300) -> Optional[float]:
    """
    CBOE VIX 현재값 반환. 20 이상 = 주의, 30 이상 = 위험.
    finnhub.io 무료 등록 후 API 키 발급.
    """
    if not finnhub_token:
        return None
    cached = _get_cache("vix")
    if cached is not None:
        return cached
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": "VIX", "token": finnhub_token},
            timeout=5,
        )
        r.raise_for_status()
        result = float(r.json()["c"])
        if result > 0:
            _set_cache("vix", result, ttl)
            logger.debug(f"[Macro] VIX: {result:.1f}")
            return result
    except Exception as e:
        logger.warning(f"[Macro] VIX 수집 실패: {e}")
    return None


# ── DXY 달러인덱스 (Alpha Vantage, 무료 API 키 필요) ─────────────────────────

def fetch_dxy(av_key: str, ttl: int = 3600) -> Optional[float]:
    """
    달러인덱스(DXY) 근사값 반환. EUR/USD로 역산 (DXY ≈ 100/EURUSD 근사).
    alphavantage.co 무료 등록 후 API 키 발급 (25콜/일 무료).
    상승 = BTC 하락 압력. 107 이상 = 강달러.
    """
    if not av_key:
        return None
    cached = _get_cache("dxy")
    if cached is not None:
        return cached
    try:
        r = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "FX_DAILY",
                "from_symbol": "EUR",
                "to_symbol": "USD",
                "apikey": av_key,
                "outputsize": "compact",
            },
            timeout=10,
        )
        r.raise_for_status()
        series = r.json().get("Time Series FX (Daily)", {})
        if series:
            latest_key = sorted(series.keys())[-1]
            eur_usd = float(series[latest_key]["4. close"])
            result = round(100.0 / eur_usd, 2)  # DXY 근사 (EURUSD 역수)
            _set_cache("dxy", result, ttl)
            logger.debug(f"[Macro] DXY(근사): {result:.2f}")
            return result
    except Exception as e:
        logger.warning(f"[Macro] DXY 수집 실패: {e}")
    return None


# ── 시가총액 조회 (CoinGecko /coins/markets) ─────────────────────────────────

def fetch_market_caps(coin_ids: list, ttl: int = 3600) -> Optional[dict]:
    """
    CoinGecko API로 코인 시가총액 조회.
    coin_ids: CoinGecko ID 리스트 (예: ["bitcoin", "ethereum", "solana", ...])
    반환: {"BTC": 1.2e12, "ETH": 4.5e11, ...} (ticker → USD 시가총액)
    1시간 캐시.
    """
    cache_key = f"mcap_{'_'.join(sorted(coin_ids))}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": ",".join(coin_ids),
                "order": "market_cap_desc",
                "per_page": 50,
                "page": 1,
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        result = {
            item["symbol"].upper(): item["market_cap"]
            for item in r.json()
            if item.get("market_cap")
        }
        _set_cache(cache_key, result, ttl)
        logger.debug(f"[Macro] 시가총액 수집: {len(result)}개 코인")
        return result
    except Exception as e:
        logger.warning(f"[Macro] 시가총액 수집 실패: {e}")
        return None


# ── 시장 코드 변환 유틸 ───────────────────────────────────────────────────────

def market_to_okx_inst(market: str) -> str:
    """KRW-BTC → BTC-USDT-SWAP"""
    coin = market.replace("KRW-", "")
    return f"{coin}-USDT-SWAP"
