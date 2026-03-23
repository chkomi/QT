"""
매크로 지표 → 확신도 보정값 변환 모듈 (Multi-Timeframe v2)

v2 변경: blocked 반환 제거. 모든 매크로 요소는 delta(점수 조정)만 반환.
극단 상황에서도 거래를 완전 차단하지 않고, Confluence Score로 자연 조절.

calc_macro_signal() 반환값:
  delta   : int   — -3 ~ +3 (음수 = 불리한 환경, 양수 = 유리한 환경)
  reason  : str   — 로그용 설명 문자열

tier별 민감도:
  daily/4h: FGI, BTC도미넌스, DXY만 적용 (장기 요소)
  1h/15m:   전체 적용 (FGI, 펀딩비, L/S비율, 도미넌스, VIX, DXY)
"""
import logging
from typing import Tuple
from macro.fetchers import (
    fetch_fear_greed,
    fetch_btc_dominance,
    fetch_okx_funding_rate,
    fetch_okx_long_short_ratio,
    fetch_vix,
    fetch_dxy,
    market_to_okx_inst,
)

logger = logging.getLogger(__name__)

# 장기 요소만 사용하는 Tier
_LONG_TERM_TIERS = {"daily", "4h"}


def calc_macro_signal(
    direction: str,
    market: str,
    finnhub_token: str = "",
    av_key: str = "",
    tier: str = "1h",
) -> Tuple[int, str]:
    """
    매크로 환경 기반 확신도 보정 계산 (v2: 차단 없음).

    Parameters
    ----------
    direction      : "long" | "short"
    market         : KRW 기준 마켓명 (예: "KRW-BTC")
    finnhub_token  : Finnhub API 키
    av_key         : Alpha Vantage API 키
    tier           : "daily" | "4h" | "1h" | "15m"

    Returns
    -------
    (delta: int, reason: str)
    """
    delta = 0
    reasons: list[str] = []
    is_long_term = tier in _LONG_TERM_TIERS

    inst_id = market_to_okx_inst(market)

    # ── 1. Fear & Greed Index (모든 Tier 적용) ───────────────────────────
    fgi = fetch_fear_greed()
    if fgi is not None:
        v = fgi["value"]
        if direction == "long":
            if v <= 20:
                delta += 1
                reasons.append(f"FGI공포({v})+1")
            elif v >= 80:
                delta -= 2
                reasons.append(f"FGI탐욕({v})-2")
        elif direction == "short":
            if v >= 75:
                delta += 1
                reasons.append(f"FGI탐욕({v})+1")
            elif v <= 10:
                delta -= 2
                reasons.append(f"FGI극도공포({v})-2")

    # ── 2. OKX 펀딩비 (1h/15m만) ────────────────────────────────────────
    if not is_long_term:
        fr = fetch_okx_funding_rate(inst_id)
        if fr is not None:
            if direction == "long" and fr > 0.001:
                delta -= 1
                reasons.append(f"펀딩비과열({fr*100:.3f}%)-1")
            elif direction == "long" and fr < -0.0005:
                delta += 1
                reasons.append(f"음수펀딩({fr*100:.3f}%)+1")
            elif direction == "short" and fr < -0.001:
                delta -= 1
                reasons.append(f"음수펀딩({fr*100:.3f}%)-1")

    # ── 3. Long/Short 비율 (1h/15m만) ───────────────────────────────────
    if not is_long_term:
        ls = fetch_okx_long_short_ratio(inst_id)
        if ls is not None:
            if direction == "long" and ls > 1.8:
                delta -= 1
                reasons.append(f"롱편중(L/S={ls:.2f})-1")
            elif direction == "short" and ls < 0.6:
                delta -= 1
                reasons.append(f"숏편중(L/S={ls:.2f})-1")
            elif direction == "long" and ls < 0.75:
                delta += 1
                reasons.append(f"숏우위(L/S={ls:.2f})+1")

    # ── 4. BTC 도미넌스 (BTC 이외, 모든 Tier) ───────────────────────────
    _MAJOR_ALTS = {"ETH", "BNB", "SOL"}
    coin = market.replace("KRW-", "")
    if coin != "BTC":
        dom = fetch_btc_dominance()
        if dom is not None:
            if coin in _MAJOR_ALTS:
                dom_high, dom_low = 65, 45
            else:
                dom_high, dom_low = 58, 48
            if direction == "long" and dom > dom_high:
                delta -= 1
                reasons.append(f"BTC도미넌스({dom:.1f}%>{dom_high}%)-1")
            elif direction == "long" and dom < dom_low:
                delta += 1
                reasons.append(f"알트시즌({dom:.1f}%<{dom_low}%)+1")

    # ── 5. VIX (모든 Tier — 극단 시 감점, 차단 없음) ────────────────────
    if finnhub_token:
        vix = fetch_vix(finnhub_token)
        if vix is not None:
            if vix > 30:
                delta -= 2
                reasons.append(f"VIX위험({vix:.1f})-2")
            elif vix > 20 and direction == "long":
                delta -= 1
                reasons.append(f"VIX주의({vix:.1f})-1")
            elif vix < 15 and direction == "long":
                delta += 1
                reasons.append(f"VIX안정({vix:.1f})+1")

    # ── 6. DXY (daily/4h만) ─────────────────────────────────────────────
    if is_long_term and av_key:
        dxy = fetch_dxy(av_key)
        if dxy is not None:
            if direction == "long" and dxy > 107:
                delta -= 1
                reasons.append(f"강달러DXY({dxy:.1f})-1")
            elif direction == "long" and dxy < 99:
                delta += 1
                reasons.append(f"약달러DXY({dxy:.1f})+1")
            elif direction == "short" and dxy < 99:
                delta -= 1
                reasons.append(f"약달러DXY({dxy:.1f})-1")

    reason_str = " | ".join(reasons) if reasons else "매크로중립"
    return (max(-3, min(3, delta)), reason_str)
