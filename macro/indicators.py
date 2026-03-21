"""
매크로 지표 → 확신도 보정값 변환 모듈

calc_macro_signal() 반환값:
  delta   : int   — -2 ~ +2 (음수 = 불리한 환경, 양수 = 유리한 환경)
  blocked : bool  — True이면 해당 신호 진입 차단
  reason  : str   — 로그용 설명 문자열

통합 위치: main.py _process_scalp() 내 Top Trader 신호 처리 직후
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


def calc_macro_signal(
    direction: str,
    market: str,
    finnhub_token: str = "",
    av_key: str = "",
) -> Tuple[int, bool, str]:
    """
    매크로 환경 기반 확신도 보정 계산.

    Parameters
    ----------
    direction      : "long" | "short"
    market         : KRW 기준 마켓명 (예: "KRW-BTC")
    finnhub_token  : Finnhub API 키 (없으면 VIX 건너뜀)
    av_key         : Alpha Vantage API 키 (없으면 DXY 건너뜀)

    Returns
    -------
    (delta, blocked, reason)
    """
    delta = 0
    blocked = False
    reasons: list[str] = []

    inst_id = market_to_okx_inst(market)  # e.g. "BTC-USDT-SWAP"

    # ── 1. Fear & Greed Index ─────────────────────────────────────────────────
    fgi = fetch_fear_greed()
    if fgi is not None:
        v = fgi["value"]
        if direction == "long":
            if v <= 20:
                # 극도 공포 → 역추종 매수 기회 (바닥 근처)
                delta += 1
                reasons.append(f"FGI극도공포({v})+1")
            elif v >= 80:
                # 극도 탐욕 → 과열 → 롱 추가 진입 차단
                blocked = True
                reasons.append(f"FGI극도탐욕({v}) 롱차단")
        elif direction == "short":
            if v >= 75:
                # 탐욕 구간 → 숏 기회 (과열 조정 임박)
                delta += 1
                reasons.append(f"FGI탐욕({v})+1")
            elif v <= 10:
                # 극도 공포 최심화 구간에서만 차단 (≤10), 10~20 구간은 하락 지속 가능
                blocked = True
                reasons.append(f"FGI극도공포({v}) 숏차단")

    if blocked:
        return (delta, blocked, " | ".join(reasons))

    # ── 2. OKX 펀딩비 ─────────────────────────────────────────────────────────
    fr = fetch_okx_funding_rate(inst_id)
    if fr is not None:
        if direction == "long" and fr > 0.001:
            # +0.1% 이상 = 롱 포지션 과열 → 신규 롱 진입 비용↑
            delta -= 1
            reasons.append(f"펀딩비과열({fr*100:.3f}%)-1")
        elif direction == "long" and fr < -0.0005:
            # 음수 펀딩 = 시장이 숏 우위 → 역추종 롱 기회
            delta += 1
            reasons.append(f"음수펀딩({fr*100:.3f}%) 롱+1")
        elif direction == "short" and fr < -0.001:
            # 음수 펀딩 = 숏 과열 → 숏 추가 진입 비용↑
            delta -= 1
            reasons.append(f"음수펀딩({fr*100:.3f}%)-1")

    # ── 3. Long/Short 비율 (롱계좌/숏계좌 비율. 1.0=균형) ─────────────────────
    ls = fetch_okx_long_short_ratio(inst_id)
    if ls is not None:
        if direction == "long" and ls > 1.8:
            # 롱이 숏보다 80% 이상 많음 = 군중이 과도하게 롱 → 역추종 주의
            delta -= 1
            reasons.append(f"롱편중(L/S={ls:.2f})-1")
        elif direction == "short" and ls < 0.6:
            # 숏이 롱보다 많음 = 숏 과열 → 추가 숏 주의
            delta -= 1
            reasons.append(f"숏편중(L/S={ls:.2f})-1")
        elif direction == "long" and ls < 0.75:
            # 숏 우위 → 역추종 롱 기회
            delta += 1
            reasons.append(f"숏우위(L/S={ls:.2f}) 롱+1")

    # ── 4. BTC 도미넌스 (BTC 이외 종목 매매 시만 적용, 티어별 임계값) ──────────
    # 메이저 알트 (ETH/BNB/SOL): BTC와 부분 독립 → 기준 완화
    # 마이너 알트 (DOGE, ADA, LINK 등): BTC 영향 더 강하게 받음 → 기준 엄격
    _MAJOR_ALTS = {"ETH", "BNB", "SOL"}
    coin = market.replace("KRW-", "")
    if coin not in ("BTC",):
        dom = fetch_btc_dominance()
        if dom is not None:
            if coin in _MAJOR_ALTS:
                dom_high, dom_low = 65, 45   # 메이저 알트: 완화된 기준
            else:
                dom_high, dom_low = 58, 48   # 마이너 알트: 엄격한 기준
            if direction == "long" and dom > dom_high:
                delta -= 1
                reasons.append(f"BTC도미넌스({dom:.1f}%>{dom_high}%)-1")
            elif direction == "long" and dom < dom_low:
                delta += 1
                reasons.append(f"알트시즌({dom:.1f}%<{dom_low}%)+1")

    # ── 5. VIX — 전통 시장 공포 지수 (Finnhub 키 필요) ───────────────────────
    if finnhub_token:
        vix = fetch_vix(finnhub_token)
        if vix is not None:
            if vix > 30:
                # 극도 공포 = 전통 시장 패닉 → 위험자산 전체 차단
                blocked = True
                reasons.append(f"VIX위험({vix:.1f}) 차단")
            elif vix > 20 and direction == "long":
                # 불안 국면 → 롱 신중
                delta -= 1
                reasons.append(f"VIX주의({vix:.1f})-1")
            elif vix < 15 and direction == "long":
                # 저변동성 = 안정적 상승 환경
                delta += 1
                reasons.append(f"VIX안정({vix:.1f})+1")

    if blocked:
        return (delta, blocked, " | ".join(reasons))

    # ── 6. DXY 달러인덱스 (Alpha Vantage 키 필요) ─────────────────────────────
    if av_key:
        dxy = fetch_dxy(av_key)
        if dxy is not None:
            if direction == "long" and dxy > 107:
                # 강달러 = 위험자산 하락 압력
                delta -= 1
                reasons.append(f"강달러DXY({dxy:.1f})-1")
            elif direction == "long" and dxy < 99:
                # 약달러 = 코인 우호적 환경
                delta += 1
                reasons.append(f"약달러DXY({dxy:.1f})+1")
            elif direction == "short" and dxy < 99:
                # 약달러 → 코인 지지 → 숏 약화
                delta -= 1
                reasons.append(f"약달러DXY({dxy:.1f}) 숏-1")

    reason_str = " | ".join(reasons) if reasons else "매크로중립"
    return (max(-2, min(2, delta)), blocked, reason_str)
