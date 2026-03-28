"""
자본 배분 모듈 (Multi-Timeframe v2)

공유 풀 방식: 전체 자본을 공유하되, 동시 포지션 수와 종목당 최대 노출로 리스크 제한.
Confluence Score에 따라 포지션 크기와 레버리지를 동적 조절.
"""
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class CapitalAllocator:
    """
    Parameters
    ----------
    config : config.yaml 전체 dict
    """

    def __init__(self, config: dict):
        ca = config.get("capital_allocation", {})
        conf = config.get("confluence", {})

        self.max_total_positions = ca.get("max_total_positions", 8)
        self.max_per_asset_pct = ca.get("max_per_asset_pct", 0.25)
        self.max_single_position_pct = ca.get("max_single_position_pct", 0.15)
        self.same_direction_limit_pct = ca.get("same_direction_limit_pct", 0.80)
        self.tier_position_limits: Dict[str, int] = ca.get("tier_position_limits", {
            "daily": 3, "4h": 3, "1h": 3, "15m": 2,
        })
        self.tier_size_pct: Dict[str, float] = ca.get("tier_size_pct", {
            "daily": 0.15, "4h": 0.12, "1h": 0.10, "15m": 0.08,
        })

        # Confluence → size multiplier / leverage 매핑
        raw_size = conf.get("score_to_size", {3: 0.5, 5: 0.75, 7: 1.0})
        raw_lev = conf.get("score_to_leverage", {3: 1, 5: 2, 7: 3, 9: 5})
        # yaml에서 키가 int로 파싱되므로 int 변환 보장
        self.score_to_size = {int(k): v for k, v in raw_size.items()}
        self.score_to_leverage = {int(k): v for k, v in raw_lev.items()}

    def _lookup(self, table: dict, score: int) -> float:
        """점수 이하 최대 키 매칭 (예: score=6, table={3:0.5, 5:0.75, 7:1.0} → 0.75)"""
        val = 0
        for threshold in sorted(table.keys()):
            if score >= threshold:
                val = table[threshold]
        return val

    def calc_position_size(
        self,
        equity: float,
        asset_weight: float,
        tier: str,
        confluence_score: int,
        min_order: float = 5.0,
    ) -> float:
        """
        포지션 크기 계산.

        Returns
        -------
        투자 금액 (USDT 또는 KRW). 0이면 진입 불가.
        """
        tier_pct = self.tier_size_pct.get(tier, 0.10)
        base_size = equity * asset_weight * tier_pct
        size_mult = self._lookup(self.score_to_size, confluence_score)
        final = base_size * size_mult

        # 최대 제한
        max_single = equity * self.max_single_position_pct
        max_asset = equity * self.max_per_asset_pct
        final = min(final, max_single, max_asset)

        if final < min_order:
            return 0.0
        return round(final, 2)

    def calc_leverage(self, tier: str, confluence_score: int, tier_max_lev: int = 5) -> int:
        """Confluence 점수 → 레버리지 결정."""
        lev = int(self._lookup(self.score_to_leverage, confluence_score))
        return max(1, min(lev, tier_max_lev))

    def can_open_position(
        self,
        tier: str,
        direction: str,
        market: str,
        open_positions: list,
        exchange: str = None,
    ) -> Tuple[bool, str]:
        """
        신규 포지션 개시 가능 여부 판단.

        Parameters
        ----------
        open_positions : [{"tier": str, "direction": str, "market": str, ...}, ...]
        exchange       : 거래소 이름 (okx/upbit 등). 지정 시 Tier 한도를 거래소별로 독립 계산.

        Returns
        -------
        (allowed: bool, reason: str)
        """
        total_open = len(open_positions)
        if total_open >= self.max_total_positions:
            return False, f"전체 포지션 한도 초과 ({total_open}/{self.max_total_positions})"

        # Tier 한도: 거래소별 + 방향별로 독립 계산
        # (OKX 1h long 3개 있어도 OKX 1h short는 별도 한도 적용)
        if exchange:
            tier_count = sum(
                1 for p in open_positions
                if p["tier"] == tier
                and p.get("exchange") == exchange
                and p.get("direction") == direction
            )
        else:
            tier_count = sum(
                1 for p in open_positions
                if p["tier"] == tier and p.get("direction") == direction
            )
        tier_limit = self.tier_position_limits.get(tier, 3)
        if tier_count >= tier_limit:
            return False, f"Tier [{tier}] {direction} 포지션 한도 초과 ({tier_count}/{tier_limit})"

        # 같은 종목 반대 방향 금지 (소자본 규칙)
        for p in open_positions:
            if p["market"] == market and p["direction"] != direction:
                return False, f"같은 종목 반대 방향 금지 ({market} 기존={p['direction']})"

        # 방향 편중 제한 (80% 이상 같은 방향이면 추가 차단)
        if total_open > 0:
            same_dir = sum(1 for p in open_positions if p["direction"] == direction)
            if same_dir / total_open > self.same_direction_limit_pct and total_open >= 3:
                return False, f"방향 편중 ({direction} {same_dir}/{total_open} >= {self.same_direction_limit_pct:.0%})"

        return True, "OK"
