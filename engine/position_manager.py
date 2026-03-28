"""
포지션 매니저 (Multi-Timeframe v2)

Tier별 포지션 추적, 충돌 해결, 디스크 직렬화(크래시 복구).
기존 main.py의 in-memory dict (long_positions, short_positions 등)를 대체.
"""
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

_POS_FILE = Path(__file__).parent.parent / "data" / "positions.json"


class Position:
    """단일 포지션 데이터."""

    __slots__ = (
        "tier", "exchange", "market", "direction",
        "entry_price", "volume", "leverage",
        "atr_sl", "atr_tp", "confluence_score",
        "entry_time", "max_holding_seconds",
    )

    def __init__(self, **kw):
        self.tier: str = kw.get("tier", "daily")
        self.exchange: str = kw.get("exchange", "")
        self.market: str = kw.get("market", "")
        self.direction: str = kw.get("direction", "long")  # "long" | "short"
        self.entry_price: float = kw.get("entry_price", 0.0)
        self.volume: float = kw.get("volume", 0.0)
        self.leverage: int = kw.get("leverage", 1)
        self.atr_sl: Optional[float] = kw.get("atr_sl")
        self.atr_tp: Optional[float] = kw.get("atr_tp")
        self.confluence_score: int = kw.get("confluence_score", 0)
        self.entry_time: float = kw.get("entry_time", 0.0) or time.time()
        self.max_holding_seconds: float = kw.get("max_holding_seconds", 0)

    @property
    def key(self) -> str:
        return f"{self.exchange}:{self.market}:{self.tier}:{self.direction}"

    @property
    def holding_hours(self) -> float:
        return (time.time() - self.entry_time) / 3600

    @property
    def is_expired(self) -> bool:
        if self.max_holding_seconds <= 0:
            return False
        return (time.time() - self.entry_time) >= self.max_holding_seconds

    def to_dict(self) -> dict:
        return {s: getattr(self, s) for s in self.__slots__}

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        return cls(**d)


_COOLDOWN_FILE = Path(__file__).parent.parent / "data" / "sl_cooldowns.json"

# Tier별 SL 쿨다운 시간 (초)
_SL_COOLDOWN_SECS: Dict[str, int] = {
    "daily": 172800,   # 2일
    "4h":    28800,    # 8시간
    "1h":    10800,    # 3시간 (같은 방향 XRP 재진입 방지)
    "15m":   1800,     # 30분
}


class PositionManager:
    """Tier별 포지션 추적 + 충돌 해결 + 디스크 직렬화."""

    def __init__(self, persistence_path: Optional[Path] = None):
        self._positions: Dict[str, Position] = {}
        self._path = persistence_path or _POS_FILE
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._cooldowns: Dict[str, float] = {}   # key → 쿨다운 만료 timestamp
        self._cooldown_path = _COOLDOWN_FILE
        self._load()
        self._load_cooldowns()

    # ── CRUD ──────────────────────────────────────────────────

    def open(self, pos: Position) -> None:
        self._positions[pos.key] = pos
        logger.info(
            f"[PosMgr] OPEN {pos.direction.upper()} {pos.market} "
            f"tier={pos.tier} lev={pos.leverage}x conf={pos.confluence_score} "
            f"@ {pos.entry_price:,.2f}"
        )
        self._save()

    def close(self, key: str, reason: str = "") -> Optional[Position]:
        pos = self._positions.pop(key, None)
        if pos:
            logger.info(
                f"[PosMgr] CLOSE {pos.direction.upper()} {pos.market} "
                f"tier={pos.tier} reason={reason} held={pos.holding_hours:.1f}h"
            )
            self._save()
        return pos

    def get(self, key: str) -> Optional[Position]:
        return self._positions.get(key)

    def get_by_market(self, exchange: str, market: str) -> List[Position]:
        return [p for p in self._positions.values()
                if p.exchange == exchange and p.market == market]

    def get_by_tier(self, tier: str) -> List[Position]:
        return [p for p in self._positions.values() if p.tier == tier]

    def get_by_exchange(self, exchange: str) -> List[Position]:
        return [p for p in self._positions.values() if p.exchange == exchange]

    def all_positions(self) -> List[Position]:
        return list(self._positions.values())

    def all_as_dicts(self) -> List[dict]:
        """CapitalAllocator.can_open_position() 호환 형식."""
        return [{"tier": p.tier, "direction": p.direction, "market": p.market,
                 "exchange": p.exchange} for p in self._positions.values()]

    def has_position(self, exchange: str, market: str, tier: str, direction: str) -> bool:
        key = f"{exchange}:{market}:{tier}:{direction}"
        return key in self._positions

    def has_any_position(self, exchange: str, market: str, direction: str) -> bool:
        """Tier 무관하게 같은 종목·방향 포지션 존재 여부."""
        return any(
            p.exchange == exchange and p.market == market and p.direction == direction
            for p in self._positions.values()
        )

    def has_opposite(self, exchange: str, market: str, direction: str) -> bool:
        """같은 종목에 반대 방향 포지션 존재 여부."""
        opp = "short" if direction == "long" else "long"
        return self.has_any_position(exchange, market, opp)

    def count_total(self) -> int:
        return len(self._positions)

    def count_by_tier(self, tier: str) -> int:
        return sum(1 for p in self._positions.values() if p.tier == tier)

    def count_direction(self, direction: str) -> int:
        return sum(1 for p in self._positions.values() if p.direction == direction)

    def expired_positions(self) -> List[Position]:
        return [p for p in self._positions.values() if p.is_expired]

    # ── SL 쿨다운 (동일 종목/방향 재진입 방지) ───────────────────

    def record_sl_hit(self, key: str, tier: str) -> None:
        """SL 손절 발생 시 쿨다운 등록."""
        secs = _SL_COOLDOWN_SECS.get(tier, 3600)
        self._cooldowns[key] = time.time() + secs
        logger.info(f"[PosMgr] SL 쿨다운 등록: {key} ({secs//3600:.1f}h)")
        self._save_cooldowns()

    def is_in_sl_cooldown(self, key: str) -> bool:
        """해당 key가 쿨다운 중이면 True."""
        expiry = self._cooldowns.get(key)
        if expiry is None:
            return False
        if time.time() < expiry:
            return True
        # 만료 → 제거
        del self._cooldowns[key]
        self._save_cooldowns()
        return False

    def _save_cooldowns(self) -> None:
        try:
            self._cooldown_path.write_text(
                json.dumps(self._cooldowns, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"[PosMgr] 쿨다운 저장 실패: {e}")

    def _load_cooldowns(self) -> None:
        if not self._cooldown_path.exists():
            return
        try:
            self._cooldowns = json.loads(self._cooldown_path.read_text(encoding="utf-8"))
            now = time.time()
            self._cooldowns = {k: v for k, v in self._cooldowns.items() if v > now}
        except Exception:
            self._cooldowns = {}

    # ── 동기화 (거래소 실제 포지션과 맞추기) ────────────────────

    def sync_clear(self, exchange: str) -> None:
        """특정 거래소의 모든 포지션 초기화 (sync 전 호출)."""
        keys_to_remove = [k for k, p in self._positions.items() if p.exchange == exchange]
        for k in keys_to_remove:
            del self._positions[k]
        if keys_to_remove:
            self._save()

    # ── 직렬화 ────────────────────────────────────────────────

    def _save(self) -> None:
        try:
            data = {k: p.to_dict() for k, p in self._positions.items()}
            self._path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logger.error(f"[PosMgr] 저장 실패: {e}")

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            for k, d in data.items():
                self._positions[k] = Position.from_dict(d)
            logger.info(f"[PosMgr] {len(self._positions)}개 포지션 복구")
        except Exception as e:
            logger.warning(f"[PosMgr] 로드 실패 (초기화): {e}")
            self._positions = {}

    def get_status(self) -> dict:
        """대시보드 API용 상태 요약."""
        tiers = {}
        for p in self._positions.values():
            t = tiers.setdefault(p.tier, {"long": 0, "short": 0, "markets": []})
            t[p.direction] += 1
            t["markets"].append(p.market)
        return {
            "total": len(self._positions),
            "tiers": tiers,
            "positions": [p.to_dict() for p in self._positions.values()],
        }
