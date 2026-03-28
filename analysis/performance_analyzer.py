"""
성과 분석기 (Performance Analyzer)
────────────────────────────────────────────────────────
trades.log에서 거래 이력을 파싱하여:
  1. 종목 / Tier / 방향별 승률·수익률 분석
  2. 분석 결과를 analysis/reports/에 저장
  3. 성과 기반으로 config.yaml의 tier_size_pct 자동 조정

스케줄: 매일 03:00 (main.py에서 호출)
"""
import re
import json
import logging
import yaml
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

ROOT      = Path(__file__).parent.parent
LOG_FILE  = ROOT / "logs" / "trades.log"
REPORT_DIR = ROOT / "analysis" / "reports"
CONFIG_FILE = ROOT / "config" / "config.yaml"

REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ── 로그 파싱 정규식 ─────────────────────────────────────────────────────────

# 진입: [v2][daily][okx][KRW-XRP] 숏 진입 | conf=5 lev=2x size=227 @ 1.39 | SL=1.402 TP=1.360 | ...
_RE_OPEN = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*"
    r"\[v2\]\[(\w+)\]\[(\w+)\]\[(KRW-\w+)\]\s+(롱|숏) 진입.*"
    r"conf=(\d+).*lev=(\d+)x.*size=([\d.]+)\s*@\s*([\d.]+)"
    r"(?:.*SL=([\d.]+).*TP=([\d.]+))?"
)

# 청산: [PosMgr] CLOSE SHORT KRW-XRP tier=1h reason=SL(1.40) held=2.7h
_RE_CLOSE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*"
    r"\[PosMgr\] CLOSE (LONG|SHORT) (KRW-\w+)\s+"
    r"tier=(\w+)\s+reason=(\S+)\s+held=([\d.]+)h"
)

# 직접 P&L 기록: [TRADE] SHORT KRW-XRP tier=1h | entry=1.39 exit=1.36 pnl=+21.5 pnl_pct=+2.2% reason=TP
_RE_TRADE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*"
    r"\[TRADE\] (LONG|SHORT) (KRW-\w+)\s+tier=(\w+)\s*\|"
    r"\s*entry=([\d.]+)\s+exit=([\d.]+)\s+pnl=([+-][\d.]+)\s+pnl_pct=([+-][\d.]+)%\s+reason=(\S+)"
)


def parse_trades(days: int = 30) -> List[dict]:
    """
    trades.log에서 최근 N일 거래 기록 파싱.

    우선순위:
    1. [TRADE] 직접 기록 (정확한 P&L)
    2. [v2] OPEN + [PosMgr] CLOSE 쌍 매칭 (SL/TP 가격으로 추정 P&L)
    """
    if not LOG_FILE.exists():
        logger.warning(f"로그 파일 없음: {LOG_FILE}")
        return []

    cutoff = datetime.now() - timedelta(days=days)
    trades: List[dict] = []
    pending: Dict[str, dict] = {}   # key = "exchange:market:tier:direction"

    with open(LOG_FILE, encoding="utf-8") as f:
        for line in f:
            # ── [TRADE] 직접 기록 우선 ─────────────────────────────────────
            m = _RE_TRADE.search(line)
            if m:
                ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                if ts < cutoff:
                    continue
                trades.append({
                    "ts":       ts,
                    "direction": m.group(2).lower(),
                    "market":   m.group(3),
                    "tier":     m.group(4),
                    "entry":    float(m.group(5)),
                    "exit":     float(m.group(6)),
                    "pnl":      float(m.group(7)),
                    "pnl_pct":  float(m.group(8)),
                    "reason":   m.group(9),
                    "source":   "direct",
                })
                continue

            # ── OPEN 감지 ─────────────────────────────────────────────────
            m = _RE_OPEN.search(line)
            if m:
                ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                if ts < cutoff:
                    continue
                tier, exchange, market = m.group(2), m.group(3), m.group(4)
                direction = "long" if m.group(5) == "롱" else "short"
                key = f"{exchange}:{market}:{tier}:{direction}"
                pending[key] = {
                    "ts":        ts,
                    "tier":      tier,
                    "exchange":  exchange,
                    "market":    market,
                    "direction": direction,
                    "confluence": int(m.group(6)),
                    "leverage":  int(m.group(7)),
                    "size":      float(m.group(8)),
                    "entry":     float(m.group(9)),
                    "sl":        float(m.group(10)) if m.group(10) else None,
                    "tp":        float(m.group(11)) if m.group(11) else None,
                }
                continue

            # ── CLOSE 감지 → OPEN과 매칭 ─────────────────────────────────
            m = _RE_CLOSE.search(line)
            if m:
                ts    = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                direction = m.group(2).lower()
                market    = m.group(3)
                tier      = m.group(4)
                reason    = m.group(5)
                held      = float(m.group(6))

                # exchange를 pending에서 추론 (okx 또는 upbit)
                key = None
                for ex in ("okx", "upbit", "bithumb"):
                    k = f"{ex}:{market}:{tier}:{direction}"
                    if k in pending:
                        key = k
                        break
                if key is None:
                    continue

                open_info = pending.pop(key)
                entry = open_info["entry"]
                sl    = open_info.get("sl")
                tp    = open_info.get("tp")

                # P&L 추정
                if "TP" in reason and tp:
                    exit_price = tp
                    pnl_pct = (tp - entry) / entry * 100 * (1 if direction == "long" else -1)
                elif "SL" in reason and sl:
                    exit_price = sl
                    pnl_pct = (sl - entry) / entry * 100 * (1 if direction == "long" else -1)
                else:
                    # 시간 초과 등 — 추정 불가, 0으로 처리
                    exit_price = entry
                    pnl_pct = 0.0

                size = open_info["size"]
                pnl  = size * pnl_pct / 100 * open_info["leverage"]

                trades.append({
                    "ts":        ts,
                    "direction": direction,
                    "market":    market,
                    "tier":      tier,
                    "exchange":  open_info["exchange"],
                    "entry":     entry,
                    "exit":      exit_price,
                    "pnl":       round(pnl, 4),
                    "pnl_pct":   round(pnl_pct, 4),
                    "reason":    reason,
                    "held_h":    held,
                    "confluence": open_info.get("confluence", 0),
                    "leverage":  open_info.get("leverage", 1),
                    "source":    "inferred",
                })

    return trades


def analyze(trades: List[dict]) -> dict:
    """거래 목록을 종목/Tier/방향/시간대별로 집계."""
    if not trades:
        return {}

    def _agg(subset):
        if not subset:
            return {"count": 0, "win": 0, "win_rate": 0, "total_pnl": 0, "avg_pnl": 0}
        wins  = [t for t in subset if t["pnl"] > 0]
        total = sum(t["pnl"] for t in subset)
        return {
            "count":    len(subset),
            "win":      len(wins),
            "win_rate": round(len(wins) / len(subset) * 100, 1),
            "total_pnl": round(total, 2),
            "avg_pnl":  round(total / len(subset), 4),
        }

    # 전체
    result = {"total": _agg(trades), "by_market": {}, "by_tier": {}, "by_direction": {}, "by_hour": {}}

    # 종목별
    by_market = defaultdict(list)
    for t in trades:
        by_market[t["market"]].append(t)
    result["by_market"] = {k: _agg(v) for k, v in sorted(by_market.items(), key=lambda x: -abs(x[1][0]["pnl"] if x[1] else 0))}

    # Tier별
    by_tier = defaultdict(list)
    for t in trades:
        by_tier[t["tier"]].append(t)
    result["by_tier"] = {k: _agg(v) for k, v in by_tier.items()}

    # 방향별
    by_dir = defaultdict(list)
    for t in trades:
        by_dir[t["direction"]].append(t)
    result["by_direction"] = {k: _agg(v) for k, v in by_dir.items()}

    # 시간대별 (진입 시각의 시간)
    by_hour = defaultdict(list)
    for t in trades:
        by_hour[t["ts"].hour].append(t)
    result["by_hour"] = {str(k): _agg(v) for k, v in sorted(by_hour.items())}

    return result


def update_tier_weights(analysis: dict, min_trades: int = 3) -> Optional[dict]:
    """
    Tier별 성과(총 P&L)에 비례해서 tier_size_pct를 자동 조정.

    - 최소 min_trades 거래가 있는 Tier만 반영
    - 모든 Tier가 부진하면 변경 없음 (리스크 관리)
    - 반영 강도: 기존값 70% + 성과비중 30% (급격한 변화 방지)
    """
    by_tier = analysis.get("by_tier", {})
    if not by_tier:
        return None

    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"config 로드 실패: {e}")
        return None

    current = cfg.get("capital_allocation", {}).get("tier_size_pct", {})
    tiers   = ["daily", "4h", "1h", "15m"]
    defaults = {"daily": 0.25, "4h": 0.20, "1h": 0.20, "15m": 0.12}

    # 성과 점수 = avg_pnl × win_rate (충분한 거래 있는 Tier만)
    scores = {}
    for t in tiers:
        info = by_tier.get(t, {})
        if info.get("count", 0) >= min_trades:
            score = info.get("avg_pnl", 0) * (info.get("win_rate", 0) / 100)
            scores[t] = max(score, 0.001)  # 최소값 보장
        else:
            scores[t] = None

    valid = {k: v for k, v in scores.items() if v is not None}
    if len(valid) < 2:
        logger.info("[성과분석] 유효 데이터 Tier < 2, tier_size_pct 업데이트 생략")
        return None

    total_score = sum(valid.values())
    # 성과 비중 계산 (전체 sum = 기존 pct sum)
    current_sum = sum(defaults.get(t, 0.20) for t in tiers)

    new_pct = {}
    for t in tiers:
        base = current.get(t, defaults.get(t, 0.20))
        if t in valid:
            perf_share = valid[t] / total_score * current_sum
            # 급격한 변화 방지: 기존 70% + 성과 30%
            new_pct[t] = round(base * 0.7 + perf_share * 0.3, 3)
            # 범위 제한: 0.05 ~ 0.50
            new_pct[t] = max(0.05, min(0.50, new_pct[t]))
        else:
            new_pct[t] = base

    # config.yaml 업데이트
    cfg["capital_allocation"]["tier_size_pct"] = new_pct
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        logger.info(f"[성과분석] tier_size_pct 업데이트: {new_pct}")
        return new_pct
    except Exception as e:
        logger.error(f"config 저장 실패: {e}")
        return None


def run_daily_report(days: int = 7, update_config: bool = True) -> str:
    """
    일일 성과 분석 실행.
    - 최근 days일 거래 분석
    - 결과 JSON 저장
    - config 자동 업데이트 (update_config=True)
    - 요약 문자열 반환 (텔레그램 전송용)
    """
    logger.info(f"[성과분석] 최근 {days}일 분석 시작")
    trades   = parse_trades(days=days)
    analysis = analyze(trades)

    if not analysis:
        msg = f"[성과분석] 최근 {days}일 거래 없음"
        logger.info(msg)
        return msg

    # JSON 저장
    ts_str = datetime.now().strftime("%Y%m%d_%H%M")
    report_path = REPORT_DIR / f"perf_{ts_str}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({"generated": ts_str, "days": days, "trades_count": len(trades), "analysis": analysis}, f,
                  ensure_ascii=False, indent=2, default=str)
    logger.info(f"[성과분석] 리포트 저장: {report_path}")

    # config 업데이트
    new_weights = None
    if update_config:
        new_weights = update_tier_weights(analysis)

    # 요약 문자열 생성
    total  = analysis.get("total", {})
    bt     = analysis.get("by_tier", {})
    bm     = analysis.get("by_market", {})

    lines = [
        f"📊 성과 리포트 (최근 {days}일)",
        f"총 거래: {total.get('count', 0)}건 | 승률: {total.get('win_rate', 0)}% | 손익: {total.get('total_pnl', 0):+.2f} USDT",
        "",
        "▸ Tier별",
    ]
    for tier in ["daily", "4h", "1h", "15m"]:
        info = bt.get(tier, {})
        if info.get("count", 0):
            lines.append(
                f"  {tier:4s}: {info['count']}건 | 승률{info['win_rate']}% | {info['total_pnl']:+.2f} USDT"
            )
    lines.append("")
    lines.append("▸ 종목별 (Top 5)")
    sorted_markets = sorted(bm.items(), key=lambda x: x[1].get("total_pnl", 0), reverse=True)
    for market, info in sorted_markets[:5]:
        if info.get("count", 0):
            lines.append(
                f"  {market.replace('KRW-',''):6s}: {info['count']}건 | 승률{info['win_rate']}% | {info['total_pnl']:+.2f} USDT"
            )

    if new_weights:
        lines.append("")
        lines.append("▸ tier_size_pct 자동 조정 완료")
        for t, v in new_weights.items():
            lines.append(f"  {t}: {v:.3f}")

    summary = "\n".join(lines)
    logger.info(f"[성과분석] 완료\n{summary}")
    return summary
