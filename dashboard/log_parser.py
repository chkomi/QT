"""
trades.log 파서 — 거래 이벤트를 구조화된 데이터로 변환

v1 로그 포맷:
  2026-03-18 07:07:56,960 [INFO] main — [upbit][KRW-BTC] 현물롱 진입 | 14,828.00 KRW
  2026-03-18 07:07:56,960 [INFO] main — [okx][KRW-SOL] 스윙 숏 진입 | 138.25 USDT

v2 로그 포맷:
  [INFO] main — [v2][1h][okx][KRW-XRP] 롱 진입 | conf=3 lev=1x size=100 @ 1.40 | SL=... TP=...
  [INFO] main — [TRADE] SHORT KRW-XRP tier=1h | entry=1.40 exit=1.35 pnl=+3.57 pnl_pct=+3.57% reason=TP(...)

구분자: — (em dash U+2014)
"""
import re
import os
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── 기본 파싱 패턴 ──────────────────────────────────────────────
BASE_RE = re.compile(
    r'^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})'
    r' \[(?P<level>\w+)\]'
    r' (?P<module>[\w.]+)'
    r' \u2014 '
    r'(?P<msg>.+)$'
)

# ── 스윙 거래 이벤트 패턴 ────────────────────────────────────────
# 롱 진입: [ex][mkt] 현물롱/선물롱 진입 | amt KRW|USDT
LONG_ENTRY_RE   = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] (?:현물롱|선물롱) 진입 \| (?P<amt>[\d,.]+) (?P<cur>KRW|USDT)$')
LONG_CLOSE_RE   = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 롱 청산 \| (?P<pnl>[+-][\d.]+)%$')
LONG_SL_RE      = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 롱 손절$')
LONG_TP_RE      = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 롱 익절$')
# 숏 진입: [ex][mkt] 스윙 숏 진입 | amt USDT
SHORT_ENTRY_RE       = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 스윙 숏 진입 \| (?P<amt>[\d,.]+) USDT$')
SHORT_SL_RE          = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 숏 손절 \((?P<pnl>[+-][\d.]+)%\)$')
SHORT_TP_RE          = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 숏 익절 \(\+(?P<pnl>[\d.]+)%\)$')
# 스윙 숏 추세반전 청산: [ex][mkt] 스윙 숏 추세반전 청산 [reason] | pnl%
SHORT_TREND_CLOSE_RE = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 스윙 숏 추세반전 청산 \[(?P<reason>[^\]]+)\] \| (?P<pnl>[+-][\d.]+)%$')

# ── 단타 거래 이벤트 패턴 ────────────────────────────────────────
# 단타 롱/숏 진입: [ex][mkt] 단타 롱/숏 진입 | amt USDT | Nx (확신도 N/5)
SCALP_LONG_ENTRY_RE       = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 단타 롱 진입 \| (?P<amt>[\d,.]+) USDT \| (?P<lev>\d+)x')
SCALP_SHORT_ENTRY_RE      = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 단타 숏 진입 \| (?P<amt>[\d,.]+) USDT \| (?P<lev>\d+)x')
SCALP_LONG_TIME_CLOSE_RE  = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 단타 롱 시간청산 \d+H \| (?P<pnl>[+-][\d.]+)%$')
SCALP_SHORT_TIME_CLOSE_RE = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 단타 숏 시간청산 \d+H \| (?P<pnl>[+-][\d.]+)%$')
SCALP_LONG_SL_RE          = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 단타 롱 손절$')
SCALP_LONG_TP_RE          = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 단타 롱 익절 \(\+(?P<pnl>[\d.]+)%\)$')
SCALP_SHORT_SL_RE         = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 단타 숏 손절$')
SCALP_SHORT_TP_RE         = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 단타 숏 익절 \(\+(?P<pnl>[\d.]+)%\)$')
PRICE_SIGNAL_RE = re.compile(
    r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 현재가: (?P<price>[\d,.]+)'
    r' \| MA200: (?P<ma200>[\d,.]+)'
    r' \| (?P<trend>[▲▼]\S+)'
    r' \| 신호: (?P<signal>-?\d+)$'
)
EQUITY_RE       = re.compile(r'^\[(?P<ex>\w+)\] 총 자산: (?P<equity>[\d,.]+) (?P<cur>KRW|USDT)$')
STRATEGY_RUN_RE = re.compile(r'^전략 실행 \| (?P<dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2})$')
SYNC_LONG_RE    = re.compile(r'^\[(?P<ex>\w+)\] 롱 동기화 \| (?P<mkt>[\w-]+) (?P<vol>[\d.]+)개 @ (?P<avg>[\d,.]+)$')
RISK_INIT_RE    = re.compile(r'^\[RiskManager\] 일일 초기화 \| 자본: (?P<capital>[\d,.]+)원$')
CAPITAL_BLOCK_RE = re.compile(r'^\[RiskManager\] 투자 가능 금액 부족')
DAILY_LIMIT_RE   = re.compile(r'거래 중단 상태|일일 손실 한도 초과|daily.*loss.*limit', re.IGNORECASE)
MACRO_BLOCK_RE   = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 단타 차단 \u2014 Macro: (?P<reason>.+)$')

# ── v2 거래 이벤트 패턴 ──────────────────────────────────────────
# 진입: [v2][tier][ex][mkt] 롱/숏 진입 | conf=N lev=Nx size=S @ P | SL=... TP=...
V2_ENTRY_RE = re.compile(
    r'^\[v2\]\[(?P<tier>\w+)\]\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] '
    r'(?P<side>롱|숏) 진입 \| '
    r'conf=(?P<conf>\d+) lev=(?P<lev>\d+)x size=(?P<size>[\d,.]+) @ (?P<price>[\d,.]+)'
)
# 청산 결과: [TRADE] LONG/SHORT KRW-XXX tier=T | entry=E exit=X pnl=P pnl_pct=PP% reason=R
V2_TRADE_RESULT_RE = re.compile(
    r'^\[TRADE\] (?P<side>LONG|SHORT) (?P<mkt>[\w-]+) tier=(?P<tier>\w+) \| '
    r'entry=(?P<entry>[\d.]+) exit=(?P<exit>[\d.]+) '
    r'pnl=(?P<pnl>[+-]?[\d.]+) pnl_pct=(?P<pnl_pct>[+-][\d.]+)% reason=(?P<reason>.+)$'
)
# v2 전략 실행: [v2][tier] ── 전략 실행 | datetime
V2_STRATEGY_RUN_RE = re.compile(r'^\[v2\]\[(?P<tier>\w+)\] \u2500\u2500 전략 실행 \| (?P<dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2})$')


def _parse_ts(ts_str: str) -> datetime:
    return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")


def _parse_float(s: str) -> float:
    return float(s.replace(",", "").rstrip("원"))


def _read_lines_reversed(filepath: str, max_lines: int = 5000):
    """파일을 역방향으로 읽기 — 대용량 로그 대응 (8KB 청크)"""
    with open(filepath, "rb") as f:
        f.seek(0, os.SEEK_END)
        file_size = f.tell()
        buf = b""
        pos = file_size
        lines_found = 0

        while pos > 0 and lines_found < max_lines:
            chunk_size = min(8192, pos)
            pos -= chunk_size
            f.seek(pos)
            chunk = f.read(chunk_size)
            buf = chunk + buf

            while lines_found < max_lines:
                end = len(buf) - 1 if buf.endswith(b"\n") else len(buf)
                nl_idx = buf.rfind(b"\n", 0, end)
                if nl_idx == -1:
                    break
                line = buf[nl_idx + 1:].decode("utf-8", errors="replace").rstrip("\n\r")
                buf = buf[:nl_idx + 1]
                if line:
                    yield line
                    lines_found += 1

        if buf:
            line = buf.decode("utf-8", errors="replace").rstrip("\n\r")
            if line:
                yield line


class LogParser:
    def __init__(self, log_path: str):
        self.log_path = log_path

    def _file_exists(self) -> bool:
        return os.path.exists(self.log_path)

    def parse_trades(self, limit: int = 100) -> list:
        """역방향 로그 파싱 → 거래 이벤트 목록 (최신순)"""
        if not self._file_exists():
            return []

        events = []
        for line in _read_lines_reversed(self.log_path, max_lines=20000):
            m = BASE_RE.match(line)
            if not m:
                continue

            module = m.group("module")
            msg    = m.group("msg")
            ts     = _parse_ts(m.group("ts"))
            ts_iso = ts.isoformat()
            ts_unix = int(ts.timestamp())

            event = None

            if module == "main":
                if mm := LONG_ENTRY_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "buy", "type": "long_entry",
                        "amount": _parse_float(mm.group("amt")),
                        "currency": mm.group("cur"), "pnl_pct": None,
                    }
                elif mm := LONG_CLOSE_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "sell", "type": "long_close",
                        "amount": None, "currency": None,
                        "pnl_pct": float(mm.group("pnl")),
                    }
                elif mm := LONG_SL_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "sell", "type": "stop_loss",
                        "amount": None, "currency": None, "pnl_pct": None,
                    }
                elif mm := LONG_TP_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "sell", "type": "take_profit",
                        "amount": None, "currency": None, "pnl_pct": None,
                    }
                elif mm := SHORT_ENTRY_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "short", "type": "short_entry",
                        "amount": _parse_float(mm.group("amt")),
                        "currency": "USDT", "pnl_pct": None,
                    }
                elif mm := SHORT_SL_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "cover", "type": "short_stop_loss",
                        "amount": None, "currency": None,
                        "pnl_pct": float(mm.group("pnl")),
                    }
                elif mm := SHORT_TP_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "cover", "type": "short_take_profit",
                        "amount": None, "currency": None,
                        "pnl_pct": float(mm.group("pnl")),
                    }
                elif mm := SHORT_TREND_CLOSE_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "cover", "type": "short_trend_close",
                        "amount": None, "currency": None,
                        "pnl_pct": float(mm.group("pnl")),
                    }
                # ── 단타 ─────────────────────────────────────────
                elif mm := SCALP_LONG_ENTRY_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "buy", "type": "scalp_long_entry",
                        "amount": _parse_float(mm.group("amt")),
                        "currency": "USDT", "pnl_pct": None,
                    }
                elif mm := SCALP_SHORT_ENTRY_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "short", "type": "scalp_short_entry",
                        "amount": _parse_float(mm.group("amt")),
                        "currency": "USDT", "pnl_pct": None,
                    }
                elif mm := SCALP_LONG_TIME_CLOSE_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "sell", "type": "scalp_long_time_close",
                        "amount": None, "currency": None,
                        "pnl_pct": float(mm.group("pnl")),
                    }
                elif mm := SCALP_SHORT_TIME_CLOSE_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "cover", "type": "scalp_short_time_close",
                        "amount": None, "currency": None,
                        "pnl_pct": float(mm.group("pnl")),
                    }
                elif mm := SCALP_LONG_SL_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "sell", "type": "scalp_stop_loss",
                        "amount": None, "currency": None, "pnl_pct": None,
                    }
                elif mm := SCALP_LONG_TP_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "sell", "type": "scalp_take_profit",
                        "amount": None, "currency": None,
                        "pnl_pct": float(mm.group("pnl")),
                    }
                elif mm := SCALP_SHORT_SL_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "cover", "type": "scalp_short_stop_loss",
                        "amount": None, "currency": None, "pnl_pct": None,
                    }
                elif mm := SCALP_SHORT_TP_RE.match(msg):
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "side": "cover", "type": "scalp_short_take_profit",
                        "amount": None, "currency": None,
                        "pnl_pct": float(mm.group("pnl")),
                    }
                # ── v2 진입 이벤트 ───────────────────────────────
                elif mm := V2_ENTRY_RE.match(msg):
                    side = mm.group("side")
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": mm.group("ex"), "market": mm.group("mkt"),
                        "tier": mm.group("tier"),
                        "side": "buy" if side == "롱" else "short",
                        "type": "v2_long_entry" if side == "롱" else "v2_short_entry",
                        "amount": _parse_float(mm.group("size")),
                        "currency": "USDT", "pnl_pct": None,
                    }
                # ── v2 청산 결과 ──────────────────────────────────
                elif mm := V2_TRADE_RESULT_RE.match(msg):
                    side = mm.group("side")
                    reason = mm.group("reason")
                    if "SL" in reason:
                        trade_type = "v2_stop_loss"
                    elif "TP" in reason:
                        trade_type = "v2_take_profit"
                    else:
                        trade_type = "v2_close"
                    event = {
                        "timestamp": ts_iso, "timestamp_unix": ts_unix,
                        "exchange": None, "market": mm.group("mkt"),
                        "tier": mm.group("tier"),
                        "side": "sell" if side == "LONG" else "cover",
                        "type": trade_type,
                        "amount": None, "currency": "USDT",
                        "pnl_pct": float(mm.group("pnl_pct")),
                        "reason": reason,
                    }

            if event:
                events.append(event)
                if len(events) >= limit:
                    break

        return events

    def get_last_strategy_run(self) -> Optional[str]:
        """최근 전략 실행 타임스탬프 (v1 + v2 포맷 지원)"""
        if not self._file_exists():
            return None
        for line in _read_lines_reversed(self.log_path, max_lines=1000):
            m = BASE_RE.match(line)
            if not m:
                continue
            msg = m.group("msg")
            if STRATEGY_RUN_RE.match(msg) or V2_STRATEGY_RUN_RE.match(msg):
                return _parse_ts(m.group("ts")).isoformat()
        return None

    def get_equity_history(self, exchange: str) -> list:
        """자산 기록 (자산 곡선 차트용) — [{time, equity, currency}]"""
        if not self._file_exists():
            return []
        results = []
        for line in _read_lines_reversed(self.log_path, max_lines=5000):
            m = BASE_RE.match(line)
            if not m or m.group("module") != "main":
                continue
            mm = EQUITY_RE.match(m.group("msg"))
            if mm and mm.group("ex") == exchange:
                ts = _parse_ts(m.group("ts"))
                results.append({
                    "time": int(ts.timestamp()),
                    "equity": _parse_float(mm.group("equity")),
                    "currency": mm.group("cur"),
                })
        return list(reversed(results))

    def get_recent_signals(self) -> list:
        """최근 가격/신호 로그 [{exchange, market, price, ma200, trend, signal, timestamp}]"""
        if not self._file_exists():
            return []
        results = []
        seen = set()
        for line in _read_lines_reversed(self.log_path, max_lines=2000):
            m = BASE_RE.match(line)
            if not m or m.group("module") != "main":
                continue
            mm = PRICE_SIGNAL_RE.match(m.group("msg"))
            if mm:
                key = (mm.group("ex"), mm.group("mkt"))
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "exchange": mm.group("ex"),
                        "market": mm.group("mkt"),
                        "price": _parse_float(mm.group("price")),
                        "ma200": _parse_float(mm.group("ma200")),
                        "trend": mm.group("trend"),
                        "signal": int(mm.group("signal")),
                        "timestamp": _parse_ts(m.group("ts")).isoformat(),
                    })
        return results

    def get_health(self) -> dict:
        """봇 활성 여부 + 거래 차단 사유 판단"""
        if not self._file_exists():
            return {"status": "no_log", "bot_running": False, "last_strategy_run": None,
                    "trading_blocked": False, "blocked_reason": None, "macro_blocks": []}

        last_run = self.get_last_strategy_run()
        bot_running = False
        if last_run:
            last_dt = datetime.fromisoformat(last_run)
            diff_seconds = (datetime.now() - last_dt).total_seconds()
            bot_running = diff_seconds < 5400  # 90분 이내

        # 최근 500줄에서 차단 사유 파싱
        trading_blocked = False
        blocked_reason  = None
        macro_blocks: dict = {}  # mkt → reason (최신 1개만)

        for line in _read_lines_reversed(self.log_path, max_lines=500):
            m = BASE_RE.match(line)
            if not m:
                continue
            msg = m.group("msg")
            level = m.group("level")

            if not trading_blocked and level == "WARNING":
                if CAPITAL_BLOCK_RE.match(msg):
                    trading_blocked = True
                    blocked_reason = "자본 부족 (최소 주문금액 미달)"
                elif DAILY_LIMIT_RE.search(msg):
                    trading_blocked = True
                    blocked_reason = "일일 손실 한도 초과"

            mm = MACRO_BLOCK_RE.match(msg)
            if mm:
                mkt = mm.group("mkt")
                if mkt not in macro_blocks:
                    macro_blocks[mkt] = mm.group("reason")

        return {
            "status": "ok",
            "bot_running": bot_running,
            "last_strategy_run": last_run,
            "trading_blocked": trading_blocked,
            "blocked_reason": blocked_reason,
            "macro_blocks": [
                {"market": mkt, "reason": reason}
                for mkt, reason in macro_blocks.items()
            ],
        }
