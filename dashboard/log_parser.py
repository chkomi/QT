"""
trades.log 파서 — 거래 이벤트를 구조화된 데이터로 변환

실제 로그 포맷:
  2026-03-18 07:07:56,960 [INFO] main — [upbit][KRW-BTC] 롱 진입 | 14,828.00 KRW
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

# ── 거래 이벤트 패턴 ────────────────────────────────────────────
LONG_ENTRY_RE   = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 롱 진입 \| (?P<amt>[\d,.]+) (?P<cur>KRW|USDT)$')
LONG_CLOSE_RE   = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 롱 청산 \| (?P<pnl>[+-][\d.]+)%$')
LONG_SL_RE      = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 롱 손절$')
LONG_TP_RE      = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 롱 익절$')
SHORT_ENTRY_RE  = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 숏 진입 \| (?P<amt>[\d,.]+) USDT$')
SHORT_SL_RE     = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 숏 손절 \((?P<pnl>[+-][\d.]+)%\)$')
SHORT_TP_RE     = re.compile(r'^\[(?P<ex>\w+)\]\[(?P<mkt>[\w-]+)\] 숏 익절 \(\+(?P<pnl>[\d.]+)%\)$')
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

            if event:
                events.append(event)
                if len(events) >= limit:
                    break

        return events

    def get_last_strategy_run(self) -> Optional[str]:
        """최근 전략 실행 타임스탬프"""
        if not self._file_exists():
            return None
        for line in _read_lines_reversed(self.log_path, max_lines=1000):
            m = BASE_RE.match(line)
            if not m:
                continue
            if STRATEGY_RUN_RE.match(m.group("msg")):
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
        """봇 활성 여부 판단"""
        if not self._file_exists():
            return {"status": "no_log", "bot_running": False, "last_strategy_run": None}

        last_run = self.get_last_strategy_run()
        bot_running = False
        if last_run:
            last_dt = datetime.fromisoformat(last_run)
            diff_seconds = (datetime.now() - last_dt).total_seconds()
            bot_running = diff_seconds < 5400  # 90분 이내

        return {
            "status": "ok",
            "bot_running": bot_running,
            "last_strategy_run": last_run,
        }
