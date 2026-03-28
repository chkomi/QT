"""
FastAPI 대시보드 서버

실행: python run_dashboard.py
URL:  http://localhost:8000
"""
import sys
import asyncio
import logging
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from fastapi import FastAPI, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

logger     = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).parent / "static"
LOG_PATH   = ROOT / "logs" / "trades.log"
BOT_LOG    = ROOT / "logs" / "trades.log"   # main.py 가 trades.log 에 기록

# 서버 시작 전에 static 디렉토리 보장
STATIC_DIR.mkdir(parents=True, exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from dashboard.log_parser import LogParser
    app.state.parser     = LogParser(str(LOG_PATH))
    app.state.aggregator = None

    # DataAggregator는 별도 스레드에서 초기화 (거래소 연결이 느려도 서버 시작 안 막힘)
    try:
        from dashboard.data_aggregator import DataAggregator
        app.state.aggregator = await asyncio.to_thread(DataAggregator)
        logger.info("DataAggregator 초기화 완료")
    except Exception as e:
        logger.error(f"DataAggregator 초기화 실패 (API 일부 비활성): {e}")

    yield


app = FastAPI(title="Quant Bot Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── 헬퍼 ─────────────────────────────────────────────────────────

def _agg(request: Request):
    agg = getattr(request.app.state, "aggregator", None)
    if agg is None:
        raise RuntimeError("거래소 연결 초기화 중입니다. 잠시 후 새로고침하세요.")
    return agg


# ── 라우터 ───────────────────────────────────────────────────────

@app.get("/")
async def index():
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        return JSONResponse({"error": "index.html 파일이 없습니다."}, status_code=404)
    return FileResponse(str(index_file))


@app.get("/api/health")
async def health(request: Request):
    parser = getattr(request.app.state, "parser", None)
    if parser is None:
        return {"status": "starting", "bot_running": False, "last_strategy_run": None}
    return parser.get_health()


@app.get("/api/portfolio")
async def portfolio(request: Request):
    try:
        return await asyncio.to_thread(_agg(request).get_portfolio)
    except RuntimeError as e:
        return JSONResponse({"error": str(e)}, status_code=503)


@app.get("/api/candles/{exchange}/{market}")
async def candles(
    request:  Request,
    exchange: str,
    market:   str,
    interval: str = Query("day"),
    count:    int = Query(200),
):
    try:
        return await asyncio.to_thread(
            _agg(request).get_candles, exchange, market, interval, count,
        )
    except RuntimeError as e:
        return JSONResponse({"error": str(e)}, status_code=503)


@app.get("/api/trades")
async def trades(
    request:  Request,
    limit:    int = Query(50),
    exchange: str = Query(None),
    market:   str = Query(None),
):
    parser = getattr(request.app.state, "parser", None)
    if parser is None:
        return {"trades": [], "total": 0}
    result = await asyncio.to_thread(parser.parse_trades, limit * 3)
    if exchange:
        result = [t for t in result if t["exchange"] == exchange]
    if market:
        result = [t for t in result if t["market"] == market]
    return {"trades": result[:limit], "total": len(result)}


@app.get("/api/risk")
async def risk(request: Request):
    try:
        return await asyncio.to_thread(_agg(request).get_risk_status)
    except RuntimeError as e:
        return JSONResponse({"error": str(e)}, status_code=503)


@app.get("/api/signals")
async def signals(request: Request):
    try:
        return await asyncio.to_thread(_agg(request).get_signals)
    except RuntimeError as e:
        return JSONResponse({"error": str(e)}, status_code=503)


@app.get("/api/positions")
async def positions(request: Request):
    """v2 PositionManager 기반 오픈 포지션 + OKX 총자산 + 일일 PnL."""
    import json, time

    pos_file = ROOT / "data" / "positions.json"
    result = {
        "total_equity_usdt": 0,
        "daily_pnl_usdt": 0,
        "daily_pnl_pct": 0,
        "open_count": 0,
        "max_positions": 14,
        "upbit_equity_krw": 0,
        "positions": [],
    }

    # OKX 총자산 조회
    agg = getattr(request.app.state, "aggregator", None)
    if agg:
        try:
            okx_ex = agg._exchanges.get("okx")
            upbit_ex = agg._exchanges.get("upbit")
            if okx_ex:
                result["total_equity_usdt"] = okx_ex.get_balance_quote()
            if upbit_ex:
                result["upbit_equity_krw"] = upbit_ex.get_balance_quote()
        except Exception as e:
            logger.warning(f"잔고 조회 실패: {e}")

    # positions.json 읽기
    if not pos_file.exists():
        return result

    try:
        raw = json.loads(pos_file.read_text(encoding="utf-8"))
    except Exception:
        return result

    now = time.time()
    pos_list = []

    for key, p in raw.items():
        entry = float(p.get("entry_price", 0))
        direction = p.get("direction", "long")
        market = p.get("market", "")

        # 현재가 조회
        current = 0
        if agg:
            try:
                okx_ex = agg._exchanges.get(p.get("exchange", "okx"))
                if okx_ex:
                    current = okx_ex.get_current_price(market) or 0
            except Exception:
                pass

        # PnL 계산
        volume = float(p.get("volume", 0))
        leverage = int(p.get("leverage", 1))
        if entry > 0 and current > 0:
            if direction == "long":
                pnl_pct = (current - entry) / entry * 100
                pnl_usdt = (current - entry) * volume * leverage
            else:
                pnl_pct = (entry - current) / entry * 100
                pnl_usdt = (entry - current) * volume * leverage
        else:
            pnl_pct = 0
            pnl_usdt = 0

        # SL-TP 진행 바
        atr_sl = p.get("atr_sl")
        atr_tp = p.get("atr_tp")
        sl_tp_progress = 0.5
        if atr_sl and atr_tp and current > 0:
            sl = float(atr_sl)
            tp = float(atr_tp)
            rng = abs(tp - sl)
            if rng > 0:
                if direction == "long":
                    sl_tp_progress = max(0, min(1, (current - sl) / rng))
                else:
                    sl_tp_progress = max(0, min(1, (sl - current) / rng))

        # 보유시간
        entry_time = float(p.get("entry_time", 0))
        holding_hours = (now - entry_time) / 3600 if entry_time > 0 else 0
        max_hold_sec = float(p.get("max_holding_seconds", 0))
        max_holding_hours = max_hold_sec / 3600 if max_hold_sec > 0 else 0

        # 투자금 추정
        invest = entry * volume if entry > 0 else 0

        pos_list.append({
            "key": key,
            "tier": p.get("tier", "daily"),
            "exchange": p.get("exchange", "okx"),
            "market": market,
            "direction": direction,
            "leverage": leverage,
            "confluence_score": int(p.get("confluence_score", 0)),
            "entry_price": round(entry, 6),
            "current_price": round(current, 6),
            "volume": volume,
            "invest_usdt": round(invest, 2),
            "unrealized_pnl_usdt": round(pnl_usdt, 2),
            "unrealized_pnl_pct": round(pnl_pct, 2),
            "atr_sl": float(atr_sl) if atr_sl else None,
            "atr_tp": float(atr_tp) if atr_tp else None,
            "sl_tp_progress": round(sl_tp_progress, 3),
            "holding_hours": round(holding_hours, 1),
            "max_holding_hours": round(max_holding_hours, 1),
        })

    result["positions"] = pos_list
    result["open_count"] = len(pos_list)
    return result


@app.get("/api/equity-history/{exchange}")
async def equity_history(request: Request, exchange: str):
    parser = getattr(request.app.state, "parser", None)
    if parser is None:
        return []
    return await asyncio.to_thread(parser.get_equity_history, exchange)


@app.get("/api/logs")
async def bot_logs(lines: int = Query(200)):
    import re
    log_path = BOT_LOG
    if not log_path.exists():
        return {"logs": []}

    LINE_RE = re.compile(
        r'^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+'
        r' \[(?P<level>\w+)\]'
        r' (?P<module>[\w.]+)'
        r' \u2014 (?P<msg>.+)$'
    )

    def _read():
        result = []
        try:
            with open(log_path, "rb") as f:
                f.seek(0, 2)
                pos = f.tell()
                buf = b""
                found = 0
                while pos > 0 and found < lines:
                    chunk = min(8192, pos)
                    pos -= chunk
                    f.seek(pos)
                    buf = f.read(chunk) + buf
                    while found < lines:
                        nl = buf.rfind(b"\n", 0, len(buf) - 1 if buf.endswith(b"\n") else len(buf))
                        if nl == -1:
                            break
                        raw = buf[nl + 1:].decode("utf-8", errors="replace").rstrip()
                        buf = buf[:nl + 1]
                        if raw:
                            m = LINE_RE.match(raw)
                            if m:
                                result.append({
                                    "ts":     m.group("ts"),
                                    "level":  m.group("level"),
                                    "module": m.group("module"),
                                    "msg":    m.group("msg"),
                                })
                            found += 1
                if buf:
                    raw = buf.decode("utf-8", errors="replace").rstrip()
                    if raw:
                        m = LINE_RE.match(raw)
                        if m:
                            result.append({
                                "ts":     m.group("ts"),
                                "level":  m.group("level"),
                                "module": m.group("module"),
                                "msg":    m.group("msg"),
                            })
        except Exception as e:
            logger.error(f"로그 읽기 오류: {e}")
        return list(reversed(result))

    logs = await asyncio.to_thread(_read)
    return {"logs": logs}
