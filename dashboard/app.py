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


@app.get("/api/equity-history/{exchange}")
async def equity_history(request: Request, exchange: str):
    parser = getattr(request.app.state, "parser", None)
    if parser is None:
        return []
    return await asyncio.to_thread(parser.get_equity_history, exchange)
