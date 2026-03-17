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
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

logger     = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).parent / "static"
LOG_PATH   = ROOT / "logs" / "trades.log"


@asynccontextmanager
async def lifespan(app: FastAPI):
    from dashboard.data_aggregator import DataAggregator
    from dashboard.log_parser      import LogParser

    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    app.state.aggregator = DataAggregator()
    app.state.parser     = LogParser(str(LOG_PATH))
    yield


app = FastAPI(title="Quant Bot Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── 라우터 ───────────────────────────────────────────────────────

@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/api/health")
async def health(request: Request):
    return request.app.state.parser.get_health()


@app.get("/api/portfolio")
async def portfolio(request: Request):
    return await asyncio.to_thread(request.app.state.aggregator.get_portfolio)


@app.get("/api/candles/{exchange}/{market}")
async def candles(
    request:  Request,
    exchange: str,
    market:   str,
    interval: str = Query("day"),
    count:    int = Query(200),
):
    return await asyncio.to_thread(
        request.app.state.aggregator.get_candles,
        exchange, market, interval, count,
    )


@app.get("/api/trades")
async def trades(
    request:  Request,
    limit:    int = Query(50),
    exchange: str = Query(None),
    market:   str = Query(None),
):
    result = await asyncio.to_thread(
        request.app.state.parser.parse_trades, limit * 3
    )
    if exchange:
        result = [t for t in result if t["exchange"] == exchange]
    if market:
        result = [t for t in result if t["market"] == market]
    return {"trades": result[:limit], "total": len(result)}


@app.get("/api/risk")
async def risk(request: Request):
    return await asyncio.to_thread(request.app.state.aggregator.get_risk_status)


@app.get("/api/signals")
async def signals(request: Request):
    return await asyncio.to_thread(request.app.state.aggregator.get_signals)


@app.get("/api/equity-history/{exchange}")
async def equity_history(request: Request, exchange: str):
    return await asyncio.to_thread(
        request.app.state.parser.get_equity_history, exchange
    )
