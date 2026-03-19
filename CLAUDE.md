# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the live trading bot
python main.py

# Run the web dashboard (http://localhost:8000)
python run_dashboard.py

# Run backtest (2022-2024 on BTC/ETH)
python backtest/run_backtest.py

# Run parameter sweep & filter analysis
python backtest/run_strategy_analysis.py

# Export data to GitHub Pages (docs/data/)
python scripts/export_exchange_data.py
python scripts/push_local_data.py
```

## Architecture

This is a multi-exchange quantitative trading bot trading crypto with a volatility breakout strategy. Three "modes" run simultaneously:

1. **Swing (일봉, daily candles)** — `run_strategy()` every hour at :00
   - KRW exchanges (Upbit/Bithumb): spot long only (MA200 uptrend)
   - OKX: futures long + short (MA200 up = long, MA200 down + EMA aligned + N consecutive down candles = short)
   - Uses `strategy_longshort` (OKX) or `strategy_long` (KRW)
   - OKX markets: 시가총액 상위 20위 알트코인 (BTC/ETH/BNB/SOL/XRP/DOGE/ADA/AVAX/LINK/DOT/TON/MATIC/UNI/LTC/BCH/APT/NEAR/OP/ARB/SUI)
   - Asset weights: CoinGecko 시가총액 기반 동적 계산 (`refresh_asset_weights()`), 폴백 시 동일 비중

2. **Scalp (1H candles)** — `run_scalp_strategy()` every hour at :30
   - OKX only, 20 markets
   - Capital: OKX 잔고의 30% (`total_capital_ratio`) → 종목별 시가총액 가중치 비례 배분
   - Dynamic leverage 1x–5x based on confidence score (1–5)
   - Top Trader ratio: follow at 55–75%, fade at >75% extreme positioning
   - Force-close after 6 hours regardless of P&L

3. **Dashboard** — separate process via `run_dashboard.py`
   - FastAPI server (`dashboard/app.py`) serving a single-page JS frontend
   - `data_aggregator.py` provides real-time portfolio/candle data
   - GitHub Pages 정적 배포: `docs/` 폴더, GitHub Actions 30분마다 자동 갱신

### Key files

| File | Role |
|------|------|
| `main.py` | Entry point; position state, scheduling, signal→order logic, `sync_positions()` |
| `strategies/volatility_breakout.py` | Signal generation (long=1, short=2); confidence score 1–5 |
| `strategies/indicators.py` | EMA, Volume Profile, Fibonacci, ATR, Supertrend, MACD, BB, RSI divergence, OBV divergence, Hammer, Shooting Star, EMA Stack 13/21 |
| `engine/okx_exchange.py` | OKX CCXT wrapper; hedge mode requires `posSide=long/short` on every futures order |
| `engine/risk_manager.py` | Position sizing, SL/TP checks, daily loss limit |
| `macro/fetchers.py` | 매크로 지표 수집 (Fear&Greed, OKX 펀딩비/LS비율, BTC 도미넌스, VIX, DXY); TTL 캐시 |
| `macro/indicators.py` | 매크로 지표 → 확신도 보정 로직 |
| `config/config.yaml` | All strategy parameters, exchange allocation, backtest settings |
| `config/.env` | API keys (Upbit, Bithumb, OKX, Telegram) — never commit |
| `scripts/export_exchange_data.py` | GitHub Actions용 데이터 내보내기 |
| `scripts/push_local_data.py` | 로컬 거래 내역/잔액 → GitHub Pages 업데이트 |

### Signal system

`VolatilityBreakoutStrategy.generate_signals()` returns a DataFrame with:
- `signal=1` → long entry, `signal=2` → short entry (no `-1`/`-2` exit signals generated)
- `confidence` 1–5 → maps directly to leverage for scalp trades
- Swing exits are SL/TP price-based + trend-reversal check (EMA crossover or MA200 reclaim)

#### Confidence score boosters / penalties (워뇨띠 + 웅크웅크 + EmperorBTC)

| 조건 | 방향 | 효과 |
|------|------|------|
| 거래량 기준배수 2배 이상 | 롱 | +1 |
| 피보나치 레벨 근접 | 모두 | +1 |
| EMA 20/55 간격 1% 이상 | 모두 | +1 |
| MA200 대비 거리 5% 이상 | 모두 | +1 |
| 연속 하락 N+1봉 | 숏 | +1 |
| RSI 상승 다이버전스 | 롱 | +1 |
| RSI 하락 다이버전스 | 숏 | +1 |
| BB Squeeze | 모두 | +1 |
| 가격↑ + 거래량↓ | 롱 | -1 (EmperorBTC) |
| 가격↓ + 거래량↓ | 숏 | -1 (EmperorBTC) |
| OBV 하락 다이버전스 | 롱 | -1 (EmperorBTC) |
| OBV 상승 다이버전스 | 숏 | -1 (EmperorBTC) |
| Hammer 패턴 | 롱 | +1 (EmperorBTC) |
| Shooting Star 패턴 | 롱 | -1 (EmperorBTC) |
| EMA 13/21 Stack 상향 | 롱 | +1 (EmperorBTC) |
| EMA 13/21 Stack 하향 | 숏 | +1 (EmperorBTC) |
| EMA 13/21 Stack 하향 | 롱 | -1 (EmperorBTC) |

### OKX futures specifics

OKX account is in **hedge mode** (양방향 포지션). Every futures order must include `posSide="long"` or `posSide="short"` explicitly. `_init_futures_settings()` calls `set_leverage` for both sides. `SPOT_MAP` and `FUTURES_MAP` in `okx_exchange.py` define symbol translation from internal KRW keys (e.g. `KRW-SOL`) to CCXT symbols.

Weight caps (쏠림 방지):
- BTC 최대 30%, ETH 최대 20%, 알트코인 종목당 최대 8%

### Position state

In-memory dicts in `main.py`, each keyed by `ex_name → market`:
- `long_positions`, `short_positions` — swing positions
- `scalp_long_positions`, `scalp_short_positions` — scalp positions (also store `entry_time`, `leverage`)

`sync_positions()` reconciles state with live exchange balances on startup.

### Macro factors (`macro_factors` in config.yaml)

실시간 외부 데이터를 단타 확신도에 반영 (API 실패 시 자동 비활성화, 매매 중단 없음):
- **Fear & Greed Index** (alternative.me): 1시간 캐시
- **OKX 펀딩비/LS비율**: 5분 캐시
- **BTC 도미넌스**: CoinGecko 무료 API
- **VIX**: Finnhub (무료 토큰 필요, `macro_factors.finnhub_token`)
- **DXY**: Alpha Vantage (무료 토큰 필요, `macro_factors.alpha_vantage_key`)

### Configuration

- Exchange enable/disable and paper_trading mode: `config.yaml → exchanges`
- OKX markets (swing + scalp): `config.yaml → exchange_markets.okx` (20종목)
- Asset weights: `config.yaml → asset_weights` (runtime에 `refresh_asset_weights()`로 동적 채움)
- Swing strategy params: `config.yaml → strategies.volatility_breakout`
- Scalp params: `config.yaml → scalp_trading`
- SL/TP/daily loss: `config.yaml → trading` (swing) and `scalp_trading` (scalp override)
- Macro factors: `config.yaml → macro_factors`
