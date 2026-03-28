# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the live trading bot (Multi-Timeframe v2)
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

## Architecture — Multi-Timeframe v2

멀티 거래소 암호화폐 자동매매 봇. 4개 타임프레임에서 롱·숏 양방향 거래. **Confluence Score(0-10)** 기반 진입 판단으로 매크로 차단 없이 모든 시장 상황에서 기회를 찾는다.

### Tier 구조 (동시 실행)

```
DAILY (1D)  ──→  구조적 방향 (Structural Bias)     │ :00 매시 정각
    │              VB k=0.08 + Supertrend + EMA
    │
4-HOUR (4H) ──→  전술적 방향 (Tactical Direction)  │ :15 매시 15분
    │              VB k=0.06 + Supertrend
    │
1-HOUR (1H) ──→  모멘텀 진입 (Momentum Entry)      │ 매 15분
    │              VB k=0.04 + BB Squeeze
    │
15-MIN (15m) ──→  평균회귀 (Mean Reversion)         │ 매 1분
                   VP + BB + RSI 극단 + 반전캔들
```

상위 TF가 방향(bias)을 설정, 하위 TF가 진입 타이밍. 15m은 역추세 전략이지만, 상위 TF 강추세 시 역방향 진입 차단.

### Signal → Confluence → Execution 흐름

```
Entry Signal (signal=1 long / signal=2 short)
    ↓
Confluence Score 계산:
  - 일봉 bias 일치      (+0~3)   ← calc_bias()
  - 4H bias 확인        (+0~2)
  - 진입 TF confidence  (+0~2)   ← generate_signals_v2()
  - 거래량 서지          (+0~1)
  - 매크로 (FGI등)      (±1)    ← calc_macro_signal() (차단 없음, delta만)
  - Top Trader           (±1)
    ↓
min_score 이상이면 진입
    ↓
CapitalAllocator: size = equity × weight × tier_pct × confluence_mult
    ↓
RiskManager: ATR 기반 SL/TP (tier별 배수)
    ↓
주문 실행 + PositionManager 저장 (positions.json)
```

### Key files

| File | Role |
|------|------|
| `main.py` | Entry point; `_run_tier(tier)` 4-TF 루프, `run_price_monitor_v2()` SL/TP, 스케줄러 |
| `strategies/tf_coordinator.py` | OHLCV 캐시 + 4TF bias 계산 + Confluence Score 엔진 |
| `strategies/volatility_breakout.py` | VB 전략: `generate_signals()` (v1), `generate_signals_v2()` (v2 — MA200 gate 제거), `calc_bias()` |
| `strategies/mean_reversion.py` | 15m 평균회귀: VP VAL/VAH + BB + RSI + 반전캔들 |
| `strategies/indicators.py` | 20+ 지표: EMA, ATR, Supertrend, MACD, BB, RSI div, OBV div, VP, Fib, Hammer, Shooting Star |
| `engine/position_manager.py` | Tier별 포지션 추적, 충돌 해결, 디스크 직렬화 (`positions.json`), SL 쿨다운 |
| `engine/capital_allocator.py` | 공유 풀 자본 관리, confluence→size/leverage 매핑, 포지션 한도 |
| `engine/risk_manager.py` | Tier별 ATR SL/TP, 일일 손실 한도 |
| `engine/okx_exchange.py` | OKX CCXT wrapper; 심볼 매핑 (KRW-BTC → BTC/USDT:USDT); 계약 크기 처리 |
| `macro/indicators.py` | 매크로 → delta 보정 (v2: blocked 없음). Tier별 민감도 (daily/4h: 장기만, 1h/15m: 전체) |
| `macro/fetchers.py` | FGI, 펀딩비, BTC 도미넌스, VIX, DXY; TTL 캐시 |
| `config/config.yaml` | 전체 설정: `confluence`, `capital_allocation`, `risk_tiers`, `timeframe_strategies`, `tier_markets` |
| `config/.env` | API keys (Upbit, Bithumb, OKX, Telegram) — never commit |
| `dashboard/app.py` | FastAPI 대시보드; 포트폴리오, 차트, 로그 뷰어 |
| `dashboard/data_aggregator.py` | 거래소 데이터 집계 (포트폴리오, 신호, 캔들) |

### Confluence Score 시스템 (0-10점)

| 요소 | 최대 점수 |
|------|----------|
| 일봉 방향 일치 | +3 |
| 4H 방향 확인 | +2 |
| 진입 TF 신호 강도 | +2 |
| 매크로 환경 | ±1 |
| Top Trader 정렬 | ±1 |
| 거래량 확인 | +1 |

`config.yaml → confluence.min_score` (현재 1)로 임계값 조절. `score_to_size`/`score_to_leverage`로 점수→크기/레버리지 매핑.

### OKX futures specifics

OKX 선물 계약 주문 시 심볼 변환 필수: `KRW-BTC` → `BTC/USDT:USDT`. `SPOT_MAP`/`FUTURES_MAP`이 매핑 담당. `_init_futures_settings()`에서 종목별 레버리지 설정.

Weight caps (쏠림 방지): BTC 최대 30%, ETH 최대 20%, 알트코인 종목당 최대 8%.

### Position state (v2)

`PositionManager` (engine/position_manager.py)가 모든 포지션을 Tier별로 추적:
- Key: `{exchange}:{market}:{tier}:{direction}`
- 디스크 직렬화: `data/positions.json` (크래시 복구)
- SL 쿨다운: `data/sl_cooldowns.json` (Tier별 재진입 방지)
- 충돌 규칙: 같은 종목 반대 방향 금지 (소자본), 동시 포지션 14개 한도

Legacy v1 상태 (`long_positions`, `short_positions` 등)도 `main.py`에 남아있으나 v2에서는 `pos_manager`가 우선.

### Configuration (v2 섹션)

| 섹션 | 위치 | 내용 |
|------|------|------|
| `confluence` | config.yaml | 점수 가중치, min_score, score→size/leverage 매핑 |
| `capital_allocation` | config.yaml | 공유풀 모드, 포지션 한도, tier별 크기 비율 |
| `risk_tiers` | config.yaml | Tier별 ATR SL/TP 배수, 최대 레버리지, 보유시간 |
| `timeframe_strategies` | config.yaml | Tier별 VB/MR 파라미터 (k값, 지표 설정) |
| `tier_markets` | config.yaml | 거래소×Tier별 거래 종목 |
| `exchange_markets` | config.yaml | 거래소별 기본 종목 (tier_markets 미설정 시 폴백) |
| `macro_factors` | config.yaml | 매크로 지표 API 키, TTL, CoinGecko ID 매핑 |

### OHLCV 캐시 (tf_coordinator.py)

API Rate Limit 대응. TTL: daily=1h, 4H=15m, 1H=5m, 15m=2m. `get_cached_ohlcv()` 함수로 통합 관리.

### Macro factors

v2에서 **차단(blocked) 제거**. 모든 매크로 요소는 Confluence delta(±1~3)로만 반영:
- FGI ≤ 10 + 숏 → -2 (기존: 차단)
- FGI ≥ 80 + 롱 → -2 (기존: 차단)
- VIX > 30 → -2 (기존: 차단)
- MA200 아래 + 롱 → 일봉 bias 점수 0 (기존: 진입 불가)

Tier별 민감도: daily/4h는 FGI+도미넌스+DXY만, 1h/15m은 펀딩비+L/S비율 포함.
