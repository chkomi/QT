"""
파라미터 자동 최적화 (Parameter Optimizer)
────────────────────────────────────────────────────────────────
백테스트 기반으로 Tier별 전략 파라미터를 주기적으로 재최적화한다.

최적화 대상:
  - VB (Volatility Breakout) Tier: k값, volume_multiplier
  - MR (Mean Reversion) 15m Tier: rsi_oversold, rsi_overbought

평가 지표: 샤프비율(주) × 승률(보조) 가중 점수
제약 조건: 최소 거래 수 10건 이상, max_drawdown < 30%

스케줄: 매주 일요일 04:00 (main.py에서 호출)
"""
import logging
import time
import yaml
import pandas as pd
import numpy as np
from itertools import product
from pathlib import Path
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)

ROOT        = Path(__file__).parent.parent
CONFIG_FILE = ROOT / "config" / "config.yaml"
OPT_REPORT  = ROOT / "analysis" / "reports"
OPT_REPORT.mkdir(parents=True, exist_ok=True)


# ── 그리드서치 파라미터 범위 ──────────────────────────────────────────────────
_GRID_VB = {
    "k":                 [0.06, 0.10, 0.15, 0.20, 0.30],
    "volume_multiplier": [1.0, 1.2, 1.5],
}
_GRID_MR = {
    "rsi_oversold":  [25, 30, 35],
    "rsi_overbought": [65, 70, 75],
}

# Tier → 최적화 전략 타입
_TIER_TYPE = {"daily": "vb", "4h": "vb", "1h": "vb", "15m": "mr"}

# OKX 심볼 → ccxt 스팟 심볼 변환
_BACKTEST_SYMBOL = {
    "KRW-BTC":  "BTC/USDT",
    "KRW-ETH":  "ETH/USDT",
    "KRW-SOL":  "SOL/USDT",
    "KRW-XRP":  "XRP/USDT",
    "KRW-BNB":  "BNB/USDT",
    "KRW-DOGE": "DOGE/USDT",
    "KRW-LINK": "LINK/USDT",
    "KRW-SUI":  "SUI/USDT",
}


def _load_ohlcv(market: str, interval: str = "1d", limit: int = 365) -> Optional[pd.DataFrame]:
    """ccxt로 OHLCV 수집 (OKX 스팟 — 공개 API)."""
    try:
        import ccxt
        from dotenv import load_dotenv
        load_dotenv(ROOT / "config" / ".env")

        ex = ccxt.okx({"options": {"defaultType": "spot"}})
        symbol = _BACKTEST_SYMBOL.get(market, market.replace("KRW-", "") + "/USDT")
        raw = ex.fetch_ohlcv(symbol, interval, limit=limit)
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df.index = pd.to_datetime(df["timestamp"], unit="ms")
        df.index.name = "datetime"
        return df[["open", "high", "low", "close", "volume"]]
    except Exception as e:
        logger.warning(f"[최적화] OHLCV 수집 실패 ({market}): {e}")
        return None


def _score(metrics: dict) -> float:
    """
    백테스트 결과 → 최적화 점수 (높을수록 좋음).
    샤프비율 0.5 + 승률 0.3 + 수익률 0.2 가중합
    """
    sharpe    = metrics.get("sharpe_ratio", 0) or 0
    win_rate  = metrics.get("win_rate", 0) or 0
    total_ret = metrics.get("total_return_pct", 0) or 0
    max_dd    = abs(metrics.get("max_drawdown_pct", 100) or 100)
    n_trades  = metrics.get("n_trades", 0) or 0

    if n_trades < 10 or max_dd > 30:
        return -999.0

    return sharpe * 0.5 + (win_rate / 100) * 0.3 + min(total_ret / 100, 1.0) * 0.2


def _run_vb_grid(df: pd.DataFrame, tier: str, cfg: dict) -> Tuple[dict, float]:
    """VB 전략 그리드서치 → (최적 params, 최고 점수)."""
    from strategies.volatility_breakout import VolatilityBreakoutStrategy
    from backtest.backtester import Backtester

    tier_cfg = cfg.get("timeframe_strategies", {}).get(tier, {})
    best_params = {}
    best_score  = -999.0

    combos = list(product(_GRID_VB["k"], _GRID_VB["volume_multiplier"]))
    logger.info(f"[최적화] {tier} VB 그리드서치: {len(combos)}개 조합")

    for k, vol_mult in combos:
        try:
            strat = VolatilityBreakoutStrategy({
                **tier_cfg,
                "k": k,
                "volume_multiplier": vol_mult,
                "use_supertrend": tier_cfg.get("use_supertrend", True),
                "use_macd_filter": False,
                "use_atr_sl": True,
            })
            bt = Backtester(
                strat,
                initial_capital=1000,
                commission=0.0005,
                slippage=0.001,
                use_atr_sl=True,
            )
            metrics = bt.run(df)
            s = _score(metrics)
            if s > best_score:
                best_score = s
                best_params = {"k": k, "volume_multiplier": vol_mult}
        except Exception as e:
            logger.debug(f"[최적화] {tier} k={k} vol={vol_mult} 실패: {e}")

    return best_params, best_score


def _run_mr_grid(df: pd.DataFrame, cfg: dict) -> Tuple[dict, float]:
    """MR 전략 그리드서치 → (최적 params, 최고 점수)."""
    from strategies.mean_reversion import MeanReversionStrategy
    from backtest.backtester import Backtester

    tier_cfg = cfg.get("timeframe_strategies", {}).get("15m", {})
    best_params = {}
    best_score  = -999.0

    combos = [
        (ov, ob)
        for ov in _GRID_MR["rsi_oversold"]
        for ob in _GRID_MR["rsi_overbought"]
        if ob - ov >= 30
    ]
    logger.info(f"[최적화] 15m MR 그리드서치: {len(combos)}개 조합")

    for rsi_os, rsi_ob in combos:
        try:
            strat = MeanReversionStrategy({
                **tier_cfg,
                "rsi_oversold":  rsi_os,
                "rsi_overbought": rsi_ob,
            })
            bt = Backtester(strat, initial_capital=1000, commission=0.0005, slippage=0.001)
            metrics = bt.run(df)
            s = _score(metrics)
            if s > best_score:
                best_score = s
                best_params = {"rsi_oversold": rsi_os, "rsi_overbought": rsi_ob}
        except Exception as e:
            logger.debug(f"[최적화] 15m rsi_os={rsi_os} rsi_ob={rsi_ob} 실패: {e}")

    return best_params, best_score


def _apply_to_config(updates: Dict[str, dict]) -> bool:
    """
    updates = {tier: {param: value}} 를 config.yaml timeframe_strategies에 반영.
    기존 값 대비 변화율이 50% 이상이면 보수적 절충 (기존 50% + 최적 50%).
    """
    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"[최적화] config 로드 실패: {e}")
        return False

    tf_cfg = cfg.setdefault("timeframe_strategies", {})
    changed = False
    for tier, params in updates.items():
        tier_cfg = tf_cfg.setdefault(tier, {})
        for key, new_val in params.items():
            old_val = tier_cfg.get(key, new_val)
            # 변화 완충: 기존과 최적값의 중간값 적용
            if isinstance(new_val, float) and old_val:
                blended = round(old_val * 0.5 + new_val * 0.5, 4)
            else:
                blended = new_val
            if tier_cfg.get(key) != blended:
                logger.info(f"[최적화] {tier}.{key}: {old_val} → {blended}")
                tier_cfg[key] = blended
                changed = True

    if not changed:
        logger.info("[최적화] 변경사항 없음")
        return True

    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        logger.info("[최적화] config.yaml 업데이트 완료")
        return True
    except Exception as e:
        logger.error(f"[최적화] config 저장 실패: {e}")
        return False


def run_weekly_optimization(
    markets: Optional[List[str]] = None,
    tiers: Optional[List[str]] = None,
) -> str:
    """
    주간 파라미터 최적화 실행.
    - markets: None이면 대표 3개 종목 (BTC, ETH, XRP) 사용
    - tiers: None이면 전체 Tier
    - 결과 요약 문자열 반환 (텔레그램 전송용)
    """
    if markets is None:
        markets = ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
    if tiers is None:
        tiers = ["daily", "4h", "1h", "15m"]

    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        return f"[최적화] config 로드 실패: {e}"

    logger.info(f"[최적화] 시작 | 종목: {markets} | Tier: {tiers}")
    t0 = time.time()
    updates: Dict[str, dict] = {}
    report_lines = ["⚙️ 파라미터 최적화 결과"]

    # Tier별 최적화 — OHLCV는 공통으로 수집 후 재사용
    interval_map = {"daily": "1d", "4h": "4h", "1h": "1h", "15m": "15m"}

    for tier in tiers:
        strat_type = _TIER_TYPE.get(tier, "vb")
        interval   = interval_map.get(tier, "1d")
        limit      = {"daily": 365, "4h": 500, "1h": 500, "15m": 500}.get(tier, 365)

        # 여러 종목 합쳐서 대표 평균 파라미터 도출
        best_by_market: List[Tuple[dict, float]] = []

        for market in markets:
            df = _load_ohlcv(market, interval=interval, limit=limit)
            if df is None or len(df) < 50:
                continue
            try:
                if strat_type == "vb":
                    params, score = _run_vb_grid(df, tier, cfg)
                else:
                    params, score = _run_mr_grid(df, cfg)
                if score > -999 and params:
                    best_by_market.append((params, score))
            except Exception as e:
                logger.warning(f"[최적화] {tier}/{market} 실패: {e}")

        if not best_by_market:
            report_lines.append(f"  {tier:4s}: 데이터 부족 → 스킵")
            continue

        # 점수 가중 평균 파라미터 계산
        total_score = sum(s for _, s in best_by_market if s > 0) or 1
        avg_params: dict = {}
        for params, score in best_by_market:
            w = max(score, 0) / total_score
            for key, val in params.items():
                avg_params[key] = avg_params.get(key, 0) + val * w

        # 반올림
        for key in avg_params:
            avg_params[key] = round(avg_params[key], 4)

        updates[tier] = avg_params
        report_lines.append(
            f"  {tier:4s}: {', '.join(f'{k}={v}' for k, v in avg_params.items())}"
            f" (평균점수 {sum(s for _, s in best_by_market)/len(best_by_market):.3f})"
        )

    if updates:
        ok = _apply_to_config(updates)
        if ok:
            report_lines.append("\n✅ config.yaml 업데이트 완료")
        else:
            report_lines.append("\n⚠️ config 저장 실패 — 수동 확인 필요")
    else:
        report_lines.append("\n데이터 부족으로 업데이트 없음")

    elapsed = time.time() - t0
    report_lines.append(f"소요시간: {elapsed:.0f}초")

    # JSON 저장
    import json
    from datetime import datetime
    ts_str = datetime.now().strftime("%Y%m%d_%H%M")
    try:
        with open(OPT_REPORT / f"opt_{ts_str}.json", "w", encoding="utf-8") as f:
            json.dump({"generated": ts_str, "updates": updates}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    summary = "\n".join(report_lines)
    logger.info(f"[최적화] 완료\n{summary}")
    return summary
