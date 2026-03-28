"""
Walk-Forward OOS 검증 (v1)

학습(IS) 4개월 → 테스트(OOS) 1개월 롤링으로 과최적화 방지.

최적화 파라미터:
  - min_score  : 1 ~ 5
  - k          : 0.03 ~ 0.10  (변동성 돌파 계수)
  - atr_sl_mult: 0.8 ~ 2.0    (ATR SL 배수)

통과 기준 (OOS):
  - Sharpe > 0.5
  - MDD   < 30%

사용법:
  python backtest/walk_forward.py --start 2023-01-01 --end 2026-03-28 --capital 1000
  python backtest/walk_forward.py --start 2023-01-01 --end 2026-03-28 --quick
"""
import sys
import argparse
import itertools
import logging
import warnings
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Walk-Forward 설정 ─────────────────────────────────────────────────────────

IS_MONTHS  = 4     # In-sample (학습) 기간 (월)
OOS_MONTHS = 1     # Out-of-sample (테스트) 기간 (월)

# 최적화 그리드 (전체)
PARAM_GRID_FULL = {
    "min_score":    [1, 2, 3, 4, 5],
    "k":            [0.03, 0.05, 0.06, 0.08, 0.10],
    "atr_sl_mult":  [0.8, 1.0, 1.2, 1.5, 2.0],
}

# 빠른 그리드 (--quick 모드)
PARAM_GRID_QUICK = {
    "min_score":    [2, 3, 4],
    "k":            [0.05, 0.08],
    "atr_sl_mult":  [1.0, 1.5],
}

# 통과 기준
OOS_MIN_SHARPE = 0.5
OOS_MAX_MDD    = 30.0   # %


# ── 성과 계산 헬퍼 ────────────────────────────────────────────────────────────

def _calc_sharpe(equity: pd.Series) -> float:
    """연환산 Sharpe (무위험금리 0 기준)."""
    if len(equity) < 5:
        return 0.0
    rets = equity.pct_change().dropna()
    if rets.std() == 0:
        return 0.0
    return float(rets.mean() / rets.std() * np.sqrt(252))


def _calc_mdd(equity: pd.Series) -> float:
    """최대 낙폭 % (양수)."""
    if len(equity) < 2:
        return 0.0
    roll_max = equity.cummax()
    drawdown = (equity - roll_max) / roll_max * 100
    return float(drawdown.min() * -1)   # 양수 반환


def _calc_metrics(result: dict) -> Dict[str, float]:
    """portfolio_backtester.run() 반환값에서 핵심 지표 추출."""
    ec     = result.get("equity_curve")
    trades = result.get("trades")

    if ec is None or len(ec) < 5:
        return {"sharpe": 0.0, "mdd": 100.0, "trades": 0, "win_rate": 0.0, "cagr": 0.0}

    sharpe = _calc_sharpe(ec)
    mdd    = _calc_mdd(ec)

    # 거래 통계
    n_trades = 0
    win_rate = 0.0
    if trades is not None and not trades.empty and "pnl" in trades.columns:
        n_trades = len(trades)
        wins     = (trades["pnl"] > 0).sum()
        win_rate = wins / n_trades * 100 if n_trades else 0.0

    # CAGR
    years = len(ec) / 252
    if years > 0.01 and ec.iloc[0] > 0:
        cagr = (ec.iloc[-1] / ec.iloc[0]) ** (1 / years) - 1
        cagr = float(cagr * 100)
    else:
        cagr = 0.0

    return {
        "sharpe":   round(sharpe, 3),
        "mdd":      round(mdd, 2),
        "trades":   n_trades,
        "win_rate": round(win_rate, 1),
        "cagr":     round(cagr, 2),
    }


# ── 윈도우 날짜 생성 ──────────────────────────────────────────────────────────

def _date_windows(start: str, end: str) -> List[Tuple[str, str, str, str]]:
    """(is_start, is_end, oos_start, oos_end) 목록 반환."""
    s = pd.Timestamp(start)
    e = pd.Timestamp(end)

    windows = []
    is_start = s
    while True:
        is_end    = is_start + pd.DateOffset(months=IS_MONTHS)
        oos_start = is_end
        oos_end   = oos_start + pd.DateOffset(months=OOS_MONTHS)
        if oos_end > e:
            break
        windows.append((
            is_start.strftime("%Y-%m-%d"),
            is_end.strftime("%Y-%m-%d"),
            oos_start.strftime("%Y-%m-%d"),
            oos_end.strftime("%Y-%m-%d"),
        ))
        is_start = is_start + pd.DateOffset(months=OOS_MONTHS)  # 1개월씩 전진

    return windows


def _param_combinations(grid: dict) -> List[Dict[str, Any]]:
    keys   = list(grid.keys())
    values = list(grid.values())
    return [dict(zip(keys, combo)) for combo in itertools.product(*values)]


# ── 단일 백테스트 실행 ────────────────────────────────────────────────────────

def _run_single(
    params: Dict[str, Any],
    start:  str,
    end:    str,
    initial_capital: float,
) -> Optional[Dict]:
    """
    파라미터 세트 + 기간으로 백테스트 1회 실행.

    portfolio_backtester 모듈의 상수를 임시 교체 후 복원하는 방식으로
    파라미터를 주입한다. CSV 캐시가 있으면 API 호출 없이 빠르게 실행됨.
    """
    import backtest.portfolio_backtester as pb

    # 모듈 상수 임시 교체
    orig_min_score = pb.MIN_SCORE
    orig_tier_cfg  = pb.TIER_CFG

    new_tier_cfg = deepcopy(orig_tier_cfg)
    for t in new_tier_cfg:
        new_tier_cfg[t]["k"]      = params["k"]
        new_tier_cfg[t]["atr_sl"] = params["atr_sl_mult"]
        new_tier_cfg[t]["atr_tp"] = params["atr_sl_mult"] * 2.0   # 1:2 R/R

    pb.MIN_SCORE = params["min_score"]
    pb.TIER_CFG  = new_tier_cfg

    try:
        bt     = pb.PortfolioBacktester(initial_capital=initial_capital, verbose=False)
        result = bt.run(start=start, end=end)
        metrics = _calc_metrics(result)
        metrics["params"] = params
        return metrics
    except Exception as ex:
        logger.debug(f"  백테스트 실패 ({params}): {ex}")
        return None
    finally:
        # 반드시 원복
        pb.MIN_SCORE = orig_min_score
        pb.TIER_CFG  = orig_tier_cfg


def _best_params(results: List[Optional[Dict]]) -> Optional[Dict[str, Any]]:
    """유효 IS 결과 중 Sharpe 최대 파라미터 반환."""
    valid = [r for r in results if r is not None and r["trades"] >= 3]
    if not valid:
        return None
    return max(valid, key=lambda r: r["sharpe"])["params"]


# ── WalkForward 클래스 ────────────────────────────────────────────────────────

class WalkForward:
    """
    롤링 IS/OOS 검증.

    각 윈도우:
      1. IS 기간에서 파라미터 그리드 탐색 → Sharpe 최대 선택
      2. 최적 파라미터를 OOS에 고정 적용
      3. OOS Sharpe > 0.5, MDD < 30% 통과 여부 기록
    """

    def __init__(
        self,
        start:           str,
        end:             str,
        initial_capital: float = 1_000.0,
        quick:           bool  = False,
    ):
        self.start           = start
        self.end             = end
        self.initial_capital = initial_capital
        self.grid            = PARAM_GRID_QUICK if quick else PARAM_GRID_FULL
        self._windows        = _date_windows(start, end)

    # ── 윈도우 단위 실행 ───────────────────────────────────────────────────────

    def _run_window(
        self,
        win_idx:   int,
        is_start:  str,
        is_end:    str,
        oos_start: str,
        oos_end:   str,
    ) -> Dict:
        logger.info(
            f"\n{'='*58}\n"
            f"Window {win_idx+1}/{len(self._windows)}\n"
            f"  IS : {is_start} ~ {is_end}\n"
            f"  OOS: {oos_start} ~ {oos_end}"
        )

        combos = _param_combinations(self.grid)
        logger.info(f"  IS 그리드 탐색: {len(combos)}개 파라미터 조합")

        # ── IS 최적화 ──────────────────────────────────────────────────────────
        is_results = []
        for i, params in enumerate(combos):
            r = _run_single(params, is_start, is_end, self.initial_capital)
            is_results.append(r)
            if (i + 1) % 10 == 0:
                done = i + 1
                logger.info(f"    진행: {done}/{len(combos)}")

        best = _best_params(is_results)
        if best is None:
            logger.warning("  IS 최적화 실패 (유효 결과 없음). 이 윈도우 건너뜀.")
            return {
                "window":     win_idx + 1,
                "is_start":   is_start,  "is_end":   is_end,
                "oos_start":  oos_start, "oos_end":  oos_end,
                "best_params": None,
                "is_sharpe":   None,
                "oos_sharpe":  None,
                "oos_mdd":     None,
                "passed":      False,
                "skip":        True,
            }

        best_is = next(r for r in is_results if r and r["params"] == best)
        logger.info(
            f"  IS 최적 파라미터: {best}\n"
            f"    IS Sharpe={best_is['sharpe']:.3f}  MDD={best_is['mdd']:.1f}%  "
            f"Trades={best_is['trades']}  WR={best_is['win_rate']:.1f}%"
        )

        # ── OOS 검증 ───────────────────────────────────────────────────────────
        oos = _run_single(best, oos_start, oos_end, self.initial_capital)

        if oos is None:
            logger.warning("  OOS 실행 실패.")
            return {
                "window":     win_idx + 1,
                "is_start":   is_start,  "is_end":   is_end,
                "oos_start":  oos_start, "oos_end":  oos_end,
                "best_params": best,
                "is_sharpe":   best_is["sharpe"],
                "is_mdd":      best_is["mdd"],
                "oos_sharpe":  None,
                "oos_mdd":     None,
                "oos_trades":  0,
                "oos_win_rate":0,
                "passed":      False,
                "skip":        False,
            }

        passed = (oos["sharpe"] >= OOS_MIN_SHARPE) and (oos["mdd"] <= OOS_MAX_MDD)
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(
            f"  OOS 결과: Sharpe={oos['sharpe']:.3f}  MDD={oos['mdd']:.1f}%  "
            f"Trades={oos['trades']}  WR={oos['win_rate']:.1f}%  → {status}"
        )

        return {
            "window":      win_idx + 1,
            "is_start":    is_start,  "is_end":    is_end,
            "oos_start":   oos_start, "oos_end":   oos_end,
            "best_params": best,
            "is_sharpe":   best_is["sharpe"],
            "is_mdd":      best_is["mdd"],
            "oos_sharpe":  oos["sharpe"],
            "oos_mdd":     oos["mdd"],
            "oos_trades":  oos["trades"],
            "oos_win_rate":oos["win_rate"],
            "passed":      passed,
            "skip":        False,
        }

    # ── 전체 실행 ──────────────────────────────────────────────────────────────

    def run(self) -> List[Dict]:
        if not self._windows:
            logger.error(
                f"기간이 너무 짧습니다. IS {IS_MONTHS}개월 + OOS {OOS_MONTHS}개월 이상 필요."
            )
            return []

        logger.info(
            f"\nWalk-Forward 시작\n"
            f"  전체 기간: {self.start} ~ {self.end}\n"
            f"  윈도우 수: {len(self._windows)}\n"
            f"  IS: {IS_MONTHS}개월 / OOS: {OOS_MONTHS}개월\n"
            f"  통과 기준: OOS Sharpe > {OOS_MIN_SHARPE}, MDD < {OOS_MAX_MDD:.0f}%\n"
        )

        results = []
        for i, (is_s, is_e, oos_s, oos_e) in enumerate(self._windows):
            r = self._run_window(i, is_s, is_e, oos_s, oos_e)
            results.append(r)

        self._print_summary(results)
        self._save_results(results)
        return results

    # ── 리포트 ────────────────────────────────────────────────────────────────

    @staticmethod
    def _print_summary(results: List[Dict]) -> None:
        valid  = [r for r in results if not r.get("skip")]
        passed = [r for r in valid if r.get("passed")]

        print("\n" + "="*72)
        print("  Walk-Forward 요약")
        print("="*72)
        print(
            f"{'Win':>4}  {'IS 기간':^22}  {'IS Sh':>6}  "
            f"{'OOS Sh':>6}  {'OOS MDD':>7}  {'거래':>4}  {'Result':>8}"
        )
        print("-"*72)

        for r in results:
            if r.get("skip"):
                print(f"{r['window']:>4}  {'(데이터 부족 — 건너뜀)':^60}")
                continue
            status  = "PASS ✅" if r["passed"] else "FAIL ❌"
            oos_sh  = f"{r['oos_sharpe']:.3f}" if r.get("oos_sharpe") is not None else "  N/A"
            oos_mdd = f"{r['oos_mdd']:.1f}%"   if r.get("oos_mdd")   is not None else "  N/A"
            is_sh   = f"{r['is_sharpe']:.3f}"   if r.get("is_sharpe") is not None else "  N/A"
            print(
                f"{r['window']:>4}  "
                f"{r['is_start']:10} ~ {r['is_end']:10}  "
                f"{is_sh:>6}  "
                f"{oos_sh:>6}  "
                f"{oos_mdd:>7}  "
                f"{r.get('oos_trades', 0):>4}  "
                f"{status:>8}"
            )

        print("="*72)
        total     = len(valid)
        n_pass    = len(passed)
        pass_rate = n_pass / max(total, 1) * 100

        print(f"\n통과율: {n_pass}/{total} ({pass_rate:.0f}%)")

        if passed:
            avg_sh  = np.mean([r["oos_sharpe"] for r in passed if r.get("oos_sharpe") is not None])
            avg_mdd = np.mean([r["oos_mdd"]    for r in passed if r.get("oos_mdd")    is not None])
            print(f"통과 윈도우 평균 OOS Sharpe: {avg_sh:.3f}")
            print(f"통과 윈도우 평균 OOS MDD   : {avg_mdd:.1f}%")
        else:
            print("통과한 윈도우 없음 — 전략 재검토 필요")

        # 파라미터 빈도 분석
        all_params = [r["best_params"] for r in valid if r["best_params"] is not None]
        if all_params:
            print("\n최적 파라미터 빈도 (IS 기준):")
            df_p = pd.DataFrame(all_params)
            for col in df_p.columns:
                print(f"  {col}: {dict(df_p[col].value_counts())}")

        # 권장 파라미터 (통과 윈도우 최빈값)
        pass_params = [r["best_params"] for r in passed if r["best_params"] is not None]
        if pass_params:
            df_pp = pd.DataFrame(pass_params)
            recommended = {col: df_pp[col].mode()[0] for col in df_pp.columns}
            print(f"\n▶ 권장 파라미터 (통과 윈도우 최빈값):\n  {recommended}")

        print("="*72 + "\n")

    @staticmethod
    def _save_results(results: List[Dict]) -> None:
        out_dir  = ROOT / "data" / "backtest"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "walk_forward_results.csv"

        rows = []
        for r in results:
            row = {
                "window":      r["window"],
                "is_start":    r["is_start"],
                "is_end":      r["is_end"],
                "oos_start":   r["oos_start"],
                "oos_end":     r["oos_end"],
                "is_sharpe":   r.get("is_sharpe"),
                "oos_sharpe":  r.get("oos_sharpe"),
                "oos_mdd":     r.get("oos_mdd"),
                "oos_trades":  r.get("oos_trades"),
                "oos_win_rate":r.get("oos_win_rate"),
                "passed":      r.get("passed", False),
                "skipped":     r.get("skip", False),
            }
            if r.get("best_params"):
                row.update({f"param_{k}": v for k, v in r["best_params"].items()})
            rows.append(row)

        pd.DataFrame(rows).to_csv(out_file, index=False)
        logger.info(f"결과 저장: {out_file}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Walk-Forward OOS Validator")
    parser.add_argument("--start",   default="2023-01-01", help="전체 시작일 (YYYY-MM-DD)")
    parser.add_argument("--end",     default="2026-03-28", help="전체 종료일 (YYYY-MM-DD)")
    parser.add_argument("--capital", type=float, default=1_000.0, help="초기 자산 (USDT)")
    parser.add_argument("--quick",   action="store_true",  help="빠른 그리드 (파라미터 수 축소)")
    parser.add_argument("--verbose", action="store_true",  help="상세 로그")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    wf = WalkForward(
        start=args.start,
        end=args.end,
        initial_capital=args.capital,
        quick=args.quick,
    )
    wf.run()


if __name__ == "__main__":
    main()
