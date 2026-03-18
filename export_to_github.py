#!/usr/bin/env python3
"""
GitHub Pages용 정적 데이터 내보내기

docs/data/*.json 을 생성한 뒤 git commit & push 까지 수행합니다.

사용법:
    python export_to_github.py          # 데이터 생성 + 자동 커밋·푸시
    python export_to_github.py --no-push  # 데이터 생성만 (푸시 안 함)
"""
import sys
import json
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# .env 로드 (가장 먼저 실행해야 API 키가 잡힘)
from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / "config" / ".env")

DOCS_DATA = ROOT / "docs" / "data"
DOCS_DATA.mkdir(parents=True, exist_ok=True)

MARKETS   = ["KRW-BTC", "KRW-ETH"]
EXCHANGES = ["upbit", "okx"]


def write_json(filename: str, data):
    path = DOCS_DATA / filename
    path.write_text(json.dumps(data, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"  ✓ {filename}")


def export_all():
    from dashboard.data_aggregator import DataAggregator
    from dashboard.log_parser      import LogParser

    print("데이터 수집 중...")
    agg    = DataAggregator()
    parser = LogParser(str(ROOT / "logs" / "trades.log"))

    # ── health ──────────────────────────────────────────────────
    write_json("health.json",    parser.get_health())

    # ── portfolio ────────────────────────────────────────────────
    write_json("portfolio.json", agg.get_portfolio())

    # ── risk ─────────────────────────────────────────────────────
    write_json("risk.json",      agg.get_risk_status())

    # ── signals ──────────────────────────────────────────────────
    write_json("signals.json",   agg.get_signals())

    # ── trades ───────────────────────────────────────────────────
    trades = parser.parse_trades(limit=200)
    write_json("trades.json", {"trades": trades, "total": len(trades)})

    # ── candles (거래소 × 종목) ────────────────────────────────
    for exchange in EXCHANGES:
        for market in MARKETS:
            try:
                data = agg.get_candles(exchange, market, "day", 200)
                key  = f"{exchange}_{market}_day"
                write_json(f"candles_{key}.json", data)
            except Exception as e:
                print(f"  ✗ candles_{exchange}_{market}: {e}")

    # ── equity history ────────────────────────────────────────────
    for exchange in EXCHANGES:
        data = parser.get_equity_history(exchange)
        write_json(f"equity_{exchange}.json", data)

    # ── 생성 시각 메타 ────────────────────────────────────────────
    write_json("meta.json", {
        "generated_at": datetime.now().isoformat(),
        "generated_at_kst": datetime.now().strftime("%Y-%m-%d %H:%M:%S KST"),
    })

    print(f"\n완료: docs/data/ ({len(list(DOCS_DATA.glob('*.json')))}개 파일)")


def git_commit_push():
    cmds = [
        ["git", "add", "docs/data/"],
        ["git", "commit", "-m",
         f"data: GitHub Pages 스냅샷 갱신 ({datetime.now().strftime('%Y-%m-%d %H:%M')})"],
        ["git", "push", "origin", "main"],
    ]
    for cmd in cmds:
        result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
        if result.returncode != 0:
            # 변경사항 없으면 commit이 실패해도 무시
            if "nothing to commit" in result.stdout + result.stderr:
                print("  ℹ 변경사항 없음 (스킵)")
                return
            print(f"  ✗ {' '.join(cmd)}\n{result.stderr}")
            return
        print(f"  ✓ {' '.join(cmd[:2])}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-push", action="store_true", help="커밋·푸시 생략")
    args = parser.parse_args()

    export_all()

    if not args.no_push:
        print("\nGit 커밋·푸시...")
        git_commit_push()
        print("\nGitHub Pages에서 1~2분 후 반영됩니다.")
