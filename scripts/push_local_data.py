"""
로컬 머신에서 실행: 잔고 포함 전체 데이터 → docs/data/*.json 저장 후 git push

GitHub Actions는 캔들/신호(공개 API)만 담당.
이 스크립트는 Upbit IP 화이트리스트가 적용된 로컬에서 실행해
포트폴리오/리스크(잔고 조회)를 포함한 나머지 데이터를 채움.

담당 파일:
  portfolio.json   ← Upbit + OKX 잔고 포함 전체 자산
  risk.json        ← SL/TP 가격, 일일 손실 한도
  health.json      ← 봇 실행 상태
  trades.json      ← 거래 이력
  equity_*.json    ← 자산 곡선

사용법:
  python scripts/push_local_data.py

로컬 cron 예시 (30분마다):
  */30 * * * * cd /path/to/QT && python scripts/push_local_data.py >> logs/push_local.log 2>&1
"""
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime

ROOT     = Path(__file__).parent.parent
DATA_DIR = ROOT / "docs" / "data"
LOG_PATH = ROOT / "logs" / "trades.log"
sys.path.insert(0, str(ROOT))
DATA_DIR.mkdir(parents=True, exist_ok=True)

from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / "config" / ".env")


def save(filename: str, data):
    path = DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    print(f"  저장: {filename}")


def git_push():
    try:
        subprocess.run(["git", "-C", str(ROOT), "add", "docs/data/"], check=True)
        result = subprocess.run(
            ["git", "-C", str(ROOT), "diff", "--cached", "--quiet"],
            capture_output=True
        )
        if result.returncode == 0:
            print("  변경 없음 — push 생략")
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        subprocess.run(
            ["git", "-C", str(ROOT), "commit", "-m", f"data: local update {now}"],
            check=True
        )
        subprocess.run(["git", "-C", str(ROOT), "push"], check=True)
        print("  git push 완료")
    except subprocess.CalledProcessError as e:
        print(f"  git 오류: {e}")


def export_local():
    print(f"=== 로컬 데이터 수출 시작 ({datetime.now().strftime('%H:%M:%S')}) ===")

    # ── 잔고 데이터 (Upbit + OKX, 로컬 실행이므로 IP 제한 없음) ──
    try:
        from dashboard.data_aggregator import DataAggregator
        agg = DataAggregator()

        try:
            portfolio = agg.get_portfolio()
            save("portfolio.json", portfolio)
        except Exception as e:
            print(f"  portfolio 실패: {e}")

        try:
            risk = agg.get_risk_status()
            save("risk.json", risk)
        except Exception as e:
            print(f"  risk 실패: {e}")

    except Exception as e:
        print(f"  DataAggregator 초기화 실패 (잔고 데이터 스킵): {e}")

    # ── 로그 기반 데이터 (봇 실행 상태 / 거래 이력 / 자산 곡선) ──
    if not LOG_PATH.exists():
        print(f"  로그 파일 없음: {LOG_PATH}")
    else:
        from dashboard.log_parser import LogParser
        parser = LogParser(str(LOG_PATH))

        try:
            health = parser.get_health()
            save("health.json", health)
        except Exception as e:
            print(f"  health 실패: {e}")

        try:
            trades = parser.parse_trades(200)
            save("trades.json", {"trades": trades, "total": len(trades)})
        except Exception as e:
            print(f"  trades 실패: {e}")

        for exchange in ["upbit", "okx"]:
            try:
                equity = parser.get_equity_history(exchange)
                save(f"equity_{exchange}.json", equity)
            except Exception as e:
                print(f"  equity_{exchange} 실패: {e}")

    git_push()
    print("=== 완료 ===")


if __name__ == "__main__":
    export_local()
