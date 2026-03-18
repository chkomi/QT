"""
로컬 머신에서 실행: trades.log → docs/data/trades.json, health.json, equity_*.json 저장 후 git push

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

    if not LOG_PATH.exists():
        print(f"  로그 파일 없음: {LOG_PATH}")
        return

    from dashboard.log_parser import LogParser
    parser = LogParser(str(LOG_PATH))

    # 헬스
    try:
        health = parser.get_health()
        save("health.json", health)
    except Exception as e:
        print(f"  health 실패: {e}")

    # 거래 이력 (최근 200건)
    try:
        trades = parser.parse_trades(200)
        save("trades.json", {"trades": trades, "total": len(trades)})
    except Exception as e:
        print(f"  trades 실패: {e}")

    # 자산 곡선
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
