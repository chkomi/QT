"""
GitHub Actions에서 실행: 거래소 API → docs/data/*.json 저장

환경변수(GitHub Secrets):
  UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY
  OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE
"""
import sys
import json
import os
from pathlib import Path

ROOT     = Path(__file__).parent.parent
DATA_DIR = ROOT / "docs" / "data"
sys.path.insert(0, str(ROOT))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# .env 로드 (로컬 실행 시)
from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / "config" / ".env")

import yaml
with open(ROOT / "config" / "config.yaml", encoding="utf-8") as f:
    _config = yaml.safe_load(f)

MARKETS = _config["markets"]


def save(filename: str, data):
    path = DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    print(f"  저장: {filename}")


def export_all():
    print("=== 거래소 데이터 수출 시작 ===")

    try:
        from dashboard.data_aggregator import DataAggregator
        agg = DataAggregator()
    except Exception as e:
        print(f"DataAggregator 초기화 실패: {e}")
        sys.exit(1)

    # 포트폴리오
    try:
        portfolio = agg.get_portfolio()
        save("portfolio.json", portfolio)
    except Exception as e:
        print(f"portfolio 실패: {e}")

    # 리스크
    try:
        risk = agg.get_risk_status()
        save("risk.json", risk)
    except Exception as e:
        print(f"risk 실패: {e}")

    # 신호
    try:
        signals = agg.get_signals()
        save("signals.json", signals)
    except Exception as e:
        print(f"signals 실패: {e}")

    # 캔들 (upbit + okx × BTC + ETH)
    for exchange in ["upbit", "okx"]:
        for market in MARKETS:
            key = f"candles_{exchange}_{market}_day"
            try:
                candles = agg.get_candles(exchange, market, "day", 200)
                save(f"{key}.json", candles)
            except Exception as e:
                print(f"{key} 실패: {e}")

    print("=== 완료 ===")


if __name__ == "__main__":
    export_all()
