"""
GitHub Actions에서 실행: 공개 API 기반 캔들 + 신호만 export

포트폴리오/리스크(잔고 조회)는 push_local_data.py에서 로컬 실행.
→ Upbit IP 화이트리스트 문제 우회.

환경변수(GitHub Secrets):
  OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE  ← 캔들/신호용
  (UPBIT_ACCESS_KEY/SECRET_KEY 불필요 — 캔들은 공개 API)
"""
import sys
import json
import os
from pathlib import Path

ROOT     = Path(__file__).parent.parent
DATA_DIR = ROOT / "docs" / "data"
sys.path.insert(0, str(ROOT))
DATA_DIR.mkdir(parents=True, exist_ok=True)

from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / "config" / ".env")

import yaml
with open(ROOT / "config" / "config.yaml", encoding="utf-8") as f:
    _config = yaml.safe_load(f)

MARKETS     = _config["markets"]
OKX_MARKETS = _config.get("exchange_markets", {}).get("okx", MARKETS)


def save(filename: str, data):
    path = DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    print(f"  저장: {filename}")


def export_candles_and_signals():
    print("=== 캔들 + 신호 수출 시작 (GitHub Actions) ===")

    try:
        from dashboard.data_aggregator import DataAggregator
        agg = DataAggregator()
    except Exception as e:
        print(f"DataAggregator 초기화 실패: {e}")
        sys.exit(1)

    # 신호 (공개 가격 데이터 기반 — Upbit/OKX 공개 API)
    try:
        signals = agg.get_signals()
        save("signals.json", signals)
    except Exception as e:
        print(f"signals 실패: {e}")

    # 캔들: Upbit (BTC/ETH 공개 API, 인증 불필요)
    for market in MARKETS:
        key = f"candles_upbit_{market}_day"
        try:
            candles = agg.get_candles("upbit", market, "day", 200)
            save(f"{key}.json", candles)
        except Exception as e:
            print(f"{key} 실패: {e}")

    # 캔들: OKX (BTC/ETH/SOL/XRP)
    for market in OKX_MARKETS:
        key = f"candles_okx_{market}_day"
        try:
            candles = agg.get_candles("okx", market, "day", 200)
            save(f"{key}.json", candles)
        except Exception as e:
            print(f"{key} 실패: {e}")

    print("=== 완료 ===")


if __name__ == "__main__":
    export_candles_and_signals()
