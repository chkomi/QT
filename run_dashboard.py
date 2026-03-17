#!/usr/bin/env python3
"""
퀀트 자동매매 웹 대시보드 실행

사용법:
    python run_dashboard.py

브라우저:
    http://localhost:8000
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

import uvicorn

if __name__ == "__main__":
    print("=" * 50)
    print("  퀀트 자동매매 대시보드")
    print("  http://localhost:8000")
    print("=" * 50)
    uvicorn.run(
        "dashboard.app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
    )
