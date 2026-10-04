"""启动临时变更联动重排后端服务。

用法：python3 tools/run_server.py [--db var/rescheduling.db] [--port 8080] [--no-seed]
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rescheduling.__main__ import main

if __name__ == "__main__":
    raise SystemExit(main())
