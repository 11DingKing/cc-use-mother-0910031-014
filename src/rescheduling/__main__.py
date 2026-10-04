"""命令行入口：python -m rescheduling --db ./data/app.db [--host 127.0.0.1] [--port 8080]"""
from __future__ import annotations

import argparse

from .api import serve
from .storage import Storage


def main() -> None:
    parser = argparse.ArgumentParser(description="临时变更联动重排后端")
    parser.add_argument("--db", default="data/rescheduling.db", help="SQLite 数据库路径")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    from .service import Service

    service = Service(Storage(args.db))
    print(f"临时变更联动重排服务已启动：http://{args.host}:{args.port}  数据库：{args.db}")
    serve(service, args.host, args.port)


if __name__ == "__main__":
    main()
