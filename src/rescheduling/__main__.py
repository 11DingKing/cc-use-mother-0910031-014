"""命令行入口：python3 -m rescheduling [--db PATH] [--host H] [--port P] [--no-seed]"""
from __future__ import annotations

import argparse

from .api import serve
from .seed import seed_if_empty
from .service import ChangeService
from .store import Store


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="临时变更联动重排后端服务")
    parser.add_argument("--db", default=":memory:", help="SQLite 数据库路径（默认内存）")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-seed", action="store_true", help="不注入演示数据")
    args = parser.parse_args(argv)

    store = Store(args.db)
    store.initialize()
    if not args.no_seed and seed_if_empty(store):
        print("已注入演示数据（V1-V3 场馆 / G1-G3 讲解员 / SE1-SE4 场次）")
    service = ChangeService(store)
    server = serve(service, args.host, args.port)
    print(f"服务已启动：http://{args.host}:{args.port} （Ctrl+C 停止）")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
