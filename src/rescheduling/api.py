"""标准库 HTTP API：变更请求、预演、冲突、续办、回滚与消息投递。

仅依赖 Python 标准库；路由 + JSON 错误格式统一为
{"error": {"code", "message", "details"}}。
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .errors import DomainError, NotFoundError, ValidationError
from .service import ChangeService

Handler = tuple[int, dict]


def _first(query: dict, key: str) -> str | None:
    values = query.get(key)
    return values[0] if values else None


class _Router:
    def __init__(self) -> None:
        self.routes: list[tuple[str, re.Pattern, object]] = []

    def add(self, method: str, pattern: str, handler) -> None:
        regex = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern)
        self.routes.append((method, re.compile(f"^{regex}$"), handler))

    def dispatch(self, service, method, path, query, body, headers) -> Handler:
        allowed = False
        for route_method, regex, handler in self.routes:
            match = regex.match(path)
            if not match:
                continue
            if route_method != method:
                allowed = True
                continue
            return handler(service, match.groupdict(), query, body, headers)
        if allowed:
            raise ValidationError(f"方法不允许：{method} {path}")
        raise NotFoundError(f"路由不存在：{method} {path}")


ROUTER = _Router()


def route(method: str, pattern: str):
    def decorator(func):
        ROUTER.add(method, pattern, func)
        return func

    return decorator


@route("GET", "/health")
def _health(service, params, query, body, headers) -> Handler:
    return 200, {"status": "ok"}


@route("GET", "/venues")
def _venues(service, params, query, body, headers) -> Handler:
    return 200, {"items": service.list_resources("venues")}


@route("GET", "/guides")
def _guides(service, params, query, body, headers) -> Handler:
    return 200, {"items": service.list_resources("guides")}


@route("GET", "/groups")
def _groups(service, params, query, body, headers) -> Handler:
    return 200, {"items": service.list_resources("groups")}


@route("GET", "/sessions")
def _sessions(service, params, query, body, headers) -> Handler:
    filters = {k: _first(query, k) for k in ("venue_id", "guide_id", "group_id", "state", "date")}
    filters = {k: v for k, v in filters.items() if v}
    return 200, {"items": service.list_sessions(filters)}


@route("POST", "/sessions")
def _create_session(service, params, query, body, headers) -> Handler:
    return 201, {"session": service.create_session(body)}


@route("POST", "/change-requests")
def _create_request(service, params, query, body, headers) -> Handler:
    detail, replayed = service.create_request(body, idempotency_key=headers.get("Idempotency-Key"))
    return (200 if replayed else 201), {"request": detail, "idempotent_replay": replayed}


@route("POST", "/change-requests/dry-run")
def _dry_run(service, params, query, body, headers) -> Handler:
    return 200, service.dry_run(body)


@route("GET", "/change-requests")
def _list_requests(service, params, query, body, headers) -> Handler:
    return 200, {"items": service.list_requests(state=_first(query, "state"))}


@route("GET", "/change-requests/{rid}")
def _get_request(service, params, query, body, headers) -> Handler:
    return 200, {"request": service.get_request(params["rid"])}


@route("POST", "/change-requests/{rid}/preview")
def _preview(service, params, query, body, headers) -> Handler:
    return 200, {"request": service.preview(params["rid"])}


@route("POST", "/change-requests/{rid}/approve")
def _approve(service, params, query, body, headers) -> Handler:
    result = service.approve(
        params["rid"],
        body.get("decisions", []),
        expected_version=body.get("expected_version"),
        actor=body.get("actor", ""),
    )
    return 200, result


@route("POST", "/change-requests/{rid}/reject")
def _reject(service, params, query, body, headers) -> Handler:
    return 200, {"request": service.reject(params["rid"], body.get("reason", ""), body.get("actor", ""))}


@route("POST", "/change-requests/{rid}/execute")
def _execute(service, params, query, body, headers) -> Handler:
    return 200, service.execute(
        params["rid"],
        expected_version=body.get("expected_version"),
        simulate_failure_at=body.get("simulate_failure_at"),
    )


@route("POST", "/change-requests/{rid}/resume")
def _resume(service, params, query, body, headers) -> Handler:
    return 200, service.resume(
        params["rid"],
        decisions=body.get("decisions"),
        expected_version=body.get("expected_version"),
    )


@route("POST", "/change-requests/{rid}/rollback")
def _rollback(service, params, query, body, headers) -> Handler:
    return 200, service.rollback(
        params["rid"],
        reason=body.get("reason", ""),
        expected_version=body.get("expected_version"),
    )


@route("GET", "/change-requests/{rid}/conflicts")
def _conflicts(service, params, query, body, headers) -> Handler:
    return 200, service.get_conflicts(params["rid"])


@route("GET", "/change-requests/{rid}/audit")
def _audit(service, params, query, body, headers) -> Handler:
    return 200, {"items": service.get_audit(params["rid"])}


@route("GET", "/outbox")
def _outbox(service, params, query, body, headers) -> Handler:
    return 200, {
        "items": service.list_outbox(
            status=_first(query, "status"), request_id=_first(query, "request_id")
        )
    }


@route("POST", "/outbox/dispatch")
def _dispatch(service, params, query, body, headers) -> Handler:
    delivered = service.dispatch_outbox(limit=body.get("limit"))
    return 200, {"delivered": delivered, "count": len(delivered)}


def make_handler(service: ChangeService):
    class RequestHandler(BaseHTTPRequestHandler):
        server_version = "Rescheduling/1.0"

        def _handle(self, method: str) -> None:
            try:
                parsed = urlparse(self.path)
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw.decode("utf-8")) if raw else {}
                if not isinstance(body, dict):
                    raise ValidationError("请求体必须是 JSON 对象")
                status, payload = ROUTER.dispatch(
                    service, method, parsed.path, parse_qs(parsed.query), body, self.headers
                )
                self._send(status, payload)
            except DomainError as exc:
                self._send(exc.http_status, {"error": exc.to_dict()})
            except (json.JSONDecodeError, UnicodeDecodeError):
                self._send(
                    400,
                    {"error": {"code": "BAD_JSON", "message": "请求体不是合法 JSON", "details": {}}},
                )
            except Exception as exc:  # noqa: BLE001 - 兜底，避免连接悬挂
                self._send(
                    500,
                    {"error": {"code": "INTERNAL", "message": str(exc), "details": {}}},
                )

        def _send(self, status: int, payload: dict) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            self._handle("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._handle("POST")

        def log_message(self, *args) -> None:  # 静默访问日志
            return

    return RequestHandler


def serve(service: ChangeService, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    """构建（未启动的）HTTP 服务；调用方负责 serve_forever。"""
    return ThreadingHTTPServer((host, port), make_handler(service))
