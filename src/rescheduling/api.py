"""HTTP API（仅依赖标准库）。

路由：
  POST /admin/venues | /admin/guides        基础资源登记
  POST /sessions  GET /sessions             场次维护
  POST /changes  GET /changes               创建变更（支持 Idempotency-Key 头）
  GET  /changes/{id}                         变更详情（含冻结步骤、未解决冲突）
  POST /changes/{id}/preview  GET .../preview 预演影响/候选/冲突
  POST /changes/{id}/submit                  提交审批
  POST /changes/{id}/approve  {approver, decisions}  部分接受并冻结
  POST /changes/{id}/reject
  POST /changes/{id}/execute                 执行 / 从失败点续办（幂等）
  POST /changes/{id}/rollback                补偿回滚（幂等）
  GET  /changes/{id}/messages                待投递/已投递消息
  POST /messages/deliver                     投递待发送消息
  GET  /changes/{id}/events  GET /events     事件流
"""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlparse

from .errors import ApiError
from .service import Service


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


class _Handler(BaseHTTPRequestHandler):
    server_version = "Rescheduling/0.2"
    service: Service  # 由 make_server 注入到类上

    def log_message(self, fmt: str, *args: Any) -> None:  # 安静
        return

    # ------------------------------------------------------------ 基础

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ApiError(f"请求体不是合法 JSON：{exc}")
        if not isinstance(data, dict):
            raise ApiError("请求体必须是 JSON 对象")
        return data

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, err: ApiError) -> None:
        self._send(err.http_status, {"error": err.to_dict()})

    # ------------------------------------------------------------ 分发

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        try:
            path = urlparse(self.path).path.rstrip("/") or "/"
            for pattern, verbs, fn in ROUTES:
                m = re.fullmatch(pattern, path)
                if m and method in verbs:
                    self._send(200, fn(self, **m.groupdict()))
                    return
            self._send(404, {"error": {"code": "NOT_FOUND",
                                       "message": f"无此路由：{method} {path}", "details": {}}})
        except ApiError as err:
            self._send_error(err)
        except Exception as exc:  # 未知异常不吞，返回 500 并保留现场供续办
            self._send(500, {"error": {"code": "INTERNAL_ERROR", "message": str(exc),
                                       "details": {}}})

    # ------------------------------------------------------------ 处理函数

    # 资源与场次
    def h_create_venue(self) -> dict:
        b = self._read_json()
        return self.service.register_venue(b["venue_id"], b["name"], b.get("manager_phone", ""))

    def h_create_guide(self) -> dict:
        b = self._read_json()
        return self.service.register_guide(b["guide_id"], b["name"], b.get("phone", ""))

    def h_create_session(self) -> dict:
        b = self._read_json()
        return self.service.create_session(
            b["session_id"], b["name"], b["venue_id"], b["guide_id"],
            b["start_ts"], b["end_ts"], b.get("school_contact", ""),
            b.get("state", "已排定"))

    def h_list_sessions(self) -> dict:
        return {"sessions": self.service.list_sessions()}

    # 变更
    def h_create_change(self) -> dict:
        b = self._read_json()
        key = self.headers.get("Idempotency-Key") or b.get("idempotency_key", "")
        return self.service.create_change(
            b["change_type"], b["resource_id"], b["unavailable_start"],
            b["unavailable_end"], b.get("reason", ""),
            change_id=b.get("change_id"), idempotency_key=key)

    def h_list_changes(self) -> dict:
        return {"changes": self.service.list_changes()}

    def h_get_change(self, cid: str) -> dict:
        return self.service.get_change(cid)

    def h_preview(self, cid: str) -> dict:
        return self.service.preview_change(cid)

    def h_get_preview(self, cid: str) -> dict:
        return self.service.get_preview(cid)

    def h_submit(self, cid: str) -> dict:
        return self.service.submit_for_approval(cid)

    def h_approve(self, cid: str) -> dict:
        b = self._read_json()
        return self.service.approve(cid, b.get("approver", "匿名审批人"),
                                    b.get("decisions", {}))

    def h_reject(self, cid: str) -> dict:
        b = self._read_json()
        return self.service.reject(cid, b.get("reason", ""))

    def h_void(self, cid: str) -> dict:
        b = self._read_json()
        return self.service.void_change(cid, b.get("reason", ""))

    def h_execute(self, cid: str) -> dict:
        return self.service.execute(cid)

    def h_rollback(self, cid: str) -> dict:
        return self.service.rollback(cid)

    def h_messages(self, cid: str) -> dict:
        return {"messages": self.service.list_messages(change_id=cid)}

    def h_events(self, cid: str) -> dict:
        return {"events": self.service.event_log(change_id=cid)}

    def h_all_events(self) -> dict:
        return {"events": self.service.event_log()}

    def h_deliver(self) -> dict:
        return self.service.deliver_pending()

    def h_health(self) -> dict:
        return {"status": "ok", "product": "临时变更联动重排"}


# (路径正则, 方法, 处理函数)
ROUTES: list[tuple[str, tuple[str, ...], Callable]] = [
    ("/health", ("GET",), _Handler.h_health),
    ("/admin/venues", ("POST",), _Handler.h_create_venue),
    ("/admin/guides", ("POST",), _Handler.h_create_guide),
    ("/sessions", ("POST",), _Handler.h_create_session),
    ("/sessions", ("GET",), _Handler.h_list_sessions),
    ("/changes", ("POST",), _Handler.h_create_change),
    ("/changes", ("GET",), _Handler.h_list_changes),
    ("/changes/(?P<cid>[^/]+)", ("GET",), _Handler.h_get_change),
    ("/changes/(?P<cid>[^/]+)/preview", ("POST",), _Handler.h_preview),
    ("/changes/(?P<cid>[^/]+)/preview", ("GET",), _Handler.h_get_preview),
    ("/changes/(?P<cid>[^/]+)/submit", ("POST",), _Handler.h_submit),
    ("/changes/(?P<cid>[^/]+)/approve", ("POST",), _Handler.h_approve),
    ("/changes/(?P<cid>[^/]+)/reject", ("POST",), _Handler.h_reject),
    ("/changes/(?P<cid>[^/]+)/void", ("POST",), _Handler.h_void),
    ("/changes/(?P<cid>[^/]+)/execute", ("POST",), _Handler.h_execute),
    ("/changes/(?P<cid>[^/]+)/rollback", ("POST",), _Handler.h_rollback),
    ("/changes/(?P<cid>[^/]+)/messages", ("GET",), _Handler.h_messages),
    ("/changes/(?P<cid>[^/]+)/events", ("GET",), _Handler.h_events),
    ("/events", ("GET",), _Handler.h_all_events),
    ("/messages/deliver", ("POST",), _Handler.h_deliver),
]


def make_server(service: Service, host: str = "127.0.0.1", port: int = 8080,
                daemon: bool = True) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (_Handler,), {"service": service})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = daemon
    return httpd


def serve(service: Service, host: str = "127.0.0.1", port: int = 8080) -> None:
    httpd = make_server(service, host, port, daemon=False)
    thread = threading.Thread(target=httpd.serve_forever, name="rescheduling-api", daemon=True)
    thread.start()
    try:
        thread.join()
    except KeyboardInterrupt:  # pragma: no cover
        httpd.shutdown()
