"""HTTP API 端到端测试：真实端口上验证统一流程与错误格式。"""
from __future__ import annotations

import json
import sys
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rescheduling.api import serve
from rescheduling.seed import seed_if_empty
from rescheduling.service import ChangeService
from rescheduling.store import Store

CLOSURE_BODY = {
    "type": "venue_closure",
    "resource_id": "V1",
    "window_start": "2026-10-10T08:00:00",
    "window_end": "2026-10-10T12:00:00",
    "reason": "消防演练临时闭馆",
    "actor": "场馆管理员",
}


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store(":memory:")
        self.store.initialize()
        seed_if_empty(self.store)
        self.server = serve(ChangeService(self.store), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def http(self, method: str, path: str, body: dict | None = None, headers: dict | None = None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    @staticmethod
    def _impact(detail, session_id):
        return next(i for i in detail["impacts"] if i["session_id"] == session_id)

    @staticmethod
    def _opt(impact, **match):
        return next(
            o for o in impact["options"] if all(o.get(k) == v for k, v in match.items())
        )

    def test_health_and_catalog(self):
        status, body = self.http("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        status, body = self.http("GET", "/venues")
        self.assertEqual(len(body["items"]), 3)
        status, body = self.http("GET", "/sessions?venue_id=V1")
        self.assertEqual([s["id"] for s in body["items"]], ["SE1", "SE2"])

    def test_full_flow_over_http(self):
        # 创建（幂等键）→ 重放
        status, body = self.http(
            "POST", "/change-requests", CLOSURE_BODY, {"Idempotency-Key": "req-1"}
        )
        self.assertEqual(status, 201)
        rid = body["request"]["id"]
        self.assertFalse(body["idempotent_replay"])
        status, replay = self.http(
            "POST", "/change-requests", CLOSURE_BODY, {"Idempotency-Key": "req-1"}
        )
        self.assertEqual(status, 200)
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(replay["request"]["id"], rid)

        # 无状态预演
        status, dry = self.http("POST", "/change-requests/dry-run", CLOSURE_BODY)
        self.assertEqual(status, 200)
        self.assertFalse(dry["persisted"])

        # 部分接受：SE1 改 V2，SE2 驳回
        detail = body["request"]
        imp1 = self._impact(detail, "SE1")
        imp2 = self._impact(detail, "SE2")
        opt1 = self._opt(imp1, venue_id="V2")
        status, approved = self.http(
            "POST",
            f"/change-requests/{rid}/approve",
            {
                "decisions": [
                    {"impact_id": imp1["id"], "action": "accept", "option_id": opt1["id"]},
                    {"impact_id": imp2["id"], "action": "reject"},
                ],
                "expected_version": 1,
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(approved["request"]["state"], "approved")

        # 重复审批 → 409
        status, err = self.http("POST", f"/change-requests/{rid}/approve", {"decisions": []})
        self.assertEqual(status, 409)
        self.assertEqual(err["error"]["code"], "STATE_CONFLICT")

        # 执行 → 部分生效
        status, summary = self.http("POST", f"/change-requests/{rid}/execute", {})
        self.assertEqual(status, 200)
        self.assertEqual(summary["state"], "partially_executed")

        # 未解决冲突可见
        status, conflicts = self.http("GET", f"/change-requests/{rid}/conflicts")
        self.assertEqual(status, 200)
        self.assertTrue(conflicts["has_unresolved"])
        self.assertEqual(conflicts["unresolved"][0]["session_id"], "SE2")

        # 待投递消息与排班一致：SE1 已改址且有 4 条待投递
        status, outbox = self.http("GET", "/outbox?status=pending")
        self.assertEqual(len(outbox["items"]), 4)
        status, sessions = self.http("GET", "/sessions?venue_id=V2")
        self.assertIn("SE1", {s["id"] for s in sessions["items"]})

        # 投递 FIFO
        status, dispatched = self.http("POST", "/outbox/dispatch", {})
        self.assertEqual(dispatched["count"], 4)
        status, outbox = self.http("GET", "/outbox?status=pending")
        self.assertEqual(outbox["items"], [])

        # 回滚：排班恢复 + 撤销通知落库
        status, rolled = self.http("POST", f"/change-requests/{rid}/rollback", {"reason": "演练取消"})
        self.assertEqual(rolled["state"], "rolled_back")
        status, sessions = self.http("GET", "/sessions?venue_id=V1")
        self.assertIn("SE1", {s["id"] for s in sessions["items"]})
        status, outbox = self.http("GET", "/outbox?status=pending")
        self.assertEqual(len(outbox["items"]), 4)
        self.assertEqual(outbox["items"][0]["payload"]["type"], "session_restored")

        # 审计轨迹
        status, audit = self.http("GET", f"/change-requests/{rid}/audit")
        events = [e["event"] for e in audit["items"]]
        self.assertIn("executed", events[0] + "".join(events))  # 含创建与执行轨迹
        self.assertEqual(events[0], "created")
        self.assertEqual(events[-1], "rolled_back")

    def test_failure_and_resume_over_http(self):
        status, body = self.http("POST", "/change-requests", CLOSURE_BODY)
        rid = body["request"]["id"]
        detail = body["request"]
        imp1 = self._impact(detail, "SE1")
        imp2 = self._impact(detail, "SE2")
        opt1 = self._opt(imp1, venue_id="V2")
        opt2_bad = self._opt(imp2, venue_id="V2")  # 容量冲突
        self.http(
            "POST",
            f"/change-requests/{rid}/approve",
            {
                "decisions": [
                    {"impact_id": imp1["id"], "action": "accept", "option_id": opt1["id"]},
                    {"impact_id": imp2["id"], "action": "accept", "option_id": opt2_bad["id"]},
                ]
            },
        )
        status, summary = self.http("POST", f"/change-requests/{rid}/execute", {})
        self.assertEqual(summary["state"], "failed")

        status, conflicts = self.http("GET", f"/change-requests/{rid}/conflicts")
        failed = [u for u in conflicts["unresolved"] if u["status"] == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertIn("容量", failed[0]["step_error"])

        opt2_good = self._opt(imp2, venue_id="V3")
        status, summary = self.http(
            "POST",
            f"/change-requests/{rid}/resume",
            {
                "decisions": [
                    {"impact_id": imp2["id"], "action": "accept", "option_id": opt2_good["id"]}
                ]
            },
        )
        self.assertEqual(summary["state"], "executed")

    def test_error_format_and_unknown_routes(self):
        status, body = self.http("GET", "/change-requests/CR-missing")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "NOT_FOUND")
        status, body = self.http("POST", "/change-requests", {"type": "bad"})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "VALIDATION")
        status, body = self.http("GET", "/no-such-route")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
