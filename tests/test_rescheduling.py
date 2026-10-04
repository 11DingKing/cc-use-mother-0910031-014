"""临时变更联动重排后端测试。

覆盖：影响预演、候选方案、未解决冲突、部分接受、同事务 Outbox、
失败点续办、重复执行幂等、补偿回滚、并发顺序闸、前态漂移、幂等键、HTTP API。
"""
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

from rescheduling.clock import FixedClock  # noqa: E402
from rescheduling import domain as d  # noqa: E402
from rescheduling.errors import (  # noqa: E402
    BlockedByEarlierChangeError,
    UnresolvedConflictError,
)
from rescheduling.seed import seed_demo  # noqa: E402
from rescheduling.service import Service  # noqa: E402
from rescheduling.storage import Storage  # noqa: E402


def make_service() -> Service:
    svc = Service(Storage(":memory:"), FixedClock("2026-10-01T08:00:00"))
    seed_demo(svc)
    return svc


def prepare(svc: Service, change_id: str, *, change_type=d.VENUE_CLOSURE,
            resource_id="V1", start="2026-10-10T00:00", end="2026-10-10T23:59",
            reason="场馆检修") -> dict:
    svc.create_change(change_type, resource_id, start, end, reason, change_id=change_id)
    svc.preview_change(change_id)
    svc.submit_for_approval(change_id)
    return svc.approve(change_id, "统筹员甲")


class PreviewTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service()

    def test_impact_graph_and_candidates(self) -> None:
        self.svc.create_change(d.VENUE_CLOSURE, "V1",
                               "2026-10-10T00:00", "2026-10-10T23:59", "检修",
                               change_id="C1")
        prev = self.svc.preview_change("C1")
        impacted = {i["session_id"] for i in prev["impacted"]}
        # 10-10 当天 V1 的两场受影响；V2 的 S201、次日的 S103 不受影响
        self.assertEqual(impacted, {"S101", "S102"})
        # 每场至少给出一个候选，且按 rank 确定性排序
        for sid in impacted:
            opts = [o for o in prev["options"] if o["session_id"] == sid]
            self.assertTrue(opts)
            self.assertEqual([o["rank"] for o in opts], list(range(1, len(opts) + 1)))
        # S101 顺延 1 天时与 S103 撞 V1，最优候选必须换馆（V2）
        s101_first = next(o for o in prev["options"]
                          if o["session_id"] == "S101" and o["rank"] == 1)
        self.assertEqual(s101_first["payload"]["venue_id"], "V2")
        self.assertEqual(prev["unresolved_count"], 0)
        # 预演是只读快照，不改变排班
        self.assertEqual(self.svc.get_session("S101")["venue_id"], "V1")

    def test_non_movable_session_is_hard_conflict(self) -> None:
        self.svc.create_session(
            "S300", "进馆中的研学营", "V1", "G1",
            "2026-10-10T09:30", "2026-10-10T12:00", "钱老师", state="执行中")
        self.svc.create_change(d.VENUE_CLOSURE, "V1",
                               "2026-10-10T00:00", "2026-10-10T23:59",
                               change_id="C1")
        prev = self.svc.preview_change("C1")
        codes = {c["code"] for c in prev["conflicts"]}
        self.assertIn("SESSION_NOT_MOVABLE", codes)
        self.svc.submit_for_approval("C1")
        with self.assertRaises(UnresolvedConflictError) as ctx:
            self.svc.approve("C1", "甲")
        self.assertEqual(ctx.exception.details["conflicts"][0]["code"], "SESSION_NOT_MOVABLE")

    def test_preview_reflects_earlier_frozen_change(self) -> None:
        # C1 先冻结：S102 改到 10-11 14:00 V1/G1
        prepare(self.svc, "C1")
        # C2：G1 10-11 下午请假，S102 的冻结目标落在请假窗口内
        self.svc.create_change(d.GUIDE_LEAVE, "G1",
                               "2026-10-11T13:00", "2026-10-11T18:00", "事假",
                               change_id="C2")
        prev = self.svc.preview_change("C2")
        impacted = {i["session_id"] for i in prev["impacted"]}
        # 投影排班：S102 已被 C1 冻结到 10-11 下午，仍落入 G1 请假窗口
        self.assertIn("S102", impacted)
        s102 = self.svc.get_change("C1")
        self.assertEqual(s102["status"], d.APPROVED)
        for o in prev["options"]:
            if o["session_id"] == "S102":
                # 候选时间必须避开请假窗口
                self.assertGreaterEqual(o["payload"]["start_ts"], "2026-10-12T00:00")


class ExecutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service()

    def test_apply_updates_schedule_and_writes_outbox_in_one_flow(self) -> None:
        prepare(self.svc, "C1")
        summary = self.svc.execute("C1")
        self.assertEqual(summary["status"], d.APPLIED)
        self.assertEqual(summary["steps_done"], summary["steps_total"])
        # 排班已更新
        s101 = self.svc.get_session("S101")
        self.assertNotEqual((s101["venue_id"], s101["start_ts"]), ("V1", "2026-10-10T09:00"))
        self.assertEqual(s101["version"], 2)
        # 同事务已写入待投递消息（学校改期通知必达）
        pending = self.svc.list_messages("C1", d.MSG_PENDING)
        self.assertTrue(any(m["channel"] == "SCHOOL" and "改期" in m["subject"]
                            for m in pending))
        # 投递后全部 DELIVERED
        result = self.svc.deliver_pending()
        self.assertEqual(result["failed_count"], 0)
        self.assertEqual(
            len(self.svc.list_messages("C1", d.MSG_PENDING)), 0)

    def test_partial_acceptance_reschedule_and_cancel(self) -> None:
        # 同一变更内部分接受：S101 改期、S102 取消
        self.svc.create_change(d.VENUE_CLOSURE, "V1",
                               "2026-10-10T00:00", "2026-10-10T23:59", change_id="C1")
        prev = self.svc.preview_change("C1")
        self.svc.submit_for_approval("C1")
        option = next(o for o in prev["options"] if o["session_id"] == "S101" and o["rank"] == 1)
        self.svc.approve("C1", "统筹员乙", {
            "S101": {"action": d.ACT_RESCHEDULE, "option_id": option["option_id"]},
            "S102": {"action": d.ACT_CANCEL},
        })
        self.svc.execute("C1")
        s102 = self.svc.get_session("S102")
        self.assertEqual(s102["cancelled"], 1)
        s101 = self.svc.get_session("S101")
        self.assertEqual(s101["cancelled"], 0)
        self.assertNotEqual(s101["start_ts"], "2026-10-10T09:00")
        subjects = [m["subject"] for m in self.svc.list_messages("C1")]
        self.assertTrue(any("取消" in s for s in subjects))
        self.assertTrue(any("改期" in s for s in subjects))

    def test_repeated_execute_is_idempotent(self) -> None:
        prepare(self.svc, "C1")
        first = self.svc.execute("C1")
        second = self.svc.execute("C1")
        self.assertTrue(second["replayed"])
        self.assertEqual(second["status"], d.APPLIED)
        # 消息不重复
        self.assertEqual(
            len(self.svc.list_messages("C1")), first["messages_total"])

    def test_resume_from_failure_point(self) -> None:
        ch = prepare(self.svc, "C1")
        total = len(ch["steps"])
        # 在第 3 步（seq=2）提交前注入一次故障
        self.svc.faults.arm("C1", 2, "before")
        stalled = None
        try:
            self.svc.execute("C1")
        except Exception as exc:
            stalled = exc
        self.assertIsNotNone(stalled)
        detail = self.svc.get_change("C1")
        self.assertEqual(detail["status"], d.PARTIALLY_APPLIED)
        states = [s["state"] for s in detail["steps"]]
        self.assertEqual(states[:2], [d.STEP_DONE, d.STEP_DONE])
        self.assertNotEqual(states[2], d.STEP_DONE)
        # 已完成步骤的消息已在同事务落库
        self.assertGreaterEqual(len(self.svc.list_messages("C1", d.MSG_PENDING)), 1)
        msgs_before = len(self.svc.list_messages("C1"))

        # 从失败点安全续办
        summary = self.svc.execute("C1")
        self.assertEqual(summary["status"], d.APPLIED)
        self.assertEqual(summary["steps_done"], total)
        # 已完成步骤未重复产生消息
        self.assertEqual(len(self.svc.list_messages("C1")), msgs_before + (total - 2))

    def test_crash_after_final_commit_keeps_applied(self) -> None:
        ch = prepare(self.svc, "C1")
        last_seq = len(ch["steps"]) - 1
        # 模拟最后一步提交后、返回前崩溃：工作已持久化，调用仍得到确定的成功
        self.svc.faults.arm("C1", last_seq, "after")
        summary = self.svc.execute("C1")
        self.assertEqual(summary["status"], d.APPLIED)
        self.assertEqual(summary["steps_done"], summary["steps_total"])
        # 重试/续办幂等
        again = self.svc.execute("C1")
        self.assertEqual(again["status"], d.APPLIED)
        self.assertTrue(again["replayed"])

    def test_concurrent_duplicate_execute(self) -> None:
        prepare(self.svc, "C1")
        errors: list[Exception] = []

        def run() -> None:
            try:
                self.svc.execute("C1")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=run) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.svc.get_change("C1")["status"], d.APPLIED)
        # 与单线程执行的消息数一致（dedup_key 防重）
        single = make_service()
        prepare(single, "CX")
        single.execute("CX")
        self.assertEqual(len(self.svc.list_messages("C1")),
                         len(single.list_messages("CX")))


class OrderingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service()

    def test_later_approved_plan_invalidated_when_earlier_approved(self) -> None:
        # C1（seq 小）创建后停留草稿；C2（seq 大）先完成审批
        self.svc.create_change(d.VENUE_CLOSURE, "V1",
                               "2026-10-10T00:00", "2026-10-10T23:59", "检修",
                               change_id="C1")
        prepare(self.svc, "C2", change_type=d.GUIDE_LEAVE, resource_id="G2",
                start="2026-10-11T00:00", end="2026-10-11T23:59", reason="事假")
        self.assertEqual(self.svc.get_change("C2")["status"], d.APPROVED)
        # 之后 C1 才预演、提交、批准 -> seq 更大的 C2 方案所依据的世界改变，失效
        self.svc.preview_change("C1")
        self.svc.submit_for_approval("C1")
        result = self.svc.approve("C1", "甲")
        invalidated = {x["change_id"] for x in result["invalidated_later"]}
        self.assertIn("C2", invalidated)
        self.assertEqual(self.svc.get_change("C2")["status"], d.DRAFT)
        self.assertEqual(self.svc.get_change("C2")["steps"], [])
        types = {e["type"] for e in self.svc.event_log("C2")}
        self.assertIn("PLAN_INVALIDATED", types)

    def test_changes_execute_in_global_seq_order(self) -> None:
        prepare(self.svc, "C1", resource_id="V1")
        prepare(self.svc, "C2", change_type=d.GUIDE_LEAVE, resource_id="G2",
                start="2026-10-11T00:00", end="2026-10-11T23:59", reason="事假")
        # 后序变更先执行 -> 被顺序闸拒绝
        with self.assertRaises(BlockedByEarlierChangeError) as ctx:
            self.svc.execute("C2")
        self.assertEqual(ctx.exception.details["blocked_by"], "C1")
        self.svc.execute("C1")
        self.svc.execute("C2")
        self.assertEqual(self.svc.get_change("C2")["status"], d.APPLIED)
        # 回滚必须逆序：C1 在 C2 未回滚前不能回滚
        with self.assertRaises(BlockedByEarlierChangeError):
            self.svc.rollback("C1")
        self.svc.rollback("C2")
        self.svc.rollback("C1")
        self.assertEqual(self.svc.get_change("C1")["status"], d.ROLLED_BACK)


class RollbackAndDriftTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service()

    def test_rollback_restores_schedule_and_notifies(self) -> None:
        prepare(self.svc, "C1")
        before = {sid: self.svc.get_session(sid) for sid in ("S101", "S102")}
        self.svc.execute("C1")
        self.svc.rollback("C1")
        for sid, original in before.items():
            cur = self.svc.get_session(sid)
            self.assertEqual((cur["venue_id"], cur["guide_id"], cur["start_ts"]),
                             (original["venue_id"], original["guide_id"], original["start_ts"]))
        rb_msgs = [m for m in self.svc.list_messages("C1") if "恢复" in m["subject"] or "解除" in m["subject"]]
        self.assertTrue(rb_msgs)
        # 回滚后可重新预演、审批、执行
        self.svc.preview_change("C1")
        self.svc.submit_for_approval("C1")
        self.svc.approve("C1", "甲")
        summary = self.svc.execute("C1")
        self.assertEqual(summary["status"], d.APPLIED)

    def test_drift_after_approval_stalls_at_step(self) -> None:
        prepare(self.svc, "C1")
        # 审批后、执行前，排班被外部改动（版本漂移）
        with self.svc.storage.transaction(immediate=True) as conn:
            conn.execute("UPDATE sessions SET version=version+1 WHERE session_id=?",
                         ("S101",))
        stalled = None
        try:
            self.svc.execute("C1")
        except Exception as exc:  # noqa: BLE001
            stalled = exc
        self.assertIsNotNone(stalled)
        change = self.svc.get_change("C1")
        self.assertEqual(change["status"], d.PARTIALLY_APPLIED)
        failed = [s for s in change["steps"] if s["state"] == d.STEP_FAILED]
        self.assertEqual(len(failed), 1)
        self.assertIn("S101", failed[0]["last_error"])
        # 回滚已生效步骤后可重来
        self.svc.rollback("C1")
        self.assertEqual(self.svc.get_change("C1")["status"], d.ROLLED_BACK)

    def test_idempotency_key_dedups_create(self) -> None:
        payload = dict(change_type=d.VENUE_CLOSURE, resource_id="V1",
                       unavailable_start="2026-10-10T00:00",
                       unavailable_end="2026-10-10T23:59", reason="x")
        first = self.svc.create_change(**payload, idempotency_key="key-1")
        second = self.svc.create_change(**payload, idempotency_key="key-1")
        self.assertEqual(first["change_id"], second["change_id"])
        self.assertTrue(second.get("idempotent_replay"))


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        from rescheduling.api import make_server

        self.svc = make_service()
        self.httpd = make_server(self.svc, "127.0.0.1", 0)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)

    def call(self, method: str, path: str, body: dict | None = None,
             headers: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def test_full_flow_over_http(self) -> None:
        status, body = self.call("POST", "/changes", {
            "change_type": "VENUE_CLOSURE", "resource_id": "V1",
            "unavailable_start": "2026-10-10T00:00",
            "unavailable_end": "2026-10-10T23:59", "reason": "检修"})
        self.assertEqual(status, 200)
        cid = body["change_id"]

        status, body = self.call("POST", f"/changes/{cid}/preview")
        self.assertEqual(status, 200)
        self.assertEqual({i["session_id"] for i in body["impacted"]}, {"S101", "S102"})

        self.assertEqual(self.call("POST", f"/changes/{cid}/submit")[0], 200)
        self.assertEqual(self.call("POST", f"/changes/{cid}/approve",
                                   {"approver": "甲"})[0], 200)
        status, body = self.call("POST", f"/changes/{cid}/execute")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "APPLIED")
        self.assertEqual(self.call("POST", "/messages/deliver")[0], 200)
        status, body = self.call("GET", f"/changes/{cid}/messages")
        self.assertTrue(all(m["status"] == "DELIVERED" for m in body["messages"]))

    def test_conflict_returned_as_409(self) -> None:
        self.svc.create_session("S300", "进行场", "V1", "G1",
                                "2026-10-10T09:00", "2026-10-10T12:00",
                                state="执行中")
        self.call("POST", "/changes", {
            "change_type": "VENUE_CLOSURE", "resource_id": "V1",
            "unavailable_start": "2026-10-10T00:00",
            "unavailable_end": "2026-10-10T23:59"})
        # 找到刚建的变更
        cid = self.call("GET", "/changes")[1]["changes"][0]["change_id"]
        self.call("POST", f"/changes/{cid}/preview")
        self.call("POST", f"/changes/{cid}/submit")
        status, body = self.call("POST", f"/changes/{cid}/approve", {"approver": "甲"})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "UNRESOLVED_CONFLICT")

    def test_idempotency_key_header(self) -> None:
        payload = {"change_type": "VENUE_CLOSURE", "resource_id": "V1",
                   "unavailable_start": "2026-10-10T00:00",
                   "unavailable_end": "2026-10-10T23:59"}
        s1, b1 = self.call("POST", "/changes", payload, {"Idempotency-Key": "K1"})
        s2, b2 = self.call("POST", "/changes", payload, {"Idempotency-Key": "K1"})
        self.assertEqual((s1, s2), (200, 200))
        self.assertEqual(b1["change_id"], b2["change_id"])


if __name__ == "__main__":
    unittest.main()
