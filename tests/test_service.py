"""统一流程服务测试：预演、部分接受、执行、幂等、失败续办、回滚与冲突视图。"""
from __future__ import annotations

import sys
import unittest
import unittest.mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rescheduling.errors import (
    IdempotencyMismatchError,
    StateConflictError,
    VersionConflictError,
)
from rescheduling.seed import seed_if_empty
from rescheduling.service import ChangeService
from rescheduling.store import Store

CLOSURE_PAYLOAD = {
    "type": "venue_closure",
    "resource_id": "V1",
    "window_start": "2026-10-10T08:00:00",
    "window_end": "2026-10-10T12:00:00",
    "reason": "消防演练临时闭馆",
    "actor": "场馆管理员",
}


class ServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store(":memory:")
        self.store.initialize()
        seed_if_empty(self.store)
        self.svc = ChangeService(self.store)

    # ------------------------------------------------------------------
    # 工具
    # ------------------------------------------------------------------
    def _create_closure(self, **over):
        payload = {**CLOSURE_PAYLOAD, **over}
        return self.svc.create_request(payload, idempotency_key=over.get("idempotency_key"))

    @staticmethod
    def _impact(detail, session_id):
        return next(i for i in detail["impacts"] if i["session_id"] == session_id)

    @staticmethod
    def _opt(impact, **match):
        for opt in impact["options"]:
            if all(opt.get(k) == v for k, v in match.items()):
                return opt
        raise AssertionError(f"未找到候选方案：{match}")

    def _session(self, session_id):
        return next(s for s in self.svc.list_sessions() if s["id"] == session_id)

    def _approve_all_clean(self):
        """闭馆 V1 → SE1 改 V2、SE2 改 V3，全部接受。"""
        detail, _ = self._create_closure()
        rid = detail["id"]
        imp1 = self._impact(detail, "SE1")
        imp2 = self._impact(detail, "SE2")
        opt1 = self._opt(imp1, venue_id="V2")
        opt2 = self._opt(imp2, venue_id="V3")
        self.svc.approve(
            rid,
            [
                {"impact_id": imp1["id"], "action": "accept", "option_id": opt1["id"]},
                {"impact_id": imp2["id"], "action": "accept", "option_id": opt2["id"]},
            ],
        )
        return rid, imp1, imp2

    # ------------------------------------------------------------------
    # 预演与候选方案
    # ------------------------------------------------------------------
    def test_preview_impacts_and_ranked_options(self):
        detail, replayed = self._create_closure()
        self.assertFalse(replayed)
        self.assertEqual(detail["state"], "pending_approval")
        self.assertEqual([i["session_id"] for i in detail["impacts"]], ["SE1", "SE2"])

        imp1 = self._impact(detail, "SE1")
        self.assertEqual(imp1["impact_type"], "venue_unavailable")
        ranked = [(o["rank"], o["venue_id"], o["kind"]) for o in imp1["options"]]
        self.assertEqual(ranked, [(1, "V2", "reassign"), (2, "V3", "reassign"), (3, None, "cancel")])
        self.assertEqual(imp1["options"][0]["conflicts"], [])

        imp2 = self._impact(detail, "SE2")
        opt_v2 = self._opt(imp2, venue_id="V2")
        self.assertEqual(opt_v2["conflicts"][0]["code"], "capacity")  # 90 人 > 科普教室 40
        opt_v3 = self._opt(imp2, venue_id="V3")
        self.assertEqual(opt_v3["conflicts"], [])
        self.assertEqual(opt_v3["rank"], 1)

    def test_dry_run_is_stateless(self):
        result = self.svc.dry_run(CLOSURE_PAYLOAD)
        self.assertFalse(result["persisted"])
        self.assertEqual([i["session_id"] for i in result["impacts"]], ["SE1", "SE2"])
        self.assertEqual(self.svc.list_requests(), [])

    # ------------------------------------------------------------------
    # 部分接受与未解决冲突
    # ------------------------------------------------------------------
    def test_partial_accept_leaves_unresolved_visible(self):
        detail, _ = self._create_closure()
        rid = detail["id"]
        imp1 = self._impact(detail, "SE1")
        imp2 = self._impact(detail, "SE2")
        opt1 = self._opt(imp1, venue_id="V2")
        self.svc.approve(
            rid,
            [
                {"impact_id": imp1["id"], "action": "accept", "option_id": opt1["id"]},
                {"impact_id": imp2["id"], "action": "reject"},
            ],
        )
        summary = self.svc.execute(rid)
        self.assertEqual(summary["state"], "partially_executed")
        self.assertEqual(self._session("SE1")["venue_id"], "V2")
        self.assertEqual(self._session("SE2")["venue_id"], "V1")  # 未接受，保持原样

        conflicts = self.svc.get_conflicts(rid)
        self.assertTrue(conflicts["has_unresolved"])
        self.assertEqual(conflicts["unresolved"][0]["session_id"], "SE2")
        self.assertEqual(conflicts["unresolved"][0]["status"], "skipped")

    # ------------------------------------------------------------------
    # 排班与通知同事务
    # ------------------------------------------------------------------
    def test_execute_updates_schedule_and_outbox_atomically(self):
        rid, _, _ = self._approve_all_clean()
        summary = self.svc.execute(rid)
        self.assertEqual(summary["state"], "executed")
        self.assertEqual(self._session("SE1")["venue_id"], "V2")
        self.assertEqual(self._session("SE2")["venue_id"], "V3")

        pending = self.svc.list_outbox(status="pending")
        self.assertEqual(len(pending), 8)  # 每场 4 个接收方
        recipients_se1 = {
            m["recipient"] for m in pending if m["session_id"] == "SE1"
        }
        self.assertEqual(
            recipients_se1, {"group:SCH1", "guide:G1", "venue:V1", "venue:V2"}
        )
        sample = pending[0]["payload"]
        self.assertIn(sample["type"], {"session_rescheduled"})
        self.assertIn("before", sample)
        self.assertIn("after", sample)

        delivered = self.svc.dispatch_outbox()
        self.assertEqual(len(delivered), 8)
        self.assertEqual(self.svc.dispatch_outbox(), [])  # 投递幂等
        self.assertEqual(self.svc.list_outbox(status="pending"), [])

    def test_failed_step_has_no_partial_side_effects(self):
        """SE2 选了有容量冲突的方案：SE1 生效且通知落库，SE2 排班与通知都不变。"""
        detail, _ = self._create_closure()
        rid = detail["id"]
        imp1 = self._impact(detail, "SE1")
        imp2 = self._impact(detail, "SE2")
        opt1 = self._opt(imp1, venue_id="V2")
        opt2_bad = self._opt(imp2, venue_id="V2")  # 容量不足
        result = self.svc.approve(
            rid,
            [
                {"impact_id": imp1["id"], "action": "accept", "option_id": opt1["id"]},
                {"impact_id": imp2["id"], "action": "accept", "option_id": opt2_bad["id"]},
            ],
        )
        self.assertTrue(result["warnings"])  # 审批时已提示冲突

        summary = self.svc.execute(rid)
        self.assertEqual(summary["state"], "failed")
        self.assertIn("容量", summary["last_error"])
        self.assertEqual(self._session("SE1")["venue_id"], "V2")
        self.assertEqual(self._session("SE2")["venue_id"], "V1")
        outbox = self.svc.list_outbox()
        self.assertEqual(len(outbox), 4)  # 只有 SE1 的通知
        self.assertTrue(all(m["session_id"] == "SE1" for m in outbox))

        # 续办：为 SE2 改选 V3，从失败点继续
        opt2_good = self._opt(imp2, venue_id="V3")
        summary = self.svc.resume(
            rid,
            decisions=[
                {"impact_id": imp2["id"], "action": "accept", "option_id": opt2_good["id"]}
            ],
        )
        self.assertEqual(summary["state"], "executed")
        self.assertEqual(self._session("SE2")["venue_id"], "V3")
        self.assertEqual(len(self.svc.list_outbox()), 8)  # 无重复通知
        self.assertEqual(summary["steps"]["applied"], 2)

    def test_simulated_crash_then_resume(self):
        """执行中途崩溃：请求停在 executing，续办从断点完成。"""
        rid, _, _ = self._approve_all_clean()

        def boom(self_, request_id, step):
            raise RuntimeError("模拟进程崩溃")

        with unittest.mock.patch.object(ChangeService, "_apply_step", boom):
            with self.assertRaises(RuntimeError):
                self.svc.execute(rid)
        self.assertEqual(self.svc.get_request(rid)["state"], "executing")
        self.assertEqual(self._session("SE1")["venue_id"], "V1")  # 未产生部分写入

        summary = self.svc.resume(rid)
        self.assertEqual(summary["state"], "executed")
        self.assertEqual(self._session("SE1")["venue_id"], "V2")
        self.assertEqual(len(self.svc.list_outbox()), 8)

    def test_simulated_failure_drill(self):
        rid, _, _ = self._approve_all_clean()
        summary = self.svc.execute(rid, simulate_failure_at=2)
        self.assertEqual(summary["state"], "failed")
        self.assertIn("模拟故障", summary["last_error"])
        self.assertEqual(self._session("SE1")["venue_id"], "V2")  # 第 1 步已提交
        summary = self.svc.resume(rid)
        self.assertEqual(summary["state"], "executed")

    # ------------------------------------------------------------------
    # 幂等
    # ------------------------------------------------------------------
    def test_idempotency_key_replay_and_mismatch(self):
        detail1, replayed1 = self._create_closure(idempotency_key="key-001")
        detail2, replayed2 = self._create_closure(idempotency_key="key-001")
        self.assertFalse(replayed1)
        self.assertTrue(replayed2)
        self.assertEqual(detail1["id"], detail2["id"])
        self.assertEqual(len(self.svc.list_requests()), 1)
        with self.assertRaises(IdempotencyMismatchError):
            self._create_closure(idempotency_key="key-001", reason="不同内容")

    def test_execute_is_idempotent(self):
        rid, _, _ = self._approve_all_clean()
        first = self.svc.execute(rid)
        second = self.svc.execute(rid)
        self.assertEqual(first["state"], "executed")
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(len(self.svc.list_outbox()), 8)
        self.assertEqual(second["steps"]["applied"], 2)

    def test_version_conflict_on_approve(self):
        detail, _ = self._create_closure()
        with self.assertRaises(VersionConflictError):
            self.svc.approve(detail["id"], [], expected_version=99)

    # ------------------------------------------------------------------
    # 回滚
    # ------------------------------------------------------------------
    def test_rollback_restores_and_notifies_in_reverse_order(self):
        rid, _, _ = self._approve_all_clean()
        self.svc.execute(rid)
        summary = self.svc.rollback(rid, reason="闭馆取消")
        self.assertEqual(summary["state"], "rolled_back")
        self.assertEqual(self._session("SE1")["venue_id"], "V1")
        self.assertEqual(self._session("SE1")["state"], "已排定")
        self.assertEqual(self._session("SE2")["venue_id"], "V1")

        outbox = self.svc.list_outbox()
        self.assertEqual(len(outbox), 16)  # 8 条改排 + 8 条撤销
        restored = [m for m in outbox if m["payload"]["type"] == "session_restored"]
        self.assertEqual(len(restored), 8)

        again = self.svc.rollback(rid)  # 回滚幂等
        self.assertTrue(again["idempotent_replay"])
        self.assertEqual(len(self.svc.list_outbox()), 16)
        with self.assertRaises(StateConflictError):
            self.svc.execute(rid)  # 已回滚不能再执行

    # ------------------------------------------------------------------
    # 并发变更的确定顺序
    # ------------------------------------------------------------------
    def test_session_lock_blocks_second_request(self):
        first, _ = self._create_closure()
        second, _ = self.svc.create_request(
            {
                "type": "guide_leave",
                "resource_id": "G1",
                "window_start": "2026-10-10T08:00:00",
                "window_end": "2026-10-10T15:00:00",
                "reason": "讲解员请假",
            }
        )
        locked = self._impact(second, "SE1")
        self.assertEqual(locked["locked_by"], first["id"])
        self.assertEqual(locked["options"], [])
        free = self._impact(second, "SE3")
        self.assertIsNone(free["locked_by"])
        self.assertTrue(free["options"])

        # 第一个请求驳回后释放锁，重新预演即可排方案
        self.svc.reject(first["id"], reason="改期")
        refreshed = self.svc.preview(second["id"])
        self.assertIsNone(self._impact(refreshed, "SE1")["locked_by"])
        self.assertTrue(self._impact(refreshed, "SE1")["options"])

    def test_executed_closure_blocks_later_option_at_execute_time(self):
        """A 已批准未执行时，B 闭馆先生效：A 执行时确定性失败并可续办改选。"""
        detail_a, _ = self._create_closure()
        rid_a = detail_a["id"]
        imp1 = self._impact(detail_a, "SE1")
        imp2 = self._impact(detail_a, "SE2")
        opt1_v2 = self._opt(imp1, venue_id="V2")
        opt2_v3 = self._opt(imp2, venue_id="V3")
        self.svc.approve(
            rid_a,
            [
                {"impact_id": imp1["id"], "action": "accept", "option_id": opt1_v2["id"]},
                {"impact_id": imp2["id"], "action": "accept", "option_id": opt2_v3["id"]},
            ],
        )
        # B：V2 同时段闭馆（不影响现有场次，但使 V2 在窗口内不可用）
        detail_b, _ = self._create_closure(resource_id="V2", reason="设备检修")
        self.svc.approve(detail_b["id"], [])
        self.assertEqual(self.svc.execute(detail_b["id"])["state"], "executed")

        summary = self.svc.execute(rid_a)
        self.assertEqual(summary["state"], "failed")
        self.assertIn("闭馆", summary["last_error"])
        self.assertEqual(self._session("SE1")["venue_id"], "V1")

        opt1_v3 = self._opt(imp1, venue_id="V3")
        summary = self.svc.resume(
            rid_a,
            decisions=[
                {"impact_id": imp1["id"], "action": "accept", "option_id": opt1_v3["id"]}
            ],
        )
        self.assertEqual(summary["state"], "executed")
        self.assertEqual(self._session("SE1")["venue_id"], "V3")
        self.assertEqual(self._session("SE2")["venue_id"], "V3")

    # ------------------------------------------------------------------
    # 审计
    # ------------------------------------------------------------------
    def test_audit_trail_records_deterministic_events(self):
        rid, _, _ = self._approve_all_clean()
        self.svc.execute(rid)
        events = [e["event"] for e in self.svc.get_audit(rid)]
        self.assertEqual(events[0], "created")
        self.assertIn("approved", events)
        self.assertIn("execute_started", events)
        self.assertEqual(events.count("step_applied"), 2)
        self.assertEqual(events[-1], "executed")


if __name__ == "__main__":
    unittest.main()
