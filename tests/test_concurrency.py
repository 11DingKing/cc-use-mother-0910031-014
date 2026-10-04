"""并发变更的确定顺序测试：文件库 + 多线程。"""
from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rescheduling.errors import DomainError
from rescheduling.seed import seed_if_empty
from rescheduling.service import ChangeService
from rescheduling.store import Store

CLOSURE_V1 = {
    "type": "venue_closure",
    "resource_id": "V1",
    "window_start": "2026-10-10T08:00:00",
    "window_end": "2026-10-10T12:00:00",
    "reason": "闭馆",
}
LEAVE_G1 = {
    "type": "guide_leave",
    "resource_id": "G1",
    "window_start": "2026-10-10T08:00:00",
    "window_end": "2026-10-10T15:00:00",
    "reason": "请假",
}
LEAVE_G3 = {
    "type": "guide_leave",
    "resource_id": "G3",
    "window_start": "2026-10-10T13:00:00",
    "window_end": "2026-10-10T16:00:00",
    "reason": "请假",
}


class ConcurrencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(str(Path(self.tmp.name) / "rescheduling.db"))
        self.store.initialize()
        seed_if_empty(self.store)
        self.svc = ChangeService(self.store)

    @staticmethod
    def _impact(detail, session_id):
        return next(i for i in detail["impacts"] if i["session_id"] == session_id)

    @staticmethod
    def _opt(impact, **match):
        return next(
            o for o in impact["options"] if all(o.get(k) == v for k, v in match.items())
        )

    def test_concurrent_creates_deterministic_session_lock(self):
        """两个请求同时争抢 SE1：恰好一个持锁，另一个预演即见冲突。"""
        barrier = threading.Barrier(2)

        def create(payload):
            barrier.wait()
            return self.svc.create_request(payload)[0]

        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.map(create, [CLOSURE_V1, LEAVE_G1])
        details = [self.svc.get_request(r["id"]) for r in (first, second)]
        locks = [self._impact(d, "SE1")["locked_by"] for d in details]
        holders = [d["id"] for d, lock in zip(details, locks) if lock is None]
        blocked = [lock for lock in locks if lock is not None]
        self.assertEqual(len(holders), 1)
        self.assertEqual(blocked, [holders[0]])

    def test_approve_race_exactly_one_wins(self):
        detail, _ = self.svc.create_request(CLOSURE_V1)
        rid = detail["id"]

        def approve():
            try:
                self.svc.approve(rid, [], expected_version=1)
                return "ok"
            except DomainError as exc:
                return exc.code

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: approve(), range(4)))
        self.assertEqual(results.count("ok"), 1)
        self.assertTrue(all(r in ("ok", "STATE_CONFLICT", "VERSION_CONFLICT") for r in results))
        self.assertEqual(self.svc.get_request(rid)["state"], "approved")

    def test_concurrent_execute_disjoint_requests(self):
        """互不相干的两个请求并发执行：都成功且通知不重不漏。"""
        detail_a, _ = self.svc.create_request(CLOSURE_V1)
        imp1 = self._impact(detail_a, "SE1")
        imp2 = self._impact(detail_a, "SE2")
        self.svc.approve(
            detail_a["id"],
            [
                {
                    "impact_id": imp1["id"],
                    "action": "accept",
                    "option_id": self._opt(imp1, venue_id="V2")["id"],
                },
                {
                    "impact_id": imp2["id"],
                    "action": "accept",
                    "option_id": self._opt(imp2, venue_id="V3")["id"],
                },
            ],
        )
        detail_b, _ = self.svc.create_request(LEAVE_G3)
        imp4 = self._impact(detail_b, "SE4")
        self.svc.approve(
            detail_b["id"],
            [
                {
                    "impact_id": imp4["id"],
                    "action": "accept",
                    "option_id": self._opt(imp4, guide_id="G2")["id"],
                }
            ],
        )
        with ThreadPoolExecutor(max_workers=2) as pool:
            summaries = list(
                pool.map(self.svc.execute, [detail_a["id"], detail_b["id"]])
            )
        self.assertEqual({s["state"] for s in summaries}, {"executed"})
        outbox = self.svc.list_outbox()
        self.assertEqual(len(outbox), 12)  # A 两场 8 条 + B 一场 4 条
        self.assertEqual(len({m["dedupe_key"] for m in outbox}), 12)
        sessions = {s["id"]: s for s in self.svc.list_sessions()}
        self.assertEqual(sessions["SE1"]["venue_id"], "V2")
        self.assertEqual(sessions["SE2"]["venue_id"], "V3")
        self.assertEqual(sessions["SE4"]["guide_id"], "G2")


if __name__ == "__main__":
    unittest.main()
