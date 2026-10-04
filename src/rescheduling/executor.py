"""有序 Saga 执行器。

保证：
* 每个步骤的「排班更新 + 待投递消息写入」在同一数据库事务（Outbox 模式），
  绝不会出现通知已发而资源未调整（或反之）。
* 步骤按冻结 seq 顺序前向执行、按逆序补偿；DONE 步骤重放时跳过，重复执行幂等。
* 步骤前态与当前数据不符（被其他改动漂移）时停在失败点：
  FAILED（首步未成）或 PARTIALLY_APPLIED（已有步骤生效），可安全续办或回滚。
* exec_lock 为全局单写者锁，并发变更的 Saga 被串行化，顺序由 changes.seq 决定。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from . import domain as d
from .clock import Clock
from .errors import (
    BlockedByEarlierChangeError,
    ConflictError,
    ExecutionStalledError,
    RollbackStalledError,
)
from .storage import Storage

# 僵死锁接管阈值（秒）
LOCK_TTL_SECONDS = 30.0


@dataclass
class FaultInjector:
    """测试/演示用故障注入：故障只触发一次，用于验证失败点续办。

    phase="before"：步骤事务前提取异常（步骤保持 PENDING，续办时重试）；
    phase="after" ：步骤已提交、推进下一步前异常（步骤保持 DONE，续办时从下一步继续）。
    """

    clock: Clock
    faults: dict[tuple[str, int, str], RuntimeError] = field(default_factory=dict)

    def arm(self, change_id: str, step_seq: int, phase: str = "before") -> None:
        err = RuntimeError(f"注入故障：change={change_id} step={step_seq} phase={phase}")
        self.faults[(change_id, step_seq, phase)] = err

    def fire(self, change_id: str, step_seq: int, phase: str) -> None:
        key = (change_id, step_seq, phase)
        if key in self.faults:
            err = self.faults.pop(key)
            raise err


class SagaExecutor:
    def __init__(self, storage: Storage, clock: Clock, faults: FaultInjector | None = None):
        self.storage = storage
        self.clock = clock
        self.faults = faults or FaultInjector(clock)
        # 每变更互斥：同一变更的并发执行/回滚串行化，重复调用得到确定结果
        self._change_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    @contextmanager
    def _change_mutex(self, change_id: str):
        with self._locks_guard:
            lock = self._change_locks.setdefault(change_id, threading.Lock())
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    # ================================================================ 锁

    def _acquire_lock(self, conn: sqlite3.Connection, change_id: str) -> None:
        row = Storage.row(conn, "SELECT * FROM exec_lock WHERE lock_id=1")
        holder = row["change_id"]
        if holder == change_id:
            return
        if holder:
            holder_change = Storage.row(
                conn, "SELECT status FROM changes WHERE change_id=?", (holder,))
            grant = False
            if holder_change is None or holder_change["status"] in (
                d.APPLIED, d.ROLLED_BACK, d.PARTIALLY_APPLIED, d.FAILED):
                grant = True
            elif holder_change["status"] in (d.EXECUTING, d.ROLLING_BACK):
                # 进程崩溃遗留的僵死锁：超过 TTL 可接管
                try:
                    age = (self.clock.now() - datetime.fromisoformat(
                        row["acquired_at"])).total_seconds()
                except Exception:
                    age = LOCK_TTL_SECONDS + 1
                if age > LOCK_TTL_SECONDS:
                    grant = True
            if not grant:
                raise ConflictError(
                    f"另一个变更（{holder}）正在执行，已按全局顺序排队",
                    {"blocked_by": holder})
        conn.execute(
            "UPDATE exec_lock SET change_id=?, acquired_at=? WHERE lock_id=1",
            (change_id, self.clock.now_iso()))

    def _release_lock(self, conn: sqlite3.Connection, change_id: str) -> None:
        conn.execute(
            "UPDATE exec_lock SET change_id='', acquired_at='' "
            "WHERE lock_id=1 AND change_id=?",
            (change_id,))

    def _order_gate_forward(self, conn: sqlite3.Connection, ch: sqlite3.Row) -> None:
        """确定的执行顺序：seq 更小的冻结变更必须先跑完或回滚。"""
        earlier = Storage.row(
            conn,
            "SELECT change_id FROM changes "
            "WHERE seq<? AND status IN (?,?,?,?,?) ORDER BY seq LIMIT 1",
            (ch["seq"], d.APPROVED, d.EXECUTING, d.PARTIALLY_APPLIED, d.FAILED,
             d.ROLLING_BACK))
        if earlier is not None:
            raise BlockedByEarlierChangeError(
                f"前序变更 {earlier['change_id']} 尚未完成，请按顺序执行",
                {"blocked_by": earlier["change_id"], "direction": "forward"})

    def _order_gate_rollback(self, conn: sqlite3.Connection, ch: sqlite3.Row) -> None:
        """确定的回滚顺序：seq 更大的变更必须先回滚。"""
        later = Storage.row(
            conn,
            "SELECT change_id FROM changes "
            "WHERE seq>? AND status IN (?,?,?,?,?,?) ORDER BY seq DESC LIMIT 1",
            (ch["seq"], d.APPLIED, d.APPROVED, d.EXECUTING, d.PARTIALLY_APPLIED,
             d.FAILED, d.ROLLING_BACK))
        if later is not None:
            raise BlockedByEarlierChangeError(
                f"后序变更 {later['change_id']} 尚未回滚，请按逆序回滚",
                {"blocked_by": later["change_id"], "direction": "rollback"})

    # ================================================================ 事件

    def _event(self, conn: sqlite3.Connection, change_id: str,
               etype: str, payload: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO event_log (change_id, type, payload, created_at) VALUES (?,?,?,?)",
            (change_id, etype, Storage.dumps(payload), self.clock.now_iso()))

    # ================================================================ 消息

    def _add_message(self, conn: sqlite3.Connection, ch: sqlite3.Row, step_seq: int,
                     channel: str, recipient: str, subject: str, body: str,
                     suffix: str = "") -> None:
        # 去重键包含方案指纹：回滚后重新审批（新指纹）可再次通知；同方案重放则幂等
        epoch = ch["plan_fingerprint"] or "v0"
        dedup_key = f"{ch['change_id']}:{epoch}:{step_seq}:{suffix or 'FWD'}:{channel}:{recipient}"
        conn.execute(
            "INSERT OR IGNORE INTO outbox "
            "(change_id, step_id, channel, recipient, subject, body, status, dedup_key, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (ch["change_id"], step_seq, channel, recipient, subject, body,
             d.MSG_PENDING, dedup_key, self.clock.now_iso()))

    def _contacts(self, conn: sqlite3.Connection, s: dict[str, Any]) -> dict[str, str]:
        venue = Storage.row(conn, "SELECT name, manager_phone FROM venues WHERE venue_id=?",
                            (s["venue_id"],))
        guide = Storage.row(conn, "SELECT name, phone FROM guides WHERE guide_id=?",
                            (s["guide_id"],))
        return {
            "venue_name": venue["name"] if venue else s["venue_id"],
            "venue_phone": venue["manager_phone"] if venue else "",
            "guide_name": guide["name"] if guide else s["guide_id"],
            "guide_phone": guide["phone"] if guide else "",
        }

    def _step_messages(self, conn: sqlite3.Connection, ch: sqlite3.Row,
                       st: sqlite3.Row) -> None:
        """根据步骤类型在当前事务内写入待投递消息。"""
        after = Storage.loads(st["after_data"])
        before = Storage.loads(st["before_data"])
        if st["op"] == d.STEP_MARK:
            if ch["resource_kind"] == d.RESOURCE_VENUE:
                v = Storage.row(conn, "SELECT name, manager_phone FROM venues WHERE venue_id=?",
                                (ch["resource_id"],))
                self._add_message(
                    conn, ch, st["seq"], "VENUE",
                    v["manager_phone"] if v else "",
                    f"{v['name'] if v else ch['resource_id']}临时闭馆通知",
                    f"场馆将于 {ch['unavailable_start']} 至 {ch['unavailable_end']} 临时闭馆，"
                    "受影响场次正在联动调整。")
            else:
                g = Storage.row(conn, "SELECT name, phone FROM guides WHERE guide_id=?",
                                (ch["resource_id"],))
                self._add_message(
                    conn, ch, st["seq"], "GUIDE",
                    g["phone"] if g else "",
                    f"讲解员{g['name'] if g else ch['resource_id']}请假登记",
                    f"{g['name'] if g else ch['resource_id']} 于 "
                    f"{ch['unavailable_start']} 至 {ch['unavailable_end']} 请假，关联场次正在调整。")
            return

        s = Storage.row(conn, "SELECT * FROM sessions WHERE session_id=?",
                        (st["target_id"],))
        contacts = self._contacts(conn, dict(s) if s else after)
        school = s["school_contact"] if s else after.get("school_contact", "")
        sname = s["name"] if s else st["target_id"]
        if st["op"] == d.STEP_CANCEL:
            self._add_message(
                conn, ch, st["seq"], "SCHOOL", school,
                f"【场次取消】{sname}",
                f"因{ch['reason'] or '资源临时变更'}，原定于 {before['start_ts']} "
                f"在{contacts['venue_name']}的活动{sname}已取消，敬请谅解。")
        elif st["op"] == d.STEP_RESCHEDULE:
            self._add_message(
                conn, ch, st["seq"], "SCHOOL", school,
                f"【改期通知】{sname}",
                f"因{ch['reason'] or '资源临时变更'}，活动{sname}由 {before['start_ts']} "
                f"{contacts['venue_name']} 调整为 {after['start_ts']} "
                f"{after['venue_id']}（讲解员：{after['guide_id']}），请确认。")
            if before["guide_id"] != after["guide_id"]:
                self._add_message(
                    conn, ch, st["seq"], "GUIDE", contacts["guide_phone"],
                    f"【讲解任务调整】{sname}",
                    f"活动{sname}已改由您于 {after['start_ts']} 在 {after['venue_id']} 接待。")
            if before["venue_id"] != after["venue_id"]:
                self._add_message(
                    conn, ch, st["seq"], "VENUE", contacts["venue_phone"],
                    f"【场地调整】{sname}",
                    f"活动{sname}改期至 {after['start_ts']}，场地变更为 {after['venue_id']}。")

    # ================================================================ 前向执行

    def _next_pending(self, conn: sqlite3.Connection, change_id: str) -> sqlite3.Row | None:
        """下一个待执行步骤：PENDING 优先；FAILED 步骤在续办时重新校验、重试。"""
        return Storage.row(
            conn,
            "SELECT * FROM plan_steps WHERE change_id=? AND state IN (?,?) "
            "ORDER BY seq ASC LIMIT 1",
            (change_id, d.STEP_PENDING, d.STEP_FAILED))

    def _done_count(self, conn: sqlite3.Connection, change_id: str) -> int:
        return Storage.row(
            conn, "SELECT COUNT(*) AS c FROM plan_steps WHERE change_id=? AND state=?",
            (change_id, d.STEP_DONE))["c"]

    def _total_count(self, conn: sqlite3.Connection, change_id: str) -> int:
        return Storage.row(
            conn, "SELECT COUNT(*) AS c FROM plan_steps WHERE change_id=?",
            (change_id,))["c"]

    def _set_status(self, conn: sqlite3.Connection, change_id: str, status: str) -> None:
        conn.execute("UPDATE changes SET status=?, updated_at=? WHERE change_id=?",
                     (status, self.clock.now_iso(), change_id))

    def _stall(self, change_id: str, any_done: bool) -> None:
        """异常后在独立事务落定中断状态并释放锁（模拟崩溃后的现场固化）。"""
        with self.storage.transaction(immediate=True) as conn:
            status = d.PARTIALLY_APPLIED if any_done else d.FAILED
            self._set_status(conn, change_id, status)
            self._release_lock(conn, change_id)
            self._event(conn, change_id, "EXECUTION_STALLED",
                        {"status": status, "at": self.clock.now_iso()})

    def drive_forward(self, change_id: str) -> dict[str, Any]:
        """从失败点/起点推进 Saga，直至完成或再次中断。幂等、可重复调用。"""
        with self._change_mutex(change_id):
            return self._drive_forward(change_id)

    def _drive_forward(self, change_id: str) -> dict[str, Any]:
        with self.storage.transaction(immediate=True) as conn:
            ch = Storage.row(conn, "SELECT * FROM changes WHERE change_id=?", (change_id,))
            if ch is None:
                from .errors import NotFoundError

                raise NotFoundError(f"变更不存在：{change_id}")
            if ch["status"] not in d.EXECUTABLE + (d.EXECUTING,):
                if ch["status"] == d.APPLIED:
                    return self._summary(conn, change_id, already=True)
                raise ConflictError(f"当前状态 {ch['status']} 不可执行",
                                    {"status": ch["status"]})
            self._acquire_lock(conn, change_id)
            self._order_gate_forward(conn, ch)
            self._set_status(conn, change_id, d.EXECUTING)
            self._event(conn, change_id, "EXECUTION_STARTED", {})

        try:
            while True:
                stall: ExecutionStalledError | None = None
                with self.storage.transaction(immediate=True) as conn:
                    lock = Storage.row(conn, "SELECT change_id FROM exec_lock WHERE lock_id=1")
                    if lock["change_id"] != change_id:
                        raise ConflictError("执行锁已被接管", {"change_id": change_id})
                    st = self._next_pending(conn, change_id)
                    if st is None:
                        self._set_status(conn, change_id, d.APPLIED)
                        self._release_lock(conn, change_id)
                        self._event(conn, change_id, "EXECUTION_COMPLETED", {})
                        return self._summary(conn, change_id)

                    self.faults.fire(change_id, st["seq"], "before")
                    ch = Storage.row(conn, "SELECT * FROM changes WHERE change_id=?",
                                     (change_id,))
                    drift = self._apply_step(conn, ch, st)
                    if drift is not None:
                        # 前态漂移：标记失败并停在该点（事务正常提交后再抛出）
                        conn.execute(
                            "UPDATE plan_steps SET state=?, attempts=attempts+1, last_error=? "
                            "WHERE step_id=?",
                            (d.STEP_FAILED, drift, st["step_id"]))
                        any_done = self._done_count(conn, change_id) > 0
                        new_status = d.PARTIALLY_APPLIED if any_done else d.FAILED
                        self._set_status(conn, change_id, new_status)
                        self._release_lock(conn, change_id)
                        self._event(conn, change_id, "STEP_DRIFT",
                                    {"step_seq": st["seq"], "error": drift})
                        stall = ExecutionStalledError(drift, {
                            "change_id": change_id, "step_seq": st["seq"],
                            "status": new_status})
                    else:
                        conn.execute(
                            "UPDATE plan_steps SET state=?, attempts=attempts+1, last_error='', "
                            "completed_at=? WHERE step_id=?",
                            (d.STEP_DONE, self.clock.now_iso(), st["step_id"]))
                        self._event(conn, change_id, "STEP_COMPLETED",
                                    {"step_seq": st["seq"], "op": st["op"]})
                        done = self._done_count(conn, change_id)
                        total = self._total_count(conn, change_id)
                        if done == total:
                            self._set_status(conn, change_id, d.APPLIED)
                            self._release_lock(conn, change_id)
                            self._event(conn, change_id, "EXECUTION_COMPLETED", {})
                            completed = True
                        else:
                            self._set_status(conn, change_id, d.EXECUTING)
                            completed = False
                if stall is not None:
                    raise stall
                self.faults.fire(change_id, st["seq"], "after")
                if completed:
                    with self.storage.transaction() as conn:
                        return self._summary(conn, change_id)
        except (ExecutionStalledError,):
            raise
        except Exception as exc:
            # 锁竞争 / 顺序闸冲突：未取得执行权，原样抛出，不得固化失败点或清锁
            if isinstance(exc, ConflictError):
                raise
            # 全部步骤已提交（如最后一步提交后、返回前崩溃）：保持 APPLIED
            with self.storage.transaction() as conn:
                chk = Storage.row(conn, "SELECT status FROM changes WHERE change_id=?",
                                  (change_id,))
                any_done = self._done_count(conn, change_id) > 0
            if chk is not None and chk["status"] == d.APPLIED:
                with self.storage.transaction() as conn:
                    return self._summary(conn, change_id)
            # 其它异常：固化失败点，等待续办
            self._stall(change_id, any_done)
            raise ExecutionStalledError(str(exc), {
                "change_id": change_id,
                "resume_hint": "POST /changes/{id}/execute 可从失败点续办",
            }) from exc

    def _apply_step(self, conn: sqlite3.Connection, ch: sqlite3.Row,
                    st: sqlite3.Row) -> str | None:
        """执行单步；返回 None 表示成功，返回字符串表示前态漂移原因。"""
        before = Storage.loads(st["before_data"])
        after = Storage.loads(st["after_data"])

        if st["op"] == d.STEP_MARK:
            # 不可用窗口由变更状态表达（occupancy 聚合 FROZEN_ACTIVE 变更）；
            # 此处仅产生消息，天然幂等。
            self._step_messages(conn, ch, st)
            return None

        cur = Storage.row(conn, "SELECT * FROM sessions WHERE session_id=?",
                          (st["target_id"],))
        if cur is None:
            return f"场次{st['target_id']}已不存在，冻结方案的前态失效"

        if st["op"] == d.STEP_CANCEL:
            if cur["cancelled"] == 1:
                pass  # 幂等重放
            elif cur["cancelled"] == 0 and cur["version"] == before["version"]:
                conn.execute("UPDATE sessions SET cancelled=1, version=? WHERE session_id=?",
                             (after["version"], st["target_id"]))
            else:
                return f"场次{st['target_id']}已被其他改动修改（version={cur['version']}），需重新预演"
            self._step_messages(conn, ch, st)
            return None

        if st["op"] == d.STEP_RESCHEDULE:
            already = (
                cur["venue_id"] == after["venue_id"]
                and cur["guide_id"] == after["guide_id"]
                and cur["start_ts"] == after["start_ts"]
                and cur["end_ts"] == after["end_ts"]
                and cur["cancelled"] == 0)
            if already:
                pass  # 幂等重放
            else:
                matches_before = (
                    cur["venue_id"] == before["venue_id"]
                    and cur["guide_id"] == before["guide_id"]
                    and cur["start_ts"] == before["start_ts"]
                    and cur["end_ts"] == before["end_ts"]
                    and cur["cancelled"] == before["cancelled"]
                    and cur["version"] == before["version"])
                if not matches_before:
                    return (f"场次{st['target_id']}已被其他改动修改"
                            f"（当前 version={cur['version']}，冻结前态 version={before['version']}），"
                            "需重新预演或回滚已生效步骤")
                conn.execute(
                    "UPDATE sessions SET venue_id=?, guide_id=?, start_ts=?, end_ts=?, "
                    "cancelled=0, version=? WHERE session_id=? AND version=?",
                    (after["venue_id"], after["guide_id"], after["start_ts"], after["end_ts"],
                     after["version"], st["target_id"], before["version"]))
            self._step_messages(conn, ch, st)
            return None

        return f"未知步骤类型：{st['op']}"

    # ================================================================ 补偿回滚

    def drive_rollback(self, change_id: str) -> dict[str, Any]:
        """逆序补偿所有已生效步骤。幂等、可从中断点续办。"""
        with self._change_mutex(change_id):
            return self._drive_rollback(change_id)

    def _drive_rollback(self, change_id: str) -> dict[str, Any]:
        with self.storage.transaction(immediate=True) as conn:
            ch = Storage.row(conn, "SELECT * FROM changes WHERE change_id=?", (change_id,))
            if ch is None:
                from .errors import NotFoundError

                raise NotFoundError(f"变更不存在：{change_id}")
            if ch["status"] not in d.ROLLBACKABLE:
                raise ConflictError(f"当前状态 {ch['status']} 不可回滚",
                                    {"status": ch["status"]})
            self._acquire_lock(conn, change_id)
            self._order_gate_rollback(conn, ch)
            self._set_status(conn, change_id, d.ROLLING_BACK)
            self._event(conn, change_id, "ROLLBACK_STARTED", {})

        try:
            while True:
                rollback_stall: RollbackStalledError | None = None
                done_all = False
                with self.storage.transaction(immediate=True) as conn:
                    lock = Storage.row(conn, "SELECT change_id FROM exec_lock WHERE lock_id=1")
                    if lock["change_id"] != change_id:
                        raise ConflictError("执行锁已被接管", {"change_id": change_id})
                    st = Storage.row(
                        conn,
                        "SELECT * FROM plan_steps WHERE change_id=? AND state=? "
                        "ORDER BY seq DESC LIMIT 1",
                        (change_id, d.STEP_DONE))
                    if st is None:
                        self._set_status(conn, change_id, d.ROLLED_BACK)
                        self._release_lock(conn, change_id)
                        self._event(conn, change_id, "ROLLBACK_COMPLETED", {})
                        return self._summary(conn, change_id, rolled_back=True)

                    self.faults.fire(change_id, st["seq"], "before")
                    ch = Storage.row(conn, "SELECT * FROM changes WHERE change_id=?",
                                     (change_id,))
                    drift = self._compensate_step(conn, ch, st)
                    if drift is not None:
                        conn.execute(
                            "UPDATE plan_steps SET last_error=? WHERE step_id=?",
                            (drift, st["step_id"]))
                        self._release_lock(conn, change_id)
                        self._event(conn, change_id, "ROLLBACK_DRIFT",
                                    {"step_seq": st["seq"], "error": drift})
                        rollback_stall = RollbackStalledError(drift, {
                            "change_id": change_id, "step_seq": st["seq"]})
                    else:
                        # 补偿完成：步骤回到 PENDING（语义=未生效），可在重新审批后再次执行
                        conn.execute(
                            "UPDATE plan_steps SET state=?, last_error='', completed_at='' "
                            "WHERE step_id=?",
                            (d.STEP_PENDING, st["step_id"]))
                        self._event(conn, change_id, "STEP_COMPENSATED",
                                    {"step_seq": st["seq"], "op": st["op"]})
                        remaining = Storage.row(
                            conn,
                            "SELECT COUNT(*) AS c FROM plan_steps WHERE change_id=? AND state=?",
                            (change_id, d.STEP_DONE))["c"]
                        if remaining == 0:
                            self._set_status(conn, change_id, d.ROLLED_BACK)
                            self._release_lock(conn, change_id)
                            self._event(conn, change_id, "ROLLBACK_COMPLETED", {})
                            done_all = True
                if rollback_stall is not None:
                    raise rollback_stall
                self.faults.fire(change_id, st["seq"], "after")
                if done_all:
                    with self.storage.transaction() as conn:
                        return self._summary(conn, change_id, rolled_back=True)
        except RollbackStalledError:
            raise
        except Exception as exc:
            if isinstance(exc, ConflictError):
                raise
            with self.storage.transaction(immediate=True) as conn:
                chk = Storage.row(conn, "SELECT status FROM changes WHERE change_id=?",
                                  (change_id,))
                if chk is not None and chk["status"] == d.ROLLED_BACK:
                    return self._summary(conn, change_id, rolled_back=True)
                self._set_status(conn, change_id, d.ROLLING_BACK)
                self._release_lock(conn, change_id)
            raise RollbackStalledError(str(exc), {
                "change_id": change_id,
                "resume_hint": "POST /changes/{id}/rollback 可继续补偿",
            }) from exc

    def _compensate_step(self, conn: sqlite3.Connection, ch: sqlite3.Row,
                         st: sqlite3.Row) -> str | None:
        before = Storage.loads(st["before_data"])
        after = Storage.loads(st["after_data"])

        if st["op"] == d.STEP_MARK:
            # 窗口占用随变更状态 ROLLED_BACK 释放，无需改资源表；通知销假/解除闭馆
            if ch["resource_kind"] == d.RESOURCE_VENUE:
                v = Storage.row(conn, "SELECT manager_phone AS p FROM venues WHERE venue_id=?",
                                (ch["resource_id"],))
                self._add_message(
                    conn, ch, st["seq"], "VENUE", v["p"] if v else "",
                    "【闭馆解除】", "临时闭馆安排已撤销，场地恢复正常开放。", suffix="RB")
            else:
                g = Storage.row(conn, "SELECT phone AS p FROM guides WHERE guide_id=?",
                                (ch["resource_id"],))
                self._add_message(
                    conn, ch, st["seq"], "GUIDE", g["p"] if g else "",
                    "【请假撤销】", "请假联动变更已回滚，原排班恢复。", suffix="RB")
            return None

        cur = Storage.row(conn, "SELECT * FROM sessions WHERE session_id=?",
                          (st["target_id"],))
        if cur is None:
            return f"场次{st['target_id']}已不存在，无法补偿"

        if st["op"] == d.STEP_CANCEL:
            if cur["cancelled"] == 0:
                pass  # 幂等
            elif cur["version"] == after["version"]:
                conn.execute(
                    "UPDATE sessions SET cancelled=0, version=version+1 WHERE session_id=?",
                    (st["target_id"],))
            else:
                return f"场次{st['target_id']}取消后又被改动（version={cur['version']}），无法自动补偿"
            self._compensation_notice(conn, ch, st, cur, restored=True)
            return None

        if st["op"] == d.STEP_RESCHEDULE:
            restored = (
                cur["venue_id"] == before["venue_id"]
                and cur["guide_id"] == before["guide_id"]
                and cur["start_ts"] == before["start_ts"]
                and cur["end_ts"] == before["end_ts"])
            if restored:
                pass  # 幂等
            elif (cur["venue_id"] == after["venue_id"]
                  and cur["guide_id"] == after["guide_id"]
                  and cur["start_ts"] == after["start_ts"]
                  and cur["end_ts"] == after["end_ts"]
                  and cur["version"] == after["version"]):
                conn.execute(
                    "UPDATE sessions SET venue_id=?, guide_id=?, start_ts=?, end_ts=?, "
                    "version=version+1 WHERE session_id=?",
                    (before["venue_id"], before["guide_id"], before["start_ts"],
                     before["end_ts"], st["target_id"]))
            else:
                return (f"场次{st['target_id']}重排后又被其他变更改动"
                        f"（version={cur['version']}），无法自动补偿")
            self._compensation_notice(conn, ch, st, cur, restored=False)
            return None

        return f"未知步骤类型：{st['op']}"

    def _compensation_notice(self, conn: sqlite3.Connection, ch: sqlite3.Row,
                             st: sqlite3.Row, cur: sqlite3.Row, restored: bool) -> None:
        before = Storage.loads(st["before_data"])
        self._add_message(
            conn, ch, st["seq"], "SCHOOL", cur["school_contact"],
            f"【排班恢复】{cur['name']}",
            f"变更已撤销，活动{cur['name']}恢复原安排：{before['start_ts']} "
            f"于 {before['venue_id']}（讲解员：{before['guide_id']}）。",
            suffix="RB")

    # ================================================================ 摘要

    def _summary(self, conn: sqlite3.Connection, change_id: str,
                 already: bool = False, rolled_back: bool = False) -> dict[str, Any]:
        ch = Storage.row(conn, "SELECT * FROM changes WHERE change_id=?", (change_id,))
        steps = Storage.rows(
            conn, "SELECT seq, op, target_id, state, attempts, last_error "
            "FROM plan_steps WHERE change_id=? ORDER BY seq", (change_id,))
        msgs = Storage.rows(
            conn, "SELECT COUNT(*) AS c FROM outbox WHERE change_id=?", (change_id,))
        pending_msgs = Storage.rows(
            conn, "SELECT COUNT(*) AS c FROM outbox WHERE change_id=? AND status=?",
            (change_id, d.MSG_PENDING))
        return {
            "change_id": change_id,
            "status": ch["status"],
            "replayed": already,
            "rolled_back": rolled_back or ch["status"] == d.ROLLED_BACK,
            "steps": [dict(s) for s in steps],
            "steps_total": len(steps),
            "steps_done": sum(1 for s in steps if s["state"] == d.STEP_DONE),
            "messages_total": msgs[0]["c"],
            "messages_pending": pending_msgs[0]["c"],
        }
