"""应用服务门面：统一编排变更请求、预演、审批、执行、回滚与消息投递。

所有写操作走单连接事务；执行类操作由 SagaExecutor 驱动。
"""
from __future__ import annotations

import uuid
from typing import Any, Callable

from . import domain as d
from .clock import Clock, SystemClock
from .errors import (
    ConflictError,
    NotFoundError,
    UnresolvedConflictError,
    ValidationError,
)
from .executor import FaultInjector, SagaExecutor
from .planning import PlanningEngine
from .storage import Storage


class Service:
    def __init__(self, storage: Storage, clock: Clock | None = None):
        self.storage = storage
        self.clock = clock or SystemClock()
        self.engine = PlanningEngine(storage)
        self.faults = FaultInjector(self.clock)
        self.executor = SagaExecutor(storage, self.clock, self.faults)

    # ================================================================ 基础数据

    def register_venue(self, venue_id: str, name: str, manager_phone: str = "") -> dict[str, Any]:
        with self.storage.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO venues (venue_id, name, manager_phone) VALUES (?,?,?) "
                "ON CONFLICT(venue_id) DO UPDATE SET name=excluded.name, "
                "manager_phone=excluded.manager_phone",
                (venue_id, name, manager_phone))
        return {"venue_id": venue_id, "name": name}

    def register_guide(self, guide_id: str, name: str, phone: str = "") -> dict[str, Any]:
        with self.storage.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO guides (guide_id, name, phone) VALUES (?,?,?) "
                "ON CONFLICT(guide_id) DO UPDATE SET name=excluded.name, phone=excluded.phone",
                (guide_id, name, phone))
        return {"guide_id": guide_id, "name": name}

    def create_session(self, session_id: str, name: str, venue_id: str, guide_id: str,
                       start_ts: str, end_ts: str, school_contact: str = "",
                       state: str = "已排定") -> dict[str, Any]:
        self._check_ts(start_ts, end_ts)
        if state not in d.SESSION_STATES:
            raise ValidationError(f"未知场次状态：{state}", {"allowed": list(d.SESSION_STATES)})
        with self.storage.transaction(immediate=True) as conn:
            if not Storage.row(conn, "SELECT 1 FROM venues WHERE venue_id=?", (venue_id,)):
                raise ValidationError(f"场馆不存在：{venue_id}")
            if not Storage.row(conn, "SELECT 1 FROM guides WHERE guide_id=?", (guide_id,)):
                raise ValidationError(f"讲解员不存在：{guide_id}")
            conn.execute(
                "INSERT INTO sessions (session_id, name, venue_id, guide_id, start_ts, end_ts, "
                "school_contact, state, cancelled, version) VALUES (?,?,?,?,?,?,?,?,0,1)",
                (session_id, name, venue_id, guide_id, start_ts, end_ts,
                 school_contact, state))
            self._event(conn, "", "SESSION_CREATED", {"session_id": session_id})
        return self.get_session(session_id)

    def get_session(self, session_id: str) -> dict[str, Any]:
        with self.storage.transaction() as conn:
            row = Storage.row(conn, "SELECT * FROM sessions WHERE session_id=?", (session_id,))
            if row is None:
                raise NotFoundError(f"场次不存在：{session_id}")
            return dict(row)

    def list_sessions(self, include_cancelled: bool = True) -> list[dict[str, Any]]:
        sql = "SELECT * FROM sessions"
        if not include_cancelled:
            sql += " WHERE cancelled=0"
        sql += " ORDER BY start_ts, session_id"
        with self.storage.transaction() as conn:
            return [dict(r) for r in Storage.rows(conn, sql)]

    # ================================================================ 变更请求

    @staticmethod
    def _check_ts(start_ts: str, end_ts: str) -> None:
        try:
            start = d.parse_ts(start_ts)
            end = d.parse_ts(end_ts)
        except ValueError as exc:
            raise ValidationError(f"时间格式错误：{exc}")
        if start >= end:
            raise ValidationError("开始时间必须早于结束时间")

    def create_change(self, change_type: str, resource_id: str, unavailable_start: str,
                      unavailable_end: str, reason: str = "", *,
                      change_id: str | None = None, idempotency_key: str = "") -> dict[str, Any]:
        if change_type not in d.CHANGE_TYPES:
            raise ValidationError(f"未知变更类型：{change_type}", {"allowed": list(d.CHANGE_TYPES)})
        self._check_ts(unavailable_start, unavailable_end)
        kind = d.RESOURCE_VENUE if change_type == d.VENUE_CLOSURE else d.RESOURCE_GUIDE

        # 重复提交：相同 idempotency_key 直接返回原请求（重复执行确定结果）
        if idempotency_key:
            with self.storage.transaction() as conn:
                existed = Storage.row(
                    conn, "SELECT change_id FROM changes WHERE idempotency_key=?",
                    (idempotency_key,))
            if existed is not None:
                result = self.get_change(existed["change_id"])
                result["idempotent_replay"] = True
                return result

        cid = change_id or f"C-{uuid.uuid4().hex[:10]}"
        with self.storage.transaction(immediate=True) as conn:
            if Storage.row(conn, "SELECT 1 FROM changes WHERE change_id=?", (cid,)):
                raise ConflictError(f"变更编号已存在：{cid}")
            if kind == d.RESOURCE_VENUE:
                if not Storage.row(conn, "SELECT 1 FROM venues WHERE venue_id=?", (resource_id,)):
                    raise ValidationError(f"场馆不存在：{resource_id}")
            elif not Storage.row(conn, "SELECT 1 FROM guides WHERE guide_id=?", (resource_id,)):
                raise ValidationError(f"讲解员不存在：{resource_id}")
            seq_row = Storage.row(conn, "SELECT COALESCE(MAX(seq),0)+1 AS next FROM changes")
            now = self.clock.now_iso()
            conn.execute(
                "INSERT INTO changes (change_id, seq, change_type, resource_kind, resource_id, "
                "unavailable_start, unavailable_end, reason, status, idempotency_key, "
                "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (cid, seq_row["next"], change_type, kind, resource_id, unavailable_start,
                 unavailable_end, reason, d.DRAFT, idempotency_key, now, now))
            self._event(conn, cid, "CHANGE_CREATED",
                        {"seq": seq_row["next"], "change_type": change_type,
                         "resource": f"{kind}:{resource_id}"})
        return self.get_change(cid)

    def get_change(self, change_id: str) -> dict[str, Any]:
        with self.storage.transaction() as conn:
            ch = Storage.row(conn, "SELECT * FROM changes WHERE change_id=?", (change_id,))
            if ch is None:
                raise NotFoundError(f"变更不存在：{change_id}")
            data = dict(ch)
            data["status_label"] = d.STATUS_LABELS.get(ch["status"], ch["status"])
            data["steps"] = [
                dict(r) for r in Storage.rows(
                    conn,
                    "SELECT seq, op, target_type, target_id, state, attempts, last_error "
                    "FROM plan_steps WHERE change_id=? ORDER BY seq", (change_id,))
            ]
            data["unresolved_conflicts"] = [
                dict(r) for r in Storage.rows(
                    conn,
                    "SELECT session_id, code, severity, message FROM conflicts "
                    "WHERE change_id=? AND status='OPEN' ORDER BY conflict_id", (change_id,))
            ]
            return data

    def list_changes(self) -> list[dict[str, Any]]:
        with self.storage.transaction() as conn:
            rows = Storage.rows(conn, "SELECT change_id, seq, change_type, resource_kind, "
                                      "resource_id, status, created_at "
                                      "FROM changes ORDER BY seq")
            return [dict(r) for r in rows]

    # ================================================================ 预演

    def preview_change(self, change_id: str) -> dict[str, Any]:
        """（重新）预演：覆盖影响/候选/冲突快照。审批后不可再预演（方案已冻结）。"""
        with self.storage.transaction(immediate=True) as conn:
            ch = self.engine.get_change(conn, change_id)
            if ch["status"] not in (d.DRAFT, d.PREVIEWED, d.SUBMITTED, d.ROLLED_BACK):
                raise ConflictError(f"变更已进入 {ch['status']}，方案已冻结，不能重新预演",
                                    {"status": ch["status"]})
            result = self.engine.preview(conn, change_id)

            conn.execute("DELETE FROM impacts WHERE change_id=?", (change_id,))
            conn.execute("DELETE FROM options WHERE change_id=?", (change_id,))
            conn.execute("DELETE FROM conflicts WHERE change_id=?", (change_id,))
            conn.execute("DELETE FROM plan_steps WHERE change_id=?", (change_id,))
            for imp in result["impacted"]:
                conn.execute(
                    "INSERT INTO impacts (change_id, session_id, impacted, action, explicit) "
                    "VALUES (?,?,?,?,0)",
                    (change_id, imp["session_id"], 1, imp["recommended_action"]))
            for opt in result["options"]:
                option_id = f"O-{change_id}-{opt['session_id']}-{opt['rank']}"
                conn.execute(
                    "INSERT INTO options (option_id, change_id, session_id, kind, rank, payload) "
                    "VALUES (?,?,?,?,?,?)",
                    (option_id, change_id, opt["session_id"], opt["kind"],
                     opt["rank"], Storage.dumps(opt["payload"])))
            for cf in result["conflicts"]:
                conn.execute(
                    "INSERT INTO conflicts (change_id, session_id, code, severity, message, status) "
                    "VALUES (?,?,?,?,?,'OPEN')",
                    (change_id, cf["session_id"], cf["code"], cf["severity"], cf["message"]))
            conn.execute("UPDATE changes SET status=?, plan_fingerprint='', "
                "updated_at=? WHERE change_id=?",
                         (d.PREVIEWED, self.clock.now_iso(), change_id))
            self._event(conn, change_id, "PREVIEW_UPDATED",
                        {"impacted": len(result["impacted"]),
                         "options": len(result["options"]),
                         "unresolved": result["unresolved_count"]})
        return self.get_preview(change_id)

    def get_preview(self, change_id: str) -> dict[str, Any]:
        """展示最近一次预演：影响场次、候选方案与未解决冲突。"""
        with self.storage.transaction() as conn:
            ch = self.engine.get_change(conn, change_id)
            impacts = [
                dict(r) for r in Storage.rows(
                    conn,
                    "SELECT i.session_id, s.name, s.state, s.venue_id, s.guide_id, "
                    "s.start_ts, s.end_ts, i.action AS recommended_action "
                    "FROM impacts i JOIN sessions s ON s.session_id=i.session_id "
                    "WHERE i.change_id=? ORDER BY s.start_ts, i.session_id", (change_id,))
            ]
            options = []
            for r in Storage.rows(
                conn,
                "SELECT option_id, session_id, kind, rank, payload FROM options "
                "WHERE change_id=? ORDER BY session_id, rank", (change_id,)):
                options.append({"option_id": r["option_id"], "session_id": r["session_id"],
                                "kind": r["kind"], "rank": r["rank"],
                                "payload": Storage.loads(r["payload"])})
            conflicts = [
                dict(r) for r in Storage.rows(
                    conn,
                    "SELECT session_id, code, severity, message, status FROM conflicts "
                    "WHERE change_id=? ORDER BY conflict_id", (change_id,))
            ]
            return {
                "change_id": change_id,
                "status": ch["status"],
                "frozen": ch["status"] in (d.APPROVED, d.EXECUTING, d.PARTIALLY_APPLIED,
                                           d.FAILED, d.APPLIED, d.ROLLING_BACK, d.ROLLED_BACK),
                "impacted": impacts,
                "options": options,
                "conflicts": conflicts,
                "unresolved_conflicts": [c for c in conflicts if c["status"] == "OPEN"],
                "unresolved_count": sum(1 for c in conflicts if c["status"] == "OPEN"),
            }

    # ================================================================ 提交与审批

    def submit_for_approval(self, change_id: str) -> dict[str, Any]:
        with self.storage.transaction(immediate=True) as conn:
            ch = self.engine.get_change(conn, change_id)
            if ch["status"] not in (d.PREVIEWED, d.SUBMITTED):
                raise ConflictError(f"状态 {ch['status']} 不可提交审批，请先预演",
                                    {"status": ch["status"]})
            conn.execute("UPDATE changes SET status=?, updated_at=? WHERE change_id=?",
                         (d.SUBMITTED, self.clock.now_iso(), change_id))
            self._event(conn, change_id, "SUBMITTED_FOR_APPROVAL", {})
        return self.get_change(change_id)

    def reject(self, change_id: str, reason: str = "") -> dict[str, Any]:
        with self.storage.transaction(immediate=True) as conn:
            ch = self.engine.get_change(conn, change_id)
            if ch["status"] != d.SUBMITTED:
                raise ConflictError(f"状态 {ch['status']} 无可驳回的审批申请",
                                    {"status": ch["status"]})
            conn.execute("UPDATE changes SET status=?, updated_at=? WHERE change_id=?",
                         (d.PREVIEWED, self.clock.now_iso(), change_id))
            self._event(conn, change_id, "REJECTED", {"reason": reason})
        return self.get_change(change_id)

    def void_change(self, change_id: str, reason: str = "") -> dict[str, Any]:
        """作废尚未生效的申请（草稿/已预演/待审批），释放其占队位置。"""
        with self.storage.transaction(immediate=True) as conn:
            ch = self.engine.get_change(conn, change_id)
            if ch["status"] not in d.VOIDABLE:
                raise ConflictError(f"状态 {ch['status']} 的变更不能作废，请改走回滚",
                                    {"status": ch["status"]})
            conn.execute("UPDATE changes SET status=?, updated_at=? WHERE change_id=?",
                         (d.VOID, self.clock.now_iso(), change_id))
            self._event(conn, change_id, "VOIDED", {"reason": reason})
        return self.get_change(change_id)

    def approve(self, change_id: str, approver: str,
                decisions: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
        """审批并冻结方案（部分接受）。

        decisions: {session_id: {"action": "CANCEL"}}
                   {session_id: {"action": "RESCHEDULE", "option_id": "O-..."}}
        未列出的场次：有候选时自动取最优候选重排，无候选则视为未解决冲突拒绝审批。
        """
        decisions = decisions or {}
        with self.storage.transaction(immediate=True) as conn:
            ch = self.engine.get_change(conn, change_id)
            if ch["status"] != d.SUBMITTED:
                raise ConflictError(f"状态 {ch['status']} 不可审批（仅待审批可审批）",
                                    {"status": ch["status"]})

            # 审批顺序闸：seq 更小、已在排队等待审批的变更必须先处理，
            # 避免后申请者抢先冻结，使先到申请的预演过期。
            earlier = Storage.row(
                conn,
                "SELECT change_id FROM changes WHERE seq<? AND status=? ORDER BY seq LIMIT 1",
                (ch["seq"], d.SUBMITTED))
            if earlier is not None:
                raise ConflictError(
                    f"前序申请 {earlier['change_id']} 仍待审批，请按提交顺序审批",
                    {"blocked_by": earlier["change_id"]})

            impacted_ids = {
                r["session_id"] for r in Storage.rows(
                    conn, "SELECT session_id FROM impacts WHERE change_id=?", (change_id,))
            }
            for sid, dec in decisions.items():
                if sid not in impacted_ids:
                    raise ValidationError(f"场次{sid}不在本次变更影响范围内", {"session_id": sid})
                if dec.get("action") not in d.DECISION_ACTIONS:
                    raise ValidationError(f"场次{sid}决策动作非法", {"session_id": sid})

            # 未解决冲突闸门
            open_conflicts = Storage.rows(
                conn, "SELECT * FROM conflicts WHERE change_id=? AND status='OPEN' "
                      "ORDER BY conflict_id", (change_id,))
            unresolved: list[dict[str, Any]] = []
            for cf in open_conflicts:
                dec = decisions.get(cf["session_id"])
                if cf["code"] == "NO_AVAILABLE_SLOT" and dec and dec["action"] == d.ACT_CANCEL:
                    continue  # 以取消方式解决
                unresolved.append({"session_id": cf["session_id"], "code": cf["code"],
                                   "severity": cf["severity"], "message": cf["message"]})
            if unresolved:
                raise UnresolvedConflictError("存在未解决冲突，不能审批",
                                              {"conflicts": unresolved})

            # option_id -> payload（必须来自最近一次预演快照）
            option_map: dict[tuple[str, str], dict[str, Any]] = {}
            for r in Storage.rows(
                conn, "SELECT session_id, option_id, payload FROM options WHERE change_id=?",
                (change_id,)):
                option_map[(r["session_id"], r["option_id"])] = Storage.loads(r["payload"])

            normalized: dict[str, dict[str, Any]] = {}
            for sid, dec in decisions.items():
                if dec["action"] == d.ACT_CANCEL:
                    normalized[sid] = {"action": d.ACT_CANCEL}
                else:
                    option_id = dec.get("option_id")
                    payload = option_map.get((sid, option_id or ""))
                    if payload is None:
                        raise ValidationError(
                            f"场次{sid}的候选方案无效，请使用预演返回的 option_id",
                            {"session_id": sid})
                    normalized[sid] = {"action": d.ACT_RESCHEDULE, "payload": payload}

            steps = self.engine.freeze_steps(conn, ch, normalized)
            slot_problems = self.engine.revalidate_decisions(conn, ch, steps)
            if slot_problems:
                raise UnresolvedConflictError(
                    "预演后排班已变化，所选槽位被占用", {"conflicts": slot_problems})

            fingerprint = self.engine.plan_fingerprint(steps)
            conn.execute("DELETE FROM plan_steps WHERE change_id=?", (change_id,))
            for i, st in enumerate(steps):
                conn.execute(
                    "INSERT INTO plan_steps (change_id, seq, op, target_type, target_id, "
                    "before_data, after_data, state) VALUES (?,?,?,?,?,?,?,?)",
                    (change_id, i, st["op"], st["target_type"], st["target_id"],
                     Storage.dumps(st["before_data"]), Storage.dumps(st["after_data"]),
                     d.STEP_PENDING))
            # 以取消解决的冲突落账
            conn.execute(
                "UPDATE conflicts SET status='RESOLVED' WHERE change_id=? AND code=?",
                (change_id, "NO_AVAILABLE_SLOT"))
            conn.execute(
                "UPDATE changes SET status=?, plan_fingerprint=?, approved_by=?, "
                "approved_at=?, updated_at=? WHERE change_id=?",
                (d.APPROVED, fingerprint, approver, self.clock.now_iso(),
                 self.clock.now_iso(), change_id))
            self._event(conn, change_id, "APPROVED",
                        {"approver": approver, "steps": len(steps),
                         "fingerprint": fingerprint,
                         "partial_acceptance": {
                             "reschedule": sum(1 for s in steps if s["op"] == d.STEP_RESCHEDULE),
                             "cancel": sum(1 for s in steps if s["op"] == d.STEP_CANCEL)}})

            # 顺序一致性：本次冻结后，seq 更大的在途申请所依据的世界已改变，
            # 全部失效，必须重新预演、重新审批（事件可驱动前端提醒）。
            later = Storage.rows(
                conn, "SELECT change_id, status FROM changes WHERE seq>? AND status IN (?,?,?) ",
                (ch["seq"], d.APPROVED, d.SUBMITTED, d.PREVIEWED))
            invalidated = []
            for row in later:
                # 统一退回草稿并清空预演/冻结快照，强制重新预演
                conn.execute(
                    "UPDATE changes SET status=?, plan_fingerprint='', updated_at=? "
                    "WHERE change_id=?",
                    (d.DRAFT, self.clock.now_iso(), row["change_id"]))
                conn.execute("DELETE FROM plan_steps WHERE change_id=?", (row["change_id"],))
                conn.execute("DELETE FROM impacts WHERE change_id=?", (row["change_id"],))
                conn.execute("DELETE FROM options WHERE change_id=?", (row["change_id"],))
                conn.execute("DELETE FROM conflicts WHERE change_id=?", (row["change_id"],))
                self._event(conn, row["change_id"], "PLAN_INVALIDATED",
                            {"by_change": change_id, "new_status": d.DRAFT})
                invalidated.append({"change_id": row["change_id"], "status": d.DRAFT})
            if invalidated:
                self._event(conn, change_id, "LATER_PLANS_INVALIDATED",
                            {"changes": invalidated})
        result = self.get_change(change_id)
        result["invalidated_later"] = invalidated
        return result

    # ================================================================ 执行 / 回滚 / 续办

    def execute(self, change_id: str) -> dict[str, Any]:
        """执行或从失败点续办。对 APPLIED 重复调用幂等返回。"""
        return self.executor.drive_forward(change_id)

    def rollback(self, change_id: str) -> dict[str, Any]:
        """补偿回滚（或从回滚中断点继续）。"""
        return self.executor.drive_rollback(change_id)

    # ================================================================ Outbox

    def list_messages(self, change_id: str | None = None,
                      status: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT msg_id, change_id, step_id, channel, recipient, subject, body, " \
              "status, attempts, last_error, dedup_key, created_at, delivered_at FROM outbox"
        where, params = [], []
        if change_id:
            where.append("change_id=?")
            params.append(change_id)
        if status:
            where.append("status=?")
            params.append(status)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY msg_id"
        with self.storage.transaction() as conn:
            return [dict(r) for r in Storage.rows(conn, sql, tuple(params))]

    def deliver_pending(self, sender: Callable[[dict[str, Any]], None] | None = None,
                        limit: int = 100) -> dict[str, Any]:
        """投递待发送消息（at-least-once，dedup_key 供接收方去重）。

        sender 抛异常时该消息保留 PENDING 并记录失败次数，可稍后重试。
        """
        sender = sender or self._default_sender
        with self.storage.transaction() as conn:
            pending = Storage.rows(
                conn, "SELECT * FROM outbox WHERE status=? ORDER BY msg_id LIMIT ?",
                (d.MSG_PENDING, limit))
        delivered, failed = [], []
        for row in pending:
            msg = dict(row)
            try:
                sender(msg)
            except Exception as exc:  # 投递失败不影响排班，留待重试
                with self.storage.transaction(immediate=True) as conn:
                    conn.execute(
                        "UPDATE outbox SET attempts=attempts+1, last_error=? WHERE msg_id=?",
                        (str(exc), msg["msg_id"]))
                failed.append({"msg_id": msg["msg_id"], "error": str(exc)})
                continue
            with self.storage.transaction(immediate=True) as conn:
                conn.execute(
                    "UPDATE outbox SET status=?, delivered_at=?, attempts=attempts+1, "
                    "last_error='' WHERE msg_id=? AND status=?",
                    (d.MSG_DELIVERED, self.clock.now_iso(), msg["msg_id"], d.MSG_PENDING))
            delivered.append(msg["msg_id"])
        return {"delivered": delivered, "delivered_count": len(delivered),
                "failed": failed, "failed_count": len(failed)}

    @staticmethod
    def _default_sender(msg: dict[str, Any]) -> None:
        """模拟短信/站内信通道，始终成功。"""
        return None

    def event_log(self, change_id: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT event_id, change_id, type, payload, created_at FROM event_log"
        params: tuple = ()
        if change_id:
            sql += " WHERE change_id=?"
            params = (change_id,)
        sql += " ORDER BY event_id"
        with self.storage.transaction() as conn:
            return [dict(r) for r in Storage.rows(conn, sql, params)]

    # ================================================================ 内部

    def _event(self, conn, change_id: str, etype: str, payload: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO event_log (change_id, type, payload, created_at) VALUES (?,?,?,?)",
            (change_id, etype, Storage.dumps(payload), self.clock.now_iso()))
