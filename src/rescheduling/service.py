"""变更请求统一流程服务。

流程：创建（自动预演影响图）→ 审批（支持部分接受）→ 执行（逐场次事务推进）
→ 失败续办 / 回滚。核心保证：

1. 排班与通知一致性：每个场次的改排与其待投递消息在同一事务提交，
   消息只在排班生效后才可被投递，杜绝“学生已收到通知但资源未调整”。
2. 确定顺序：执行步骤按 (场次开始时间, 场次编号) 排序；并发写由
   BEGIN IMMEDIATE 串行化；乐观版本号拒绝过期决策。
3. 幂等：创建支持 Idempotency-Key；执行步骤与通知消息均有唯一去重键，
   重复执行/续办不会产生重复改排或重复通知。
4. 失败续办：每个步骤独立事务并落库，失败点之后可从断点安全继续。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from . import planner
from .errors import (
    DomainError,
    IdempotencyMismatchError,
    NotFoundError,
    StateConflictError,
    ValidationError,
    VersionConflictError,
)
from .models import (
    IMPACTABLE_SESSION_STATES,
    ChangeType,
    DecisionAction,
    ImpactStatus,
    OptionKind,
    OutboxStatus,
    RequestState,
    SessionState,
)
from .store import Store, one, rows


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _norm_ts(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field} 必须是 ISO 时间字符串")
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValidationError(f"{field} 不是合法 ISO 时间：{value}") from exc
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.isoformat(timespec="seconds")


class ChangeService:
    """临时变更联动重排的应用服务。"""

    def __init__(self, store: Store, clock: Callable[[], datetime] | None = None) -> None:
        self.store = store
        self._clock = clock or _utcnow

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    def _now(self) -> str:
        return self._clock().isoformat(timespec="seconds")

    def _audit(self, conn, request_id: str, event: str, detail: dict | None = None) -> None:
        conn.execute(
            "INSERT INTO audit_log (request_id, event, detail, created_at) VALUES (?,?,?,?)",
            (request_id, event, json.dumps(detail or {}, ensure_ascii=False), self._now()),
        )

    @staticmethod
    def _must_get(conn, request_id: str) -> dict:
        req = one(conn.execute("SELECT * FROM change_requests WHERE id = ?", (request_id,)))
        if req is None:
            raise NotFoundError(f"变更请求不存在：{request_id}")
        return req

    @staticmethod
    def _check_version(req: dict, expected_version: int | None) -> None:
        if expected_version is not None and int(expected_version) != req["version"]:
            raise VersionConflictError(
                f"版本冲突：期望 v{expected_version}，当前 v{req['version']}，请刷新后重试",
                {"current_version": req["version"]},
            )

    # ------------------------------------------------------------------
    # 基础数据：场次与资源
    # ------------------------------------------------------------------
    def list_resources(self, kind: str) -> list[dict]:
        table = {"venues": "venues", "guides": "guides", "groups": "groups"}.get(kind)
        if table is None:
            raise ValidationError(f"未知资源类型：{kind}")
        with self.store.read() as conn:
            return rows(conn.execute(f"SELECT * FROM {table} ORDER BY id"))

    def create_session(self, payload: dict) -> dict:
        required = ("group_id", "venue_id", "guide_id", "start_time", "end_time")
        missing = [k for k in required if not payload.get(k)]
        if missing:
            raise ValidationError("缺少字段：" + "、".join(missing))
        start = _norm_ts(payload["start_time"], "start_time")
        end = _norm_ts(payload["end_time"], "end_time")
        if start >= end:
            raise ValidationError("start_time 必须早于 end_time")
        state = payload.get("state", SessionState.SCHEDULED.value)
        if state not in IMPACTABLE_SESSION_STATES:
            raise ValidationError(f"新建场次状态仅支持：{'、'.join(IMPACTABLE_SESSION_STATES)}")
        session = {
            "id": payload.get("id") or _new_id("SE"),
            "group_id": payload["group_id"],
            "venue_id": payload["venue_id"],
            "guide_id": payload["guide_id"],
            "start_time": start,
            "end_time": end,
        }
        with self.store.transaction() as conn:
            for table, key in (("groups", "group_id"), ("venues", "venue_id"), ("guides", "guide_id")):
                if one(conn.execute(f"SELECT id FROM {table} WHERE id = ?", (session[key],))) is None:
                    raise NotFoundError(f"{table} 不存在：{session[key]}")
            group = one(conn.execute("SELECT * FROM groups WHERE id = ?", (session["group_id"],)))
            venue = one(conn.execute("SELECT * FROM venues WHERE id = ?", (session["venue_id"],)))
            conflicts: list[dict] = []
            if venue["capacity"] < group["size"]:
                conflicts.append(
                    {
                        "code": "capacity",
                        "ref": venue["id"],
                        "detail": f"场馆 {venue['id']} 容量 {venue['capacity']} 小于团队人数 {group['size']}",
                    }
                )
            conflicts += planner.venue_conflicts(conn, session, session["venue_id"], None)
            conflicts += planner.guide_conflicts(conn, session, session["guide_id"], None)
            if conflicts:
                raise StateConflictError("场次与现有资源占用冲突", {"conflicts": conflicts})
            conn.execute(
                """
                INSERT INTO sessions (id, group_id, venue_id, guide_id, start_time, end_time, state, version)
                VALUES (?,?,?,?,?,?,?,1)
                """,
                (
                    session["id"],
                    session["group_id"],
                    session["venue_id"],
                    session["guide_id"],
                    start,
                    end,
                    state,
                ),
            )
            return one(conn.execute("SELECT * FROM sessions WHERE id = ?", (session["id"],)))

    def list_sessions(self, filters: dict | None = None) -> list[dict]:
        filters = filters or {}
        clauses, params = [], []
        for key in ("venue_id", "guide_id", "group_id", "state"):
            if filters.get(key):
                clauses.append(f"{key} = ?")
                params.append(filters[key])
        if filters.get("date"):
            clauses.append("substr(start_time, 1, 10) = ?")
            params.append(filters["date"])
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.store.read() as conn:
            return rows(
                conn.execute(
                    f"SELECT * FROM sessions{where} ORDER BY start_time, id", params
                )
            )

    # ------------------------------------------------------------------
    # 变更请求：创建与预演
    # ------------------------------------------------------------------
    def _validate_request_payload(self, payload: dict) -> dict:
        change_type = payload.get("type")
        if change_type not in (ChangeType.VENUE_CLOSURE.value, ChangeType.GUIDE_LEAVE.value):
            raise ValidationError(
                f"type 仅支持：{ChangeType.VENUE_CLOSURE.value} / {ChangeType.GUIDE_LEAVE.value}"
            )
        resource_id = payload.get("resource_id")
        if not resource_id:
            raise ValidationError("缺少字段：resource_id")
        window_start = _norm_ts(payload.get("window_start"), "window_start")
        window_end = _norm_ts(payload.get("window_end"), "window_end")
        if window_start >= window_end:
            raise ValidationError("window_start 必须早于 window_end")
        return {
            "type": change_type,
            "resource_id": resource_id,
            "window_start": window_start,
            "window_end": window_end,
            "reason": str(payload.get("reason") or ""),
            "actor": str(payload.get("actor") or ""),
        }

    def create_request(self, payload: dict, idempotency_key: str | None = None) -> tuple[dict, bool]:
        """创建变更请求并自动预演影响图。返回 (请求详情, 是否幂等重放)。"""
        data = self._validate_request_payload(payload)
        key = idempotency_key or payload.get("idempotency_key") or None
        canonical = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self.store.transaction() as conn:
            if key:
                existing = one(
                    conn.execute("SELECT * FROM change_requests WHERE idempotency_key = ?", (key,))
                )
                if existing is not None:
                    if existing["request_payload"] != canonical:
                        raise IdempotencyMismatchError(
                            "同一 Idempotency-Key 提交了不同的变更内容",
                            {"request_id": existing["id"]},
                        )
                    return self._detail(conn, existing["id"]), True
            table = "venues" if data["type"] == ChangeType.VENUE_CLOSURE.value else "guides"
            if one(conn.execute(f"SELECT id FROM {table} WHERE id = ?", (data["resource_id"],))) is None:
                raise NotFoundError(f"资源不存在：{data['resource_id']}")
            request_id = _new_id("CR")
            now = self._now()
            conn.execute(
                """
                INSERT INTO change_requests
                    (id, idempotency_key, request_payload, type, resource_id,
                     window_start, window_end, reason, actor, state, version,
                     last_error, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,1,NULL,?,?)
                """,
                (
                    request_id,
                    key,
                    canonical,
                    data["type"],
                    data["resource_id"],
                    data["window_start"],
                    data["window_end"],
                    data["reason"],
                    data["actor"],
                    RequestState.PENDING_APPROVAL.value,
                    now,
                    now,
                ),
            )
            req = one(conn.execute("SELECT * FROM change_requests WHERE id = ?", (request_id,)))
            self._refresh_preview(conn, req)
            self._audit(conn, request_id, "created", {"payload": data})
            return self._detail(conn, request_id), False

    def dry_run(self, payload: dict) -> dict:
        """无状态预演：只计算影响图与候选方案，不落库。"""
        data = self._validate_request_payload(payload)
        with self.store.read() as conn:
            table = "venues" if data["type"] == ChangeType.VENUE_CLOSURE.value else "guides"
            if one(conn.execute(f"SELECT id FROM {table} WHERE id = ?", (data["resource_id"],))) is None:
                raise NotFoundError(f"资源不存在：{data['resource_id']}")
            impacts = planner.analyze(conn, {"id": None, **data})
        return {"request": data, "impacts": impacts, "persisted": False}

    def preview(self, request_id: str) -> dict:
        """重新预演（仅审批前允许）：刷新影响图与候选方案。"""
        with self.store.transaction() as conn:
            req = self._must_get(conn, request_id)
            if req["state"] != RequestState.PENDING_APPROVAL.value:
                raise StateConflictError(f"当前状态 {req['state']} 不允许重新预演")
            self._refresh_preview(conn, req)
            self._audit(conn, request_id, "previewed")
            return self._detail(conn, request_id)

    def _refresh_preview(self, conn, req: dict) -> None:
        impacts = planner.analyze(conn, req)
        conn.execute(
            "DELETE FROM plan_options WHERE impact_id IN (SELECT id FROM impact_items WHERE request_id = ?)",
            (req["id"],),
        )
        conn.execute("DELETE FROM impact_items WHERE request_id = ?", (req["id"],))
        now = self._now()
        for impact in impacts:
            impact_id = _new_id("IMP")
            conn.execute(
                """
                INSERT INTO impact_items (id, request_id, session_id, impact_type, status, seq, created_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    impact_id,
                    req["id"],
                    impact["session_id"],
                    impact["impact_type"],
                    ImpactStatus.PENDING.value,
                    impact["seq"],
                    now,
                ),
            )
            for opt in impact["options"]:
                conn.execute(
                    """
                    INSERT INTO plan_options (id, impact_id, kind, venue_id, guide_id, score, rank, conflicts)
                    VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        _new_id("OPT"),
                        impact_id,
                        opt["kind"],
                        opt.get("venue_id"),
                        opt.get("guide_id"),
                        opt["score"],
                        opt["rank"],
                        json.dumps(opt["conflicts"], ensure_ascii=False),
                    ),
                )

    # ------------------------------------------------------------------
    # 审批（支持部分接受）
    # ------------------------------------------------------------------
    def approve(
        self,
        request_id: str,
        decisions: list[dict],
        expected_version: int | None = None,
        actor: str = "",
    ) -> dict:
        if not isinstance(decisions, list):
            raise ValidationError("decisions 必须是数组")
        with self.store.transaction() as conn:
            req = self._must_get(conn, request_id)
            if req["state"] != RequestState.PENDING_APPROVAL.value:
                raise StateConflictError(f"当前状态 {req['state']} 不能审批")
            self._check_version(req, expected_version)
            impacts = rows(
                conn.execute("SELECT * FROM impact_items WHERE request_id = ?", (request_id,))
            )
            impact_map = {i["id"]: i for i in impacts}
            warnings: list[str] = []
            now = self._now()
            for decision in decisions:
                impact_id = decision.get("impact_id")
                action = decision.get("action")
                if impact_id not in impact_map:
                    raise ValidationError(f"影响项不属于本请求：{impact_id}")
                if action not in (DecisionAction.ACCEPT.value, DecisionAction.REJECT.value):
                    raise ValidationError(f"不支持的决策动作：{action}")
                option_id = None
                new_status = ImpactStatus.SKIPPED.value
                if action == DecisionAction.ACCEPT.value:
                    option_id = decision.get("option_id")
                    option = one(
                        conn.execute(
                            "SELECT * FROM plan_options WHERE id = ? AND impact_id = ?",
                            (option_id, impact_id),
                        )
                    )
                    if option is None:
                        raise ValidationError(f"候选方案不属于该影响项：{option_id}")
                    conflicts = json.loads(option["conflicts"])
                    if conflicts:
                        warnings.append(
                            f"影响项 {impact_id} 所选方案存在 {len(conflicts)} 个未解决冲突，执行将失败"
                        )
                    new_status = ImpactStatus.PLANNED.value
                conn.execute(
                    """
                    INSERT INTO decisions (request_id, impact_id, action, option_id, actor, decided_at)
                    VALUES (?,?,?,?,?,?)
                    ON CONFLICT (request_id, impact_id)
                    DO UPDATE SET action=excluded.action, option_id=excluded.option_id,
                                  actor=excluded.actor, decided_at=excluded.decided_at
                    """,
                    (request_id, impact_id, action, option_id, actor, now),
                )
                conn.execute(
                    "UPDATE impact_items SET status = ? WHERE id = ?", (new_status, impact_id)
                )
            conn.execute(
                "UPDATE change_requests SET state = ?, version = version + 1, updated_at = ? WHERE id = ?",
                (RequestState.APPROVED.value, now, request_id),
            )
            self._audit(
                conn,
                request_id,
                "approved",
                {"decisions": len(decisions), "actor": actor, "warnings": warnings},
            )
            return {"request": self._detail(conn, request_id), "warnings": warnings}

    def reject(self, request_id: str, reason: str = "", actor: str = "") -> dict:
        with self.store.transaction() as conn:
            req = self._must_get(conn, request_id)
            if req["state"] != RequestState.PENDING_APPROVAL.value:
                raise StateConflictError(f"当前状态 {req['state']} 不能驳回")
            now = self._now()
            conn.execute(
                "UPDATE impact_items SET status = ? WHERE request_id = ? AND status = ?",
                (ImpactStatus.SKIPPED.value, request_id, ImpactStatus.PENDING.value),
            )
            conn.execute(
                "UPDATE change_requests SET state = ?, version = version + 1, updated_at = ? WHERE id = ?",
                (RequestState.REJECTED.value, now, request_id),
            )
            self._audit(conn, request_id, "rejected", {"reason": reason, "actor": actor})
            return self._detail(conn, request_id)

    # ------------------------------------------------------------------
    # 执行 / 续办
    # ------------------------------------------------------------------
    def execute(
        self,
        request_id: str,
        expected_version: int | None = None,
        simulate_failure_at: int | None = None,
    ) -> dict:
        """执行已审批请求。simulate_failure_at 为故障演练参数（第 N 步注入失败）。"""
        with self.store.transaction() as conn:
            req = self._must_get(conn, request_id)
            if req["state"] in (
                RequestState.EXECUTED.value,
                RequestState.PARTIALLY_EXECUTED.value,
            ):
                summary = self._summary(conn, request_id)
                summary["idempotent_replay"] = True
                return summary
            if req["state"] != RequestState.APPROVED.value:
                raise StateConflictError(
                    f"当前状态 {req['state']} 不能执行；失败或中断的请求请使用续办（resume）"
                )
            self._check_version(req, expected_version)
            now = self._now()
            # 未决策的影响项确定为“未解决”，保持锁定并对外可见。
            conn.execute(
                "UPDATE impact_items SET status = ? WHERE request_id = ? AND status = ?",
                (ImpactStatus.SKIPPED.value, request_id, ImpactStatus.PENDING.value),
            )
            conn.execute(
                "UPDATE change_requests SET state = ?, version = version + 1, updated_at = ? WHERE id = ?",
                (RequestState.EXECUTING.value, now, request_id),
            )
            self._audit(conn, request_id, "execute_started")
        return self._run_steps(request_id, simulate_failure_at)

    def resume(
        self,
        request_id: str,
        decisions: list[dict] | None = None,
        expected_version: int | None = None,
    ) -> dict:
        """从失败点/中断点安全续办；可同时为失败影响项改选新方案。"""
        with self.store.transaction() as conn:
            req = self._must_get(conn, request_id)
            if req["state"] == RequestState.EXECUTED.value:
                summary = self._summary(conn, request_id)
                summary["idempotent_replay"] = True
                return summary
            if req["state"] not in (
                RequestState.FAILED.value,
                RequestState.EXECUTING.value,
                RequestState.PARTIALLY_EXECUTED.value,
            ):
                raise StateConflictError(f"当前状态 {req['state']} 不能续办")
            self._check_version(req, expected_version)
            now = self._now()
            if decisions:
                self._apply_decision_updates(conn, request_id, decisions, now)
            conn.execute(
                "UPDATE change_requests SET state = ?, version = version + 1, updated_at = ? WHERE id = ?",
                (RequestState.EXECUTING.value, now, request_id),
            )
            self._audit(conn, request_id, "resumed", {"decisions": len(decisions or [])})
        return self._run_steps(request_id, None)

    def _apply_decision_updates(self, conn, request_id: str, decisions: list[dict], now: str) -> None:
        updatable = (
            ImpactStatus.PENDING.value,
            ImpactStatus.PLANNED.value,
            ImpactStatus.FAILED.value,
            ImpactStatus.SKIPPED.value,
        )
        for decision in decisions:
            impact_id = decision.get("impact_id")
            impact = one(
                conn.execute(
                    "SELECT * FROM impact_items WHERE id = ? AND request_id = ?",
                    (impact_id, request_id),
                )
            )
            if impact is None:
                raise ValidationError(f"影响项不属于本请求：{impact_id}")
            if impact["status"] not in updatable:
                raise StateConflictError(f"影响项 {impact_id} 已生效，不能改选方案")
            action = decision.get("action")
            option_id = None
            new_status = ImpactStatus.SKIPPED.value
            if action == DecisionAction.ACCEPT.value:
                option_id = decision.get("option_id")
                if one(
                    conn.execute(
                        "SELECT id FROM plan_options WHERE id = ? AND impact_id = ?",
                        (option_id, impact_id),
                    )
                ) is None:
                    raise ValidationError(f"候选方案不属于该影响项：{option_id}")
                new_status = ImpactStatus.PLANNED.value
            elif action != DecisionAction.REJECT.value:
                raise ValidationError(f"不支持的决策动作：{action}")
            conn.execute(
                """
                INSERT INTO decisions (request_id, impact_id, action, option_id, actor, decided_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT (request_id, impact_id)
                DO UPDATE SET action=excluded.action, option_id=excluded.option_id,
                              decided_at=excluded.decided_at
                """,
                (request_id, impact_id, action, option_id, "resume", now),
            )
            conn.execute(
                "UPDATE impact_items SET status = ? WHERE id = ?", (new_status, impact_id)
            )

    def _pending_steps(self, request_id: str) -> list[dict]:
        """待执行步骤：按 (场次开始时间, 场次编号) 确定排序。"""
        with self.store.read() as conn:
            return rows(
                conn.execute(
                    """
                    SELECT i.id AS impact_id, i.session_id, d.option_id,
                           s.start_time AS session_start
                    FROM impact_items i
                    JOIN decisions d
                      ON d.request_id = i.request_id AND d.impact_id = i.id
                     AND d.action = ?
                    JOIN sessions s ON s.id = i.session_id
                    WHERE i.request_id = ? AND i.status IN (?, ?)
                    ORDER BY s.start_time, s.id
                    """,
                    (
                        DecisionAction.ACCEPT.value,
                        request_id,
                        ImpactStatus.PLANNED.value,
                        ImpactStatus.FAILED.value,
                    ),
                )
            )

    def _run_steps(self, request_id: str, simulate_failure_at: int | None) -> dict:
        steps = self._pending_steps(request_id)
        for index, step in enumerate(steps, start=1):
            if simulate_failure_at is not None and index == simulate_failure_at:
                self._fail_step(request_id, step, "模拟故障演练：步骤被注入失败")
                break
            if not self._apply_step(request_id, step):
                break
        return self._finish(request_id)

    def _fail_step(self, request_id: str, step: dict, error: str) -> None:
        with self.store.transaction() as conn:
            now = self._now()
            self._record_step(conn, request_id, step, "failed", None, None, error, now)
            conn.execute(
                "UPDATE impact_items SET status = ? WHERE id = ?",
                (ImpactStatus.FAILED.value, step["impact_id"]),
            )
            conn.execute(
                """
                UPDATE change_requests
                SET state = ?, last_error = ?, version = version + 1, updated_at = ?
                WHERE id = ?
                """,
                (RequestState.FAILED.value, error, now, request_id),
            )
            self._audit(conn, request_id, "step_failed", {"impact_id": step["impact_id"], "error": error})

    def _record_step(self, conn, request_id, step, status, before, after, error, now) -> None:
        existing = one(
            conn.execute(
                "SELECT * FROM execution_steps WHERE request_id = ? AND impact_id = ? AND kind = 'apply'",
                (request_id, step["impact_id"]),
            )
        )
        if existing is None:
            seq = one(
                conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM execution_steps WHERE request_id = ?",
                    (request_id,),
                )
            )["seq"]
            conn.execute(
                """
                INSERT INTO execution_steps
                    (request_id, impact_id, seq, kind, status, before_json, after_json, error, created_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    request_id,
                    step["impact_id"],
                    seq,
                    "apply",
                    status,
                    json.dumps(before, ensure_ascii=False) if before else None,
                    json.dumps(after, ensure_ascii=False) if after else None,
                    error,
                    now,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE execution_steps
                SET status = ?, before_json = COALESCE(?, before_json),
                    after_json = ?, error = ?, created_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    json.dumps(before, ensure_ascii=False) if before else None,
                    json.dumps(after, ensure_ascii=False) if after else None,
                    error,
                    now,
                    existing["id"],
                ),
            )

    def _apply_step(self, request_id: str, step: dict) -> bool:
        """单个场次的改排 + 通知消息在同一事务提交（排班通知一致性）。"""
        with self.store.transaction() as conn:
            req = self._must_get(conn, request_id)
            existing = one(
                conn.execute(
                    "SELECT * FROM execution_steps WHERE request_id = ? AND impact_id = ? AND kind = 'apply'",
                    (request_id, step["impact_id"]),
                )
            )
            if existing is not None and existing["status"] == "applied":
                return True  # 幂等：已完成的步骤直接跳过
            session = one(conn.execute("SELECT * FROM sessions WHERE id = ?", (step["session_id"],)))
            option = one(conn.execute("SELECT * FROM plan_options WHERE id = ?", (step["option_id"],)))
            now = self._now()
            if option is None:
                self._fail_step_tx(conn, request_id, req, step, "候选方案不存在", now)
                return False
            if session["state"] not in IMPACTABLE_SESSION_STATES:
                self._fail_step_tx(
                    conn, request_id, req, step, f"场次状态已变为 {session['state']}，无法改排", now
                )
                return False
            group = one(conn.execute("SELECT * FROM groups WHERE id = ?", (session["group_id"],)))
            conflicts = planner.conflicts_for_option(conn, req, session, option, group["size"])
            if conflicts:
                self._fail_step_tx(
                    conn,
                    request_id,
                    req,
                    step,
                    "方案执行前校验发现冲突：" + "；".join(c["detail"] for c in conflicts),
                    now,
                )
                return False
            before = dict(session)
            if option["kind"] == OptionKind.CANCEL.value:
                conn.execute(
                    "UPDATE sessions SET state = ?, version = version + 1 WHERE id = ?",
                    (SessionState.CANCELLED.value, session["id"]),
                )
            else:
                conn.execute(
                    """
                    UPDATE sessions
                    SET venue_id = COALESCE(?, venue_id),
                        guide_id = COALESCE(?, guide_id),
                        version = version + 1
                    WHERE id = ?
                    """,
                    (option.get("venue_id"), option.get("guide_id"), session["id"]),
                )
            after = one(conn.execute("SELECT * FROM sessions WHERE id = ?", (session["id"],)))
            self._record_step(conn, request_id, step, "applied", before, after, None, now)
            conn.execute(
                "UPDATE impact_items SET status = ? WHERE id = ?",
                (ImpactStatus.APPLIED.value, step["impact_id"]),
            )
            self._enqueue_notifications(conn, req, step["impact_id"], before, after, "apply", now)
            self._audit(
                conn,
                request_id,
                "step_applied",
                {"impact_id": step["impact_id"], "session_id": session["id"]},
            )
            return True

    def _fail_step_tx(self, conn, request_id, req, step, error, now) -> None:
        self._record_step(conn, request_id, step, "failed", None, None, error, now)
        conn.execute(
            "UPDATE impact_items SET status = ? WHERE id = ?",
            (ImpactStatus.FAILED.value, step["impact_id"]),
        )
        conn.execute(
            """
            UPDATE change_requests
            SET state = ?, last_error = ?, version = version + 1, updated_at = ?
            WHERE id = ?
            """,
            (RequestState.FAILED.value, error, now, request_id),
        )
        self._audit(conn, request_id, "step_failed", {"impact_id": step["impact_id"], "error": error})

    def _enqueue_notifications(self, conn, req, impact_id, before, after, kind, now) -> None:
        """写入待投递消息（与排班更新同事务）。去重键保证重复执行不重复通知。"""
        recipients: dict[str, str] = {f"group:{before['group_id']}": "school_contact"}
        for guide_id in {before["guide_id"], after["guide_id"]}:
            recipients[f"guide:{guide_id}"] = "guide"
        for venue_id in {before["venue_id"], after["venue_id"]}:
            recipients[f"venue:{venue_id}"] = "venue_admin"
        if kind == "rollback":
            msg_type = "session_restored"
            text = (
                f"【调整撤销】场次 {before['id']} 已恢复：场馆 {after['venue_id']}、"
                f"讲解员 {after['guide_id']}、状态 {after['state']}。"
            )
        elif after["state"] == SessionState.CANCELLED.value:
            msg_type = "session_cancelled"
            text = (
                f"【场次取消】场次 {before['id']}（{before['start_time']}）已取消。"
                f"原因：{req['reason'] or '资源临时变更'}。"
            )
        else:
            msg_type = "session_rescheduled"
            text = (
                f"【场次调整】场次 {before['id']}：场馆 {before['venue_id']}→{after['venue_id']}，"
                f"讲解员 {before['guide_id']}→{after['guide_id']}，"
                f"时间 {after['start_time']} 至 {after['end_time']}。"
                f"原因：{req['reason'] or '资源临时变更'}。"
            )
        payload = {
            "type": msg_type,
            "request_id": req["id"],
            "impact_id": impact_id,
            "session_id": before["id"],
            "before": before,
            "after": after,
            "message": text,
        }
        for recipient in sorted(recipients):
            conn.execute(
                """
                INSERT OR IGNORE INTO outbox
                    (dedupe_key, request_id, session_id, recipient, role, payload, status, created_at)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    f"{req['id']}:{impact_id}:{kind}:{recipient}",
                    req["id"],
                    before["id"],
                    recipient,
                    recipients[recipient],
                    json.dumps(payload, ensure_ascii=False),
                    OutboxStatus.PENDING.value,
                    now,
                ),
            )

    def _finish(self, request_id: str) -> dict:
        with self.store.transaction() as conn:
            req = self._must_get(conn, request_id)
            counts = {
                r["status"]: r["n"]
                for r in rows(
                    conn.execute(
                        "SELECT status, COUNT(*) AS n FROM impact_items WHERE request_id = ? GROUP BY status",
                        (request_id,),
                    )
                )
            }
            if counts.get(ImpactStatus.FAILED.value):
                new_state = RequestState.FAILED.value
            elif counts.get(ImpactStatus.SKIPPED.value):
                new_state = RequestState.PARTIALLY_EXECUTED.value
            else:
                new_state = RequestState.EXECUTED.value
            now = self._now()
            if req["state"] != new_state:
                conn.execute(
                    "UPDATE change_requests SET state = ?, version = version + 1, updated_at = ? WHERE id = ?",
                    (new_state, now, request_id),
                )
            self._audit(conn, request_id, new_state, {"impacts": counts})
            return self._summary(conn, request_id)

    # ------------------------------------------------------------------
    # 回滚
    # ------------------------------------------------------------------
    def rollback(
        self, request_id: str, reason: str = "", expected_version: int | None = None
    ) -> dict:
        """按相反顺序回滚已生效步骤，并在同一事务写入撤销通知。"""
        with self.store.transaction() as conn:
            req = self._must_get(conn, request_id)
            if req["state"] == RequestState.ROLLED_BACK.value:
                summary = self._summary(conn, request_id)
                summary["idempotent_replay"] = True
                return summary
            if req["state"] not in (
                RequestState.EXECUTED.value,
                RequestState.PARTIALLY_EXECUTED.value,
                RequestState.FAILED.value,
            ):
                raise StateConflictError(f"当前状态 {req['state']} 不能回滚")
            self._check_version(req, expected_version)
        with self.store.read() as conn:
            applied = rows(
                conn.execute(
                    """
                    SELECT * FROM execution_steps
                    WHERE request_id = ? AND kind = 'apply' AND status = 'applied'
                    ORDER BY seq DESC
                    """,
                    (request_id,),
                )
            )
        for step in applied:
            self._revert_step(request_id, step)
        with self.store.transaction() as conn:
            now = self._now()
            conn.execute(
                """
                UPDATE change_requests
                SET state = ?, version = version + 1, updated_at = ?, last_error = NULL
                WHERE id = ?
                """,
                (RequestState.ROLLED_BACK.value, now, request_id),
            )
            self._audit(conn, request_id, "rolled_back", {"reason": reason})
            return self._summary(conn, request_id)

    def _revert_step(self, request_id: str, step: dict) -> None:
        with self.store.transaction() as conn:
            current = one(
                conn.execute("SELECT * FROM execution_steps WHERE id = ?", (step["id"],))
            )
            if current is None or current["status"] != "applied":
                return  # 幂等：已回滚的步骤跳过
            req = self._must_get(conn, request_id)
            before = json.loads(current["before_json"])
            conn.execute(
                """
                UPDATE sessions
                SET venue_id = ?, guide_id = ?, state = ?, version = version + 1
                WHERE id = ?
                """,
                (before["venue_id"], before["guide_id"], before["state"], before["id"]),
            )
            after = one(conn.execute("SELECT * FROM sessions WHERE id = ?", (before["id"],)))
            now = self._now()
            conn.execute(
                "UPDATE execution_steps SET status = 'rolled_back', created_at = ? WHERE id = ?",
                (now, step["id"]),
            )
            conn.execute(
                "UPDATE impact_items SET status = ? WHERE id = ?",
                (ImpactStatus.ROLLED_BACK.value, current["impact_id"]),
            )
            applied_after = json.loads(current["after_json"])
            self._enqueue_notifications(
                conn, req, current["impact_id"], applied_after, after, "rollback", now
            )
            self._audit(
                conn, request_id, "step_reverted", {"impact_id": current["impact_id"]}
            )

    # ------------------------------------------------------------------
    # 查询：详情 / 冲突 / 审计 / 消息
    # ------------------------------------------------------------------
    def get_request(self, request_id: str) -> dict:
        with self.store.read() as conn:
            return self._detail(conn, request_id)

    def list_requests(self, state: str | None = None) -> list[dict]:
        with self.store.read() as conn:
            if state:
                return rows(
                    conn.execute(
                        "SELECT * FROM change_requests WHERE state = ? ORDER BY created_at, id",
                        (state,),
                    )
                )
            return rows(conn.execute("SELECT * FROM change_requests ORDER BY created_at, id"))

    def _detail(self, conn, request_id: str) -> dict:
        req = self._must_get(conn, request_id)
        impacts = rows(
            conn.execute(
                "SELECT * FROM impact_items WHERE request_id = ? ORDER BY seq", (request_id,)
            )
        )
        for impact in impacts:
            impact["session"] = one(
                conn.execute("SELECT * FROM sessions WHERE id = ?", (impact["session_id"],))
            )
            impact["locked_by"] = planner.active_lock_for_session(
                conn, impact["session_id"], request_id
            )
            options = rows(
                conn.execute(
                    "SELECT * FROM plan_options WHERE impact_id = ? ORDER BY rank",
                    (impact["id"],),
                )
            )
            for opt in options:
                opt["conflicts"] = json.loads(opt["conflicts"])
            impact["options"] = options
            impact["decision"] = one(
                conn.execute(
                    "SELECT * FROM decisions WHERE request_id = ? AND impact_id = ?",
                    (request_id, impact["id"]),
                )
            )
            impact["steps"] = rows(
                conn.execute(
                    "SELECT * FROM execution_steps WHERE request_id = ? AND impact_id = ? ORDER BY seq",
                    (request_id, impact["id"]),
                )
            )
        req["impacts"] = impacts
        return req

    def get_conflicts(self, request_id: str) -> dict:
        """未解决冲突视图：未生效的影响项、冲突方案、失败原因。"""
        with self.store.read() as conn:
            req = self._must_get(conn, request_id)
            unresolved_statuses = (
                ImpactStatus.PENDING.value,
                ImpactStatus.PLANNED.value,
                ImpactStatus.SKIPPED.value,
                ImpactStatus.FAILED.value,
            )
            marks = ",".join("?" for _ in unresolved_statuses)
            impacts = rows(
                conn.execute(
                    f"SELECT * FROM impact_items WHERE request_id = ? AND status IN ({marks}) ORDER BY seq",
                    (request_id, *unresolved_statuses),
                )
            )
            unresolved = []
            for impact in impacts:
                options = rows(
                    conn.execute(
                        "SELECT * FROM plan_options WHERE impact_id = ? ORDER BY rank",
                        (impact["id"],),
                    )
                )
                conflicted = []
                for opt in options:
                    opt["conflicts"] = json.loads(opt["conflicts"])
                    if opt["conflicts"]:
                        conflicted.append(opt)
                step_error = one(
                    conn.execute(
                        """
                        SELECT error FROM execution_steps
                        WHERE request_id = ? AND impact_id = ? AND status = 'failed'
                        ORDER BY id DESC LIMIT 1
                        """,
                        (request_id, impact["id"]),
                    )
                )
                locked_by = planner.active_lock_for_session(conn, impact["session_id"], request_id)
                unresolved.append(
                    {
                        "impact_id": impact["id"],
                        "session_id": impact["session_id"],
                        "status": impact["status"],
                        "impact_type": impact["impact_type"],
                        "step_error": step_error["error"] if step_error else None,
                        "locked_by": locked_by,
                        "conflicted_options": conflicted,
                    }
                )
            return {
                "request_id": request_id,
                "state": req["state"],
                "last_error": req["last_error"],
                "has_unresolved": bool(unresolved),
                "unresolved": unresolved,
            }

    def get_audit(self, request_id: str) -> list[dict]:
        with self.store.read() as conn:
            self._must_get(conn, request_id)
            events = rows(
                conn.execute(
                    "SELECT * FROM audit_log WHERE request_id = ? ORDER BY id", (request_id,)
                )
            )
            for event in events:
                event["detail"] = json.loads(event["detail"])
            return events

    def _summary(self, conn, request_id: str) -> dict:
        req = self._must_get(conn, request_id)
        impact_counts = {
            r["status"]: r["n"]
            for r in rows(
                conn.execute(
                    "SELECT status, COUNT(*) AS n FROM impact_items WHERE request_id = ? GROUP BY status",
                    (request_id,),
                )
            )
        }
        step_counts = {
            r["status"]: r["n"]
            for r in rows(
                conn.execute(
                    "SELECT status, COUNT(*) AS n FROM execution_steps WHERE request_id = ? GROUP BY status",
                    (request_id,),
                )
            )
        }
        outbox_counts = {
            r["status"]: r["n"]
            for r in rows(
                conn.execute(
                    "SELECT status, COUNT(*) AS n FROM outbox WHERE request_id = ? GROUP BY status",
                    (request_id,),
                )
            )
        }
        return {
            "request_id": request_id,
            "state": req["state"],
            "version": req["version"],
            "last_error": req["last_error"],
            "impacts": impact_counts,
            "steps": step_counts,
            "outbox": outbox_counts,
        }

    # ------------------------------------------------------------------
    # 待投递消息（outbox）
    # ------------------------------------------------------------------
    def list_outbox(self, status: str | None = None, request_id: str | None = None) -> list[dict]:
        clauses, params = [], []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if request_id:
            clauses.append("request_id = ?")
            params.append(request_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.store.read() as conn:
            messages = rows(conn.execute(f"SELECT * FROM outbox{where} ORDER BY id", params))
        for message in messages:
            message["payload"] = json.loads(message["payload"])
        return messages

    def dispatch_outbox(self, limit: int | None = None) -> list[dict]:
        """投递待发送消息：按写入顺序 FIFO，投递动作幂等（已投递不再重复）。"""
        with self.store.transaction() as conn:
            sql = "SELECT * FROM outbox WHERE status = ? ORDER BY id"
            params: list = [OutboxStatus.PENDING.value]
            if limit is not None:
                sql += " LIMIT ?"
                params.append(int(limit))
            pending = rows(conn.execute(sql, params))
            now = self._now()
            for message in pending:
                conn.execute(
                    "UPDATE outbox SET status = ?, delivered_at = ? WHERE id = ?",
                    (OutboxStatus.DELIVERED.value, now, message["id"]),
                )
            for message in pending:
                message["status"] = OutboxStatus.DELIVERED.value
                message["delivered_at"] = now
                message["payload"] = json.loads(message["payload"])
            return pending
