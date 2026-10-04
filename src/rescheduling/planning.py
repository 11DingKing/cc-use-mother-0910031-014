"""规划引擎：影响图、候选方案、资源冲突、冻结步骤。

预演（preview）只读当前排班与其他已冻结变更，产出：
  impacted  受影响场次及推荐动作
  options   每个场次的候选方案（确定性排序）
  conflicts 未解决的硬冲突
审批时把选中方案冻结为 plan_steps，并计算指纹：
  执行前逐步骤比对当前数据与 before_data，排班已被改动则失败点暂停，
  必须重新预演/审批，避免覆盖他人修改。

占用视图（occupancy）= 现存未取消场次 + 其他冻结变更的现实不可用窗口
+ 其他冻结变更已预占/已生效的重排目标。
"""
from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from typing import Any

from . import domain as d
from .errors import NotFoundError, ValidationError
from .storage import Storage

# 联动重排优先顺延的天数候选
RESCHEDULE_DAY_CANDIDATES = (1, 2, 3, 7, 14)
# 每个场次最多保留的候选方案数
MAX_OPTIONS_PER_SESSION = 5


@dataclass(frozen=True)
class Target:
    resource_kind: str
    resource_id: str
    start: str
    end: str
    session_id: str = ""  # 非空=占用来自某场次；空=现实不可用窗口


class PlanningEngine:
    def __init__(self, storage: Storage):
        self.storage = storage

    # ---------------------------------------------------------------- 数据读取

    @staticmethod
    def get_change(conn: sqlite3.Connection, change_id: str) -> sqlite3.Row:
        row = Storage.row(conn, "SELECT * FROM changes WHERE change_id=?", (change_id,))
        if row is None:
            raise NotFoundError(f"变更不存在：{change_id}")
        return row

    @staticmethod
    def _frozen_changes(conn: sqlite3.Connection, exclude_id: str,
                        statuses: tuple[str, ...] = d.FROZEN_ACTIVE,
                        before_seq: int | None = None) -> list[sqlite3.Row]:
        """其他未回滚变更，按全局 seq 升序（确定顺序）。

        before_seq 给出时只取 seq 更小者：方案只能叠加执行顺序在自己之前的变更。
        """
        sql = (
            "SELECT * FROM changes WHERE status IN (%s) AND change_id<>?"
            % ",".join("?" for _ in statuses)
        )
        params: list[Any] = [*statuses, exclude_id]
        if before_seq is not None:
            sql += " AND seq<?"
            params.append(before_seq)
        sql += " ORDER BY seq ASC"
        return Storage.rows(conn, sql, tuple(params))

    def projected_sessions(self, conn: sqlite3.Connection,
                           exclude_id: str) -> dict[str, dict[str, Any]]:
        """投影排班：现存有效场次叠加其他冻结变更尚未生效（PENDING）的步骤。

        DONE 步骤的效果已在 sessions 表中；PENDING 步骤按 seq 顺序叠加，
        CANCEL 移除场次，RESCHEDULE 替换时间与资源。这样后序变更看到的是
        「前序变更执行完后」的世界，与其被顺序闸约束的执行时机一致，
        冻结出的前态在轮到它执行时不会漂移。
        """
        proj: dict[str, dict[str, Any]] = {
            r["session_id"]: dict(r)
            for r in Storage.rows(conn, "SELECT * FROM sessions WHERE cancelled=0")
        }
        own = Storage.row(conn, "SELECT seq FROM changes WHERE change_id=?", (exclude_id,))
        before_seq = own["seq"] if own is not None else None
        for ch in self._frozen_changes(conn, exclude_id, before_seq=before_seq):
            # 若该变更停在失败点，失败步骤之后的计划不再可信，只叠加其之前的步骤
            fail_row = Storage.row(
                conn,
                "SELECT MIN(seq) AS m FROM plan_steps WHERE change_id=? AND state=?",
                (ch["change_id"], d.STEP_FAILED))
            min_failed = fail_row["m"] if fail_row is not None else None
            for st in Storage.rows(
                conn,
                "SELECT * FROM plan_steps WHERE change_id=? AND op IN (?,?) AND state=? "
                "ORDER BY seq ASC",
                (ch["change_id"], d.STEP_RESCHEDULE, d.STEP_CANCEL, d.STEP_PENDING),
            ):
                if min_failed is not None and st["seq"] >= min_failed:
                    continue
                sid = st["target_id"]
                if sid not in proj:
                    continue
                if st["op"] == d.STEP_CANCEL:
                    proj.pop(sid, None)
                else:
                    after = Storage.loads(st["after_data"])
                    proj[sid].update({
                        "venue_id": after["venue_id"], "guide_id": after["guide_id"],
                        "start_ts": after["start_ts"], "end_ts": after["end_ts"],
                        "version": after["version"],
                    })
        return proj

    def occupancy(self, conn: sqlite3.Connection, exclude_id: str,
                  include_self_window: sqlite3.Row | None = None) -> dict[str, list[Target]]:
        """聚合当前/投影资源占用：投影场次 + 各冻结变更的现实不可用窗口。"""
        blocked: dict[str, list[Target]] = {}
        seen: set[tuple[str, str, str]] = set()

        def add(kind: str, rid: str, start: str, end: str, session_id: str = "") -> None:
            dedup = (kind, rid, session_id)
            if session_id and dedup in seen:
                return
            if session_id:
                seen.add(dedup)
            blocked.setdefault(f"{kind}:{rid}", []).append(
                Target(kind, rid, start, end, session_id))

        own = Storage.row(conn, "SELECT seq FROM changes WHERE change_id=?", (exclude_id,))
        before_seq = own["seq"] if own is not None else None
        for ch in self._frozen_changes(conn, exclude_id, d.WINDOW_ACTIVE,
                                       before_seq=before_seq):
            # 现实不可用窗口：执行期间始终占用；回滚（ROLLED_BACK）/作废（VOID）后释放
            add(ch["resource_kind"], ch["resource_id"],
                ch["unavailable_start"], ch["unavailable_end"])

        # 投影场次（已含前序冻结变更的重排目标）
        for s in self.projected_sessions(conn, exclude_id).values():
            add(d.RESOURCE_VENUE, s["venue_id"], s["start_ts"], s["end_ts"], s["session_id"])
            add(d.RESOURCE_GUIDE, s["guide_id"], s["start_ts"], s["end_ts"], s["session_id"])

        if include_self_window is not None:
            ch = include_self_window
            add(ch["resource_kind"], ch["resource_id"],
                ch["unavailable_start"], ch["unavailable_end"])
        return blocked

    # ---------------------------------------------------------------- 影响识别

    def _overlapping(self, conn: sqlite3.Connection, ch: sqlite3.Row,
                     movable_only: bool) -> list[dict[str, Any]]:
        """基于投影排班识别与变更窗口重叠的场次。"""
        projection = self.projected_sessions(conn, ch["change_id"])
        hit: list[dict[str, Any]] = []
        for s in projection.values():
            if movable_only and s["state"] not in d.MOVABLE_STATES:
                continue
            if ch["change_type"] == d.VENUE_CLOSURE and s["venue_id"] != ch["resource_id"]:
                continue
            if ch["change_type"] == d.GUIDE_LEAVE and s["guide_id"] != ch["resource_id"]:
                continue
            if d.overlaps(s["start_ts"], s["end_ts"],
                          ch["unavailable_start"], ch["unavailable_end"]):
                hit.append(s)
        hit.sort(key=lambda s: (s["start_ts"], s["session_id"]))
        return hit

    def _impacted_sessions(self, conn: sqlite3.Connection, ch: sqlite3.Row) -> list[dict[str, Any]]:
        return self._overlapping(conn, ch, movable_only=True)

    # ---------------------------------------------------------------- 候选方案

    @staticmethod
    def _slot_free(blocked: dict[str, list[Target]], venue_id: str, guide_id: str,
                   start: str, end: str, ignore_session: str = "") -> bool:
        for kind, rid in ((d.RESOURCE_VENUE, venue_id), (d.RESOURCE_GUIDE, guide_id)):
            for t in blocked.get(f"{kind}:{rid}", []):
                if t.session_id == ignore_session:
                    continue
                if d.overlaps(start, end, t.start, t.end):
                    return False
        return True

    def _assignments(self, conn: sqlite3.Connection, s: sqlite3.Row) -> list[tuple[str, str]]:
        """资源替换优先级：同馆同员 > 同馆换员 > 换馆（同员优先）。"""
        venue_ids = [r["venue_id"] for r in Storage.rows(conn, "SELECT venue_id FROM venues ORDER BY venue_id")]
        guide_ids = [r["guide_id"] for r in Storage.rows(conn, "SELECT guide_id FROM guides ORDER BY guide_id")]
        result: list[tuple[str, str]] = [(s["venue_id"], s["guide_id"])]
        for gid in guide_ids:
            if gid != s["guide_id"]:
                result.append((s["venue_id"], gid))
        for vid in venue_ids:
            if vid == s["venue_id"]:
                continue
            result.append((vid, s["guide_id"]))
            for gid in guide_ids:
                result.append((vid, gid))
        # 去重保序
        seen: set[tuple[str, str]] = set()
        unique: list[tuple[str, str]] = []
        for item in result:
            if item not in seen:
                seen.add(item)
                unique.append(item)
        return unique

    def _reschedule_options(self, conn: sqlite3.Connection, ch: sqlite3.Row,
                            s: sqlite3.Row, blocked: dict[str, list[Target]]) -> list[dict[str, Any]]:
        options: list[dict[str, Any]] = []
        rank = 0
        for day in RESCHEDULE_DAY_CANDIDATES:
            new_start = d.shift_ts(s["start_ts"], day)
            new_end = d.shift_ts(s["end_ts"], day)
            # 新时间不得再次落入本次变更窗口
            if d.overlaps(new_start, new_end, ch["unavailable_start"], ch["unavailable_end"]):
                continue
            for venue_id, guide_id in self._assignments(conn, s):
                if self._slot_free(blocked, venue_id, guide_id, new_start, new_end,
                                   ignore_session=s["session_id"]):
                    rank += 1
                    options.append({
                        "kind": d.ACT_RESCHEDULE,
                        "rank": rank,
                        "payload": {
                            "session_id": s["session_id"],
                            "venue_id": venue_id,
                            "guide_id": guide_id,
                            "start_ts": new_start,
                            "end_ts": new_end,
                            "reason": f"顺延{day}天",
                        },
                    })
                    break  # 该顺延天数取优先级最高的可行槽位
            if rank >= MAX_OPTIONS_PER_SESSION:
                break
        return options

    # ---------------------------------------------------------------- 预演

    def preview(self, conn: sqlite3.Connection, change_id: str) -> dict[str, Any]:
        ch = self.get_change(conn, change_id)
        blocked = self.occupancy(conn, change_id, include_self_window=ch)
        impacted_rows = self._impacted_sessions(conn, ch)

        impacts: list[dict[str, Any]] = []
        options_out: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []

        for s in impacted_rows:
            opts = self._reschedule_options(conn, ch, s, blocked)
            for o in opts:
                options_out.append({"session_id": s["session_id"], **o})
            impacts.append({
                "session_id": s["session_id"],
                "name": s["name"],
                "state": s["state"],
                "venue_id": s["venue_id"],
                "guide_id": s["guide_id"],
                "start_ts": s["start_ts"],
                "end_ts": s["end_ts"],
                "recommended_action": d.ACT_RESCHEDULE if opts else d.ACT_CANCEL,
                "option_count": len(opts),
            })
            if not opts:
                conflicts.append({
                    "session_id": s["session_id"],
                    "code": "NO_AVAILABLE_SLOT",
                    "severity": "HARD",
                    "message": f"场次{s['name']}在候选窗口内无可用场地/讲解员，需明确取消",
                })

        # 不可联动的场次（执行中/已结算）撞上变更窗口：无法解决的硬冲突
        for s in self._overlapping(conn, ch, movable_only=False):
            if s["state"] not in d.MOVABLE_STATES:
                conflicts.append({
                    "session_id": s["session_id"],
                    "code": "SESSION_NOT_MOVABLE",
                    "severity": "HARD",
                    "message": f"场次{s['name']}已处于{s['state']}，无法调整",
                })

        return {
            "change_id": change_id,
            "status": ch["status"],
            "impacted": impacts,
            "options": options_out,
            "conflicts": conflicts,
            "unresolved_count": len(conflicts),
            "recommendation": {
                "reschedule": sum(1 for i in impacts if i["recommended_action"] == d.ACT_RESCHEDULE),
                "cancel": sum(1 for i in impacts if i["recommended_action"] == d.ACT_CANCEL),
            },
        }

    # ---------------------------------------------------------------- 审批冻结

    def freeze_steps(self, conn: sqlite3.Connection, ch: sqlite3.Row,
                     decisions: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        """把审批决策冻结为确定性步骤序列。

        顺序固定：先标记资源不可用，再按 (start_ts, session_id) 逐场次处理。
        decisions: {session_id: {"action": "RESCHEDULE", "payload": {...}}}
        未给决策的场次：有候选默认取 rank 第一的重排方案，无候选则报错。
        """
        impacted = sorted(self._impacted_sessions(conn, ch),
                          key=lambda s: (s["start_ts"], s["session_id"]))
        steps: list[dict[str, Any]] = [{
            "op": d.STEP_MARK,
            "target_type": ch["resource_kind"],
            "target_id": ch["resource_id"],
            "before_data": {"unavailable": False},
            "after_data": {
                "unavailable": True,
                "start": ch["unavailable_start"],
                "end": ch["unavailable_end"],
            },
        }]
        for s in impacted:
            dec = decisions.get(s["session_id"])
            if dec is None:
                blocked = self.occupancy(conn, ch["change_id"], include_self_window=ch)
                auto = self._reschedule_options(conn, ch, s, blocked)
                if not auto:
                    raise ValidationError(
                        f"场次{s['session_id']}无可用重排槽位，必须明确决策",
                        {"session_id": s["session_id"], "code": "DECISION_REQUIRED"})
                dec = {"action": d.ACT_RESCHEDULE, "payload": auto[0]["payload"]}
            action = dec.get("action")
            if action == d.ACT_CANCEL:
                steps.append({
                    "op": d.STEP_CANCEL,
                    "target_type": "SESSION",
                    "target_id": s["session_id"],
                    "before_data": self._snapshot_before(s),
                    "after_data": {
                        "session_id": s["session_id"], "cancelled": 1,
                        "state": s["state"], "version": s["version"] + 1,
                    },
                })
            elif action == d.ACT_RESCHEDULE:
                payload = dec.get("payload")
                if not payload or payload.get("session_id") != s["session_id"]:
                    raise ValidationError("重排方案缺失或与场次不匹配",
                                          {"session_id": s["session_id"]})
                steps.append({
                    "op": d.STEP_RESCHEDULE,
                    "target_type": "SESSION",
                    "target_id": s["session_id"],
                    "before_data": self._snapshot_before(s),
                    "after_data": {
                        "session_id": s["session_id"],
                        "venue_id": payload["venue_id"],
                        "guide_id": payload["guide_id"],
                        "start_ts": payload["start_ts"],
                        "end_ts": payload["end_ts"],
                        "state": s["state"],
                        "cancelled": 0,
                        "version": s["version"] + 1,
                        "reason": payload.get("reason", ""),
                    },
                })
            else:
                raise ValidationError(f"未知决策动作：{action!r}",
                                      {"session_id": s["session_id"]})
        return steps

    @staticmethod
    def _snapshot_before(s: sqlite3.Row) -> dict[str, Any]:
        return {
            "session_id": s["session_id"],
            "venue_id": s["venue_id"], "guide_id": s["guide_id"],
            "start_ts": s["start_ts"], "end_ts": s["end_ts"],
            "state": s["state"], "cancelled": s["cancelled"],
            "version": s["version"],
        }

    @staticmethod
    def plan_fingerprint(steps: list[dict[str, Any]]) -> str:
        """对冻结步骤的 前态+后态 计算指纹。"""
        material = Storage.dumps([
            {"op": st["op"], "target": st["target_id"],
             "before": st["before_data"], "after": st["after_data"]}
            for st in steps
        ])
        return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

    def revalidate_decisions(self, conn: sqlite3.Connection, ch: sqlite3.Row,
                             steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """审批提交时复验：重排目标槽位是否仍空闲（防并发改动/并发审批）。"""
        blocked = self.occupancy(conn, ch["change_id"], include_self_window=ch)
        problems: list[dict[str, Any]] = []
        for st in steps:
            if st["op"] != d.STEP_RESCHEDULE:
                continue
            a = st["after_data"]
            if not self._slot_free(blocked, a["venue_id"], a["guide_id"],
                                   a["start_ts"], a["end_ts"], ignore_session=a["session_id"]):
                problems.append({
                    "session_id": a["session_id"],
                    "code": "SLOT_TAKEN",
                    "severity": "HARD",
                    "message": "所选槽位已被占用，请重新预演后再审批",
                })
        return problems
