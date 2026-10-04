"""影响分析、冲突检测与候选方案生成。

本模块全部为纯读取函数：预演（dry-run）与正式分析共用同一套逻辑，
保证“预演看到的影响图”与“审批后实际执行”的判定完全一致。
所有排序规则确定（分数降序、资源编号升序），同一输入必得同一方案序列。
"""
from __future__ import annotations

import sqlite3

from .models import (
    IMPACTABLE_SESSION_STATES,
    LOCK_REQUEST_STATES,
    OCCUPYING_SESSION_STATES,
    UNAVAILABLE_RESOURCE_STATES,
    ChangeType,
    ImpactType,
    OptionKind,
)


def overlaps(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    """半开区间重叠判定（ISO 时间字符串按字典序可比）。"""
    return a_start < b_end and b_start < a_end


def _in_clause(values: tuple[str, ...]) -> str:
    return ",".join("?" for _ in values)


def _exclude_own(own_request_id: str | None) -> tuple[str, list]:
    if own_request_id is None:
        return "", []
    return " AND id != ?", [own_request_id]


def find_impacted_sessions(
    conn: sqlite3.Connection,
    change_type: str,
    resource_id: str,
    window_start: str,
    window_end: str,
) -> list[dict]:
    """找出变更窗口内受影响的场次，按 (开始时间, 场次编号) 确定排序。"""
    column = "venue_id" if change_type == ChangeType.VENUE_CLOSURE.value else "guide_id"
    states = _in_clause(IMPACTABLE_SESSION_STATES)
    cursor = conn.execute(
        f"""
        SELECT * FROM sessions
        WHERE {column} = ? AND state IN ({states})
          AND start_time < ? AND ? < end_time
        ORDER BY start_time, id
        """,
        (resource_id, *IMPACTABLE_SESSION_STATES, window_end, window_start),
    )
    return [dict(r) for r in cursor.fetchall()]


def active_lock_for_session(
    conn: sqlite3.Connection, session_id: str, own_request_id: str | None
) -> str | None:
    """场次锁：若该场次被更早提交的活跃变更请求占用，返回对方请求编号。

    确定顺序规则：锁优先级 = 请求的提交顺序（change_requests.rowid 单调递增），
    先提交者持锁；后到者在预演时即看到冲突。持锁请求进入终态
    （驳回/执行完/回滚）后，锁自动释放给下一个请求。
    """
    from .models import OPEN_IMPACT_STATUSES

    states = _in_clause(LOCK_REQUEST_STATES)
    statuses = _in_clause(OPEN_IMPACT_STATUSES)
    params: list = [session_id, *LOCK_REQUEST_STATES, *OPEN_IMPACT_STATUSES]
    age_filter = ""
    if own_request_id is not None:
        age_filter = (
            " AND r.rowid < (SELECT rowid FROM change_requests WHERE id = ?)"
        )
        params.append(own_request_id)
    cursor = conn.execute(
        f"""
        SELECT r.id FROM impact_items i
        JOIN change_requests r ON r.id = i.request_id
        WHERE i.session_id = ? AND r.state IN ({states}) AND i.status IN ({statuses})
          {age_filter}
        ORDER BY r.rowid
        LIMIT 1
        """,
        params,
    )
    row = cursor.fetchone()
    return row[0] if row else None


def venue_conflicts(
    conn: sqlite3.Connection,
    session: dict,
    venue_id: str,
    own_request_id: str | None,
) -> list[dict]:
    """目标场馆在场次时间窗内的冲突：被占用 / 被其他已生效闭馆覆盖。"""
    conflicts: list[dict] = []
    states = _in_clause(OCCUPYING_SESSION_STATES)
    cursor = conn.execute(
        f"""
        SELECT id FROM sessions
        WHERE venue_id = ? AND state IN ({states}) AND id != ?
          AND start_time < ? AND ? < end_time
        ORDER BY start_time, id
        """,
        (
            venue_id,
            *OCCUPYING_SESSION_STATES,
            session["id"],
            session["end_time"],
            session["start_time"],
        ),
    )
    for row in cursor.fetchall():
        conflicts.append(
            {
                "code": "venue_occupied",
                "ref": row[0],
                "detail": f"场馆 {venue_id} 在该时段已被场次 {row[0]} 占用",
            }
        )
    exclude, params = _exclude_own(own_request_id)
    states2 = _in_clause(UNAVAILABLE_RESOURCE_STATES)
    cursor = conn.execute(
        f"""
        SELECT id FROM change_requests
        WHERE type = ? AND resource_id = ? AND state IN ({states2})
          AND window_start < ? AND ? < window_end {exclude}
        ORDER BY id
        """,
        (
            ChangeType.VENUE_CLOSURE.value,
            venue_id,
            *UNAVAILABLE_RESOURCE_STATES,
            session["end_time"],
            session["start_time"],
            *params,
        ),
    )
    for row in cursor.fetchall():
        conflicts.append(
            {
                "code": "venue_closed",
                "ref": row[0],
                "detail": f"场馆 {venue_id} 在该时段被变更 {row[0]} 闭馆",
            }
        )
    return conflicts


def guide_conflicts(
    conn: sqlite3.Connection,
    session: dict,
    guide_id: str,
    own_request_id: str | None,
) -> list[dict]:
    """目标讲解员在场次时间窗内的冲突：已有场次 / 已生效请假。"""
    conflicts: list[dict] = []
    states = _in_clause(OCCUPYING_SESSION_STATES)
    cursor = conn.execute(
        f"""
        SELECT id FROM sessions
        WHERE guide_id = ? AND state IN ({states}) AND id != ?
          AND start_time < ? AND ? < end_time
        ORDER BY start_time, id
        """,
        (
            guide_id,
            *OCCUPYING_SESSION_STATES,
            session["id"],
            session["end_time"],
            session["start_time"],
        ),
    )
    for row in cursor.fetchall():
        conflicts.append(
            {
                "code": "guide_busy",
                "ref": row[0],
                "detail": f"讲解员 {guide_id} 在该时段已有场次 {row[0]}",
            }
        )
    exclude, params = _exclude_own(own_request_id)
    states2 = _in_clause(UNAVAILABLE_RESOURCE_STATES)
    cursor = conn.execute(
        f"""
        SELECT id FROM change_requests
        WHERE type = ? AND resource_id = ? AND state IN ({states2})
          AND window_start < ? AND ? < window_end {exclude}
        ORDER BY id
        """,
        (
            ChangeType.GUIDE_LEAVE.value,
            guide_id,
            *UNAVAILABLE_RESOURCE_STATES,
            session["end_time"],
            session["start_time"],
            *params,
        ),
    )
    for row in cursor.fetchall():
        conflicts.append(
            {
                "code": "guide_on_leave",
                "ref": row[0],
                "detail": f"讲解员 {guide_id} 在该时段因变更 {row[0]} 请假",
            }
        )
    return conflicts


def conflicts_for_option(
    conn: sqlite3.Connection,
    request: dict,
    session: dict,
    option: dict,
    group_size: int | None = None,
) -> list[dict]:
    """重算某候选方案的实时冲突（预演与执行复用同一判定）。"""
    if option["kind"] == OptionKind.CANCEL.value:
        return []
    own = request.get("id")
    conflicts: list[dict] = []
    venue_id = option.get("venue_id") or session["venue_id"]
    guide_id = option.get("guide_id") or session["guide_id"]
    if option.get("venue_id") and group_size is not None:
        row = conn.execute("SELECT capacity FROM venues WHERE id = ?", (venue_id,)).fetchone()
        capacity = row[0] if row else 0
        if capacity < group_size:
            conflicts.append(
                {
                    "code": "capacity",
                    "ref": venue_id,
                    "detail": f"场馆 {venue_id} 容量 {capacity} 小于团队人数 {group_size}",
                }
            )
    conflicts += venue_conflicts(conn, session, venue_id, own)
    conflicts += guide_conflicts(conn, session, guide_id, own)
    return conflicts


def _daily_guide_load(conn: sqlite3.Connection, guide_id: str, day: str) -> int:
    states = _in_clause(OCCUPYING_SESSION_STATES)
    cursor = conn.execute(
        f"""
        SELECT COUNT(*) FROM sessions
        WHERE guide_id = ? AND state IN ({states}) AND substr(start_time, 1, 10) = ?
        """,
        (guide_id, *OCCUPYING_SESSION_STATES, day),
    )
    return int(cursor.fetchone()[0])


def build_options(
    conn: sqlite3.Connection, request: dict, session: dict, group_size: int
) -> list[dict]:
    """为一个受影响场次生成候选方案：无冲突改派优先，取消兜底，冲突方案置后。"""
    options: list[dict] = []
    if request["type"] == ChangeType.VENUE_CLOSURE.value:
        cursor = conn.execute(
            "SELECT * FROM venues WHERE status = 'open' AND id != ? ORDER BY id",
            (session["venue_id"],),
        )
        for venue in cursor.fetchall():
            venue = dict(venue)
            option = {
                "kind": OptionKind.REASSIGN.value,
                "venue_id": venue["id"],
                "guide_id": None,
                "score": 1000.0 - abs(venue["capacity"] - group_size),
            }
            option["conflicts"] = conflicts_for_option(conn, request, session, option, group_size)
            options.append(option)
    else:
        day = session["start_time"][:10]
        cursor = conn.execute(
            "SELECT * FROM guides WHERE status = 'active' AND id != ? ORDER BY id",
            (session["guide_id"],),
        )
        for guide in cursor.fetchall():
            guide = dict(guide)
            option = {
                "kind": OptionKind.REASSIGN.value,
                "venue_id": None,
                "guide_id": guide["id"],
                "score": 1000.0 - 10.0 * _daily_guide_load(conn, guide["id"], day),
            }
            option["conflicts"] = conflicts_for_option(conn, request, session, option, group_size)
            options.append(option)
    options.append(
        {
            "kind": OptionKind.CANCEL.value,
            "venue_id": None,
            "guide_id": None,
            "score": 0.0,
            "conflicts": [],
        }
    )

    def sort_key(opt: dict) -> tuple:
        return (
            1 if opt["conflicts"] else 0,  # 无冲突优先
            0 if opt["kind"] == OptionKind.REASSIGN.value else 1,  # 改派优先于取消
            -opt["score"],
            opt.get("venue_id") or opt.get("guide_id") or "",
        )

    options.sort(key=sort_key)
    for rank, opt in enumerate(options, start=1):
        opt["rank"] = rank
    return options


def analyze(conn: sqlite3.Connection, request: dict) -> list[dict]:
    """生成变更影响图：受影响场次 + 每个场次的候选方案（含冲突标注）。"""
    change_type = request["type"]
    impact_type = (
        ImpactType.VENUE_UNAVAILABLE.value
        if change_type == ChangeType.VENUE_CLOSURE.value
        else ImpactType.GUIDE_UNAVAILABLE.value
    )
    sessions = find_impacted_sessions(
        conn,
        change_type,
        request["resource_id"],
        request["window_start"],
        request["window_end"],
    )
    impacts: list[dict] = []
    for seq, session in enumerate(sessions, start=1):
        locked_by = active_lock_for_session(conn, session["id"], request.get("id"))
        group = conn.execute(
            "SELECT size FROM groups WHERE id = ?", (session["group_id"],)
        ).fetchone()
        group_size = int(group[0]) if group else 0
        if locked_by:
            options: list[dict] = []
        else:
            options = build_options(conn, request, session, group_size)
        impacts.append(
            {
                "session_id": session["id"],
                "impact_type": impact_type,
                "seq": seq,
                "locked_by": locked_by,
                "options": options,
            }
        )
    return impacts
