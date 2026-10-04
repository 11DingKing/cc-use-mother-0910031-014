"""演示数据：场馆、讲解员、学校团队与已排场次。"""
from __future__ import annotations

from .store import Store, one

VENUES = [
    ("V1", "主展厅", 120),
    ("V2", "科普教室", 40),
    ("V3", "报告厅", 100),
]

GUIDES = [
    ("G1", "王讲解员"),
    ("G2", "李讲解员"),
    ("G3", "赵讲解员"),
]

GROUPS = [
    ("SCH1", "育才小学四年级", 35, "138-0000-0001"),
    ("SCH2", "滨江中学二年级", 90, "138-0000-0002"),
]

SESSIONS = [
    ("SE1", "SCH1", "V1", "G1", "2026-10-10T09:00:00", "2026-10-10T10:30:00", "已排定"),
    ("SE2", "SCH2", "V1", "G2", "2026-10-10T10:30:00", "2026-10-10T12:00:00", "已排定"),
    ("SE3", "SCH1", "V2", "G1", "2026-10-10T13:00:00", "2026-10-10T14:30:00", "待确认"),
    ("SE4", "SCH2", "V3", "G3", "2026-10-10T14:00:00", "2026-10-10T15:30:00", "已排定"),
]


def seed_if_empty(store: Store) -> bool:
    """空库时注入演示数据；返回是否执行了注入。"""
    with store.transaction() as conn:
        if one(conn.execute("SELECT id FROM venues LIMIT 1")) is not None:
            return False
        conn.executemany(
            "INSERT INTO venues (id, name, capacity, status) VALUES (?,?,?,'open')", VENUES
        )
        conn.executemany(
            "INSERT INTO guides (id, name, status) VALUES (?,?,'active')", GUIDES
        )
        conn.executemany(
            "INSERT INTO groups (id, name, size, contact) VALUES (?,?,?,?)", GROUPS
        )
        conn.executemany(
            """
            INSERT INTO sessions (id, group_id, venue_id, guide_id, start_time, end_time, state, version)
            VALUES (?,?,?,?,?,?,?,1)
            """,
            SESSIONS,
        )
        return True
