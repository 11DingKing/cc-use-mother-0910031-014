"""演示数据：搭建场馆/讲解员/场次的基础排班。"""
from __future__ import annotations

from .service import Service


def seed_demo(service: Service) -> dict[str, str]:
    service.register_venue("V1", "自然探索馆", "010-1001")
    service.register_venue("V2", "科学体验馆", "010-1002")
    service.register_guide("G1", "王讲解", "13800000001")
    service.register_guide("G2", "李讲解", "13800000002")

    sessions = [
        ("S101", "古生物研学课", "V1", "G1", "2026-10-10T09:00", "2026-10-10T11:00", "张老师 13900000001"),
        ("S102", "天文观测课", "V1", "G1", "2026-10-10T14:00", "2026-10-10T16:00", "陈老师 13900000002"),
        ("S103", "机器人体验课", "V1", "G2", "2026-10-11T09:00", "2026-10-11T11:00", "赵老师 13900000003"),
        ("S201", "化学实验秀", "V2", "G2", "2026-10-10T10:00", "2026-10-10T12:00", "孙老师 13900000004"),
    ]
    for sid, name, vid, gid, st, et, contact in sessions:
        service.create_session(sid, name, vid, gid, st, et, contact)
    return {"venues": "V1,V2", "guides": "G1,G2", "sessions": ",".join(s[0] for s in sessions)}
