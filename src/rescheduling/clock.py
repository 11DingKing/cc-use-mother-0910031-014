"""可注入的时钟，保证顺序判定与测试可重放。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone


class Clock(ABC):
    @abstractmethod
    def now(self) -> datetime: ...

    def now_iso(self) -> str:
        return self.now().isoformat(timespec="seconds")


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)


class FixedClock(Clock):
    """测试用固定时钟，可手动推进。"""

    def __init__(self, moment: datetime | str = "2026-10-01T08:00:00"):
        self._moment = moment if isinstance(moment, datetime) else datetime.fromisoformat(moment)

    def now(self) -> datetime:
        return self._moment

    def advance(self, seconds: int) -> None:
        from datetime import timedelta

        self._moment += timedelta(seconds=seconds)
