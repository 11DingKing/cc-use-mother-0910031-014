"""SQLite 持久化层。

写事务一律 BEGIN IMMEDIATE：SQLite 据此把并发写者串行化，
配合 changes.seq 形成并发变更的确定顺序。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS venues (
    venue_id      TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    manager_phone TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS guides (
    guide_id TEXT PRIMARY KEY,
    name     TEXT NOT NULL,
    phone    TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS sessions (
    session_id     TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    venue_id       TEXT NOT NULL,
    guide_id       TEXT NOT NULL,
    start_ts       TEXT NOT NULL,
    end_ts         TEXT NOT NULL,
    school_contact TEXT NOT NULL DEFAULT '',
    state          TEXT NOT NULL,
    cancelled      INTEGER NOT NULL DEFAULT 0,
    version        INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS changes (
    change_id         TEXT PRIMARY KEY,
    seq               INTEGER NOT NULL UNIQUE,
    change_type       TEXT NOT NULL,
    resource_kind     TEXT NOT NULL,
    resource_id       TEXT NOT NULL,
    unavailable_start TEXT NOT NULL,
    unavailable_end   TEXT NOT NULL,
    reason            TEXT NOT NULL DEFAULT '',
    status            TEXT NOT NULL,
    plan_fingerprint  TEXT NOT NULL DEFAULT '',
    approved_by       TEXT NOT NULL DEFAULT '',
    approved_at       TEXT NOT NULL DEFAULT '',
    idempotency_key   TEXT NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS impacts (
    change_id  TEXT NOT NULL REFERENCES changes(change_id),
    session_id TEXT NOT NULL,
    impacted   INTEGER NOT NULL DEFAULT 1,
    action     TEXT NOT NULL DEFAULT '',
    option_id  TEXT NOT NULL DEFAULT '',
    explicit   INTEGER NOT NULL DEFAULT 0,
    note       TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (change_id, session_id)
);

CREATE TABLE IF NOT EXISTS options (
    option_id  TEXT PRIMARY KEY,
    change_id  TEXT NOT NULL,
    session_id TEXT NOT NULL,
    kind       TEXT NOT NULL,
    rank       INTEGER NOT NULL,
    payload    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    change_id   TEXT NOT NULL,
    session_id  TEXT NOT NULL DEFAULT '',
    code        TEXT NOT NULL,
    severity    TEXT NOT NULL DEFAULT 'HARD',
    message     TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'OPEN'
);

CREATE TABLE IF NOT EXISTS plan_steps (
    step_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    change_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    op           TEXT NOT NULL,
    target_type  TEXT NOT NULL DEFAULT '',
    target_id    TEXT NOT NULL DEFAULT '',
    before_data  TEXT NOT NULL DEFAULT '',
    after_data   TEXT NOT NULL DEFAULT '',
    state        TEXT NOT NULL DEFAULT 'PENDING',
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT NOT NULL DEFAULT '',
    completed_at TEXT NOT NULL DEFAULT '',
    UNIQUE (change_id, seq)
);

CREATE TABLE IF NOT EXISTS outbox (
    msg_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    change_id    TEXT NOT NULL,
    step_id      INTEGER,
    channel      TEXT NOT NULL,
    recipient    TEXT NOT NULL DEFAULT '',
    subject      TEXT NOT NULL DEFAULT '',
    body         TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'PENDING',
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT NOT NULL DEFAULT '',
    dedup_key    TEXT NOT NULL UNIQUE,
    created_at   TEXT NOT NULL,
    delivered_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS event_log (
    event_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    change_id  TEXT NOT NULL DEFAULT '',
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

-- 全局单写者锁：同一时刻只允许一个 Saga 在推进。
CREATE TABLE IF NOT EXISTS exec_lock (
    lock_id     INTEGER PRIMARY KEY CHECK (lock_id = 1),
    change_id   TEXT NOT NULL DEFAULT '',
    acquired_at TEXT NOT NULL DEFAULT ''
);
INSERT OR IGNORE INTO exec_lock (lock_id, change_id, acquired_at) VALUES (1, '', '');
"""


class Storage:
    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        self._keeper: sqlite3.Connection | None = None
        self._uri = ""
        # 进程内单写者锁：串行化写事务（共享缓存内存库依赖它，
        # 文件库也借此消除 SQLITE_BUSY 竞争）。
        self._tx_lock = threading.RLock()
        if self.path == ":memory:":
            # 共享缓存内存库：跨连接可见，生命周期随 Storage
            db = uuid.uuid4().hex
            self._uri = f"file:rescheduling-mem-{db}?mode=memory&cache=shared"
        else:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def connect(self) -> sqlite3.Connection:
        if self._uri:
            conn = sqlite3.connect(self._uri, uri=True, timeout=10)
            if self._keeper is None:
                # 持有一条长连接，防止共享内存库被回收
                self._keeper = sqlite3.connect(self._uri, uri=True, timeout=10)
        else:
            conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.isolation_level = None  # 显式事务
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA foreign_keys=ON")
        if not self._uri:
            conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init_schema(self) -> None:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        with self._tx_lock:
            try:
                if immediate:
                    conn.execute("BEGIN IMMEDIATE")
                else:
                    conn.execute("BEGIN")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    # ---------- 通用小工具 ----------

    @staticmethod
    def dumps(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def loads(value: str) -> Any:
        return json.loads(value) if value else None

    @staticmethod
    def row(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        return conn.execute(sql, params).fetchone()

    @staticmethod
    def rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return list(conn.execute(sql, params))
