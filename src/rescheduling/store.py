"""SQLite 持久化层：表结构、连接与事务助手。

并发策略：
- 所有写事务使用 BEGIN IMMEDIATE，在数据库层面串行化并发写，
  使并发变更获得确定的先后次序（先拿到写锁者先提交）。
- 进程内再用可重入锁保护，:memory: 模式下多线程共享同一连接也安全。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS venues (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    capacity INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS guides (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    size INTEGER NOT NULL,
    contact TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL REFERENCES groups(id),
    venue_id TEXT NOT NULL REFERENCES venues(id),
    guide_id TEXT NOT NULL REFERENCES guides(id),
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS change_requests (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    request_payload TEXT NOT NULL,
    type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    actor TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS impact_items (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL REFERENCES change_requests(id),
    session_id TEXT NOT NULL REFERENCES sessions(id),
    impact_type TEXT NOT NULL,
    status TEXT NOT NULL,
    seq INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_impact_request ON impact_items(request_id);
CREATE INDEX IF NOT EXISTS idx_impact_session ON impact_items(session_id);

CREATE TABLE IF NOT EXISTS plan_options (
    id TEXT PRIMARY KEY,
    impact_id TEXT NOT NULL REFERENCES impact_items(id),
    kind TEXT NOT NULL,
    venue_id TEXT,
    guide_id TEXT,
    score REAL NOT NULL,
    rank INTEGER NOT NULL,
    conflicts TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_option_impact ON plan_options(impact_id);

CREATE TABLE IF NOT EXISTS decisions (
    request_id TEXT NOT NULL,
    impact_id TEXT NOT NULL,
    action TEXT NOT NULL,
    option_id TEXT,
    actor TEXT NOT NULL DEFAULT '',
    decided_at TEXT NOT NULL,
    PRIMARY KEY (request_id, impact_id)
);

CREATE TABLE IF NOT EXISTS execution_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    impact_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    before_json TEXT,
    after_json TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (request_id, impact_id, kind)
);

CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    request_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    recipient TEXT NOT NULL,
    role TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    delivered_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, id);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    event TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_request ON audit_log(request_id, id);
"""


def rows(cursor: sqlite3.Cursor) -> list[dict]:
    return [dict(r) for r in cursor.fetchall()]


def one(cursor: sqlite3.Cursor) -> dict | None:
    row = cursor.fetchone()
    return dict(row) if row is not None else None


class Store:
    """SQLite 存储。path=":memory:" 时进程内共享单一连接。"""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self._shared: sqlite3.Connection | None = None
        if self.path == ":memory:":
            self._shared = self._connect()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        if self.path != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def initialize(self) -> None:
        # executescript 自行管理事务，不能包在 BEGIN IMMEDIATE 里。
        with self._lock:
            conn = self._shared or self._connect()
            try:
                conn.executescript(SCHEMA)
            finally:
                if conn is not self._shared:
                    conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """写事务：BEGIN IMMEDIATE 串行化并发写，保证确定的提交次序。"""
        with self._lock:
            conn = self._shared or self._connect()
            begun = False
            try:
                conn.execute("BEGIN IMMEDIATE")
                begun = True
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                if begun:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                raise
            finally:
                if conn is not self._shared:
                    conn.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        """只读视图（进程内与写互斥，保证读到一致的快照）。"""
        with self._lock:
            conn = self._shared or self._connect()
            try:
                yield conn
            finally:
                if conn is not self._shared:
                    conn.close()
