"""The SQLite-backed outbox: schema, state machine, publish(). No sender
or transport in here — that's a separate piece and comes later.
"""

from __future__ import annotations

import os
import sqlite3
import time
from typing import Self

from edgekeep._uuid7 import uuid7_bytes

SCHEMA_VERSION = 1

STATE_PENDING = 0
STATE_INFLIGHT = 1
STATE_DEAD = 2

_SCHEMA_SQL = """
CREATE TABLE keep_messages (
    id              INTEGER PRIMARY KEY,
    idempotency_key BLOB    NOT NULL UNIQUE,
    source_id       TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    topic           TEXT    NOT NULL,
    payload         BLOB    NOT NULL,
    content_type    TEXT,
    created_at      INTEGER NOT NULL,
    state           INTEGER NOT NULL DEFAULT 0,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_retry_at   INTEGER,
    last_error      TEXT,
    UNIQUE (source_id, seq)
);

CREATE INDEX idx_keep_pending
    ON keep_messages (source_id, id)
    WHERE state = 0;

CREATE TABLE keep_sources (
    source_id  TEXT PRIMARY KEY,
    next_seq   INTEGER NOT NULL DEFAULT 1,
    bytes_used INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE keep_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Keep:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = path
        self._conn: sqlite3.Connection | None = None

    async def __aenter__(self) -> Self:
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        meta_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'keep_meta'"
        ).fetchone()
        if meta_table is None:
            conn.executescript(_SCHEMA_SQL)
            conn.execute(
                "INSERT INTO keep_meta (key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
        else:
            row = conn.execute(
                "SELECT value FROM keep_meta WHERE key = 'schema_version'"
            ).fetchone()
            version = int(row[0]) if row else None
            if version != SCHEMA_VERSION:
                raise RuntimeError(
                    f"keep at {self.path!r} is on schema_version {version!r}, "
                    f"this build only knows {SCHEMA_VERSION} — no migration yet"
                )

        # a crash mid-send leaves rows claimed but never ack'd or requeued;
        # put them back before publish() or anything else can touch the table
        conn.execute(
            "UPDATE keep_messages SET state = ? WHERE state = ?",
            (STATE_PENDING, STATE_INFLIGHT),
        )

        self._conn = conn
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def publish(
        self,
        *,
        topic: str,
        payload: bytes,
        source_id: str,
        content_type: str | None = None,
    ) -> int:
        assert self._conn is not None, "publish() called before Keep was opened"
        conn = self._conn
        idempotency_key = uuid7_bytes()
        created_at = time.time_ns() // 1_000_000

        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO keep_sources (source_id, next_seq) VALUES (?, 1) "
                "ON CONFLICT (source_id) DO NOTHING",
                (source_id,),
            )
            (seq,) = conn.execute(
                "SELECT next_seq FROM keep_sources WHERE source_id = ?",
                (source_id,),
            ).fetchone()
            conn.execute(
                "UPDATE keep_sources SET next_seq = next_seq + 1 WHERE source_id = ?",
                (source_id,),
            )
            conn.execute(
                "INSERT INTO keep_messages "
                "(idempotency_key, source_id, seq, topic, payload, content_type, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (idempotency_key, source_id, seq, topic, payload, content_type, created_at),
            )
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        return seq

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
