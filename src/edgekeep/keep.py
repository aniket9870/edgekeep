"""Outbox core: schema, state machine, publish(), and the writer-side
half of the sender's claim/ack/retry/dead operations.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Self

from edgekeep._uuid7 import uuid7_bytes

SCHEMA_VERSION = 1

STATE_PENDING = 0
STATE_INFLIGHT = 1
STATE_DEAD = 2

DEFAULT_COMMIT_WINDOW = 0.05

# executescript() forces its own commit and won't honor an outer BEGIN,
# so it can't take part in a transaction. Running each statement here
# individually is just what lets them join the BEGIN/COMMIT below -
# the atomicity comes from that transaction, not from the looping.
_SCHEMA_STATEMENTS = (
    """
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
    )
    """,
    """
    CREATE INDEX idx_keep_pending
        ON keep_messages (source_id, id)
        WHERE state = 0
    """,
    """
    CREATE TABLE keep_sources (
        source_id  TEXT PRIMARY KEY,
        next_seq   INTEGER NOT NULL DEFAULT 1,
        bytes_used INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE keep_meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
)

# sentinel telling the writer "no more work is coming, flush and stop"
_CLOSE = object()


@dataclass
class _QueuedPublish:
    topic: str
    payload: bytes
    source_id: str
    content_type: str | None
    future: asyncio.Future[int]


@dataclass
class _QueuedClaim:
    """Claim the oldest eligible PENDING row across all sources that don't
    already have one INFLIGHT. Result is None if nothing's eligible.
    """

    future: asyncio.Future[ClaimedMessage | None]


@dataclass
class _QueuedAck:
    message_id: int
    future: asyncio.Future[None]


@dataclass
class _QueuedRetry:
    message_id: int
    attempts: int
    next_retry_at_ms: int
    error: str
    future: asyncio.Future[None]


@dataclass
class _QueuedDead:
    message_id: int
    attempts: int
    error: str
    future: asyncio.Future[None]


_QueuedItem = _QueuedPublish | _QueuedClaim | _QueuedAck | _QueuedRetry | _QueuedDead


@dataclass(frozen=True)
class ClaimedMessage:
    """What the sender gets back from a successful claim -- everything it
    needs to attempt delivery without ever touching SQLite itself.
    """

    id: int
    idempotency_key: bytes
    source_id: str
    seq: int
    topic: str
    payload: bytes
    content_type: str | None
    attempts: int


@dataclass(frozen=True)
class Metrics:
    pending_messages: int
    inflight_messages: int
    dead_messages: int
    keep_bytes_used: int
    published_total: int
    acked_total: int
    retried_total: int
    oldest_pending_age_seconds: float | None


class Keep:
    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        commit_window: float = DEFAULT_COMMIT_WINDOW,
    ) -> None:
        self.path = path
        self.commit_window = commit_window
        self._conn: sqlite3.Connection | None = None
        self._queue: asyncio.Queue[object] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._published_total = 0
        self._acked_total = 0
        self._retried_total = 0

    async def __aenter__(self) -> Self:
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        meta_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'keep_meta'"
        ).fetchone()
        if meta_table is None:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for statement in _SCHEMA_STATEMENTS:
                    conn.execute(statement)
                conn.execute(
                    "INSERT INTO keep_meta (key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        else:
            row = conn.execute(
                "SELECT value FROM keep_meta WHERE key = 'schema_version'"
            ).fetchone()
            version = int(row[0]) if row else None
            if version != SCHEMA_VERSION:
                raise RuntimeError(
                    f"keep at {self.path!r} is on schema_version {version!r}, "
                    f"this build only knows {SCHEMA_VERSION} - no migration yet"
                )

        # a crash mid-send leaves rows claimed but never ack'd or requeued;
        # put them back before publish() or anything else can touch the table
        conn.execute(
            "UPDATE keep_messages SET state = ? WHERE state = ?",
            (STATE_PENDING, STATE_INFLIGHT),
        )

        self._conn = conn
        self._queue = asyncio.Queue()
        self._writer_task = asyncio.create_task(self._run_writer())
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
        """Queue a message for durable delivery and return its per-source seq.

        Returns once the row is committed locally, never once it's sent.
        Cancelling the await after the message is enqueued doesn't pull it
        back out - once queued, whether and when it gets committed is the
        writer's call, not the caller's.
        """
        if self._queue is None:
            raise RuntimeError("Keep is not open")
        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        await self._queue.put(
            _QueuedPublish(
                topic=topic,
                payload=payload,
                source_id=source_id,
                content_type=content_type,
                future=future,
            )
        )
        return await future

    async def close(self) -> None:
        if self._writer_task is not None:
            if self._queue is None:
                raise RuntimeError("Keep is not open")
            await self._queue.put(_CLOSE)
            await self._writer_task
            self._writer_task = None
            self._queue = None
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    async def metrics(self) -> Metrics:
        """A cheap read-only snapshot for health checks and alerting.

        published_total is an in-memory counter for this process, not a
        table scan — it resets if the process restarts, same as any other
        in-memory counter would.
        """
        if self._conn is None:
            raise RuntimeError("Keep is not open")
        conn = self._conn

        pending, inflight, dead, bytes_used, oldest_created_at = conn.execute(
            """
            SELECT
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END),
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END),
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END),
                COALESCE(SUM(LENGTH(payload)), 0),
                MIN(CASE WHEN state = ? THEN created_at END)
            FROM keep_messages
            """,
            (STATE_PENDING, STATE_INFLIGHT, STATE_DEAD, STATE_PENDING),
        ).fetchone()

        if oldest_created_at is None:
            oldest_pending_age_seconds = None
        else:
            now_ms = time.time_ns() // 1_000_000
            oldest_pending_age_seconds = (now_ms - oldest_created_at) / 1000

        return Metrics(
            pending_messages=pending or 0,
            inflight_messages=inflight or 0,
            dead_messages=dead or 0,
            keep_bytes_used=bytes_used,
            published_total=self._published_total,
            acked_total=self._acked_total,
            retried_total=self._retried_total,
            oldest_pending_age_seconds=oldest_pending_age_seconds,
        )

    # -- sender-facing internals -------------------------------------
    #
    # The sender lives in its own module and drives these instead of
    # touching SQLite itself. Everything still goes through the one
    # writer task that owns the connection -- claiming, acking, and
    # retrying are just other kinds of work that queue can carry.

    async def _claim_next(self) -> ClaimedMessage | None:
        if self._queue is None:
            raise RuntimeError("Keep is not open")
        future: asyncio.Future[ClaimedMessage | None] = (
            asyncio.get_running_loop().create_future()
        )
        await self._queue.put(_QueuedClaim(future=future))
        return await future

    async def _finalize_ack(self, message_id: int) -> None:
        if self._queue is None:
            raise RuntimeError("Keep is not open")
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        await self._queue.put(_QueuedAck(message_id=message_id, future=future))
        await future

    async def _finalize_retry(
        self, message_id: int, *, attempts: int, next_retry_at_ms: int, error: str
    ) -> None:
        if self._queue is None:
            raise RuntimeError("Keep is not open")
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        await self._queue.put(
            _QueuedRetry(
                message_id=message_id,
                attempts=attempts,
                next_retry_at_ms=next_retry_at_ms,
                error=error,
                future=future,
            )
        )
        await future

    async def _finalize_dead(self, message_id: int, *, attempts: int, error: str) -> None:
        if self._queue is None:
            raise RuntimeError("Keep is not open")
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        await self._queue.put(
            _QueuedDead(message_id=message_id, attempts=attempts, error=error, future=future)
        )
        await future

    async def _run_writer(self) -> None:
        assert self._queue is not None
        queue = self._queue

        try:
            while True:
                first = await queue.get()
                if first is _CLOSE:
                    return

                batch = [first]
                closing = False

                # only publishes wait around for company -- claim/ack/retry
                # are latency-sensitive control-plane calls from the sender,
                # so they get committed right away instead of sitting through
                # someone else's commit_window
                if self.commit_window > 0 and isinstance(first, _QueuedPublish):
                    deadline = time.monotonic() + self.commit_window
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        try:
                            item = await asyncio.wait_for(queue.get(), timeout=remaining)
                        except TimeoutError:
                            break
                        if item is _CLOSE:
                            closing = True
                            break
                        batch.append(item)
                else:
                    # either commit_window=0 or this round didn't start with
                    # a publish -- grab whatever's already sitting in the
                    # queue for free, but don't wait around for more
                    while True:
                        try:
                            item = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        if item is _CLOSE:
                            closing = True
                            break
                        batch.append(item)

                self._commit_batch(batch)

                if closing:
                    return
        finally:
            # a publish() racing close() can land behind the _CLOSE marker
            # and never get picked up above - reject those rather than
            # leaving the caller's future hanging forever
            while True:
                try:
                    leftover = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if leftover is _CLOSE:
                    continue
                if not leftover.future.done():  # type: ignore[attr-defined]
                    leftover.future.set_exception(  # type: ignore[attr-defined]
                        RuntimeError("Keep was closed before this publish could be committed")
                    )

    def _commit_batch(self, batch: list[_QueuedItem]) -> None:
        conn = self._conn
        assert conn is not None

        try:
            conn.execute("BEGIN IMMEDIATE")
            results: list[object] = [self._apply(conn, item) for item in batch]
            conn.execute("COMMIT")
        except BaseException as exc:
            try:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass  # connection's already broken, nothing left to roll back
            for item in batch:
                # caller may have cancelled its own await while this batch
                # was in flight - don't try to resolve a future twice
                if not item.future.done():
                    item.future.set_exception(exc)
            return

        self._published_total += sum(1 for item in batch if isinstance(item, _QueuedPublish))
        self._acked_total += sum(1 for item in batch if isinstance(item, _QueuedAck))
        self._retried_total += sum(1 for item in batch if isinstance(item, _QueuedRetry))

        for item, result in zip(batch, results):
            if not item.future.done():
                item.future.set_result(result)

    def _apply(self, conn: sqlite3.Connection, item: _QueuedItem) -> object:
        if isinstance(item, _QueuedPublish):
            return self._apply_publish(conn, item)
        if isinstance(item, _QueuedClaim):
            return self._apply_claim(conn)
        if isinstance(item, _QueuedAck):
            return self._apply_ack(conn, item)
        if isinstance(item, _QueuedRetry):
            return self._apply_retry(conn, item)
        if isinstance(item, _QueuedDead):
            return self._apply_dead(conn, item)
        raise AssertionError(f"unhandled queued item: {item!r}")

    def _apply_publish(self, conn: sqlite3.Connection, item: _QueuedPublish) -> int:
        idempotency_key = uuid7_bytes()
        created_at = time.time_ns() // 1_000_000
        conn.execute(
            "INSERT INTO keep_sources (source_id, next_seq) VALUES (?, 1) "
            "ON CONFLICT (source_id) DO NOTHING",
            (item.source_id,),
        )
        (seq,) = conn.execute(
            "SELECT next_seq FROM keep_sources WHERE source_id = ?",
            (item.source_id,),
        ).fetchone()
        conn.execute(
            "UPDATE keep_sources SET next_seq = next_seq + 1 WHERE source_id = ?",
            (item.source_id,),
        )
        conn.execute(
            "INSERT INTO keep_messages "
            "(idempotency_key, source_id, seq, topic, payload, content_type, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                idempotency_key,
                item.source_id,
                seq,
                item.topic,
                item.payload,
                item.content_type,
                created_at,
            ),
        )
        return seq

    def _apply_claim(self, conn: sqlite3.Connection) -> ClaimedMessage | None:
        now_ms = time.time_ns() // 1_000_000
        row = conn.execute(
            """
            UPDATE keep_messages
            SET state = ?
            WHERE id = (
                SELECT m.id FROM keep_messages m
                WHERE m.state = ?
                  AND (m.next_retry_at IS NULL OR m.next_retry_at <= ?)
                  AND NOT EXISTS (
                      SELECT 1 FROM keep_messages i
                      WHERE i.source_id = m.source_id AND i.state = ?
                  )
                ORDER BY m.source_id, m.id
                LIMIT 1
            )
            RETURNING id, idempotency_key, source_id, seq, topic, payload,
                      content_type, attempts
            """,
            (STATE_INFLIGHT, STATE_PENDING, now_ms, STATE_INFLIGHT),
        ).fetchone()
        if row is None:
            return None
        (id_, idempotency_key, source_id, seq, topic, payload, content_type, attempts) = row
        return ClaimedMessage(
            id=id_,
            idempotency_key=idempotency_key,
            source_id=source_id,
            seq=seq,
            topic=topic,
            payload=payload,
            content_type=content_type,
            attempts=attempts,
        )

    def _apply_ack(self, conn: sqlite3.Connection, item: _QueuedAck) -> None:
        conn.execute("DELETE FROM keep_messages WHERE id = ?", (item.message_id,))
        return None

    def _apply_retry(self, conn: sqlite3.Connection, item: _QueuedRetry) -> None:
        conn.execute(
            "UPDATE keep_messages "
            "SET state = ?, attempts = ?, next_retry_at = ?, last_error = ? "
            "WHERE id = ?",
            (STATE_PENDING, item.attempts, item.next_retry_at_ms, item.error, item.message_id),
        )
        return None

    def _apply_dead(self, conn: sqlite3.Connection, item: _QueuedDead) -> None:
        conn.execute(
            "UPDATE keep_messages SET state = ?, attempts = ?, last_error = ? WHERE id = ?",
            (STATE_DEAD, item.attempts, item.error, item.message_id),
        )
        return None
