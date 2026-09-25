"""Outbox core: schema, state machine, publish(), and the writer-side
half of the sender's claim/ack/retry/dead operations.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Self

from edgekeep._uuid7 import uuid7_bytes
from edgekeep.eviction import DropOldest, EvictedMessage, EvictionPolicy
from edgekeep.transform import Draft, PluginStorage, Transform, TransformTimeoutError

_logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2

STATE_PENDING = 0
STATE_INFLIGHT = 1
STATE_DEAD = 2

DEFAULT_COMMIT_WINDOW = 0.05
DEFAULT_MAX_BYTES = 256 * 2**20
DEFAULT_MAX_MESSAGES = 1_000_000
DEFAULT_TRANSFORM_TIMEOUT = 5.0

# running total of rows in keep_messages, kept in sync on every
# insert/ack/evict so we're not doing a COUNT(*) on every publish
_MESSAGE_COUNT_KEY = "message_count"

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
    """
    CREATE TABLE plugin_storage (
        namespace TEXT NOT NULL,
        key       TEXT NOT NULL,
        value     BLOB NOT NULL,
        PRIMARY KEY (namespace, key)
    )
    """,
)

# forward-only migrations, keyed on the version they bring a db up to.
# a fresh db skips this entirely (it gets _SCHEMA_STATEMENTS directly,
# already at SCHEMA_VERSION) -- this is only for opening an older one.
_MIGRATIONS: dict[int, tuple[str, ...]] = {
    2: (
        """
        CREATE TABLE plugin_storage (
            namespace TEXT NOT NULL,
            key       TEXT NOT NULL,
            value     BLOB NOT NULL,
            PRIMARY KEY (namespace, key)
        )
        """,
    ),
}

# sentinel telling the writer "no more work is coming, flush and stop"
_CLOSE = object()


@dataclass
class _QueuedPublish:
    topic: str
    payload: bytes
    source_id: str
    content_type: str | None
    future: asyncio.Future[list[int]]


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


def _bump_message_count(conn: sqlite3.Connection, delta: int) -> None:
    conn.execute(
        "UPDATE keep_meta SET value = CAST(CAST(value AS INTEGER) + ? AS TEXT) WHERE key = ?",
        (delta, _MESSAGE_COUNT_KEY),
    )


class _EvictionContextImpl:
    """What a policy sees during a publish's own transaction. Only
    PENDING rows are ever eligible -- INFLIGHT means a sender is mid
    delivery on it, and DEAD messages just sit there taking up space
    for now, not reclaimable by DropOldest.
    """

    def __init__(self, conn: sqlite3.Connection, keep: Keep) -> None:
        self._conn = conn
        self._keep = keep

    def over_bound(self) -> bool:
        conn = self._conn
        (total_bytes,) = conn.execute(
            "SELECT COALESCE(SUM(bytes_used), 0) FROM keep_sources"
        ).fetchone()
        (total_messages,) = conn.execute(
            "SELECT value FROM keep_meta WHERE key = ?", (_MESSAGE_COUNT_KEY,)
        ).fetchone()
        return total_bytes > self._keep.max_bytes or int(total_messages) > self._keep.max_messages

    def evict_oldest_pending(self) -> EvictedMessage | None:
        conn = self._conn
        row = conn.execute(
            """
            DELETE FROM keep_messages
            WHERE id = (
                SELECT id FROM keep_messages WHERE state = ? ORDER BY id LIMIT 1
            )
            RETURNING id, idempotency_key, source_id, seq, topic, LENGTH(payload)
            """,
            (STATE_PENDING,),
        ).fetchone()
        if row is None:
            return None
        id_, idempotency_key, source_id, seq, topic, payload_size = row
        conn.execute(
            "UPDATE keep_sources SET bytes_used = bytes_used - ? WHERE source_id = ?",
            (payload_size, source_id),
        )
        _bump_message_count(conn, -1)
        evicted = EvictedMessage(
            id=id_,
            idempotency_key=idempotency_key,
            source_id=source_id,
            seq=seq,
            topic=topic,
            payload_size=payload_size,
        )
        self._keep._batch_evictions.append(evicted)
        return evicted


@dataclass(frozen=True)
class Metrics:
    pending_messages: int
    inflight_messages: int
    dead_messages: int
    keep_bytes_used: int
    published_total: int
    acked_total: int
    retried_total: int
    evicted_total: int
    oldest_pending_age_seconds: float | None


class Keep:
    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        commit_window: float = DEFAULT_COMMIT_WINDOW,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_messages: int = DEFAULT_MAX_MESSAGES,
        eviction: EvictionPolicy | None = None,
        on_evict: Callable[[EvictedMessage], None] | None = None,
        transforms: Sequence[Transform] = (),
        transform_timeout: float = DEFAULT_TRANSFORM_TIMEOUT,
    ) -> None:
        self.path = path
        self.commit_window = commit_window
        self.max_bytes = max_bytes
        self.max_messages = max_messages
        self.eviction = eviction if eviction is not None else DropOldest()
        self.transforms = list(transforms)
        self.transform_timeout = transform_timeout
        self.on_evict = on_evict
        self._conn: sqlite3.Connection | None = None
        self._queue: asyncio.Queue[object] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._published_total = 0
        self._acked_total = 0
        self._retried_total = 0
        self._evicted_total = 0
        # collects evictions produced while applying the batch currently
        # being committed -- only acted on (counter/log/callback) once
        # COMMIT actually succeeds, never while still inside the txn
        self._batch_evictions: list[EvictedMessage] = []

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
            if version is None or version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"keep at {self.path!r} is on schema_version {version!r}, "
                    f"this build only knows up to schema_version {SCHEMA_VERSION}"
                )
            if version < SCHEMA_VERSION:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for target_version in range(version + 1, SCHEMA_VERSION + 1):
                        for statement in _MIGRATIONS[target_version]:
                            conn.execute(statement)
                    conn.execute(
                        "UPDATE keep_meta SET value = ? WHERE key = 'schema_version'",
                        (str(SCHEMA_VERSION),),
                    )
                except BaseException:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")

        # a crash mid-send leaves rows claimed but never ack'd or requeued;
        # put them back before publish() or anything else can touch the table
        conn.execute(
            "UPDATE keep_messages SET state = ? WHERE state = ?",
            (STATE_PENDING, STATE_INFLIGHT),
        )

        # backfill the counter if it's missing -- fresh db or an old one
        # from before this existed, doesn't matter, both start from a
        # real count and stay in sync from here on
        count_row = conn.execute(
            "SELECT 1 FROM keep_meta WHERE key = ?", (_MESSAGE_COUNT_KEY,)
        ).fetchone()
        if count_row is None:
            (existing_count,) = conn.execute("SELECT COUNT(*) FROM keep_messages").fetchone()
            conn.execute(
                "INSERT INTO keep_meta (key, value) VALUES (?, ?)",
                (_MESSAGE_COUNT_KEY, str(existing_count)),
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
    ) -> list[int]:
        """Queue a message for durable delivery and return the seq(s) it
        landed at.

        Usually a list of one, but a transform can turn this into zero
        (dropped) or several (fan-out) -- there's no honest single int
        that covers that range, so this always returns a list.

        Returns once everything's committed locally, never once it's
        sent. Cancelling the await after the message is enqueued doesn't
        pull it back out - once queued, whether and when it gets
        committed is the writer's call, not the caller's.
        """
        if self._queue is None:
            raise RuntimeError("Keep is not open")
        future: asyncio.Future[list[int]] = asyncio.get_running_loop().create_future()
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

    def plugin_storage(self, namespace: str) -> PluginStorage:
        """A namespaced key/value handle for a plugin's own state, backed
        by the same connection as everything else here. Wire it into a
        Transform however that plugin wants (constructor arg, attribute,
        whatever) -- just only actually call get()/set() on it from
        inside that transform's on_ingest().
        """
        return PluginStorage(self, namespace)

    async def metrics(self) -> Metrics:
        """A cheap read-only snapshot for health checks and alerting.

        published_total is an in-memory counter for this process, not a
        table scan — it resets if the process restarts, same as any other
        in-memory counter would.
        """
        if self._conn is None:
            raise RuntimeError("Keep is not open")
        conn = self._conn

        pending, inflight, dead, oldest_created_at = conn.execute(
            """
            SELECT
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END),
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END),
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END),
                MIN(CASE WHEN state = ? THEN created_at END)
            FROM keep_messages
            """,
            (STATE_PENDING, STATE_INFLIGHT, STATE_DEAD, STATE_PENDING),
        ).fetchone()

        # same query over_bound() uses -- used to scan keep_messages for
        # this instead, now it's one number both places agree on
        (bytes_used,) = conn.execute(
            "SELECT COALESCE(SUM(bytes_used), 0) FROM keep_sources"
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
            evicted_total=self._evicted_total,
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

                await self._commit_batch(batch)

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

    async def _commit_batch(self, batch: list[_QueuedItem]) -> None:
        conn = self._conn
        assert conn is not None

        # reset before applying -- evictions produced while working through
        # this batch aren't real until COMMIT succeeds, so nothing here
        # should act on them yet
        self._batch_evictions = []

        # transforms can await, so this loop isn't atomic in the "never
        # yields" sense the rest of the writer relies on elsewhere -- but
        # it's still one BEGIN...COMMIT, and each item gets its own
        # SAVEPOINT so a transform rejecting its own publish doesn't take
        # down whatever else happened to land in this batch. a real
        # sqlite3.Error is different -- that means the connection itself
        # is in trouble, so it aborts everything rather than just one item.
        #
        # one side effect worth knowing: since this now has await points
        # mid-transaction, something like metrics() could in principle
        # run concurrently and read this transaction's uncommitted state.
        # not a correctness bug (nothing's corrupted, the eventual commit
        # is still atomic) but a possible stale/dirty read if you call
        # metrics() while a slow transform is stuck mid-publish.
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as exc:
            for item in batch:
                if not item.future.done():
                    item.future.set_exception(exc)
            return

        results: list[object] = []
        try:
            for index, item in enumerate(batch):
                conn.execute(f"SAVEPOINT item_{index}")
                try:
                    result = await self._apply(conn, item)
                except Exception as exc:
                    if isinstance(exc, sqlite3.Error):
                        raise
                    conn.execute(f"ROLLBACK TO item_{index}")
                    conn.execute(f"RELEASE item_{index}")
                    results.append(exc)
                else:
                    conn.execute(f"RELEASE item_{index}")
                    results.append(result)
            conn.execute("COMMIT")
        except BaseException as exc:
            try:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass  # connection's already broken, nothing left to roll back
            self._batch_evictions = []  # rolled back -- these never happened
            for item in batch:
                # caller may have cancelled its own await while this batch
                # was in flight - don't try to resolve a future twice
                if not item.future.done():
                    item.future.set_exception(exc)
            return

        succeeded = [not isinstance(result, BaseException) for result in results]
        self._published_total += sum(
            1
            for item, ok in zip(batch, succeeded)
            if ok and isinstance(item, _QueuedPublish)
        )
        self._acked_total += sum(
            1 for item, ok in zip(batch, succeeded) if ok and isinstance(item, _QueuedAck)
        )
        self._retried_total += sum(
            1 for item, ok in zip(batch, succeeded) if ok and isinstance(item, _QueuedRetry)
        )

        # only now, after COMMIT, are these evictions real -- counter, log,
        # and callback each fire exactly once per evicted message
        for evicted in self._batch_evictions:
            self._evicted_total += 1
            _logger.info(
                "evicted message id=%s source_id=%s seq=%s topic=%s payload_size=%s",
                evicted.id, evicted.source_id, evicted.seq, evicted.topic, evicted.payload_size,
            )
            if self.on_evict is not None:
                try:
                    self.on_evict(evicted)
                except Exception:
                    # a caller's callback misbehaving shouldn't be able to
                    # take the writer task down with it
                    _logger.exception(
                        "on_evict callback raised for evicted message id=%s", evicted.id
                    )
        self._batch_evictions = []

        for item, result in zip(batch, results):
            if item.future.done():
                continue
            if isinstance(result, BaseException):
                item.future.set_exception(result)
            else:
                item.future.set_result(result)

    async def _apply(self, conn: sqlite3.Connection, item: _QueuedItem) -> object:
        if isinstance(item, _QueuedPublish):
            return await self._apply_publish(conn, item)
        if isinstance(item, _QueuedClaim):
            return self._apply_claim(conn)
        if isinstance(item, _QueuedAck):
            return self._apply_ack(conn, item)
        if isinstance(item, _QueuedRetry):
            return self._apply_retry(conn, item)
        if isinstance(item, _QueuedDead):
            return self._apply_dead(conn, item)
        raise AssertionError(f"unhandled queued item: {item!r}")

    async def _apply_publish(self, conn: sqlite3.Connection, item: _QueuedPublish) -> list[int]:
        drafts: list[Draft] = [
            Draft(
                topic=item.topic,
                payload=item.payload,
                source_id=item.source_id,
                content_type=item.content_type,
            )
        ]

        for transform in self.transforms:
            if not drafts:
                break  # already dropped -- nothing left for later transforms
            next_drafts: list[Draft] = []
            for draft in drafts:
                try:
                    result = await asyncio.wait_for(
                        transform.on_ingest(draft), timeout=self.transform_timeout
                    )
                except TimeoutError as exc:
                    raise TransformTimeoutError(
                        f"{transform!r}.on_ingest() did not return within "
                        f"{self.transform_timeout}s"
                    ) from exc
                if result is not None:
                    next_drafts.extend(result)
            drafts = next_drafts

        seqs = [self._insert_draft(conn, draft) for draft in drafts]

        if seqs:
            # bound enforcement happens right here, inside the same txn as
            # the inserts above -- there's never a window where a reader
            # could see the keep sitting over its bound
            ctx = _EvictionContextImpl(conn, self)
            self.eviction.enforce(ctx)

        return seqs

    def _insert_draft(self, conn: sqlite3.Connection, draft: Draft) -> int:
        idempotency_key = uuid7_bytes()
        created_at = time.time_ns() // 1_000_000
        conn.execute(
            "INSERT INTO keep_sources (source_id, next_seq) VALUES (?, 1) "
            "ON CONFLICT (source_id) DO NOTHING",
            (draft.source_id,),
        )
        (seq,) = conn.execute(
            "SELECT next_seq FROM keep_sources WHERE source_id = ?",
            (draft.source_id,),
        ).fetchone()
        conn.execute(
            "UPDATE keep_sources SET next_seq = next_seq + 1, bytes_used = bytes_used + ? "
            "WHERE source_id = ?",
            (len(draft.payload), draft.source_id),
        )
        conn.execute(
            "INSERT INTO keep_messages "
            "(idempotency_key, source_id, seq, topic, payload, content_type, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                idempotency_key,
                draft.source_id,
                seq,
                draft.topic,
                draft.payload,
                draft.content_type,
                created_at,
            ),
        )
        _bump_message_count(conn, 1)
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
        # this deletes a row too, so bytes_used/message_count need to
        # come down here just like they do on eviction
        row = conn.execute(
            "DELETE FROM keep_messages WHERE id = ? RETURNING source_id, LENGTH(payload)",
            (item.message_id,),
        ).fetchone()
        if row is not None:
            source_id, payload_size = row
            conn.execute(
                "UPDATE keep_sources SET bytes_used = bytes_used - ? WHERE source_id = ?",
                (payload_size, source_id),
            )
            _bump_message_count(conn, -1)
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
