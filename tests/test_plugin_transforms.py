"""Ingest-side transform pipeline (M4): transforms run inside publish(),
before the outbox commit, and can mutate, drop, fan out, or reject a
message. Draft/Transform/PluginStorage exist and Keep accepts
transforms=/transform_timeout=, but _apply_publish doesn't call into any
of it yet, so everything here is red until it does.

publish() now returns list[int] instead of a bare int -- zero seqs for a
drop, one for the normal case, N for a fan-out. There's no honest way to
represent that range as a single int, so every existing call site that
does `seq = await keep.publish(...)` needs updating to `[seq] = ...` (or
similar) as part of wiring this up, in the same change that makes these
tests pass.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path

import pytest

from edgekeep import Draft, Keep
from edgekeep.transform import TransformTimeoutError


class _UppercaseTransform:
    async def on_ingest(self, draft: Draft) -> list[Draft]:
        draft.payload = draft.payload.upper()
        return [draft]


class _AppendSuffixTransform:
    def __init__(self, suffix: bytes) -> None:
        self._suffix = suffix

    async def on_ingest(self, draft: Draft) -> list[Draft]:
        draft.payload = draft.payload + self._suffix
        return [draft]


class _DropTransform:
    """Drops one specific payload, passes everything else through --
    conditional so a test can mix a dropped publish in with ones that
    should still land normally.
    """

    def __init__(self, drop_payload: bytes) -> None:
        self._drop_payload = drop_payload

    async def on_ingest(self, draft: Draft) -> list[Draft] | None:
        if draft.payload == self._drop_payload:
            return None
        return [draft]


class _FanOutTransform:
    async def on_ingest(self, draft: Draft) -> list[Draft]:
        return [
            Draft(topic=draft.topic, payload=draft.payload + b"-1", source_id=draft.source_id),
            Draft(topic=draft.topic, payload=draft.payload + b"-2", source_id=draft.source_id),
        ]


class _RejectingTransform:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def on_ingest(self, draft: Draft) -> None:
        raise self._exc


class _ConditionalRejectTransform:
    """Raises only for one specific payload, passes everything else
    straight through -- lets a test mix a failing publish in with ones
    that should succeed untouched.
    """

    def __init__(self, bad_payload: bytes, exc: Exception) -> None:
        self._bad_payload = bad_payload
        self._exc = exc

    async def on_ingest(self, draft: Draft) -> list[Draft]:
        if draft.payload == self._bad_payload:
            raise self._exc
        return [draft]


class _HangingTransform:
    """Hangs forever for one specific payload, passes everything else
    straight through -- for exercising transform_timeout in isolation.
    """

    def __init__(self, hang_payload: bytes) -> None:
        self._hang_payload = hang_payload

    async def on_ingest(self, draft: Draft) -> list[Draft]:
        if draft.payload == self._hang_payload:
            await asyncio.sleep(999)
        return [draft]


class _StorageWriteTransform:
    def __init__(self, keep: Keep, namespace: str) -> None:
        self._storage = keep.plugin_storage(namespace)

    async def on_ingest(self, draft: Draft) -> list[Draft]:
        self._storage.set("last_seen", draft.payload)
        return [draft]


class _StorageWriteThenRejectTransform:
    def __init__(self, keep: Keep, namespace: str) -> None:
        self._storage = keep.plugin_storage(namespace)

    async def on_ingest(self, draft: Draft) -> None:
        self._storage.set("partial", b"should never be visible")
        raise ValueError("rejected after a partial storage write")


async def test_transform_can_mutate_the_draft(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path, transforms=[_UppercaseTransform()]) as keep:
        await keep.publish(topic="t", payload=b"hello", source_id="s")

    conn = sqlite3.connect(db_path)
    (payload,) = conn.execute("SELECT payload FROM keep_messages").fetchone()
    conn.close()
    assert payload == b"HELLO"


async def test_transform_returning_none_drops_the_message_and_consumes_no_seq(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path, transforms=[_DropTransform(drop_payload=b"dropped")]) as keep:
        seqs = await keep.publish(topic="t", payload=b"dropped", source_id="s")
        assert seqs == []

        # the dropped publish never happened as far as seq allocation
        # goes -- the next real publish gets seq 1, not seq 2
        next_seqs = await keep.publish(topic="t", payload=b"kept", source_id="s")

    assert next_seqs == [1]
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT payload FROM keep_messages").fetchall()
    conn.close()
    assert rows == [(b"kept",)]


async def test_transform_can_fan_out_into_multiple_messages(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path, transforms=[_FanOutTransform()]) as keep:
        seqs = await keep.publish(topic="t", payload=b"reading", source_id="s")

    assert seqs == [1, 2]
    conn = sqlite3.connect(db_path)
    payloads = {p for (p,) in conn.execute("SELECT payload FROM keep_messages")}
    conn.close()
    assert payloads == {b"reading-1", b"reading-2"}


async def test_transforms_run_in_order(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    transforms = [_AppendSuffixTransform(b"-a"), _AppendSuffixTransform(b"-b")]
    async with Keep(db_path, transforms=transforms) as keep:
        await keep.publish(topic="t", payload=b"x", source_id="s")

    conn = sqlite3.connect(db_path)
    (payload,) = conn.execute("SELECT payload FROM keep_messages").fetchone()
    conn.close()
    assert payload == b"x-a-b"


async def test_transform_raising_rejects_the_publish_and_leaves_keep_unchanged(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path, transforms=[_RejectingTransform(ValueError("nope"))]) as keep:
        with pytest.raises(ValueError, match="nope"):
            await keep.publish(topic="t", payload=b"x", source_id="s")
        metrics = await keep.metrics()

    assert metrics.pending_messages == 0
    assert metrics.published_total == 0


async def test_a_failing_transform_only_fails_its_own_publish(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    transform = _ConditionalRejectTransform(b"boom", ValueError("boom"))
    async with Keep(db_path, transforms=[transform]) as keep:
        # fired concurrently rather than awaited one at a time so they
        # land in the same commit-window batch -- that's the scenario
        # where "one bad item poisons the batch" would show up
        results = await asyncio.gather(
            keep.publish(topic="t", payload=b"ok-1", source_id="s1"),
            keep.publish(topic="t", payload=b"boom", source_id="s2"),
            keep.publish(topic="t", payload=b"ok-2", source_id="s3"),
            return_exceptions=True,
        )

    ok_1, boom, ok_2 = results
    assert ok_1 == [1]
    assert isinstance(boom, ValueError)
    assert ok_2 == [1]  # s3's own first seq -- unaffected by s2's rejection

    conn = sqlite3.connect(db_path)
    payloads = {p for (p,) in conn.execute("SELECT payload FROM keep_messages")}
    conn.close()
    assert payloads == {b"ok-1", b"ok-2"}


async def test_transform_timeout_rejects_that_publish_without_wedging_others(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "keep.db"
    transform = _HangingTransform(hang_payload=b"hang")
    async with Keep(db_path, transforms=[transform], transform_timeout=0.2) as keep:
        start = time.monotonic()
        results = await asyncio.gather(
            keep.publish(topic="t", payload=b"hang", source_id="s1"),
            keep.publish(topic="t", payload=b"fine", source_id="s2"),
            return_exceptions=True,
        )
        elapsed = time.monotonic() - start

    hang_result, fine_result = results
    assert isinstance(hang_result, TransformTimeoutError)
    assert fine_result == [1]
    # the writer is single-threaded, so "fine" genuinely had to wait
    # behind "hang" hitting its timeout -- this is the stall the timeout
    # bounds, not one it eliminates
    assert elapsed >= 0.2
    assert elapsed < 5.0  # bounded, not actually hung for the test's sake


async def test_plugin_storage_persists_atomically_with_its_message(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    keep = Keep(db_path)
    keep.transforms.append(_StorageWriteTransform(keep, "ns"))
    async with keep:
        await keep.publish(topic="t", payload=b"reading-1", source_id="s")

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT value FROM plugin_storage WHERE namespace = 'ns' AND key = 'last_seen'"
    ).fetchone()
    message_exists = conn.execute("SELECT 1 FROM keep_messages").fetchone() is not None
    conn.close()

    assert row is not None and row[0] == b"reading-1"
    assert message_exists


async def test_plugin_storage_write_rolls_back_with_its_rejected_publish(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "keep.db"
    keep = Keep(db_path)
    keep.transforms.append(_StorageWriteThenRejectTransform(keep, "ns"))
    async with keep:
        with pytest.raises(ValueError):
            await keep.publish(topic="t", payload=b"x", source_id="s")

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT 1 FROM plugin_storage WHERE namespace = 'ns' AND key = 'partial'"
    ).fetchone()
    conn.close()
    assert row is None  # rolled back along with the rejected publish


async def test_plugin_storage_used_outside_on_ingest_is_refused(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path) as keep:
        storage = keep.plugin_storage("ns")
        with pytest.raises(RuntimeError, match="on_ingest"):
            storage.get("anything")


async def test_opening_a_v1_db_migrates_it_to_v2_without_touching_existing_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "keep.db"

    # hand-build exactly what a pre-plugin-storage (schema_version 1) keep
    # looked like -- no plugin_storage table, and one real message in it
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute(
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
        """
    )
    conn.execute(
        "CREATE TABLE keep_sources (source_id TEXT PRIMARY KEY, "
        "next_seq INTEGER NOT NULL DEFAULT 1, bytes_used INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute("CREATE TABLE keep_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO keep_meta (key, value) VALUES ('schema_version', '1')")
    conn.execute(
        "INSERT INTO keep_messages "
        "(idempotency_key, source_id, seq, topic, payload, content_type, created_at) "
        "VALUES (?, 's', 1, 't', ?, NULL, 0)",
        (b"0" * 16, b"pre-existing"),
    )
    conn.execute("INSERT INTO keep_sources (source_id, next_seq) VALUES ('s', 2)")
    conn.commit()
    conn.close()

    # opening with the current code should migrate forward, not blow up
    # with "no migration yet" and not touch the row that predates this
    async with Keep(db_path) as keep:
        metrics = await keep.metrics()

    assert metrics.pending_messages == 1

    conn = sqlite3.connect(db_path)
    version = conn.execute(
        "SELECT value FROM keep_meta WHERE key = 'schema_version'"
    ).fetchone()[0]
    payload = conn.execute("SELECT payload FROM keep_messages").fetchone()[0]
    conn.close()

    assert int(version) == 2
    assert payload == b"pre-existing"
