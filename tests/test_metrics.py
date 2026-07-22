"""Metrics snapshot: cheap counts a host app can poll for health checks
and alerting. No sender exists yet, so acked/dead transitions are faked
here with a direct SQL write standing in for what the sender will do.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from edgekeep import Keep
from edgekeep.keep import STATE_DEAD


async def test_metrics_on_empty_keep(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path) as keep:
        metrics = await keep.metrics()

    assert metrics.pending_messages == 0
    assert metrics.inflight_messages == 0
    assert metrics.dead_messages == 0
    assert metrics.keep_bytes_used == 0
    assert metrics.published_total == 0
    assert metrics.oldest_pending_age_seconds is None


async def test_metrics_after_publish(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    payloads = [b"a", b"bb", b"ccc"]

    async with Keep(db_path) as keep:
        for payload in payloads:
            await keep.publish(topic="t", payload=payload, source_id="s")

        metrics = await keep.metrics()

    assert metrics.pending_messages == len(payloads)
    assert metrics.dead_messages == 0
    assert metrics.keep_bytes_used == sum(len(p) for p in payloads)
    assert metrics.published_total == len(payloads)
    assert metrics.oldest_pending_age_seconds is not None
    assert metrics.oldest_pending_age_seconds >= 0


def _mark_acked(db_path: Path, source_id: str, seq: int) -> None:
    # ACKED rows are deleted, not flagged -- this is what a real sender
    # will do once the broker PUBACKs a message
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "DELETE FROM keep_messages WHERE source_id = ? AND seq = ?",
            (source_id, seq),
        )
        conn.commit()
    finally:
        conn.close()


def _mark_dead(db_path: Path, source_id: str, seq: int) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE keep_messages SET state = ? WHERE source_id = ? AND seq = ?",
            (STATE_DEAD, source_id, seq),
        )
        conn.commit()
    finally:
        conn.close()


async def test_metrics_after_simulated_ack_and_dead(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"

    async with Keep(db_path) as keep:
        await keep.publish(topic="t", payload=b"one", source_id="s")
        await keep.publish(topic="t", payload=b"two", source_id="s")
        await keep.publish(topic="t", payload=b"three", source_id="s")

    _mark_acked(db_path, "s", 1)
    _mark_dead(db_path, "s", 2)

    async with Keep(db_path) as keep:
        metrics = await keep.metrics()

    assert metrics.pending_messages == 1  # only seq 3 is left pending
    assert metrics.dead_messages == 1  # seq 2
    assert metrics.keep_bytes_used == len(b"two") + len(b"three")  # seq 1 deleted
    assert metrics.published_total == 0  # fresh process -- in-memory counter, not persisted
    assert metrics.oldest_pending_age_seconds is not None
    assert metrics.oldest_pending_age_seconds >= 0
