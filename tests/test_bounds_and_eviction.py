"""Bounded storage & eviction (M3): max_bytes and max_messages, whichever
hits first. DropOldest (default) evicts oldest PENDING rows to make room;
DropNewest rejects the publish that would push the keep over instead.

Keep accepts the new constructor parameters but doesn't enforce them yet,
and keep_sources.bytes_used still isn't maintained -- so every assertion
below is red until bound enforcement, byte accounting, and the two
policies are actually wired into the writer's publish path.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest

from edgekeep import Keep
from edgekeep.eviction import DropNewest, DropOldest, EvictedMessage, KeepFullError

STATE_DEAD = 2


async def _publish_n(keep: Keep, source_id: str, n: int, payload_size: int = 10) -> list[int]:
    return [
        await keep.publish(topic="t", payload=bytes(payload_size), source_id=source_id)
        for _ in range(n)
    ]


def _seqs(db_path: Path, source_id: str = "s") -> list[int]:
    conn = sqlite3.connect(db_path)
    try:
        return sorted(
            seq
            for (seq,) in conn.execute(
                "SELECT seq FROM keep_messages WHERE source_id = ?", (source_id,)
            )
        )
    finally:
        conn.close()


async def test_max_messages_bound_evicts_oldest_first(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path, max_messages=5, eviction=DropOldest()) as keep:
        await _publish_n(keep, "s", 8)
        metrics = await keep.metrics()

    assert metrics.pending_messages <= 5
    # oldest 3 (seq 1-3) evicted; newest 5 remain
    assert _seqs(db_path) == [4, 5, 6, 7, 8]


async def test_max_bytes_bound_evicts_oldest_first(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    payload_size = 100
    async with Keep(db_path, max_bytes=payload_size * 3, eviction=DropOldest()) as keep:
        await _publish_n(keep, "s", 5, payload_size=payload_size)
        metrics = await keep.metrics()

    assert metrics.keep_bytes_used <= payload_size * 3
    assert _seqs(db_path) == [3, 4, 5]


async def test_drop_oldest_preserves_seq_gap(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path, max_messages=3, eviction=DropOldest()) as keep:
        await _publish_n(keep, "s", 5)  # seq 1, 2 evicted along the way
        new_seq = await keep.publish(topic="t", payload=b"x", source_id="s")

    # next_seq keeps counting past what got evicted instead of being
    # rewound or compacted -- the gap where 1 and 2 used to be is exactly
    # what tells a downstream consumer those two are gone for good
    assert new_seq == 6
    assert _seqs(db_path) == [4, 5, 6]


async def test_drop_newest_rejects_and_leaves_keep_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path, max_messages=3, eviction=DropNewest()) as keep:
        await _publish_n(keep, "s", 3)

        conn = sqlite3.connect(db_path)
        before_rows = conn.execute("SELECT id, seq FROM keep_messages ORDER BY id").fetchall()
        before_next_seq = conn.execute(
            "SELECT next_seq FROM keep_sources WHERE source_id = 's'"
        ).fetchone()[0]
        conn.close()

        with pytest.raises(KeepFullError):
            await keep.publish(topic="t", payload=b"x", source_id="s")

        metrics = await keep.metrics()

    assert metrics.pending_messages == 3

    conn = sqlite3.connect(db_path)
    after_rows = conn.execute("SELECT id, seq FROM keep_messages ORDER BY id").fetchall()
    after_next_seq = conn.execute(
        "SELECT next_seq FROM keep_sources WHERE source_id = 's'"
    ).fetchone()[0]
    conn.close()

    # rejected means rejected -- not even the sequence counter should
    # have moved, since the whole transaction (insert + next_seq bump)
    # rolled back together
    assert after_rows == before_rows
    assert after_next_seq == before_next_seq


async def test_eviction_increments_counter_fires_callback_and_logs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "keep.db"
    evicted: list[EvictedMessage] = []

    with caplog.at_level(logging.INFO):
        async with Keep(
            db_path, max_messages=3, eviction=DropOldest(), on_evict=evicted.append
        ) as keep:
            await _publish_n(keep, "s", 5)
            metrics = await keep.metrics()

    assert metrics.evicted_total == 2
    assert [e.seq for e in evicted] == [1, 2]
    assert any("evict" in record.message.lower() for record in caplog.records)


async def test_inflight_row_survives_eviction_pressure(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path, max_messages=3, eviction=DropOldest()) as keep:
        await _publish_n(keep, "s", 3)  # seq 1, 2, 3 -- all PENDING

        claimed = await keep._claim_next()  # seq 1 goes INFLIGHT
        assert claimed is not None
        assert claimed.seq == 1

        # keep publishing well past the bound -- if DropOldest ever
        # reached for the INFLIGHT row instead of stopping at PENDING,
        # the message a sender is mid-delivery on would vanish under it
        await _publish_n(keep, "s", 5)
        metrics = await keep.metrics()

    assert metrics.inflight_messages == 1
    # seq 1 (INFLIGHT) survives untouched; PENDING rows get evicted down
    # to the bound around it
    assert _seqs(db_path) == [1, 7, 8]


async def test_dead_messages_count_toward_the_bound(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    async with Keep(db_path, max_messages=3, eviction=DropOldest()) as keep:
        await _publish_n(keep, "s", 3)  # seq 1, 2, 3 -- all PENDING

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE keep_messages SET state = ? WHERE seq IN (1, 2)", (STATE_DEAD,))
        conn.commit()
        conn.close()
        # seq 1, 2 are DEAD; seq 3 is the only PENDING row -- already at
        # max_messages=3 even though only one message is actually pending

        await keep.publish(topic="t", payload=b"x", source_id="s")  # seq 4
        metrics = await keep.metrics()

    # DropOldest only evicts PENDING rows, so the two DEAD rows are never
    # touched -- the only evictable row (seq 3) has to go to make room for
    # them, proving DEAD rows count toward the bound rather than being free
    assert metrics.dead_messages == 2
    assert metrics.pending_messages == 1
    assert _seqs(db_path) == [1, 2, 4]
