"""Sender state-machine tests against a fake transport - deterministic,
no broker required. The sender doesn't claim or publish anything yet, so
everything here is red until it does.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import time
from pathlib import Path

from edgekeep import Keep
from edgekeep.keep import STATE_DEAD
from edgekeep.sender import Sender
from edgekeep.transport import PermanentError, TransportError
from _fake_transport import FakeTransport


async def _run_sender_briefly(sender: Sender, seconds: float) -> None:
    task = asyncio.create_task(sender.run_forever())
    await asyncio.sleep(seconds)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_pending_message_gets_acked_and_row_deleted(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    transport = FakeTransport()

    async with Keep(db_path) as keep:
        await keep.publish(topic="site-01/telemetry/s", payload=b"hello", source_id="s")
        await _run_sender_briefly(Sender(keep, transport), 0.5)
        metrics = await keep.metrics()

    assert [p.payload for p in transport.published] == [b"hello"]
    assert metrics.pending_messages == 0
    assert metrics.acked_total == 1

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM keep_messages").fetchone()[0] == 0


async def test_fifo_delivery_order_per_source(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    transport = FakeTransport()

    async with Keep(db_path) as keep:
        for i in range(5):
            await keep.publish(topic="t", payload=str(i).encode(), source_id="s")
        await _run_sender_briefly(Sender(keep, transport), 0.5)

    delivered = [p.payload for p in transport.published]
    assert delivered == [str(i).encode() for i in range(5)]


async def test_transient_failure_retries_with_backoff(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    transport = FakeTransport()
    transport.fail_next(TransportError("broker unreachable"), times=2)

    async with Keep(db_path) as keep:
        await keep.publish(topic="t", payload=b"x", source_id="s")
        await _run_sender_briefly(Sender(keep, transport, base_backoff=0.05, cap_backoff=0.2), 2)
        metrics = await keep.metrics()

    assert [p.payload for p in transport.published] == [b"x"]
    assert metrics.retried_total == 2
    assert metrics.acked_total == 1
    assert metrics.pending_messages == 0


async def test_permanent_error_moves_to_dead_after_max_attempts(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    transport = FakeTransport()
    max_attempts = 3
    transport.fail_next(PermanentError("payload too large"), times=max_attempts)

    async with Keep(db_path) as keep:
        await keep.publish(topic="t", payload=b"x", source_id="s")
        await _run_sender_briefly(
            Sender(keep, transport, max_attempts=max_attempts, base_backoff=0.02, cap_backoff=0.05),
            2,
        )
        metrics = await keep.metrics()

    assert metrics.dead_messages == 1
    assert metrics.pending_messages == 0
    assert metrics.acked_total == 0

    conn = sqlite3.connect(db_path)
    state = conn.execute("SELECT state FROM keep_messages").fetchone()[0]
    assert state == STATE_DEAD


async def test_replay_is_rate_limited(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    transport = FakeTransport()
    rate = 20  # msg/s, deliberately low so the test doesn't take forever

    async with Keep(db_path) as keep:
        for i in range(rate * 2):
            await keep.publish(topic="t", payload=str(i).encode(), source_id="s")

        start = time.monotonic()
        await _run_sender_briefly(Sender(keep, transport, replay_rate_limit=rate), 3)
        elapsed = time.monotonic() - start

    # draining 2x the per-second rate should take at least ~1s, not be instant
    assert len(transport.published) == rate * 2
    assert elapsed >= 1.0


async def test_max_inflight_bounds_concurrency(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    transport = FakeTransport()
    transport.delay = 0.2
    max_inflight = 4
    concurrent = 0
    peak_concurrent = 0
    original_publish = transport.publish

    async def tracking_publish(*, topic: str, payload: bytes, qos: int) -> None:
        nonlocal concurrent, peak_concurrent
        concurrent += 1
        peak_concurrent = max(peak_concurrent, concurrent)
        try:
            await original_publish(topic=topic, payload=payload, qos=qos)
        finally:
            concurrent -= 1

    transport.publish = tracking_publish  # type: ignore[method-assign]

    async with Keep(db_path) as keep:
        for i in range(20):
            await keep.publish(topic="t", payload=str(i).encode(), source_id=f"s{i % 5}")
        await _run_sender_briefly(Sender(keep, transport, max_inflight=max_inflight), 2)

    assert peak_concurrent <= max_inflight
    assert peak_concurrent > 1  # actually exercised concurrency, not accidentally serial
