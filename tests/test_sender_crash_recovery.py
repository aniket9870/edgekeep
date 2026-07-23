"""New crash surface once a sender exists: SIGKILL between claiming a row
(INFLIGHT) and getting the broker's ack. A confirmed message must survive
that - either it's still sitting in the keep waiting to be resent, or it
already made it to the "broker" (a duplicate is fine, detectable via its
idempotency_key) - but it must never just vanish.

The sender doesn't claim or publish anything yet, so the liveness check
at the bottom (everything eventually gets delivered) is red until it does.
The safety check above it (nothing vanishes) should already hold, since a
no-op sender can't lose anything it never touches.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import re
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from edgekeep import Keep
from edgekeep.sender import Sender
from _fake_transport import DurableFakeTransport

WORKER = Path(__file__).parent / "_crash_worker_sender.py"
LINE_RE = re.compile(r"^(?P<source_id>[^\t]+)\t(?P<seq>\d+)\t(?P<i>\d+)$")

# single run for now while this is still red, once the sender is real
# and this passes quickly, bump to N-iterations like the M1 harness


def _confirmed(stdout: str) -> list[tuple[str, int, int]]:
    out = []
    for line in stdout.splitlines():
        match = LINE_RE.match(line)
        if match is None:
            continue
        out.append((match["source_id"], int(match["seq"]), int(match["i"])))
    return out


async def _drain_with_a_live_sender(db_path: Path, ack_log_path: Path, timeout: float) -> None:
    transport = DurableFakeTransport(str(ack_log_path))
    async with Keep(db_path) as keep:
        sender_task = asyncio.create_task(Sender(keep, transport).run_forever())
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            metrics = await keep.metrics()
            if metrics.pending_messages == 0 and metrics.inflight_messages == 0:
                break
            await asyncio.sleep(0.05)
        sender_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sender_task


async def test_sigkill_between_claim_and_ack_never_loses_a_message(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    ack_log_path = tmp_path / "broker_acks.log"

    proc = subprocess.Popen(
        [sys.executable, str(WORKER), str(db_path), str(ack_log_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(random.uniform(0.05, 0.5))
    proc.send_signal(signal.SIGKILL)
    stdout, _stderr = proc.communicate(timeout=5)

    confirmed = _confirmed(stdout)

    # recovery, then give a real sender a chance to finish delivering
    # whatever the crash left behind
    await _drain_with_a_live_sender(db_path, ack_log_path, timeout=5)

    acked_payloads: set[int] = set()
    if ack_log_path.exists():
        acked_payloads = {int(line) for line in ack_log_path.read_text().splitlines() if line}

    conn = sqlite3.connect(db_path)
    try:
        remaining = set(
            conn.execute("SELECT source_id, seq FROM keep_messages").fetchall()
        )

        for source_id, seq, i in confirmed:
            assert i in acked_payloads or (source_id, seq) in remaining, (
                f"{source_id} seq {seq} (payload {i}) vanished: never acked "
                "and not left behind in keep_messages either"
            )

        # the property that actually needs a real sender: nothing should
        # still be stuck once it's had a chance to run after recovery
        assert not remaining, (
            f"{len(remaining)} row(s) never got delivered even with a live "
            "sender running after recovery"
        )
    finally:
        conn.close()
