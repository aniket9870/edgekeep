"""Crash surface added by the sender: SIGKILL between claiming a row
(INFLIGHT) and getting the broker's ack. A confirmed message must survive
that - either it's still sitting in the keep waiting to be resent, or it
already made it to the "broker" (a duplicate is fine, detectable via its
idempotency_key) - but it must never just vanish. And once a live sender
gets a chance to run after recovery, nothing should still be stuck.
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
import threading
import time
from pathlib import Path

import pytest

from edgekeep import Keep
from edgekeep.sender import Sender
from _fake_transport import DurableFakeTransport

WORKER = Path(__file__).parent / "_crash_worker_sender.py"
LINE_RE = re.compile(r"^(?P<source_id>[^\t]+)\t(?P<seq>\d+)\t(?P<i>\d+)$")

N_ITERATIONS = 50


def _confirmed(stdout: str) -> list[tuple[str, int, int]]:
    out = []
    for line in stdout.splitlines():
        match = LINE_RE.match(line)
        if match is None:
            continue
        out.append((match["source_id"], int(match["seq"]), int(match["i"])))
    return out


class _StdoutReader:
    """Drains a subprocess's stdout on a background thread for its whole
    lifetime, so the test can wait for the *first confirmed line* instead
    of a fixed sleep-then-kill -- a blind sleep races process-spawn and
    scheduler jitter and produces vacuous runs under load.
    """

    def __init__(self, proc: subprocess.Popen[str]) -> None:
        self._lock = threading.Lock()
        self._lines: list[str] = []
        self._got_first = threading.Event()
        self._thread = threading.Thread(target=self._run, args=(proc,), daemon=True)
        self._thread.start()

    def _run(self, proc: subprocess.Popen[str]) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            with self._lock:
                self._lines.append(line.rstrip("\n"))
            if LINE_RE.match(self._lines[-1]):
                self._got_first.set()

    def wait_for_first_confirmation(self, timeout: float) -> bool:
        return self._got_first.wait(timeout=timeout)

    def join_and_collect(self, timeout: float) -> str:
        self._thread.join(timeout=timeout)
        with self._lock:
            return "\n".join(self._lines)


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


@pytest.mark.parametrize("iteration", range(N_ITERATIONS))
async def test_sigkill_between_claim_and_ack_never_loses_a_message(
    tmp_path: Path, iteration: int
) -> None:
    db_path = tmp_path / "keep.db"
    ack_log_path = tmp_path / "broker_acks.log"

    proc = subprocess.Popen(
        [sys.executable, str(WORKER), str(db_path), str(ack_log_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    reader = _StdoutReader(proc)
    got_first = reader.wait_for_first_confirmation(timeout=5)

    # if the kill landed before the worker even got one publish() back, the
    # rest of this test is vacuous -- both checks below would trivially
    # "pass" on zero evidence. fail loudly instead of pretending that's a
    # real crash-recovery run.
    assert got_first, (
        "worker never confirmed a single publish within the timeout; this "
        "run can't exercise crash recovery -- is the worker hanging?"
    )

    # let a random extra bit of run happen so the kill doesn't always land
    # right after the very first ack
    time.sleep(random.uniform(0, 0.3))
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=5)

    confirmed = _confirmed(reader.join_and_collect(timeout=5))

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
