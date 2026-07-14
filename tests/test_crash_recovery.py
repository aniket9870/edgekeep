"""Crash-recovery harness.

Spawns the worker, lets it run for a random 50-500ms, SIGKILLs it, then
pokes the resulting sqlite file directly to see whether the delivery
guarantees actually held up.

Keep is still a stub with no real persistence, so right now everything
here is red — and it should fail as a plain AssertionError, not blow up
with something unrelated like a missing table or a bad import. Once the
schema and publish() are real, these should start going green one by one.
"""

from __future__ import annotations

import random
import re
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from edgekeep import Keep

WORKER = Path(__file__).parent / "_crash_worker.py"
LINE_RE = re.compile(r"^(?P<source_id>[^\t]+)\t(?P<seq>\d+)$")

STATE_PENDING = 0
STATE_INFLIGHT = 1
STATE_DEAD = 2

N_ITERATIONS = 50


def _confirmed_messages(stdout: str) -> dict[str, list[int]]:
    """Parse worker stdout into {source_id: [confirmed seq, ...]}.

    The final line may be cut off mid-write by SIGKILL; such partial lines
    are dropped rather than treated as confirmations.
    """
    by_source: dict[str, list[int]] = {}
    for line in stdout.splitlines():
        match = LINE_RE.match(line)
        if match is None:
            continue
        by_source.setdefault(match["source_id"], []).append(int(match["seq"]))
    return by_source


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone()
    return row is not None


@pytest.mark.parametrize("iteration", range(N_ITERATIONS))
async def test_crash_recovery_invariants(tmp_path: Path, iteration: int) -> None:
    db_path = tmp_path / "keep.db"

    proc = subprocess.Popen(
        [sys.executable, str(WORKER), str(db_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(random.uniform(0.05, 0.5))
    proc.send_signal(signal.SIGKILL)
    stdout, _stderr = proc.communicate(timeout=5)

    confirmed = _confirmed_messages(stdout)

    # reopening is what's supposed to kick off recovery (INFLIGHT -> PENDING)
    async with Keep(db_path):
        pass

    conn = sqlite3.connect(db_path)
    try:
        assert _table_exists(conn, "keep_messages"), (
            "keep_messages table does not exist — schema isn't implemented yet"
        )

        rows = conn.execute(
            "SELECT idempotency_key, source_id, seq, state FROM keep_messages"
        ).fetchall()

        persisted_by_source: dict[str, set[int]] = {}
        for _key, source_id, seq, _state in rows:
            persisted_by_source.setdefault(source_id, set()).add(seq)

        # if we saw it printed, publish() had already returned, which means
        # it was supposedly committed — so it needs to actually be there
        for source_id, seqs in confirmed.items():
            for seq in seqs:
                assert seq in persisted_by_source.get(source_id, set()), (
                    f"seq {seq} for source {source_id!r} was confirmed on "
                    "stdout but is missing from keep_messages"
                )

        # dedup keys should never collide, crash or no crash
        keys = [key for key, *_rest in rows]
        assert len(keys) == len(set(keys)), (
            "duplicate idempotency_key found in keep_messages"
        )

        source_seq_pairs = [(source_id, seq) for _key, source_id, seq, _state in rows]
        assert len(source_seq_pairs) == len(set(source_seq_pairs)), (
            "duplicate (source_id, seq) found in keep_messages"
        )

        # anything caught mid-send when we killed the process shouldn't be
        # left stuck as INFLIGHT once we reopen
        inflight = conn.execute(
            "SELECT COUNT(*) FROM keep_messages WHERE state = ?", (STATE_INFLIGHT,)
        ).fetchone()[0]
        assert inflight == 0, "INFLIGHT rows were not reverted to PENDING on reopen"

        # no gaps below the highest seq we actually saw confirmed — gaps
        # are only supposed to happen from eviction, and nothing evicts here
        for source_id, seqs in confirmed.items():
            highest = max(seqs)
            have = persisted_by_source.get(source_id, set())
            missing = [seq for seq in range(1, highest + 1) if seq not in have]
            assert not missing, (
                f"gap in seq for source {source_id!r}: missing {missing} "
                f"below highest confirmed seq {highest}"
            )
    finally:
        conn.close()
