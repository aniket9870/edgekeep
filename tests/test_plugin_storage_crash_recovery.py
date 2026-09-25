"""Crash surface added by transforms: SIGKILL between a transform's
plugin_storage write and the outbox commit for the message(s) it produced.
They're supposed to be the same transaction, so this is really testing
that the transaction boundary is where we think it is -- no counter bump
without both of its messages, no message pair without its counter bump.
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

from _stdout_reader import StdoutReader

WORKER = Path(__file__).parent / "_crash_worker_plugin.py"
LINE_RE = re.compile(r"^(?P<i>\d+)$")

N_ITERATIONS = 20


@pytest.mark.parametrize("iteration", range(N_ITERATIONS))
async def test_sigkill_never_splits_plugin_storage_from_its_messages(
    tmp_path: Path, iteration: int
) -> None:
    db_path = tmp_path / "keep.db"

    proc = subprocess.Popen(
        [sys.executable, str(WORKER), str(db_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    reader = StdoutReader(proc, LINE_RE)
    got_first = reader.wait_for_first_confirmation(timeout=5)
    assert got_first, (
        "worker never confirmed a single publish within the timeout; this "
        "run can't exercise crash recovery -- is the worker hanging?"
    )

    time.sleep(random.uniform(0, 0.3))
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=5)

    stdout = reader.join_and_collect(timeout=5)
    confirmed = [int(m["i"]) for line in stdout.splitlines() if (m := LINE_RE.match(line))]

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT value FROM plugin_storage WHERE namespace = 'counter' AND key = 'count'"
        ).fetchone()
        persisted_counter = int(row[0]) if row is not None else 0

        payloads = {payload for (payload,) in conn.execute("SELECT payload FROM keep_messages")}
    finally:
        conn.close()

    # a stub that never actually runs the transform would leave the
    # counter at 0 and satisfy every invariant below on zero evidence --
    # fail loudly instead of letting that pass as "atomicity holds"
    assert persisted_counter > 0, (
        "plugin_storage counter is still 0 -- the transform never ran, so "
        "this run proves nothing about atomicity"
    )

    # every confirmed publish left both halves of its pair behind, or
    # neither -- never just one
    for i in confirmed:
        a, b = f"{i}-a".encode(), f"{i}-b".encode()
        assert (a in payloads) == (b in payloads), (
            f"pair {i} is split: -a present={a in payloads}, -b present={b in payloads}"
        )

    committed_pairs = {int(p[:-2]) for p in payloads if p.endswith(b"-a")}

    # the counter can't be ahead of the messages it's supposed to have
    # produced, or behind them either -- they only ever move together
    assert persisted_counter == len(committed_pairs)
    assert committed_pairs == set(range(1, persisted_counter + 1))
