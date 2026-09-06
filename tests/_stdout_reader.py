"""Shared by the SIGKILL crash-recovery tests: drains a subprocess's
stdout on a background thread for its whole lifetime, so a test can wait
for the *first confirmed line* instead of a fixed sleep-then-kill -- a
blind sleep races process-spawn and scheduler jitter and produces vacuous
runs under load.
"""

from __future__ import annotations

import re
import subprocess
import threading


class StdoutReader:
    def __init__(self, proc: subprocess.Popen[str], line_re: re.Pattern[str]) -> None:
        self._line_re = line_re
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
            if self._line_re.match(self._lines[-1]):
                self._got_first.set()

    def wait_for_first_confirmation(self, timeout: float) -> bool:
        return self._got_first.wait(timeout=timeout)

    def join_and_collect(self, timeout: float) -> str:
        self._thread.join(timeout=timeout)
        with self._lock:
            return "\n".join(self._lines)
