"""Placeholder Keep — just enough API for the crash-recovery harness to
import and drive. No schema, no disk writes, nothing durable yet.
"""

from __future__ import annotations

import os
from typing import Self


class Keep:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = path
        self._next_seq: dict[str, int] = {}

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def publish(self, *, topic: str, payload: bytes, source_id: str) -> int:
        """Hands back a seq number. Doesn't touch disk yet."""
        seq = self._next_seq.get(source_id, 0) + 1
        self._next_seq[source_id] = seq
        return seq

    async def close(self) -> None:
        pass
