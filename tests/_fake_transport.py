"""In-process stand-ins for a real broker connection, scriptable enough
to drive the sender's state-machine paths without touching a network.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field

from edgekeep.transport import TransportError


@dataclass
class _Published:
    topic: str
    payload: bytes
    qos: int


@dataclass
class FakeTransport:
    """Records everything that gets through, and can be told to fail the
    next N publishes, add latency before acking, or act disconnected.
    """

    published: list[_Published] = field(default_factory=list)
    _queued_failures: list[Exception] = field(default_factory=list)
    delay: float = 0.0
    connected: bool = True

    def fail_next(self, exc: Exception, times: int = 1) -> None:
        self._queued_failures.extend([exc] * times)

    async def publish(self, *, topic: str, payload: bytes, qos: int) -> None:
        if not self.connected:
            raise TransportError("not connected")
        if self._queued_failures:
            raise self._queued_failures.pop(0)
        if self.delay:
            await asyncio.sleep(self.delay)
        self.published.append(_Published(topic, payload, qos))


class DurableFakeTransport:
    """Like FakeTransport, but acks are appended to a file on disk instead
    of an in-memory list, so a SIGKILL'd worker still leaves a record of
    what actually made it to the "broker" behind.
    """

    def __init__(self, ack_log_path: str) -> None:
        self._ack_log_path = ack_log_path

    async def publish(self, *, topic: str, payload: bytes, qos: int) -> None:
        fd = os.open(self._ack_log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
        try:
            os.write(fd, payload + b"\n")
            os.fsync(fd)
        finally:
            os.close(fd)
