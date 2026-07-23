"""Thin integration check against a real broker on localhost:1883. Skips
itself automatically if nothing's listening there - CI doesn't need to
run a broker for the rest of the suite to mean anything.

MqttTransport isn't wired up to aiomqtt yet, so this stays red (or
skipped) until both it and the sender are real.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
from pathlib import Path

import pytest

from edgekeep import Keep
from edgekeep.sender import Sender
from edgekeep.transport import MqttTransport


def _broker_reachable(host: str = "localhost", port: int = 1883) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _broker_reachable(), reason="no MQTT broker listening on localhost:1883"
)


async def test_publish_is_delivered_through_a_real_broker(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"
    transport = MqttTransport(hostname="localhost", port=1883)

    async with Keep(db_path) as keep:
        await keep.publish(topic="edgekeep/test", payload=b"hello", source_id="s")

        task = asyncio.create_task(Sender(keep, transport).run_forever())
        deadline = asyncio.get_event_loop().time() + 5
        while asyncio.get_event_loop().time() < deadline:
            metrics = await keep.metrics()
            if metrics.pending_messages == 0:
                break
            await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        metrics = await keep.metrics()

    assert metrics.pending_messages == 0
    assert metrics.acked_total == 1
