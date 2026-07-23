"""What the sender needs from a broker connection. aiomqtt-backed
MqttTransport is the real implementation; tests get to swap in something
scriptable instead of talking to a real broker.
"""

from __future__ import annotations

from typing import Protocol


class TransportError(Exception):
    """Worth retrying: broker unreachable, timed out waiting for PUBACK,
    connection dropped mid-publish, that sort of thing.
    """


class PermanentError(Exception):
    """Not worth retrying: the broker or the message itself rejected this
    in a way that trying again won't fix (oversized payload, bad topic,
    auth failure).
    """


class Transport(Protocol):
    async def publish(self, *, topic: str, payload: bytes, qos: int) -> None:
        """Publish and wait for the broker's ack. Returns once acked;
        raises TransportError or PermanentError on failure.
        """
        ...


class MqttTransport:
    """aiomqtt-backed Transport. Not built yet -- wiring this up is the
    last piece of M2, once the sender itself exists to drive it.
    """

    def __init__(self, *, hostname: str, port: int = 1883, **kwargs: object) -> None:
        self.hostname = hostname
        self.port = port

    async def publish(self, *, topic: str, payload: bytes, qos: int) -> None:
        raise NotImplementedError("MqttTransport isn't wired up to aiomqtt yet")
