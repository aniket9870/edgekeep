"""What the sender needs from a broker connection. aiomqtt-backed
MqttTransport is the real implementation; tests get to swap in something
scriptable instead of talking to a real broker.
"""

from __future__ import annotations

import asyncio
import ssl
from dataclasses import dataclass
from typing import Protocol

import aiomqtt


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


@dataclass(frozen=True)
class Will:
    """Last Will and Testament, published by the broker if we drop off
    without a clean disconnect.
    """

    topic: str
    payload: bytes
    qos: int = 1
    retain: bool = False


class MqttTransport:
    """aiomqtt-backed Transport. Holds one connection open across calls
    and reconnects lazily on the next publish() after a drop -- the
    sender's own backoff is what paces those retries, so this doesn't
    need any retry logic of its own.
    """

    def __init__(
        self,
        *,
        hostname: str,
        port: int = 1883,
        username: str | None = None,
        password: str | None = None,
        ca_cert_path: str | None = None,
        client_cert_path: str | None = None,
        client_key_path: str | None = None,
        tls_context: ssl.SSLContext | None = None,
        will: Will | None = None,
    ) -> None:
        self.hostname = hostname
        self.port = port
        self._username = username
        self._password = password
        self._ca_cert_path = ca_cert_path
        self._client_cert_path = client_cert_path
        self._client_key_path = client_key_path
        self._tls_context = tls_context
        self._will = will
        self._client: aiomqtt.Client | None = None
        self._connect_lock = asyncio.Lock()

    async def publish(self, *, topic: str, payload: bytes, qos: int) -> None:
        if qos != 1:
            raise ValueError(f"edgekeep only ever publishes at QoS 1, got {qos}")
        client = await self._ensure_connected()
        try:
            await client.publish(topic, payload=payload, qos=qos)
        except aiomqtt.MqttError as exc:
            # connection might be dead -- drop it so the next publish()
            # reconnects instead of retrying a socket that's already gone
            await self._drop_connection()
            raise TransportError(str(exc)) from exc

    async def close(self) -> None:
        await self._drop_connection()

    async def _ensure_connected(self) -> aiomqtt.Client:
        async with self._connect_lock:
            if self._client is not None:
                return self._client
            client = aiomqtt.Client(
                hostname=self.hostname,
                port=self.port,
                username=self._username,
                password=self._password,
                tls_context=self._tls_context,
                tls_params=self._tls_params(),
                will=self._aiomqtt_will(),
            )
            try:
                await client.__aenter__()
            except aiomqtt.MqttError as exc:
                raise TransportError(str(exc)) from exc
            self._client = client
            return client

    def _tls_params(self) -> aiomqtt.TLSParameters | None:
        # an explicit SSLContext wins outright -- tls_params is just the
        # passthrough for people who don't want to build their own
        if self._tls_context is not None:
            return None
        if not (self._ca_cert_path or self._client_cert_path):
            return None
        return aiomqtt.TLSParameters(
            ca_certs=self._ca_cert_path,
            certfile=self._client_cert_path,
            keyfile=self._client_key_path,
        )

    def _aiomqtt_will(self) -> aiomqtt.Will | None:
        if self._will is None:
            return None
        return aiomqtt.Will(
            topic=self._will.topic,
            payload=self._will.payload,
            qos=self._will.qos,
            retain=self._will.retain,
        )

    async def _drop_connection(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        try:
            await client.__aexit__(None, None, None)
        except aiomqtt.MqttError:
            pass  # already broken, nothing left to clean up
