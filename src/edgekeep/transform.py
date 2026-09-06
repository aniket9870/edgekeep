"""Ingest-side transform pipeline: transforms run inside publish(), before
the outbox commit, and can rewrite, fan out, or drop a message before it
ever becomes a row. Plugin state that has to change atomically with the
outbox commit lives in plugin_storage, backed by the same connection and
the same transaction as the publish that triggered it.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from edgekeep.keep import Keep


class TransformTimeoutError(Exception):
    """A transform's on_ingest() didn't return within transform_timeout.

    The writer awaits transforms inline, one at a time, on the same task
    that services every other publish/claim/ack/retry -- so a transform
    that's merely slow (a cooperative await that takes too long) stalls
    every one of those until this fires. A transform that does *actually*
    blocking (non-async) I/O is worse: there's no await point for asyncio
    to cancel, so this guard can't help and the whole process's event loop
    stalls with it, not just this Keep. Transforms must use async I/O.
    """


@dataclass
class Draft:
    """Mutable pre-commit message. Transforms can edit these in place or
    return replacements entirely; once a publish actually commits, the
    resulting row is immutable like any other keep_messages row.
    """

    topic: str
    payload: bytes
    source_id: str
    content_type: str | None = None


class Transform(Protocol):
    async def on_ingest(self, draft: Draft) -> Sequence[Draft] | None:
        """Return replacement Draft(s) -- fan-out is fine, e.g. a raw
        reading plus a derived value -- or None to drop the message
        entirely. Raising rejects just this publish, not anything else
        queued alongside it in the same commit batch.
        """
        ...


class PluginStorage:
    """Namespaced key/value storage for one plugin, backed by a table in
    the same database as the outbox. Only ever call get()/set() from
    inside on_ingest() -- that's the only place a call is guaranteed to
    land inside the same transaction as the message being published.
    Calling it from anywhere else would be a second, uncoordinated writer
    touching the connection the writer task owns exclusively, so it's
    refused outright rather than left to corrupt something under load.
    """

    def __init__(self, keep: Keep, namespace: str) -> None:
        self._keep = keep
        self._namespace = namespace

    def get(self, key: str) -> bytes | None:
        conn = self._require_writer_conn()
        row = conn.execute(
            "SELECT value FROM plugin_storage WHERE namespace = ? AND key = ?",
            (self._namespace, key),
        ).fetchone()
        return row[0] if row is not None else None

    def set(self, key: str, value: bytes) -> None:
        conn = self._require_writer_conn()
        conn.execute(
            "INSERT INTO plugin_storage (namespace, key, value) VALUES (?, ?, ?) "
            "ON CONFLICT (namespace, key) DO UPDATE SET value = excluded.value",
            (self._namespace, key, value),
        )

    def _require_writer_conn(self) -> sqlite3.Connection:
        if asyncio.current_task() is not self._keep._writer_task:
            raise RuntimeError(
                "plugin_storage can only be used from inside on_ingest() -- "
                "the writer task is the only thing allowed to touch this connection"
            )
        conn = self._keep._conn
        if conn is None:
            raise RuntimeError("Keep is not open")
        return conn
