"""Bounded storage: what happens when the keep hits max_bytes or
max_messages. Two policies for now -- DropOldest evicts to make room,
DropNewest rejects the publish that would have gone over. Downsample
isn't decided yet and isn't built.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class KeepFullError(Exception):
    """The keep is at its bound and the policy in use rejects new
    publishes instead of evicting -- the publish that raised this never
    got committed.
    """


@dataclass(frozen=True)
class EvictedMessage:
    """What a policy gets back after evicting a row, for on_evict/logging."""

    id: int
    idempotency_key: bytes
    source_id: str
    seq: int
    topic: str
    payload_size: int


class EvictionContext(Protocol):
    """What a policy needs from the keep to do its job, nothing more.
    Everything here runs inside the writer's transaction for the publish
    that triggered it.
    """

    def over_bound(self) -> bool:
        """True if the keep is currently over max_bytes or max_messages."""
        ...

    def evict_oldest_pending(self) -> EvictedMessage | None:
        """Delete the oldest PENDING row and return what it was, or None
        if there's nothing PENDING left to evict.
        """
        ...


class EvictionPolicy(Protocol):
    def enforce(self, ctx: EvictionContext) -> None:
        """Called right after a publish lands, inside the same
        transaction. Either bring the keep back under bound or raise
        KeepFullError to reject the publish (which rolls back everything,
        including the insert that triggered this).
        """
        ...


class DropOldest:
    """Default policy: evict oldest PENDING rows until back under bound,
    or until there's nothing PENDING left to evict.
    """

    def enforce(self, ctx: EvictionContext) -> None:
        while ctx.over_bound():
            if ctx.evict_oldest_pending() is None:
                break


class DropNewest:
    """Reject the publish that would push the keep over bound. Nothing
    already in the keep gets touched.
    """

    def enforce(self, ctx: EvictionContext) -> None:
        if ctx.over_bound():
            raise KeepFullError("keep is full; publish rejected by DropNewest policy")
