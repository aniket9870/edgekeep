"""Claims PENDING rows and drives them through the broker. Doesn't do
any of that yet - this is just enough surface for the M2 harness to run
against and fail red before the claim/backoff/retry logic exists.
"""

from __future__ import annotations

import asyncio

from edgekeep.keep import Keep
from edgekeep.transport import Transport

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BASE_BACKOFF = 1.0
DEFAULT_CAP_BACKOFF = 120.0
DEFAULT_REPLAY_RATE_LIMIT = 100.0
DEFAULT_MAX_INFLIGHT = 32


class Sender:
    def __init__(
        self,
        keep: Keep,
        transport: Transport,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_backoff: float = DEFAULT_BASE_BACKOFF,
        cap_backoff: float = DEFAULT_CAP_BACKOFF,
        replay_rate_limit: float = DEFAULT_REPLAY_RATE_LIMIT,
        max_inflight: int = DEFAULT_MAX_INFLIGHT,
    ) -> None:
        self.keep = keep
        self.transport = transport
        self.max_attempts = max_attempts
        self.base_backoff = base_backoff
        self.cap_backoff = cap_backoff
        self.replay_rate_limit = replay_rate_limit
        self.max_inflight = max_inflight

    async def run_forever(self) -> None:
        # doesn't claim or publish anything yet - just parks here so
        # callers have something to await without blowing up on import
        await asyncio.Event().wait()
