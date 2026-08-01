"""Claims PENDING rows and drives them through the broker: publish over
the transport at QoS 1, delete on ack, back off and retry on failure,
give up to DEAD after enough permanent failures.

Never touches SQLite directly -- every read or write goes through Keep's
writer task via the _claim_next/_finalize_* methods. Two writers on one
connection is exactly the kind of race that passes happy-path tests and
corrupts under load.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time

from edgekeep.keep import ClaimedMessage, Keep
from edgekeep.transport import PermanentError, Transport, TransportError

_logger = logging.getLogger(__name__)

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BASE_BACKOFF = 1.0
DEFAULT_CAP_BACKOFF = 120.0
DEFAULT_REPLAY_RATE_LIMIT = 100.0
DEFAULT_MAX_INFLIGHT = 32

_QOS = 1


class _RateLimiter:
    """A plain leaky bucket: reserves the next slot synchronously so
    concurrent workers can't all slip through the gate at once, then
    sleeps off whatever's left of the wait.
    """

    def __init__(self, rate_per_second: float) -> None:
        self._interval = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
        self._next_slot = time.monotonic()

    async def wait_for_slot(self) -> None:
        if self._interval <= 0:
            return
        now = time.monotonic()
        slot = max(self._next_slot, now)
        self._next_slot = slot + self._interval
        delay = slot - now
        if delay > 0:
            await asyncio.sleep(delay)


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
        self._rate_limiter = _RateLimiter(replay_rate_limit)

    async def run_forever(self) -> None:
        # each worker only ever has one claim in flight, and a claim never
        # succeeds for a source that already has one INFLIGHT -- so the
        # worker count alone is what caps concurrency, no semaphore needed
        workers = [
            asyncio.create_task(self._worker()) for _ in range(max(1, self.max_inflight))
        ]
        await asyncio.gather(*workers)

    async def _worker(self) -> None:
        while True:
            try:
                await self._rate_limiter.wait_for_slot()
                claimed = await self.keep._claim_next()
                if claimed is None:
                    await asyncio.sleep(0.05)
                    continue
                await self._handle(claimed)
            except asyncio.CancelledError:
                raise
            except Exception:
                # last-resort net: whatever this was, it happened outside
                # a claimed message (rate limiter, claim() itself), so
                # there's no row to requeue -- just don't let it take the
                # whole worker down with it
                _logger.exception("sender worker hit an unexpected error")
                await asyncio.sleep(0.05)

    async def _handle(self, claimed: ClaimedMessage) -> None:
        try:
            await self.transport.publish(
                topic=claimed.topic, payload=claimed.payload, qos=_QOS
            )
        except PermanentError as exc:
            await self._fail(claimed, exc, permanent=True)
        except TransportError as exc:
            await self._fail(claimed, exc, permanent=False)
        except Exception as exc:
            # anything neither error type anticipated -- a transport bug,
            # something from aiomqtt's internals -- still leaves the row
            # claimed INFLIGHT with nobody driving it if we let it propagate.
            # treat it like any other transient failure instead of orphaning
            # the message until the next process restart
            _logger.exception("unexpected error publishing message %s", claimed.id)
            await self._fail(claimed, exc, permanent=False)
        else:
            await self.keep._finalize_ack(claimed.id)

    async def _fail(self, claimed: ClaimedMessage, exc: Exception, *, permanent: bool) -> None:
        attempts = claimed.attempts + 1

        # transport errors are unlimited -- only a permanent error's own
        # attempt count can send a message to DEAD
        if permanent and attempts >= self.max_attempts:
            await self.keep._finalize_dead(claimed.id, attempts=attempts, error=str(exc))
            return

        backoff = min(self.cap_backoff, self.base_backoff * (2**attempts))
        backoff *= random.uniform(0.5, 1.5)
        next_retry_at_ms = time.time_ns() // 1_000_000 + int(backoff * 1000)
        await self.keep._finalize_retry(
            claimed.id, attempts=attempts, next_retry_at_ms=next_retry_at_ms, error=str(exc)
        )
