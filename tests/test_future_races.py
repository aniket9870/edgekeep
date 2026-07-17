"""Covers a couple of ways a publish() future can end up in a weird state:
a message landing behind the close() sentinel, and a caller cancelling its
own await while the message is still sitting in the writer's queue/batch.
Both used to either hang forever or kill the writer task outright.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from edgekeep import Keep
from edgekeep.keep import _CLOSE, _QueuedPublish


async def test_publish_rejected_if_stranded_behind_close(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"

    async with Keep(db_path, commit_window=0.05) as keep:
        assert keep._queue is not None

        # manufacture the race directly: a message queued right behind the
        # close sentinel, same as a publish() that loses to a concurrent close()
        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        await keep._queue.put(_CLOSE)
        await keep._queue.put(
            _QueuedPublish(
                topic="t", payload=b"x", source_id="s", content_type=None, future=future
            )
        )

        with pytest.raises(RuntimeError, match="closed before this publish"):
            await asyncio.wait_for(future, timeout=2)


async def test_writer_survives_caller_cancelling_pending_publish(tmp_path: Path) -> None:
    db_path = tmp_path / "keep.db"

    async with Keep(db_path, commit_window=0.2) as keep:
        task = asyncio.create_task(
            keep.publish(topic="t", payload=b"x", source_id="s")
        )
        await asyncio.sleep(0.01)  # let it enqueue and start waiting on its future
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # the cancelled publish's row may still land (nothing pulls it back
        # out of the batch), but the writer itself must still be alive
        seq = await asyncio.wait_for(
            keep.publish(topic="t", payload=b"y", source_id="s"), timeout=2
        )
        assert seq == 2
