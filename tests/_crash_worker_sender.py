"""Worker for the sender's crash-recovery extension: runs Keep + Sender
against a transport that logs acks to a file instead of memory, so a
SIGKILL'd worker still leaves a record of what actually reached the
"broker" behind, independent of whatever state the keep DB ends up in.
"""

from __future__ import annotations

import asyncio
import sys

from edgekeep import Keep
from edgekeep.sender import Sender
from _fake_transport import DurableFakeTransport

SOURCES = ("sensor-a", "sensor-b", "sensor-c")


async def main(db_path: str, ack_log_path: str) -> None:
    transport = DurableFakeTransport(ack_log_path)
    async with Keep(db_path) as keep:
        asyncio.create_task(Sender(keep, transport).run_forever())
        i = 0
        while True:
            source_id = SOURCES[i % len(SOURCES)]
            seq = await keep.publish(
                topic=f"site-01/telemetry/{source_id}",
                payload=str(i).encode(),
                source_id=source_id,
            )
            print(f"{source_id}\t{seq}\t{i}", flush=True)
            i += 1


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2]))
