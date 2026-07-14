"""Worker process for the crash-recovery test.

Opens a Keep at argv[1] and publishes numbered messages in a tight loop
across a few sources, printing "<source_id>\\t<seq>" only once the matching
publish() call has actually returned — that's our proof the message landed.
The test spawns this, kills it mid-run, and checks what made it to disk
against what got printed here.

Underscore prefix so pytest leaves this alone during collection.
"""

from __future__ import annotations

import asyncio
import sys

from edgekeep import Keep

SOURCES = ("sensor-a", "sensor-b", "sensor-c")


async def main(db_path: str) -> None:
    async with Keep(db_path) as keep:
        i = 0
        while True:
            source_id = SOURCES[i % len(SOURCES)]
            seq = await keep.publish(
                topic=f"site-01/telemetry/{source_id}",
                payload=str(i).encode(),
                source_id=source_id,
            )
            print(f"{source_id}\t{seq}", flush=True)
            i += 1


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
