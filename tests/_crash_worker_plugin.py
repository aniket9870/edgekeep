"""Worker for the plugin-storage atomicity crash test: a transform that
reads a counter out of plugin_storage, bumps it, and fans out into two
messages tagged with the new counter value -- all from one publish()
call, so all of it has to land in one transaction. A SIGKILL landing
anywhere in here must never split the counter bump from the two
messages it produced.
"""

from __future__ import annotations

import asyncio
import sys

from edgekeep import Draft, Keep


class _CountingFanOut:
    def __init__(self, keep: Keep) -> None:
        self._storage = keep.plugin_storage("counter")

    async def on_ingest(self, draft: Draft) -> list[Draft]:
        current = self._storage.get("count")
        count = int(current) + 1 if current is not None else 1
        self._storage.set("count", str(count).encode())
        return [
            Draft(topic=draft.topic, payload=f"{count}-a".encode(), source_id=draft.source_id),
            Draft(topic=draft.topic, payload=f"{count}-b".encode(), source_id=draft.source_id),
        ]


async def main(db_path: str) -> None:
    keep = Keep(db_path)
    keep.transforms.append(_CountingFanOut(keep))
    async with keep:
        i = 0
        while True:
            await keep.publish(topic="t", payload=b"raw", source_id="s")
            i += 1
            print(i, flush=True)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
