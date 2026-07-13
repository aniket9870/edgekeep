# edgekeep

An embedded Python library that gives IoT gateways a persistent,
SQLite-backed outbox for telemetry - so an unreliable network never
means lost or duplicated data.

**The promise: no datapoint lost, no datapoint duplicated, disk never fills.**

## Why

Edge gateways lose connectivity - for minutes, sometimes for days.
When they do, telemetry either vanishes, or floods back on reconnect
as duplicates, or a hand-rolled buffer quietly fills the SD card.
Most teams rebuild the same store-and-forward logic from scratch,
and most get at least one of the failure modes wrong.

edgekeep does it once, properly: telemetry is committed to a local
outbox (*the keep*) before your publish call returns, replayed in
order after reconnect, deduplicated with idempotency keys, and
bounded so an outage can never exhaust local storage — with an
honest at-least-once delivery contract rather than a false
exactly-once promise.

## Status

**Early development — not usable yet.** The design is settled;
the code is being built in the open. Watch the repo or check
[edgekeep.dev](https://edgekeep.dev) for progress toward v0.1.

## Licence

Apache-2.0
