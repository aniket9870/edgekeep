"""Minimal UUIDv7 (RFC 9562) generator. Neither 3.11 nor 3.12's uuid module
ships uuid7() yet, and pulling in a dependency for 16 bytes felt excessive.
"""

from __future__ import annotations

import os
import time


def uuid7_bytes() -> bytes:
    ts_ms = time.time_ns() // 1_000_000
    rand = os.urandom(10)

    out = bytearray(16)
    out[0:6] = ts_ms.to_bytes(6, "big")
    out[6] = 0x70 | (rand[0] & 0x0F)  # version 7
    out[7] = rand[1]
    out[8] = 0x80 | (rand[2] & 0x3F)  # variant 10
    out[9:16] = rand[3:10]
    return bytes(out)
