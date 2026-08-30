"""Latency sink both halves write into from inside the Workflow sandbox.

A Workflow's return value only exists for the live run, and wall-clock time is
not reachable from Workflow code. This module is passed through the sandbox, so
the stamping happens here where the real clock still is. Both designs use the
same module, so their latency numbers are produced identically.
"""

from __future__ import annotations

import time

LATENCIES_MS: list[float] = []
COUNT: list[int] = [0]


def observe(sent_epoch_ms: float) -> None:
    LATENCIES_MS.append(time.time() * 1000.0 - sent_epoch_ms)
    COUNT[0] += 1


def reset() -> None:
    LATENCIES_MS.clear()
    COUNT[0] = 0
