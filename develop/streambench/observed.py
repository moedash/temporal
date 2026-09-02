"""Latency sink both halves write into from inside the Workflow sandbox.

A Workflow's return value only exists for the live run, and wall-clock time is
not reachable from Workflow code. This module is passed through the sandbox, so
the stamping happens here where the real clock still is. Both designs use the
same module, so their latency numbers are produced identically.

This state belongs to the worker process, not to the Workflow, so it does not
rewind when a Workflow task replays. Counting a token by identity is what keeps
a replayed observation from being counted twice. Keying on replay flags would
not: an SDK reports a sticky cache hit as a replay, which would drop live
observations instead.
"""

from __future__ import annotations

import time

LATENCIES_MS: list[float] = []
COUNT: list[int] = [0]

_SEEN: set[str] = set()


def observe(token: str) -> None:
    """Stamp and count one token, at most once however often it is replayed.

    The token carries its own send time, so it is unique per token and doubles
    as the identity that makes this idempotent. The first observation is the
    live one, since a replay can only follow it, so the latency stays honest.
    """
    if token in _SEEN:
        return
    _SEEN.add(token)
    sent_epoch_ms = float(token.split("|", 1)[0])
    LATENCIES_MS.append(time.time() * 1000.0 - sent_epoch_ms)
    COUNT[0] += 1


def reset() -> None:
    LATENCIES_MS.clear()
    COUNT[0] = 0
    _SEEN.clear()
