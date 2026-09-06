"""Shared measurement plumbing for the bucket 2 comparison.

Both halves import this, so the workload, the latency discipline and the
counters are identical by construction. A benchmark whose halves measure
themselves differently measures the harness.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.request
from dataclasses import dataclass, field, asdict


@dataclass(frozen=True)
class Workload:
    """What both designs are asked to do. Identical for each."""

    token_rate: int = 40
    duration_s: int = 20
    message_bytes: int = 20

    @property
    def total_tokens(self) -> int:
        return self.token_rate * self.duration_s


def scrape_temporal_ops(addr: str = "127.0.0.1:8000") -> dict[str, float]:
    """Persistence request counters, by operation.

    Read from the server's own Prometheus endpoint rather than from either SDK,
    so neither design can be measured by a mechanism the other does not use.
    """
    out: dict[str, float] = {}
    try:
        with urllib.request.urlopen(f"http://{addr}/metrics", timeout=10) as resp:
            body = resp.read().decode()
    except Exception:
        return out
    for line in body.splitlines():
        if not line.startswith("persistence_requests"):
            continue
        if "{" not in line:
            continue
        labels, value = line.rsplit(" ", 1)
        op = ""
        for part in labels[labels.index("{") + 1 : labels.rindex("}")].split(","):
            k, _, v = part.partition("=")
            if k.strip() == "operation":
                op = v.strip().strip('"')
        out[op] = out.get(op, 0.0) + float(value)
    return out


def scrape_redis_ops(container: str = "bench-redis", port: int = 6399) -> dict[str, int]:
    """Redis command counts, so the cost that moved out of Temporal is still counted.

    Reads a local server first and falls back to a container, because either is
    a legitimate way to run the Option 7 half and a silently empty scrape
    reports the cost that moved to Redis as zero.
    """
    raw = ""
    for cmd in (
        ["redis-cli", "-p", str(port), "info", "commandstats"],
        ["docker", "exec", container, "redis-cli", "info", "commandstats"],
    ):
        try:
            done = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        except Exception:
            continue
        if done.returncode == 0 and "cmdstat_" in done.stdout:
            raw = done.stdout
            break
    if not raw:
        raise RuntimeError("no Redis commandstats from either a local server or a container")
    out: dict[str, int] = {}
    for line in raw.splitlines():
        if not line.startswith("cmdstat_"):
            continue
        name, _, rest = line.partition(":")
        for part in rest.split(","):
            k, _, v = part.partition("=")
            if k == "calls":
                out[name[len("cmdstat_"):]] = int(v)
    return out


def delta(after: dict, before: dict) -> dict:
    keys = set(after) | set(before)
    return {k: after.get(k, 0) - before.get(k, 0) for k in keys if after.get(k, 0) - before.get(k, 0)}


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, int(len(s) * q))]


@dataclass
class Result:
    design: str
    workload: dict
    tokens_published: int = 0
    tokens_observed: int = 0
    latency_p50_ms: float = 0.0
    latency_p90_ms: float = 0.0
    latency_p99_ms: float = 0.0
    latency_max_ms: float = 0.0
    latency_first10_ms: list = field(default_factory=list)
    latency_last10_ms: list = field(default_factory=list)
    wall_s: float = 0.0
    temporal_ops: dict = field(default_factory=dict)
    redis_ops: dict = field(default_factory=dict)
    workflow_tasks: int = 0
    workflow_task_seconds: float = 0.0
    history_events: int = 0
    history_bytes: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def temporal_ops_total(self) -> int:
        return int(sum(self.temporal_ops.values()))

    @property
    def redis_ops_total(self) -> int:
        return int(sum(self.redis_ops.values()))

    def write(self, path: str) -> None:
        d = asdict(self)
        d["temporal_ops_total"] = self.temporal_ops_total
        d["redis_ops_total"] = self.redis_ops_total
        with open(path, "w") as f:
            json.dump(d, f, indent=2)
        print(f"wrote {path}")
