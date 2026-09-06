"""Bucket 2 through Option 7: payload in Redis, Workflow reads it directly.

Run inside Max's sdk-python checkout so `temporalio.contrib.external_workflow_streams`
resolves. The workload, the latency discipline and the counters come from
common.py, which the Option 5 half imports too.
"""

from __future__ import annotations

import asyncio
import sys
import time
import uuid
from datetime import timedelta

sys.path.insert(0, "/tmp/bench")

import common
import observed

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._record import RecordKind, StreamRecord
from temporalio.contrib.external_workflow_streams._redis import RedisStreamBackend
import temporalio.converter

from wf7 import ConsumeWorkflow


async def main() -> None:
    wl = common.Workload()
    target = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:7333"
    result = common.Result(design="option7-external-redis", workload=vars(wl))

    client = await Client.connect(target)
    backend = RedisStreamBackend(url="redis://127.0.0.1:6399")
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    observed.reset()

    tq = f"bench7-{uuid.uuid4().hex[:8]}"
    wf_id = f"bench7-wf-{uuid.uuid4().hex[:8]}"
    filler = "x" * max(0, wl.message_bytes - 14)

    async with Worker(
        client, task_queue=tq, workflows=[ConsumeWorkflow],
        external_stream_backend=backend,
    ):
        handle = await client.start_workflow(
            ConsumeWorkflow.run, wl.total_tokens, id=wf_id, task_queue=tq
        )
        desc = await handle.describe()
        key = StreamKey(
            client.namespace, handle.id,
            desc.raw_description.workflow_execution_info.first_run_id, "tokens",
        )
        await asyncio.sleep(1.0)

        before_t = common.scrape_temporal_ops()
        before_r = common.scrape_redis_ops()
        started = time.time()

        interval = 1.0 / wl.token_rate
        for i in range(wl.total_tokens):
            body = f"{time.time() * 1000.0}|{filler}"
            await backend.append(
                key, StreamRecord(RecordKind.DATA, await codec.encode(body), "bench", i)
            )
            result.tokens_published += 1
            await asyncio.sleep(interval)

        try:
            result.tokens_observed = await asyncio.wait_for(handle.result(), timeout=120)
        except asyncio.TimeoutError:
            result.notes.append("workflow did not finish inside 120s")
            result.tokens_observed = observed.COUNT[0]

        result.wall_s = time.time() - started
        result.temporal_ops = common.delta(common.scrape_temporal_ops(), before_t)
        result.redis_ops = common.delta(common.scrape_redis_ops(), before_r)

    lat = observed.LATENCIES_MS
    result.latency_p50_ms = common.percentile(lat, 0.50)
    result.latency_p90_ms = common.percentile(lat, 0.90)
    result.latency_p99_ms = common.percentile(lat, 0.99)
    result.latency_max_ms = max(lat) if lat else 0.0
    # Where the slow ones sit tells a cold start apart from a real tail.
    result.latency_first10_ms = [round(x, 1) for x in lat[:10]]
    result.latency_last10_ms = [round(x, 1) for x in lat[-10:]]

    desc = await handle.describe()
    result.history_events = desc.raw_description.workflow_execution_info.history_length
    result.history_bytes = desc.raw_description.workflow_execution_info.history_size_bytes
    result.write("/tmp/bench/option7.json")


if __name__ == "__main__":
    asyncio.run(main())
