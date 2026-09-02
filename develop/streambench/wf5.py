"""Option 5 consumer, in its own module so both halves have the same shape.

Mirrors wf7.py: subscribe, then drain, stamping observation time in the
passed-through sink.
"""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    import observed as _observed


@workflow.defn
class ConsumeWorkflow:
    @workflow.run
    async def run(self, args: list) -> int:
        stream_id, expected = args[0], int(args[1])
        workflow.subscribe_stream(stream_id, start_offset=0)
        seen = 0
        while seen < expected:
            for body in await workflow.read_stream(stream_id):
                _observed.observe(body.decode())
                seen += 1
        return seen
