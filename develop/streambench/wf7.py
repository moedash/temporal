"""Option 7 consumer, in its own module so the sandbox never re-imports the backend.

Max's design refuses a backend import from Workflow code on purpose, which is
why the runner and the Workflow cannot share a module.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream
    import observed as _observed


@workflow.defn
class ConsumeWorkflow:
    @workflow.run
    async def run(self, expected: int) -> int:
        tokens = external_stream.with_options(
            idle_timeout=timedelta(seconds=60)
        ).topic("tokens", type=str)
        seen = 0
        async for token in tokens.subscribe():
            sent = float(token.split("|", 1)[0])
            _observed.observe(sent)
            seen += 1
            if seen >= expected:
                break
        return seen
