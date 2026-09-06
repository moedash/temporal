# Bucket 2 comparison harness

Drives the same workload through two designs so their numbers can be compared:

- **Option 5**, this branch. Payload in a Temporal-owned log, read with
  `workflow.subscribe_stream` and `workflow.read_stream`. Needs the
  `moedash/sdk-python` checkout on `moe/AI-198-stream-client`.
- **Option 7**, Max's prototype. Payload in Redis, read with
  `external_stream`. Needs `mfateev/sdk-python` on `task/python-sdk-streaming`.

The two live in different checkouts with different `sdk-core` builds, so each
half runs in its own environment. `common.py` and `observed.py` are shared, which
is what makes the halves comparable: same workload, same latency stamping, same
counters.

## Running

    docker run -d --name bench-redis -p 6399:6379 redis:7-alpine
    ./temporal-server --root <cfg> --config . --env development --allow-no-auth start

The server config must expose Prometheus, which `development-sqlite.yaml` does
on `127.0.0.1:8000`. Then, from each checkout:

    cd <moedash-sdk-python> && uv run python develop/streambench/run_option5.py 127.0.0.1:7333
    cd <mfateev-sdk-python> && uv run python develop/streambench/run_option7.py 127.0.0.1:7333
    python3 develop/streambench/compare.py

## Reading the output

Temporal work comes from the server's own Prometheus counters and Redis work
from `info commandstats`, so neither design is measured by a mechanism the other
does not use. Count them as counts. Redis is in-memory and SQLite is on disk, so
they are not the same unit of cost.

Both halves publish one token per append. That is the natural shape for LLM
tokens and the worst case for Option 5, whose guidance is to batch.
