# The `go.temporal.io/api` changes Paths A and C need

Stages 5 and 6 of AI-198 cannot be built without changing the public API module. This records exactly what changes, why, and what blocks applying them, so the next attempt does not rediscover it.

`api-go-stream-changes.patch` applies cleanly to `temporalio/api` at `e80f8e2`.

## What the patch adds

| Change | Why |
|---|---|
| `temporal/api/stream/v1/message.proto` with `StreamMessage`, `StreamSlice`, `StreamCursor` | The public shapes. The library's own copies under `chasm/lib/stream/proto` are server-internal and no SDK can import them |
| `COMMAND_TYPE_ADD_STREAM_MESSAGES = 19` | Path A: a workflow publishing to its own stream |
| `AddStreamMessagesCommandAttributes` at field 20 of the `Command` oneof | The `attributes` oneof is closed and has no extension point, so this is the only way |
| `stream_slices` on `PollWorkflowTaskQueueResponse` | Path C: the slice reaches the worker out of band, so payloads never enter History |
| `stream_cursors` on `WorkflowTaskCompletedEventAttributes` | Path C: only the offset range is recorded, on an event that already exists once per task, so consumption adds no events at all |

Note there is **no new event type**. That is deliberate: putting the range on `WorkflowTaskCompleted` is what makes recording an empty range free, and an empty range has to be recorded on every task where a subscription is active (see `streaming-detailed-design.md` §8.2).

## What blocks it

**Generation does not work from a clean clone.** `buf.gen.yaml` runs a `go-helpers` plugin from `./protoc-gen-go-helpers`, a directory that is not in the repository, and the repository has no `go.mod`. The published module is a reshaped artifact: generated Go is emitted under `temporal/api/...` and then flattened to the module root by the Makefile's `fix-path`. So a fork needs the generation toolchain sorted out before it produces anything importable.

**A local `replace` would not be enough.** It would make this branch unbuildable for anyone without the same checkout at the same path, which defeats the point of a prototype meant to be reviewed.

The unblock is a branch pushed to `temporalio/api` and pinned by pseudo-version. That is a change to a shared repository and needs a decision from someone who owns it, not a unilateral push.

## What is not blocked

Path B, an off-shard producer with client consumers, needs none of this and is what the benchmark measures. It is also the path LLM token streaming actually takes, since tokens come from an activity rather than from workflow code.
