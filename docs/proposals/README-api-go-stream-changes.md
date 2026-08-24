# The `go.temporal.io/api` changes Paths A and C need

Stages 5 and 6 of AI-198 cannot be built without changing the public API module. This records exactly what changes, why, and what blocks applying them, so the next attempt does not rediscover it.

**Status: applied.** The branch lives at `moedash/api` on `moe/AI-198-stream-commands`, and this repo pins it by pseudo-version through a `replace` in `go.mod`. `api-go-stream-changes.patch` is the proto-only diff against `temporalio/api` at `e80f8e2`, kept so the change can be proposed upstream without the vendoring noise.

## What the patch adds

| Change | Why |
|---|---|
| `temporal/api/stream/v1/message.proto` with `StreamMessage`, `StreamSlice`, `StreamCursor` | The public shapes. The library's own copies under `chasm/lib/stream/proto` are server-internal and no SDK can import them |
| `COMMAND_TYPE_ADD_STREAM_MESSAGES = 19` | Path A: a workflow publishing to its own stream |
| `AddStreamMessagesCommandAttributes` at field 20 of the `Command` oneof | The `attributes` oneof is closed and has no extension point, so this is the only way |
| `stream_slices` on `PollWorkflowTaskQueueResponse` | Path C: the slice reaches the worker out of band, so payloads never enter History |
| `stream_cursors` on `WorkflowTaskCompletedEventAttributes` | Path C: only the offset range is recorded, on an event that already exists once per task, so consumption adds no events at all |

Note there is **no new event type**. That is deliberate: putting the range on `WorkflowTaskCompleted` is what makes recording an empty range free, and an empty range has to be recorded on every task where a subscription is active (see `streaming-detailed-design.md` §8.2).

## What it took to build a fork

Worth recording, because none of it is obvious and all of it cost time.

**A clean clone cannot generate.** `buf.gen.yaml` runs a `go-helpers` plugin from `./protoc-gen-go-helpers`, a directory absent from the repository, and there is no `go.mod`. Both live in the published module, so the fork takes them from there.

**Plain `buf generate` produces a module the server cannot compile against.** It leaves the `CommandType_` prefix on enum constants, while the published module has them bare. The stripping is done by `protogen`'s const rewriter (`cmd/protogen/const_rewriter.go`), so generation has to go through `protogen` rather than `buf` directly.

**The pipeline does not emit everything the module ships.** Missing after a full generate: `operatorservicemock`, `proxy`, `serviceerror`, `temporalnexus`, `temporalproto`, `workflowservicemock`, the grpc-gateway `.pb.gw.go` files, and a few hand-written `.go` files sitting beside generated ones such as `common/v1/payload_json.go`. All are taken from the published module unchanged, since none are touched by the stream changes.

**The generated output is reshaped.** Go is emitted under `temporal/api/...` and flattened to the module root by the Makefile's `fix-path`. Flattening before generating breaks the next generation, because the copied `.proto` files then collide with the originals as duplicate definitions.

## Verifying it

`tests/api_fork_test.go` round-trips all three new shapes through proto marshalling rather than merely compiling against them. A field added without its descriptor compiles fine and silently drops on the wire, which is the failure this guards.

## What is not blocked

Path B, an off-shard producer with client consumers, needs none of this and is what the benchmark measures. It is also the path LLM token streaming actually takes, since tokens come from an activity rather than from workflow code.
