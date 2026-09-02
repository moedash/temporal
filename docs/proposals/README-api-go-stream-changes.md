# The API changes Paths A and C need

Stages 5 and 6 of AI-198 cannot be built without changing the public API, because the `Command.attributes` oneof is closed and has no extension point. This records what changes and how the change is built.

## Two repositories, not one

This tripped me up, so it is worth stating plainly.

- **`temporalio/api`** holds the `.proto` files and nothing else. It has no `go.mod` and generates no Go. That is by design, not an omission.
- **`temporalio/api-go`** is the Go module `go.temporal.io/api`. It carries `temporalio/api` as a git submodule at `proto/api` and regenerates from it with `make update-proto`.

Changing the API means a branch on each: protos in `api`, regenerated output in `api-go` with its submodule pointed at that branch.

| Repository | Branch | Contents |
|---|---|---|
| `moedash/api` | `moe/AI-198-stream-protos` | 65 lines of proto, nothing else |
| `moedash/api-go` | `moe/AI-198-stream-commands` | Regenerated module, submodule pointed at the above |

The server pins `moedash/api-go` by pseudo-version through a `replace` in `go.mod`.

## What the protos add

| Change | Why |
|---|---|
| `temporal/api/stream/v1/message.proto` with `StreamMessage`, `StreamSlice`, `StreamCursor` | The public shapes. The library's copies under `chasm/lib/stream/proto` are server-internal and no SDK can import them |
| `COMMAND_TYPE_ADD_STREAM_MESSAGES = 19` | Path A: a workflow publishing to its own stream |
| `AddStreamMessagesCommandAttributes` at field 20 of the `Command` oneof | The oneof is closed, so this is the only way |
| `stream_slices` on `PollWorkflowTaskQueueResponse` | Path C: the slice reaches the worker out of band, so payloads never enter History |
| `stream_cursors` on `WorkflowTaskCompletedEventAttributes` | Path C: only the offset range is recorded, on an event that already exists once per task |

There is **no new event type**, deliberately. Putting the range on `WorkflowTaskCompleted` is what makes recording an empty range free, and an empty range must be recorded on every task where a subscription is active (see `streaming-detailed-design.md` §8.2).

## Regenerating

In an `api-go` checkout with submodules, with the `proto/api` submodule on the proto branch:

```
make grpc-install mockgen-install   # needs pnpm for the nexus plugin
make update-proto
```

Two steps were skipped here and their output taken from upstream unchanged, since nothing in this change touches nexus:

- `nexus-gen` and `system-nexus` need `pnpm`, which was not set up. They produce `workflowservice/v1/workflowservicenexus` and `systemnexus`.

Running `go-grpc` on its own is not enough. `make clean` removes the mocks and proxy, and only the full `proto` target puts them back, so the module ends up missing `workflowservicemock`, `operatorservicemock`, and the proxy.

The wider diff across generated files in the `api-go` branch is `protoc-gen-go` version drift from regenerating, not a change in shape.

## Verifying

`tests/api_fork_test.go` round-trips all three new shapes through proto marshalling rather than merely compiling against them. A field added without its descriptor compiles fine and silently drops on the wire, which is the failure that guards against.

## What is not blocked by any of this

Path B, an off-shard producer with client consumers, needs none of it and is what the benchmark measures. It is also the path LLM token streaming actually takes, since tokens come from an activity rather than from workflow code.
