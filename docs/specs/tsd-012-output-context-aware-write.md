# Context-Aware Output Connect/Write

## Objective

Give output plugins an opt-in way to receive a `context.Context` on
`Connect()` and `Write()` so a stuck network call can be bounded by a
configurable `write_timeout` instead of hanging forever, and define the
process for migrating existing output plugins onto it.

## Keywords

outputs, agent, context, cancellation, write-timeout, shutdown

## Overview

`telegraf.Output` currently exposes `Connect() error` and
`Write(metrics []Metric) error`. Neither method takes a `context.Context`,
so once the agent calls into a plugin it has no way to unblock that call.
If the underlying client library performs a blocking network operation that
never returns — because a broker stopped acknowledging traffic, a TCP
connection is half-open, or a DNS/dial call hangs — the plugin's goroutine
is stuck indefinitely. The metric buffer fills, `Close()` cannot run because
`Write()` never returns, and the only recovery is killing the process.

This is not hypothetical: issue [#19446][issue_19446] documents
`outputs.kafka`'s `SyncProducer.SendMessages` stuck for roughly 15 days in
production (Telegraf 1.39.2, sarama v1.60.0) after a broker-side condition
left the call hanging with no error and no timeout.

Some client libraries expose their own timeout knobs (e.g. sarama's
`config.Producer.Timeout` / `config.Net.WriteTimeout`), and those should
still be set where available — they are the first line of defense and are
cheaper than cancellation. But coverage is inconsistent: not every client
library exposes a full set of timeouts, some timeout options only bound
part of the call path (e.g. a single request but not retries, or the dial
but not the write), and users have no uniform, plugin-agnostic way to say
"never let a write to this output run longer than N seconds." A
context-based mechanism gives Telegraf that uniform backstop regardless of
what the underlying client supports, without requiring every client library
to get its own bespoke timeout wiring.

Reference implementation: PR [#19447][pr_19447] adds the interfaces and
agent plumbing described below and converts `outputs.kafka` as the first,
motivating example (commits `feat(core): add context-aware output write
timeouts` and `fix(outputs.kafka): bound cancellation of stuck producers`).
This spec generalizes that work and defines how the remaining output
plugins should be converted.

## Design

### New optional interfaces

```go
// OutputWithContext is an output whose write operation can return on context cancellation.
type OutputWithContext interface {
    Output

    // WriteContext takes in group of points to be written to the Output.
    // It must return when the context is cancelled. The implementation is
    // responsible for any cleanup of an underlying operation that cannot be cancelled.
    WriteContext(ctx context.Context, metrics []Metric) error
}

// OutputWithConnectContext is an output whose connection attempt can be cancelled.
type OutputWithConnectContext interface {
    Output

    // ConnectContext performs any connection setup required for writing, like
    // Connect. It must return when the context is cancelled.
    ConnectContext(ctx context.Context) error
}
```

Both are separate, optional interfaces a plugin may implement in addition
to the existing `Connect()`/`Write()`. Plugins that implement neither
continue to work exactly as before — `RunningOutput` type-asserts for the
context-aware interfaces and falls back to the plain methods when absent.
This keeps the change fully backward compatible and lets plugins be
migrated one at a time.

A plugin implementing `WriteContext` does not need to also implement
`ConnectContext`, and vice versa, though most plugins that block on the
network in `Write` will have the same risk in `Connect`.

### Contract

A plugin's `WriteContext`/`ConnectContext` implementation must return
promptly once `ctx.Done()` fires. If the underlying client call cannot
itself be cancelled (no context support, no deadline option, blocking
syscall), the plugin must race it in a goroutine and return on
`ctx.Done()` without waiting for that goroutine, e.g.:

```go
done := make(chan error, 1)
go func() { done <- underlyingBlockingCall() }()
select {
case err := <-done:
    return err
case <-ctx.Done():
    return ctx.Err()
}
```

Abandoning the goroutine leaks it until the underlying call eventually
returns (or forever, in the pathological case). The plugin is responsible
for making that abandonment safe: closing/discarding whatever the
goroutine was using so a future `Connect`/`Write` doesn't reuse a poisoned
resource. `outputs.kafka`'s producer replacement logic
(`acquireProducer`/`createProducer`/`disposeProducer` in
`plugins/outputs/kafka/kafka.go`) is the reference pattern — it caps the
number of abandoned producers in flight (`maxPoisonedProducers`) rather
than leaking without bound, and disposes of them with their own bounded
close timeout.

For a raw `net.Conn`, which has no context-aware read/write API, the way
to unblock an in-flight operation is to force its deadline to expire.
Plugins must not hand-roll that watcher: use
`internal.CancelConnOnContext`, which gets three easily-missed details
right. It operates only on the exact connection passed to it (so it cannot
race an error path that closed the connection and installed a
replacement); its `stop` function waits for the watcher goroutine to exit
before returning (so no deadline can land after the caller has moved on);
and `stop` reports whether the deadline was actually forced, including
when cancellation raced a *successful* operation. That last case matters:
the connection is then still live but permanently poisoned, and the caller
must discard it rather than hand it to the next flush. `outputs.graphite`,
`outputs.graylog`, `outputs.socket_writer`, `outputs.syslog`,
`outputs.opentsdb` and `outputs.instrumental` all use it.

`Close()` must remain safe to call after a cancelled `Write`/`Connect`,
including while an abandoned goroutine from the pattern above is still
running.

This narrows the existing `Output.Close` contract, which promises that
"Close will not be called until all writes have finished ... so locking is
not necessary". That promise still holds for plain `Output`
implementations. It cannot hold for an `OutputWithContext` implementation
that detaches an uncancellable operation, because the whole point of the
pattern is that `WriteContext` returns while the operation is still in
flight. Such implementations own the synchronization between `Close` and
their own abandoned goroutines; the `Output` interface documentation in
`output.go` has been amended to say so, so that plugin authors do not rely
on a guarantee their own cancellation strategy has given up.

### Configuration: `write_timeout`

A new per-output option, `write_timeout` (duration, default: disabled),
already added to `models.OutputConfig` in `config.go`. When set, the agent
wraps each write attempt in `context.WithTimeout(ctx, write_timeout)`
before calling `WriteContext`/`ConnectContext` (see `agent/agent.go`
`flushOnce`/`flushBatch`/`connectOutputAttempt`).

What the deadline covers, precisely:

* **One flush, not one plugin call.** `flushOnce` and `flushBatch` create a
  single timeout around `RunningOutput.WriteContext`, which may in turn
  hand several metric batches to the plugin's `WriteContext`. So
  `write_timeout = 30s` means "30 seconds for this whole flush", not
  "30 seconds per underlying plugin write". This is deliberate: the value
  users care about bounding is how long a flush can wedge the output loop.
* **Connect is bounded only for `OutputWithConnectContext`.** Each connect
  attempt in `connectOutput` gets its own fresh deadline via
  `connectOutputAttempt`, including the initial startup connection. An
  output that implements `OutputWithContext` but *not*
  `OutputWithConnectContext` gets bounded writes and unbounded connects;
  the two interfaces are independent and so is the protection they buy.

`write_timeout` only has an effect on plugins implementing
`OutputWithContext`/`OutputWithConnectContext`. Setting it on a plugin that
has not been converted would otherwise be a silent no-op, leaving users
believing they are protected when they are not. `NewRunningOutput`
(`models/running_output.go`) therefore warns at config-load time when
`write_timeout` is set on an output implementing neither interface.

The warning deliberately fires only for "neither". A plugin implementing
just one of the two is a valid, documented state — the option really does
bound what that interface covers — so warning there would be noise. What
each interface buys is described above.

### Final flush on shutdown

On agent shutdown the normal run context is already cancelled, which would
make any bounded final-flush attempt immediately fail for a context-aware
output. `finalFlushContext` (`agent/agent.go`) special-cases this: if the
output is context-aware, it gets one real final attempt on a fresh
`context.Background()`, bounded either by the configured `write_timeout`
(applied inside `flushOnce`) or a default `finalWriteTimeout` (15s) if none
is set. Non-context-aware outputs keep receiving the (already-cancelled)
shutdown context, i.e. unchanged behavior.

## Plugin Requirements

Converting an existing output plugin to be context-aware means:

1. Implement `WriteContext(ctx, metrics) error`, threading `ctx` down to
   the underlying client call. Keep `Write(metrics) error` as
   `return p.WriteContext(context.Background(), metrics)` for callers that
   still use the plain interface (aggregators, tests, etc.).
2. If the client library's calls accept a `context.Context` natively
   (many modern Go clients do — HTTP-based outputs via
   `http.NewRequestWithContext`, gRPC, etc.), pass it straight through.
   Prefer this over the goroutine-race pattern wherever available since it
   avoids leaking anything.
3. If the client library has no context support, use the goroutine-race
   pattern from the Contract section above, and handle safe abandonment/
   cleanup of the resource the goroutine was using.
4. Implement `ConnectContext` the same way if `Connect()` can block on the
   network (dialing, auth handshake, metadata fetch, etc.). Not every
   plugin needs this — some `Connect()` implementations are local/
   non-blocking and don't need conversion.
5. Also set any client-library-native timeout the plugin already exposes
   or could expose (e.g. dial timeout, request timeout) — context
   cancellation is a backstop, not a replacement for those.
6. Add tests that assert `WriteContext`/`ConnectContext` return once the
   context is cancelled, without relying on wall-clock sleeps racing
   against a hang (inject a hook/fake client that blocks until signaled,
   cancel the context, assert the call returns and the error wraps
   `ctx.Err()`).
7. Document `write_timeout` support in the plugin's `README.md`.

### Rollout process

* Each plugin conversion is its own PR against this spec — no bundled,
  repo-wide PR. This mirrors how `startup_error_behavior`
  ([TSD-006][tsd_006]) was rolled out incrementally.
* Prioritize plugins with a history of hang/stuck-write reports over a
  blanket pass through all 69 output plugins. `outputs.kafka` is done;
  candidates for the next passes should be pulled from open issues
  describing a stuck or hung write.
* A plugin PR should be small: the `WriteContext`/`ConnectContext`
  addition, the goroutine-race wrapper only if the client needs it, tests,
  and a README note. It should not bundle unrelated refactors.
* Plugins whose underlying client is fundamentally synchronous and cannot
  safely be raced (e.g. cgo bindings that are not thread-safe to abandon
  mid-call) should stay on the plain interface and document why in the
  plugin README rather than implementing a race that could corrupt shared
  state.

## Is/Is-not

**Is:**

* An opt-in pair of interfaces (`OutputWithContext`,
  `OutputWithConnectContext`) that plugins implement incrementally.
* A `write_timeout` config option that bounds a write/connect attempt for
  plugins that implement the interfaces.
* A per-plugin migration checklist and rollout process for converting the
  remaining output plugins over time, each as its own PR.

**Is-not:**

* Not a guarantee that every plugin can be interrupted immediately —
  plugins with non-cancellable underlying clients must use the
  goroutine-race-and-abandon pattern, which leaks a goroutine/resource
  until the underlying call eventually returns (bounded, not eliminated).
* Not a replacement for a client library's own timeout configuration;
  plugins should set both where available.
* Not a change to `inputs`, `processors`, or `aggregators`. `Gather()` has
  the same class of risk, but is out of scope here — see Open Questions.
* Not a single repo-wide conversion PR — plugins are converted
  incrementally, each independently reviewable.

## Prior art

* Issue [#19446][issue_19446] — `outputs.kafka` `SendMessages` stuck for
  ~15 days in production; sanitized incident evidence.
* PR [#19447][pr_19447] — reference implementation: `OutputWithContext`/
  `OutputWithConnectContext` interfaces, `write_timeout` config plumbing in
  `agent.go`/`config.go`/`models/running_output.go`, and the first
  conversion (`outputs.kafka`), including the bounded-abandonment producer
  pattern.
* [TSD-006][tsd_006] (`startup_error_behavior`) — precedent for defining a
  shared behavior/config option once and rolling it out per-plugin rather
  than in one PR.
* Previously closed issues referencing the same class of problem in
  `outputs.kafka`: [#11427](https://github.com/influxdata/telegraf/issues/11427),
  [#8349](https://github.com/influxdata/telegraf/issues/8349).

[issue_19446]: https://github.com/influxdata/telegraf/issues/19446
[pr_19447]: https://github.com/influxdata/telegraf/pull/19447
[tsd_006]: /docs/specs/tsd-006-startup-error-behavior.md

## Open questions

* `write_timeout` set on an output implementing neither context interface
  now logs a warning at config-load time. Should it be stronger — an error
  that refuses to start? Needs maintainer sign-off on severity.
* Should `inputs.Gather()` get an equivalent `GatherContext`, given the
  same hang risk exists there? Proposed as a separate, later spec rather
  than folding into this one.
* Is there an agreed list/priority order of output plugins to convert
  first, or does it stay purely issue-driven?
* Should there be a documented cap or metric exposed for abandoned/leaked
  goroutines from the race pattern (per plugin, or agent-wide), so a
  string of repeated timeouts is observable rather than silent?
