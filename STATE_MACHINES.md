# Lifecycle state machines

This index covers the Rust SDK, TypeScript host, and Swift host lifecycle paths inspected on 2026-11-18.
The source baselines are Rust `6ed92be87`, TypeScript `c9e92eb`, and Swift `150faee`.
`Explicit` means a lifecycle already has an enum or discriminated union. It does not mean every transition has exhaustive checks.
`Remaining` identifies a representation or transition audit that this change does not complete.
Configuration flags, capability flags, counters, and independently concurrent operations are not mutually exclusive lifecycle phases.

## Changes in this patch

| Machine | Representation | Invariant |
| --- | --- | --- |
| Durable session ownership | `Ownership::Unacquired(Seed)`, `Deferred(OwnerToken)`, `Owned(OwnerToken)` in [session.rs](crates/runtime/durability/src/session.rs) | Only an acquired owner can require initial ordered settlement. Successful admission transitions Deferred to Owned. Repeated settlement is idempotent. Unacquired settlement returns an error. |

Acquisition retains existing behavior: a deferred root start enters Deferred, and other acquisitions enter Owned.
An admission that fails to persist leaves Deferred unchanged. Both admission entrypoints reject an unacquired owner before any mutation. Existing batching and logged-prompt paths retain their settlement behavior.
No public API, stored record, or wire name changes.

The common command handler acquires ownership before either Admit or AdmitAutomatic is dispatched.
AcquireAgent is an explicit acquisition command. CurrentOwner and PendingTurn are the only other commands exempt from automatic acquisition.
Automatic admission reaches the same admission method for both recovered and candidate operation IDs.
Both private admission methods now call `require_acquired` before abandonment, hydration, reducer application, or claimed-operation mutation.
This preflight prevents an invalid ownership error from first appearing after an admission mutates state.
The source call sites are `session.rs:761-775`, `965-1010`, `1746-1754`, and `1826-1854` at this patch.

## Changed transition contracts

| Ownership before | Event | Result |
| --- | --- | --- |
| Unacquired | Admission preflight or initial settlement | Error; phase and token unchanged |
| Unacquired | Successful initial deferred root acquisition | Deferred with the acquired owner token |
| Any phase | Successful non-deferred acquisition | Owned with the acquired owner token |
| Deferred | Initial admission persistence fails | Deferred; no operation is claimed |
| Deferred | Initial admission succeeds | Owned with the same token |
| Owned | Initial settlement | Owned, idempotently |

The acquisition method determines whether a new root start is deferred; re-acquisition is not a new root start. Admission of an already-recorded operation follows the existing pending/terminal replay paths rather than creating another transition.

| Socket before | Event | Result |
| --- | --- | --- |
| Idle | Read with no buffered frame | Waiting with one identified reader |
| Waiting | Second read | Error; original reader remains |
| Waiting | Matching reader cancellation | Idle; stale cancellation cannot remove another reader |
| Idle or Waiting | Text/binary frame | Buffer it; resolve an existing reader and return to Idle |
| Idle or Waiting | Peer/client/error/deadline close | Closed with first reason; settle reader and watchers |
| Closed | Further frame or close | Ignore; Closed is absorbing |
| Any phase | Read with buffered frame | Drain the frame before inspecting closure |
| Idle or Waiting | Send | Allowed only if admission finishes before closure |
| Idle, unused and unbuffered | Runtime spare release | Closed; ownership transfers to host exactly once |

The frame buffer and close watchers are orthogonal to the socket phase. These tables specify the changed machines, not exhaustive event coverage for every lifecycle indexed below.

## Rust SDK

Paths in this section are relative to this repository.

| Area | Machine and source | Current representation and remaining work |
| --- | --- | --- |
| Turn admission | [agent/session_log.rs](crates/runtime/agent/src/session_log.rs), `TurnState` | Explicit Queued, Running, Completed, Failed, Cancelled. Cancellation request is separate persisted intent. Remaining: audit all row update fences against the same transition table. |
| Turn scheduling | [agent/turn_inbox.rs](crates/runtime/agent/src/turn_inbox.rs), `Run`, `Request`, `Delivered` | Inbox versus Resumed and message delivery phases are explicit. Request combines optional running source and cancellation intent. Cancellation while idle is valid queued intent. Remaining: enumerate request events before combining these axes. |
| Pass policy | [agent/turn_inbox/policy.rs](crates/runtime/agent/src/turn_inbox/policy.rs), `PassPhase`, `PassOrigin` | Explicit Running with an admitted mark, Ended with the finished pass, Finishing with the chosen outcome, and Closed; each is saved in the turn log before the step it guards. Origin fixes the inbox or live path at creation. Remaining: fence pass records against a takeover. |
| Turn replay | [turnlog/fold.rs](crates/core/turnlog/src/fold.rs), `Phase` | Explicit Queued with retry metadata, Running with cancellation intent, Finished with outcome and result. Remaining: mutation coverage for every reducer edge. |
| Durable operation | [durability/state.rs](crates/core/durability/src/state.rs), `OperationStatus`, `Transition` | Explicit Pending, Completed, Failed, Cancelled. Reducer owns legal transitions. Remaining: complete operation-by-transition rejection matrix. |
| Durable effect | [durability/state.rs](crates/core/durability/src/state.rs), `StepStatus` | Explicit EffectPending and Completed. Dispatch outcome uncertainty must not cause silent replay. Remaining: retain receipt and crash-window mutation checks. |
| Session admission | [durability/session.rs](crates/runtime/durability/src/session.rs), `Admission`, `BeginStep`, `PendingTurn`, `Resolution` | Explicit admission, recovery, pending-turn, and committed/absent/unknown outcomes. These are separate machines, not one global state. Remaining: end-to-end failure injection after initial deferred acquisition. |
| Session ownership | [durability/session.rs](crates/runtime/durability/src/session.rs), `Ownership` | Changed here. Owner token and initial ordered-settlement obligation now share one enum. |
| Session owner release | [durability/session.rs](crates/runtime/durability/src/session.rs), `OwnerReleaseState` | Remaining: raw atomic states OWNER_ACTIVE, OWNER_RELEASING, OWNER_RELEASED plus completion watch. A typed atomic boundary must preserve acquire/release ordering and existing CAS behavior. |
| Session store health | [durability/session.rs](crates/runtime/durability/src/session.rs), `Driver` | Remaining: poisoned flag fences command processing after uncertain store failure. Running, abandonable, claimed, staged, and batch collections encode separate per-operation state. Do not collapse them without a cross-operation trace model. |
| Subagent status | [subagents/model.rs](crates/runtime/subagents/src/model.rs), `AgentStatus` | Explicit Pending, Running, Completed(output), Interrupted, Failed(error), Closing, Closed. Remaining: replace scattered assignments with checked events while retaining recoverable interruption and reusable completion semantics. |
| Subagent restoration | [subagents/runtime.rs](crates/runtime/subagents/src/runtime.rs), `RestorationState` | Explicit Loading, Resuming, Ready, Failed(error). Remaining: complete restoration edge and journal-failure coverage. Capacity and residency policy flags are orthogonal. |
| Subagent harness | [subagents/harness.rs](crates/runtime/subagents/src/harness.rs), `Harness`, `Contract`, `Started` | Contract delivery and recovery-only start are explicit. Remaining: agent, active turn, capacity waiter, submitted deadline, and context are independent option fields. Model valid hibernated, acquiring, active, and submitted combinations before conversion. |
| Subagent effects | [subagents/durable.rs](crates/runtime/subagents/src/durable.rs), `CallState` | Explicit durable call state. Remaining: verify journal persistence precedes each outward call and that unknown dispatched work is never replayed automatically. |
| Code Mode cell | [code_mode/replay.rs](crates/runtime/code-mode/src/code_mode/replay.rs), `CellLife`, `Replay`, `Hold` | Explicit Active(replay, hold), Finished, Retired. Replay and hold are independent only while active. Remaining: observer ID and observer timing option fields can still form partial records. Preserve journal wire compatibility. |
| Code Mode nested call | [code_mode/replay.rs](crates/runtime/code-mode/src/code_mode/replay.rs), `JournaledCallState` | Explicit Queued, Running, Completed(encoded, success). A restored Running call has an unknown outcome, not permission to dispatch again. |
| Interpreter execution | [code-mode-engine/host.rs](crates/runtime/code-mode-engine/src/host.rs), `Machine`, `Phase`, `Body`, `Ran` | Running/Idle and Awake/Parked are explicit. Remaining: held, blocked call, pump, and optional settled reason still combine into execution substates. Preserve outbound ordering and checkpoint boundaries. |
| Claude authentication | [subscription.rs](crates/backends/claude/client/src/subscription.rs), `State` | Explicit SignedOut, AccountOnHold, Pending, Exchanging, Validating, Ready with phase-owned credentials. Remaining: exhaustive refresh, expiry, host-write failure, and cancellation event coverage. |
| Codex authentication | [auth/subscription.rs](crates/backends/codex/client/src/auth/subscription.rs), `PersistedState` | Remaining: credential and pending device login are independent options. Determine whether reauthentication intentionally retains prior credentials before choosing four explicit variants. Preserve persisted format and generation fencing. |
| Claude conversation | [thread.rs](crates/backends/claude/client/src/thread.rs), `State` | Explicit Fresh(reason), Stored(anchor), Unthreaded. Remaining: transition matrix for thread-not-found recovery versus unrelated input. |
| Claude event stream | [client/lib.rs](crates/backends/claude/client/src/lib.rs), `SseState`, `FrameData` | Line versus Joined frame payload is explicit. Remaining: optional frame data, line-start flags, cursor, and CRLF handling need protocol-boundary traces, not a single global lifecycle enum. |
| Responses HTTP stream | [transport/http.rs](crates/backends/codex/client/src/transport/http.rs), `SseStream`, `SseBody` | Body provider is explicit. Optional body marks drainage. Hook callback has a separate pending future. Remaining: body EOF, callback failure, cancellation, and decoder-finalization matrix. |
| Managed provider session | [session/state.rs](crates/backends/codex/client/src/session/state.rs), `ManagedSessionState` | Remaining: previous-response anchor, delta offset, context revision, and server reasoning metadata encode continuation validity. Preserve history-prefix and stale-revision validation. |
| Provider streaming | [transport/chat.rs](crates/backends/codex/client/src/transport/chat.rs), `ChatPhase` | Explicit Reading(body), Drained, Done. Buffered output queue survives input drainage. Remaining: distinguish protocol item kinds currently held in `LiveItem.kind` strings without changing emitted wire data. |
| Payment channel | [mpp/session.rs](crates/integrations/mpp/src/session.rs), `ChannelEntry`, `PendingOpen`, `PaymentLease` | Remaining: opened flag and separate pending-open map encode channel establishment. Pending payload can be absent during recovery. Merge only after preserving durable store schema, transaction uncertainty, lease ordering, and receipt reconciliation. |
| Payment operations | [mpp/session.rs](crates/integrations/mpp/src/session.rs), `PaymentHold`, `TopUpRoute`, `PreparedTopUp` | Hold class and HTTP/management route are explicit. Remaining: close, top-up, and open need a shared documented transition inventory with store recovery. No payment policy changed here. |
| Remote hand | [remote/hand.rs](crates/integrations/tool-runtime/src/remote/hand.rs), `Phase`, `Settled`, `Next`, `HandEvent` | Ended/Moved/Fenced, Current/Left, and Stop/Redial are explicit. Remaining: `Redial { fenced: bool }`, serving close metadata, and overlapping old/new connections require identity-aware reconnect traces. |
| Subagent capacity | [subagents/capacity.rs](crates/runtime/subagents/src/capacity.rs), `Waiter`, `Acquire` | Resident/Spawn waiter class is explicit. Acquisition holds an optional grant receiver. Remaining: cancellation before and after grant must conserve capacity. |
| Sandbox admission | [sandbox/budget.rs](crates/integrations/sandbox/src/budget.rs), `Busy`, `Stopped`, `Stop` | Refusal and stop outcomes are explicit. Limits and concurrent reservations are resource accounting. Remaining: stop-versus-completion and cancelled-reservation traces. |
| Sandbox execution | [sandbox/run.rs](crates/integrations/sandbox/src/run.rs), `Confinement`, `Seen`, `Ran` | Explicit Bubblewrap/Landlock, Mounted/InPlace, and Exited/TimedOut/Overflowed/Stopped. Tailnet access and resource limits are policy, not phases. Remaining: budget admission/cancellation and proxy teardown traces. |

## TypeScript host

Paths below refer to [nanocode-ts at c9e92eb](https://github.com/nanocodexyz/nanocode-ts/tree/c9e92eb906d19b958553e2ee85f57319579d4976).
The socket lifecycle conversion is a separate companion patch, not part of the Rust ownership change.

| Machine | Evidence | Current representation and remaining work |
| --- | --- | --- |
| Runtime lifetime | `src/AgentRuntime.ts:160-269,1077-1101,1177-1181` | Remaining: opened, stopped, failure, disposal. Disposal can be pending while stopped is false. The opened setter is public. Preserve first failure and shutdown order. |
| Provider socket | `src/AgentRuntime.ts:11-20,701-784,965-1024` | Companion conversion: idle, waiting(waiter), closed(reason). Whole TypeScript runtime bridge checks cover seven socket scenarios. Actual release-lane WASM and SQLite complete two mocked Codex socket turns per trial, three trials each for baseline and candidate, including buffered frames immediately followed by close and unused-spare release. External provider/network behavior remains untested. |
| Session placement | `src/Session.ts:39-85,194-198` | Remaining: disposed, runtime, placement, hand, leases, reopening. A changed hand while leased is valid deferred relocation. Keep lease counting orthogonal. |
| Session draining and relay | `src/Session.ts:87-104,129-146,178-185` | Drain promise is single-flight state. Relay token, controller, and started flag encode a separate relay machine. Remaining: race coverage for token creation, abort, and disposal. |
| SSE response | `src/EventHub.ts:308-407` | Remaining: pulled, closed, complete, release, consumed. Model streaming, final-enqueued, closed separately from first-pull consumption. Final bytes must precede close. |
| Client follow/reconnect | `src/cloudflare/Transport.ts:69-126` | Remaining: boolean completion result distinguishes terminal from disconnected. Parser buffering is separate. Preserve abort and retry behavior. |
| Browser wallet/cloud authentication | `src/react/Machines.tsx:43-134,172-229` | Remaining: connected, working, device, checkout, link. Wallet approval can succeed before cloud registration fails. Funding checkout is an independent operation. |
| Paid-tool authentication | `src/react/Machines.tsx:346-445`, `src/machines/Keyring.ts:37-87` | Remaining: key, busy, prompt. Keyring target registration is a per-target cache, not a global auth phase. |
| Server auth and payer | `src/machines/internal/Core.ts:145-191,252-256,517-527` | Durable key validity and expiry are authoritative. Remaining: optional status metadata can be partial at the public Account boundary. In-memory state must not replace fresh storage reads. |
| Payment channel and close | `src/machines/Payment.ts:21-31`, `src/machines/Payments.ts:197-242` | Channel opened flag mirrors the payment protocol. Closing is already a success/failure union. Remaining: coordinate channel schema changes in Rust, not a host-only reinterpretation. |
| Payment receipt stream | `src/machines/Payment.ts:176-242` | Remaining: final, reader, buffer, chained promise. Cancellation before model final cancels upstream. Cancellation after final drains payment receipts. Preserve reader ownership and callback ordering. |
| Container workspace | `src/sandbox/Container.ts:54-106,130-132` | Remaining: boot promise and external running status. Running with incomplete or failed setup is possible. Stop-versus-setup policy must be explicit. |
| Vercel sandbox | `src/sandbox/VercelSandbox.ts:17-76` | Optional promise conflates acquisition and readiness. Remaining: concurrent stop/open policy before introducing acquiring/ready/stopping phases. |
| Prepared hand | `src/Hand.ts:450-604` | Remaining: used, finished, stopped, socket; per-attempt connection, ready, heard, ended. Preserve catalog-before-ready and no replay of uncertain posts. |
| Hand sender and keeper | `src/Hand.ts:637-722,763-852` | Remaining: sender and connection are concurrent submachines. Keep identity guards against callbacks from replaced connections. |
| Remote executor | `src/Remote.ts:148-186,245-264,278-350` | Remaining: catalog, failure, parked timer/queue, waiter. Candidate variants: awaiting catalog, connected, parked, failed. Buffered input must remain readable after failure. |
| Bridge reconnect | `src/Bridge.ts:37-75` | Remaining: cataloged and ended flags. Document allowed reconnect messages and partial-send uncertainty before conversion. |

## Swift host

Paths below refer to [nanocode-swift at 150faee](https://github.com/nanocodexyz/nanocode-swift/tree/150faeea0dd835626147c1f7c16d649603629162).
The audit read source on macOS. This patch changes no Swift source or released FFI artifact.

| Machine | Evidence | Current representation and remaining work |
| --- | --- | --- |
| Runtime router | `Sources/Nanocode/Runtime.swift:149-190` | Remaining: optional FFI pointer and stopped flag. Candidate starting/running(pointer)/stopped states must coordinate request continuations and runtime shutdown under the existing lock. |
| Session API | `Sources/Nanocode/Session.swift:408-489` | Public methods delegate turn lifecycle and settlement to the SDK. Configuration booleans are policies, not a host turn state machine. No duplicate turn coordinator should be introduced here. |
| Remote queue | `Sources/Nanocode/Remote.swift:216-226,350-369,433-484` | Remaining: running, queue, record.open, posting, unadmitted, stopped requests. Separate per-request admission from the queue-drain lifecycle. Persisted conversation records remain compatible. |
| Remote stream | `Sources/Nanocode/Remote.swift:662-674` | Remaining: streams dictionary and settled dictionary. Stream task can race settlement. Preserve task identity checks so an older task cannot delete its replacement. |
| Remote recovery | `Sources/Nanocode/Remote.swift:645-659` | Retry, settled report, and cancellation intent interact. Remaining: reconcile HTTP acceptance uncertainty before retry, never treat an uncertain dispatch as not executed. |
| Hand connection | `Sources/Nanocode/Hand.swift:52-103` | Explicit idle, connecting, connected(endpoint), disconnected(reason), retrying(delay), stopped(error). Remaining: validate event ordering without rejecting legitimate reconnect events. |
| Chat projection | `Sources/Nanocode/Chat.swift:47-95` | Pending derives from driving or outstanding requests. These are concurrent observations, not necessarily exclusive states. Remaining: attach/detach and stale-listener identity traces. |
| Wallet status | `Sources/Nanocode/Wallet.swift:41-62,99-125` | Connected derives from an optional account. Key expiry derives from time. Remaining: wallet approval, account loading, and revocation event coverage. Payment channels remain SDK-owned. |

## Validation boundary

The ownership transition is checked with a standalone harness extracted from the actual production enum and method.
It covers all three source phases, illegal pre-acquisition settlement, rejection immutability, repeated settlement, owner-token preservation, and 10000 deterministic traces.
A mutation that retains Deferred after successful settlement fails the harness.
These standalone checks do not compile the complete crate or simulate storage I/O, crashes, or WASM.
The tracked check in `bin/nanocode-sim/src/whole/ownership.rs` runs on the runtime scenario's deterministic executor.
It exercises actual session actors across eight deferred/immediate, direct/agent, and explicit/automatic admission combinations.
It checks acquisition before writes, active/conflicting admission without mutation, failed commit retry, completed replay, and owner reacquisition.
`bin/nanocode-sim/ownership-mutations.sh` runs seed 17 twice, then repeats with a production transition mutation in an isolated local clone.
The mutation run must reach the named ownership invariant. A compiler failure does not count as a detected mutation.
On 2026-11-18, the tracked runtime scenario passed seed 17 twice on the existing Linux qualification box.
Every epoch executes all eight combinations through the actual session actors.
The production mutation retained Deferred after successful admission and compiled successfully.
It failed at `initial settlement obligation survived successful admission`, with Ordered instead of Durable.
The final runner and both simulator logs are preserved in `output/state-machines/rust-qualification50/`.
The simulator harness begins an operation attempt before completion, as the production API requires.

The companion TypeScript patch also passes seven scenarios through the entire imported runtime and its asynchronous dispatch/complete/cancel bridge with an injected engine. A second harness uses the actual release-lane WASM engine, SQLite, and a mocked Codex socket: baseline and candidate each complete six turns over three independent runtime instances, drain ten frames per instance despite immediate close, and return the unused prewarmed socket to the host. Fetch and HTTP fallback throw, so these checks make no provider calls. The WASM binary SHA256 is `f56383371ba34143169f81e208f1cf97169a1391f102c871fc79b4fe917f2edb`; it does not contain the ownership candidate. These are correctness checks, not latency or memory comparisons.

Native and WASM `cargo check --offline --locked --lib -p nanocode-durability` pass on Rust 1.99.0.
Clippy passes for the same native and WASM package with `--no-deps -- -D warnings`.
The complete `nanocode-sim` package also passes Clippy with `--no-deps -- -D warnings`.
The final runtime simulator and production mutation commands both run under the approved four-CPU lease.
These checks do not build a new host WASM artifact or qualify every lifecycle in this index.
No downstream SDK pin, FFI fingerprint, or release is advanced by this source change.
Existing simulator paths include `bin/nanocode-sim/src/turns.rs`, `children.rs`, `cellcalls.rs`, `hands.rs`, `streams.rs`, and `mpp.rs`.
Any new simulator invariant must include a mutation that the simulator rejects.

## Next priority

1. Integrate the qualified ownership and socket patches with the final release candidate, then validate the combined host artifacts.
2. Model Swift router shutdown and TypeScript runtime lifetime without changing public API setters or pending-disposal behavior.
3. Convert remote connection and stream ownership substates with deterministic stale-callback and partial-send traces.
4. Specify durable payment channel recovery before changing channel representation or storage schema.
5. Build exhaustive event matrices for existing enums. An enum alone does not enforce legal moves.
