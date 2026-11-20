use std::{
    collections::HashMap,
    ops::Range,
    pin::Pin,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};

use nanocode::tool_contract::{
    Recovered, Tool, ToolCallFuture, ToolContext, ToolDefinition, ToolInput, ToolOutput, ToolResult,
};
use nanocode_code_mode::code_mode::{CellJournal, CellJournals};
use nanocode_code_mode_engine::{CellTools, EmbeddedHost, Interrupt, RuntimeEvent};
use nanocode_sync::Atom;
use nanocode_tool_runtime::{SessionEnvironment, Tools, runtime::ToolRuntime};
use serde_json::{Value, json};

use crate::{
    executor::{Clock, Ended, Executor, Task},
    rng::{FNV_OFFSET, Rng, fnv},
};

const CALLS_MIN: u64 = 1_000;
const CALLS_MAX: u64 = 30_000;
const BATCH_MAX: u64 = 500;
const PAD_MAX: u64 = 64;
const MOVED_SESSION: &str = "cell-calls-moved";
const MOVED_CALL: &str = "cell-calls-moved-exec";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellCallsMutation {
    None,
    CrossResults,
    RootRecoveryUnbound,
    HostClaimsUnsent,
    HostForgetsUnsent,
}

impl CellCallsMutation {
    const ALL: [Self; 5] = [
        Self::None,
        Self::CrossResults,
        Self::RootRecoveryUnbound,
        Self::HostClaimsUnsent,
        Self::HostForgetsUnsent,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::CrossResults => "cross_results",
            Self::RootRecoveryUnbound => "root_recovery_unbound",
            Self::HostClaimsUnsent => "host_claims_unsent",
            Self::HostForgetsUnsent => "host_forgets_unsent",
        }
    }

    pub fn names() -> String {
        Self::ALL[1..]
            .iter()
            .map(|mutation| mutation.name())
            .collect::<Vec<_>>()
            .join("|")
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CellCallsStats {
    pub seeds: u64,
    pub calls: u64,
    pub batches: u64,
}

impl CellCallsStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.calls += other.calls;
        self.batches += other.batches;
    }
}

pub struct CellCallsOutcome {
    pub digest: u64,
    pub shape: String,
    pub stats: CellCallsStats,
    pub wrong: Option<String>,
}

fn block<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        std::thread::yield_now();
    }
}

fn drive(
    host: &mut EmbeddedHost,
    calls: u64,
    pad: &str,
    mutation: CellCallsMutation,
    digest: &mut u64,
) -> Result<(), String> {
    let mut seen = vec![false; usize::try_from(calls).expect("a call count fits in usize")];
    let mut received = 0_u64;
    let mut previous = 0_u64;

    loop {
        match block(host.read_event()).map_err(|error| format!("read_event: {error}"))? {
            RuntimeEvent::ToolCall { id, input, .. } => {
                let index = input.get("i").and_then(Value::as_u64).ok_or_else(|| {
                    format!("cell_call_once: call {id} carries no index: {input}")
                })?;
                let slot = usize::try_from(index)
                    .ok()
                    .and_then(|index| seen.get_mut(index))
                    .ok_or_else(|| {
                        format!("cell_call_once: call {id} names index {index} of {calls}")
                    })?;
                if *slot {
                    return Err(format!(
                        "cell_call_once: index {index} reached the host twice (call {id}, {received} received)"
                    ));
                }
                *slot = true;
                received += 1;
                *digest = fnv(*digest, &index.to_le_bytes());

                let answer = if mutation == CellCallsMutation::CrossResults && received > 1 {
                    previous
                } else {
                    index
                };
                previous = index;
                let encoded = json!({ "i": answer, "p": pad }).to_string();
                host.send_tool_result(1, id, encoded, true)
                    .map_err(|error| format!("send_tool_result: {error}"))?;
            }
            RuntimeEvent::Done { stored, .. } => {
                if received != calls {
                    return Err(format!(
                        "cell_call_once: {received} of {calls} calls reached the host"
                    ));
                }

                let expected = json!([calls, calls * (calls - 1) / 2]);
                let reported = stored
                    .get("done")
                    .and_then(|raw| serde_json::from_str::<Value>(raw.get()).ok());
                if reported.as_ref() != Some(&expected) {
                    return Err(format!(
                        "cell_call_once: the cell reported {reported:?}, not {expected}"
                    ));
                }
                return Ok(());
            }
            RuntimeEvent::Error { message, .. } => {
                return Err(format!("cell failed after {received} calls: {message}"));
            }
            _ => {}
        }
    }
}

fn root_recovery(seed: u64, mutation: CellCallsMutation) -> Result<(), String> {
    let root = format!("cell-calls-{seed}");
    let child = format!("{root}.child");

    for session in [&root, &child] {
        let journals = CellJournals::new();
        let tools = Tools::builder()
            .without_defaults()
            .cell_journals(journals.clone())
            .build()
            .map_err(|error| format!("tools: {error}"))?;
        let tools = if mutation == CellCallsMutation::RootRecoveryUnbound {
            tools
        } else {
            tools.for_session(&SessionEnvironment::new(session, &root))
        };
        drop(ToolRuntime::new_with_tools(
            std::env::temp_dir(),
            Vec::new(),
            &tools,
        ));

        let preempts = journals.preempts_recovery(session);
        if preempts != (*session == root) {
            return Err(format!(
                "root_recovery_preemptible: session {session} of root {root} lets a steer cut its recovered observation short: {preempts}"
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reached {
    Dispatched,
    NotDispatched,
}

enum Side {
    Before(CellJournals, Arc<Atom<Option<Vec<CellJournal>>>>),
    After,
}

struct Moved {
    side: Side,
    reached: Reached,
    runs: Arc<AtomicU64>,
    mutation: CellCallsMutation,
}

impl Tool for Moved {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "m",
            "Answer an index",
            json!({
                "type": "object",
                "properties": { "i": { "type": "integer" } },
                "required": ["i"]
            }),
        )
    }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        input: ToolInput,
        _context: ToolContext<'life1>,
    ) -> ToolCallFuture<'async_trait, ToolResult>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let index = input.decode_json::<Value>()?["i"].clone();
            if let Side::Before(journals, snapshot) = &self.side {
                let cells = journals.live_journals(MOVED_SESSION);
                snapshot.update(|slot| *slot = Some(cells.clone()));
                if self.reached == Reached::NotDispatched {
                    return Ok(ToolOutput::text("the cell moved first"));
                }
            }
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutput::text(format!("m:{index}")))
        })
    }

    fn recover<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _input: ToolInput,
        _context: ToolContext<'life1>,
    ) -> Option<Pin<Box<dyn Future<Output = Recovered> + Send + 'async_trait>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let unsent = match (self.reached, self.mutation) {
            (Reached::Dispatched, CellCallsMutation::HostClaimsUnsent) => true,
            (Reached::NotDispatched, CellCallsMutation::HostForgetsUnsent) => false,
            (reached, _) => reached == Reached::NotDispatched,
        };
        Some(Box::pin(async move {
            if unsent {
                Recovered::NotDispatched
            } else {
                Recovered::Unknown
            }
        }))
    }
}

fn settle<T>(seed: u64, clock: &Arc<Clock>, future: impl Future<Output = T>) -> Option<T> {
    let slot = std::cell::RefCell::new(None);
    let task: Task<'_> = Box::pin(async {
        *slot.borrow_mut() = Some(future.await);
    });
    match Executor::new(
        Arc::clone(clock),
        Rng::derive(seed, "cell_calls_moved_scheduler"),
    )
    .run(vec![task], None)
    {
        Ended::Finished => slot.into_inner(),
        Ended::Crashed | Ended::Stuck => None,
    }
}

fn moved_runtime(tool: Moved, journals: &CellJournals) -> Result<ToolRuntime, String> {
    let tools = Tools::builder()
        .without_defaults()
        .add(tool)
        .cell_journals(journals.clone())
        .build()
        .map_err(|error| format!("tools: {error}"))?;
    Ok(ToolRuntime::new_with_tools(
        std::env::temp_dir(),
        Vec::new(),
        &tools,
    ))
}

fn moved_call(
    seed: u64,
    clock: &Arc<Clock>,
    reached: Reached,
    index: u64,
    mutation: CellCallsMutation,
) -> Result<(), String> {
    let code = format!("text(await tools.m({{ i: {index} }}));");
    let context = ToolContext::new("sim", MOVED_SESSION, MOVED_CALL, &[], usize::MAX);
    let runs = Arc::new(AtomicU64::new(0));
    let snapshot = Arc::new(Atom::new(None));
    let before = CellJournals::new();
    let first = moved_runtime(
        Moved {
            side: Side::Before(before.clone(), Arc::clone(&snapshot)),
            reached,
            runs: Arc::clone(&runs),
            mutation,
        },
        &before,
    )?;
    settle(seed, clock, first.execute_code(&code, context))
        .ok_or("moved_call: the first cell never finished")?
        .map_err(|error| format!("moved_call: the first cell failed: {error}"))?;
    let cells = snapshot
        .read(Option::clone)
        .ok_or("moved_call: the call never saw its cell's journal")?;
    let running = cells.iter().flat_map(CellJournal::unknown_calls).count();
    if running != 1 {
        return Err(format!(
            "moved_call: the moved journal holds {running} running calls, not 1"
        ));
    }

    let after = CellJournals::new();
    after.restore(MOVED_SESSION, cells, None, HashMap::new());
    let second = moved_runtime(
        Moved {
            side: Side::After,
            reached,
            runs: Arc::clone(&runs),
            mutation,
        },
        &after,
    )?;
    let execution = settle(seed, clock, second.recover_code(context))
        .ok_or("moved_call: the replayed cell never finished")?
        .map_err(|error| format!("moved_call: the replayed cell failed: {error}"))?
        .ok_or("moved_call: the moved cell was not recovered")?;
    let output = serde_json::to_string(&execution.output).unwrap_or_default();
    let ran = runs.load(Ordering::SeqCst);
    if ran != 1 {
        return Err(format!(
            "nested_runs_once: a nested call {reached:?} before its cell moved ran {ran} times"
        ));
    }
    let answer = format!("m:{index}");
    if reached == Reached::NotDispatched && !output.contains(&answer) {
        return Err(format!(
            "nested_runs_once: the replayed cell never received {answer}: {output}"
        ));
    }
    Ok(())
}

fn simulate(seed: u64, mutation: CellCallsMutation) -> CellCallsOutcome {
    let mut rng = Rng::derive(seed, "cell_calls");
    let calls = rng.range(CALLS_MIN, CALLS_MAX);
    let batch = rng.range(1, BATCH_MAX);
    let pad = "x".repeat(usize::try_from(rng.below(PAD_MAX)).expect("a pad fits in usize"));
    let index = Rng::derive(seed, "cell_calls_moved").below(CALLS_MAX);
    let shape = format!(
        "calls={calls} batch={batch} pad={} moved={index}",
        pad.len()
    );

    let code = format!(
        "const n = {calls}, b = {batch}; let c = 0, s = 0; for (let i = 0; i < n; i += b) {{ const r = await Promise.all(Array.from({{ length: Math.min(b, n - i) }}, (_, j) => tools.t({{ i: i + j }}))); for (const v of r) {{ const o = typeof v === 'string' ? JSON.parse(v) : v; c += 1; s += o.i; }} }} store('done', [c, s]);"
    );
    let tools = CellTools::encode(&[json!({ "kind": "function", "name": "t", "tool_name": "t" })])
        .expect("the cell tool encodes");
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let mut host = EmbeddedHost::spawn().expect("the embedded host starts");
    let mut digest = fnv(FNV_OFFSET, shape.as_bytes());

    let wrong = root_recovery(seed, mutation)
        .and_then(|()| moved_call(seed, &clock, Reached::Dispatched, index, mutation))
        .and_then(|()| moved_call(seed, &clock, Reached::NotDispatched, index, mutation))
        .and_then(|()| {
            host.start_cell(
                1,
                &code,
                HashMap::new(),
                Arc::new(tools),
                None,
                None,
                Interrupt::new(),
            )
            .map_err(|error| format!("start_cell: {error}"))
        })
        .and_then(|()| drive(&mut host, calls, &pad, mutation, &mut digest))
        .err();

    CellCallsOutcome {
        digest,
        shape,
        stats: CellCallsStats {
            seeds: 1,
            calls,
            batches: calls.div_ceil(batch),
        },
        wrong,
    }
}

pub fn judge(
    seed: u64,
    mutation: CellCallsMutation,
    twice: bool,
    verbose: bool,
    totals: &mut CellCallsStats,
) -> Option<String> {
    let first = simulate(seed, mutation);
    totals.add(&first.stats);
    if verbose {
        println!("seed {seed} digest {:016x} {}", first.digest, first.shape);
    }
    if first.wrong.is_some() || !twice {
        return first
            .wrong
            .map(|message| format!("{} :: {message}", first.shape));
    }
    let second = simulate(seed, mutation);
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: trace digest {:016x} then {:016x}",
            first.shape, first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: CellCallsMutation) -> String {
    let mut command = format!("nanocode-sim --scenario cell_calls --seed {seed}");
    if mutation != CellCallsMutation::None {
        command.push_str(" --mutation ");
        command.push_str(mutation.name());
    }
    command
}

pub fn report(
    seeds: &Range<u64>,
    passed: u64,
    failed: u64,
    twice: bool,
    mutation: CellCallsMutation,
    totals: &CellCallsStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=cell_calls mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        CellCallsMutation::None if failed == 0 => ExitCode::SUCCESS,
        CellCallsMutation::None => ExitCode::FAILURE,
        _ if failed > 0 => {
            println!("MUTATION {} caught by {failed} seeds", mutation.name());
            ExitCode::SUCCESS
        }
        _ => {
            println!("MUTATION {} NOT caught", mutation.name());
            ExitCode::FAILURE
        }
    }
}
