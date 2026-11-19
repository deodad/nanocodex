use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    ops::Range,
    path::PathBuf,
    pin::pin,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

use nanocode::{
    ClaudeClient, HarnessFamily, HostFuture, ModelSelection, Nanocode, PromptRequest, Thinking,
    Tools,
    backend::Prompt,
    claude::transport::{HttpError, HttpFuture, HttpRequest, HttpResponse, HttpTransport},
    durability::SessionRecord,
    session_host::{
        AfterPass, Delivery, Driven, FinishedPass, InboxHost, LoggedEvent, Next, PassContext,
        PassEnd, PassOutcome, PassPhase, PassPolicy, PassRecord, PassSpec, PolicyError, PolicyTurn,
        Submission, TurnInbox, TurnRecord, TurnState,
    },
    turnlog::OneLog,
};
use nanocode_sync::Atom;
use serde_json::{Map, json};

use crate::{
    disk::Disk,
    executor::{Clock, Ended, Executor, Task},
    rng::{FNV_OFFSET, Rng, fnv},
    turnlog::SimHost,
};

const SESSION: &str = "after-pass";
const MODEL: &str = "claude-haiku-5-5";
const CHILD_A: &str = "child-A-r1";
const CHILD_B: &str = "child-B-r1";
const STEERED: &str = "user-message-7";
const ORPHAN: &str = "orphan question";
const LOOKUP: &str = "lookup";

struct Lookup;

impl nanocode::FunctionHandler for Lookup {
    fn call<'a>(
        &'a self,
        _name: &'a str,
        _input: serde_json::Value,
        _context: nanocode::tools::ToolContext<'a>,
    ) -> HostFuture<'a, Result<serde_json::Value, String>> {
        Box::pin(async { Ok(json!({"found": true})) })
    }
}
const EPOCHS_MAX: u64 = 6;
const CRASH_AFTER: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AfterPassMutation {
    None,
    ForgetsLateChild,
    FreshDecision,
    AcksBeforeAdmission,
}

impl AfterPassMutation {
    const ALL: [Self; 4] = [
        Self::None,
        Self::ForgetsLateChild,
        Self::FreshDecision,
        Self::AcksBeforeAdmission,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ForgetsLateChild => "forgets_late_child",
            Self::FreshDecision => "fresh_decision",
            Self::AcksBeforeAdmission => "acks_before_admission",
        }
    }

    pub fn names() -> String {
        Self::ALL[1..]
            .iter()
            .map(|mutation| mutation.name())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AfterPassStats {
    pub seeds: u64,
    pub cases: u64,
    pub epochs: u64,
    pub crashes: u64,
    pub model_calls: u64,
    pub decisions: u64,
}

impl AfterPassStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.cases += other.cases;
        self.epochs += other.epochs;
        self.crashes += other.crashes;
        self.model_calls += other.model_calls;
        self.decisions += other.decisions;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Crash {
    None,
    BeforeDecisionSaved,
    AfterDecisionSaved,
    AfterAdmission,
    AfterFinishSaved,
    WhileClosing,
    DuringContinuation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    Continue(Crash),
    Live(Crash),
    LiveRacingDrive,
    HostError,
    CancelBeforeAdmission,
    CancelAfterAdmission,
    CancelAfterAdmittedCrash,
    Orphan,
    Legacy,
    Steered,
}

const CASES: [Case; 17] = [
    Case::Continue(Crash::None),
    Case::Continue(Crash::BeforeDecisionSaved),
    Case::Continue(Crash::AfterDecisionSaved),
    Case::Continue(Crash::AfterAdmission),
    Case::Continue(Crash::AfterFinishSaved),
    Case::Continue(Crash::WhileClosing),
    Case::Continue(Crash::DuringContinuation),
    Case::Live(Crash::None),
    Case::Live(Crash::AfterDecisionSaved),
    Case::LiveRacingDrive,
    Case::HostError,
    Case::CancelBeforeAdmission,
    Case::CancelAfterAdmission,
    Case::CancelAfterAdmittedCrash,
    Case::Orphan,
    Case::Legacy,
    Case::Steered,
];

#[derive(Clone, Debug, PartialEq, Eq)]
enum Saved {
    Finish,
    Continue {
        pass_id: String,
        prompt: String,
        carries: Vec<String>,
    },
}

#[derive(Clone, Default)]
struct Db {
    released: Vec<String>,
    carried: BTreeMap<String, String>,
    decisions: BTreeMap<(String, String), Saved>,
    returned: Vec<(String, Saved)>,
    acked: BTreeSet<String>,
    ack_calls: u64,
    after_pass_calls: u64,
    seen: Vec<FinishedPass>,
    crash: Option<Crash>,
    crashed: bool,
    fail_next: bool,
    cancel_at: Option<String>,
    cancel_now: bool,
    fresh: u64,
}

struct App {
    db: Arc<Atom<Db>>,
    mutation: AfterPassMutation,
}

impl App {
    fn db<R>(&self, change: impl FnMut(&mut Db) -> R) -> R {
        self.db.update(change)
    }

    fn crash_at(&self, point: Crash) -> bool {
        self.db(|db| {
            let now = db.crash == Some(point) && !db.crashed;
            db.crashed |= now;
            now
        })
    }

    fn decide(&self, request_id: &str, pass: &FinishedPass) -> Result<Saved, String> {
        self.db(|db| {
            if db.fail_next {
                db.fail_next = false;
                return Err("the host store is unavailable".to_owned());
            }
            if self.mutation != AfterPassMutation::ForgetsLateChild
                && !db.released.iter().any(|child| child == CHILD_B)
            {
                db.released.push(CHILD_B.to_owned());
            }
            let unread: Vec<String> = db
                .released
                .iter()
                .filter(|child| !db.carried.contains_key(*child))
                .cloned()
                .collect();
            let saved = if unread.is_empty() {
                Saved::Finish
            } else {
                let number = db
                    .decisions
                    .keys()
                    .filter(|(id, _)| id == request_id)
                    .count()
                    + 2;
                let pass_id = if self.mutation == AfterPassMutation::FreshDecision {
                    db.fresh += 1;
                    format!("{request_id}/pass-{number}-{}", db.fresh)
                } else {
                    format!("{request_id}/pass-{number}")
                };
                Saved::Continue {
                    prompt: format!("Child results arrived: {}", unread.join(", ")),
                    pass_id,
                    carries: unread,
                }
            };
            if let Saved::Continue {
                pass_id, carries, ..
            } = &saved
            {
                for child in carries {
                    db.carried.insert(child.clone(), pass_id.clone());
                }
                if self.mutation == AfterPassMutation::AcksBeforeAdmission {
                    db.acked.extend(carries.iter().cloned());
                }
            }
            db.decisions
                .insert((request_id.to_owned(), pass.pass_id.clone()), saved.clone());
            Ok(saved)
        })
    }
}

fn choice(saved: &Saved) -> AfterPass {
    match saved {
        Saved::Finish => AfterPass::Finish,
        Saved::Continue {
            pass_id, prompt, ..
        } => AfterPass::Continue(PassSpec {
            pass_id: pass_id.clone(),
            prompt: Prompt::new(prompt.clone()),
        }),
    }
}

impl PassPolicy for App {
    fn after_pass<'a>(
        &'a self,
        context: PassContext<'a>,
    ) -> HostFuture<'a, Result<AfterPass, String>> {
        Box::pin(async move {
            let key = (context.request_id.to_owned(), context.pass.pass_id.clone());
            let saved = self.db(|db| {
                db.after_pass_calls += 1;
                db.seen.push(context.pass.clone());
                db.decisions.get(&key).cloned()
            });
            let saved = match saved {
                Some(saved) if self.mutation != AfterPassMutation::FreshDecision => saved,
                _ => {
                    if self.crash_at(Crash::BeforeDecisionSaved) {
                        std::future::pending::<()>().await;
                    }
                    let saved = self.decide(context.request_id, context.pass)?;
                    let point = if saved == Saved::Finish {
                        Crash::AfterFinishSaved
                    } else {
                        Crash::AfterDecisionSaved
                    };
                    if self.crash_at(point) {
                        std::future::pending::<()>().await;
                    }
                    saved
                }
            };
            self.db(|db| {
                db.returned
                    .push((context.pass.pass_id.clone(), saved.clone()))
            });
            Ok(choice(&saved))
        })
    }

    fn admitted<'a>(
        &'a self,
        _session_id: &'a str,
        request_id: &'a str,
        pass_id: &'a str,
    ) -> HostFuture<'a, ()> {
        Box::pin(async move {
            if pass_id != request_id && self.crash_at(Crash::AfterAdmission) {
                std::future::pending::<()>().await;
            }
            self.db(|db| {
                if db.cancel_at.as_deref() == Some(pass_id) {
                    db.cancel_now = true;
                }
                let carried: Vec<String> = db
                    .carried
                    .iter()
                    .filter(|(_, pass)| pass.as_str() == pass_id)
                    .map(|(child, _)| child.clone())
                    .collect();
                for child in carried {
                    db.ack_calls += 1;
                    db.acked.insert(child);
                }
            });
        })
    }
}

#[derive(Default)]
struct Wire {
    bodies: Atom<Vec<String>>,
    hang: Atom<Vec<String>>,
}

const TOOLED: &str = "tool_result";

struct WireTransport(Arc<Wire>);

impl HttpTransport for WireTransport {
    fn post(&self, request: HttpRequest) -> HttpFuture<'_, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let body = request.body;
            let hang = self.0.hang.get();
            if !hang.is_empty() && hang.iter().all(|marker| body.contains(marker.as_str())) {
                std::future::pending::<()>().await;
            }
            self.0.bodies.update(|bodies| bodies.push(body.clone()));
            nanocode_platform::sleep(Duration::from_millis(50)).await;
            let answer = if body.contains(CHILD_B) {
                "answer-with-B"
            } else if body.contains(CHILD_A) {
                "answer-with-A"
            } else {
                "answer"
            };
            let count = self.0.bodies.read(Vec::len);
            let looks_up =
                (body.contains(CHILD_B) || body.contains(ORPHAN)) && !body.contains(TOOLED);
            let sse = if looks_up {
                crate::server::events(
                    MODEL,
                    &format!("ap{count}"),
                    &json!({"type": "tool_use", "id": format!("toolu_ap{count}"), "name": LOOKUP, "input": {}}),
                    Some(json!({"type": "input_json_delta", "partial_json": "{}"})),
                    "tool_use",
                )
            } else {
                crate::server::events(
                    MODEL,
                    &format!("ap{count}"),
                    &json!({"type": "text", "text": ""}),
                    Some(json!({"type": "text_delta", "text": answer})),
                    "end_turn",
                )
            };
            let mut headers = nanocode_http::types::HeaderMap::new();
            headers.insert(
                nanocode_http::types::header::CONTENT_TYPE,
                nanocode_http::types::HeaderValue::from_static("text/event-stream"),
            );
            headers.insert(
                "request-id",
                nanocode_http::types::HeaderValue::from_static("req_sim"),
            );
            Ok(HttpResponse::new(
                nanocode_http::types::StatusCode::OK,
                headers,
                Box::pin(nanocode_sync::stream::once(
                    async move { Ok(sse.into_bytes()) },
                )),
            ))
        })
    }
}

#[derive(Default)]
struct Inbox {
    next_calls: AtomicU64,
    hang_result: AtomicBool,
}

impl InboxHost for Inbox {
    fn publish(&self, events: Vec<LoggedEvent>) -> HostFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let result = events
                .iter()
                .any(|event| event.event.get().contains("\"type\":\"result\""));
            let hang = result && self.hang_result.swap(false, Ordering::AcqRel);
            if hang {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }

    fn next<'a>(
        &'a self,
        _request_id: &'a str,
        _end: PassEnd,
        _delivery: &'a Delivery,
    ) -> HostFuture<'a, Next> {
        self.next_calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { Next::Done })
    }
}

struct Process {
    inbox: TurnInbox<OneLog>,
    pump: HostFuture<'static, ()>,
    _runtime: Nanocode,
}

async fn open(
    disk: &Arc<Disk>,
    wire: &Arc<Wire>,
    host: &Arc<Inbox>,
    durable: bool,
) -> Result<Process, String> {
    let log_host: Arc<dyn nanocode::turnlog::LogHost> = Arc::new(SimHost::new(Arc::clone(disk)));
    let policy = nanocode::turnlog::Policy {
        effects: nanocode::turnlog::Effects::default(),
        max_row_bytes: None,
        restarts: u64::MAX,
        retain_finished: None,
        snapshot_ratio: 2,
        snapshot_rows: None,
    };
    let parts = nanocode::turnlog(log_host, "sim", policy)
        .await
        .map_err(|error| format!("turn log: {error}"))?;
    if durable {
        parts.log.settle_durably();
    }
    let claude = ClaudeClient::messages_api(
        "http://model.sim/v1",
        "sk-sim",
        Duration::from_secs(10),
        Duration::from_secs(120),
    )
    .map_err(|error| format!("claude client: {error}"))?
    .with_transport(WireTransport(Arc::clone(wire)));
    let tools = Tools::builder()
        .without_defaults()
        .tool(nanocode::FunctionTool::new(
            LOOKUP,
            "Look something up",
            json!({"type": "object", "properties": {}}),
            Arc::new(Lookup),
        ))
        .effect_gate(Arc::clone(&parts.gate))
        .cell_journals(parts.journals.clone())
        .build()
        .map_err(|error| format!("tools: {error}"))?;
    let runtime = Nanocode::builder()
        .claude(nanocode::ClaudeHarness::new(claude))
        .tools(tools)
        .runner_management(false)
        .thinking(Thinking::Low)
        .workspace(PathBuf::from("/nonexistent/nanocode-sim"))
        .durability(parts.store.clone())
        .build()
        .map_err(|error| format!("runtime build: {error}"))?;
    let record = SessionRecord::root(
        SESSION,
        ModelSelection::new(HarnessFamily::Claude, MODEL),
        None,
    );
    let host: Arc<dyn InboxHost> = Arc::clone(host) as Arc<dyn InboxHost>;
    let (inbox, pump) = TurnInbox::open(
        (*runtime).clone(),
        &parts.store,
        record,
        Arc::clone(&parts.log),
        host,
        Duration::from_millis(5),
    )
    .await
    .map_err(|error| format!("open inbox: {error}"))?;
    Ok(Process {
        inbox,
        pump,
        _runtime: runtime,
    })
}

async fn pumped<T>(pump: &mut HostFuture<'static, ()>, work: impl Future<Output = T>) -> T {
    let mut work = pin!(work);
    let mut pumping = true;
    std::future::poll_fn(|context| {
        if pumping && pump.as_mut().poll(context).is_ready() {
            pumping = false;
        }
        work.as_mut().poll(context)
    })
    .await
}

async fn alongside<T>(work: impl Future<Output = T>, side: impl Future<Output = ()>) -> T {
    let mut work = pin!(work);
    let mut side = pin!(side);
    let mut siding = true;
    std::future::poll_fn(|context| {
        if siding && side.as_mut().poll(context).is_ready() {
            siding = false;
        }
        match work.as_mut().poll(context) {
            Poll::Ready(value) => Poll::Ready(value),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

fn submission(request_id: &str, prompt: &str) -> Submission {
    Submission {
        request_id: request_id.to_owned(),
        prompt: Prompt::new(prompt),
        hand: None,
        metadata: serde_json::value::to_raw_value(&json!({})).expect("an empty object encodes"),
    }
}

fn first_prompt() -> String {
    format!("Summarize the delegated work. Child results so far: {CHILD_A}")
}

fn text_of(prompt: &Prompt) -> String {
    serde_json::to_string(prompt).unwrap_or_default()
}

#[derive(Default)]
struct Run {
    wrong: Vec<String>,
    epochs: u64,
    crashes: u64,
}

impl Run {
    fn check(&mut self, ok: bool, what: impl Into<String>) {
        if !ok {
            self.wrong.push(what.into());
        }
    }
}

enum Step {
    Again,
    Done,
}

struct World {
    durable: bool,
    disk: Arc<Disk>,
    wire: Arc<Wire>,
    host: Arc<Inbox>,
    app: App,
}

type EpochStep<'s> = dyn for<'p> Fn(
        u64,
        &'p mut Process,
        &'p World,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Step, String>> + 'p>>
    + 's;

fn run_epochs(
    seed: u64,
    clock: &Arc<Clock>,
    world: &World,
    case: Case,
    run: &mut Run,
    step: &EpochStep<'_>,
) {
    for number in 0..EPOCHS_MAX {
        run.epochs += 1;
        let result = std::cell::RefCell::new(None);
        let mut executor = Executor::new(
            Arc::clone(clock),
            Rng::derive(seed, &format!("after_pass {case:?} {number}")),
        );
        let task: Task<'_> = Box::pin(async {
            let outcome = match open(&world.disk, &world.wire, &world.host, world.durable).await {
                Ok(mut process) => {
                    let mut pump = std::mem::replace(&mut process.pump, Box::pin(async {}));
                    let work = step(number, &mut process, world);
                    pumped(&mut pump, work).await
                }
                Err(error) => Err(error),
            };
            result.replace(Some(outcome));
        });
        let crash_at = clock.now_ns() + u64::try_from(CRASH_AFTER.as_nanos()).unwrap_or(u64::MAX);
        let ended = executor.run(vec![task], Some(crash_at));
        drop(executor);
        drop_spawned();
        match (ended, result.into_inner()) {
            (Ended::Finished, Some(Ok(Step::Done))) => return,
            (Ended::Finished, Some(Ok(Step::Again))) => {}
            (Ended::Finished, Some(Err(error))) => {
                run.wrong.push(format!("epoch {number}: {error}"));
                return;
            }
            (Ended::Crashed | Ended::Stuck, _) => run.crashes += 1,
            (Ended::Finished, None) => {
                run.wrong
                    .push(format!("epoch {number} ended without a result"));
                return;
            }
        }
    }
    run.wrong
        .push(format!("still open after {EPOCHS_MAX} epochs"));
}

fn drop_spawned() {
    loop {
        let spawned = crate::host::spawned();
        if spawned.is_empty() {
            return;
        }
        drop(spawned);
    }
}

fn driven(result: Result<Driven, PolicyError>) -> Result<Step, String> {
    match result {
        Ok(Driven::Finished(_) | Driven::Resumed(_) | Driven::Idle) => Ok(Step::Done),
        Ok(Driven::Waiting(_) | Driven::Interrupted(_) | Driven::Busy) => Ok(Step::Again),
        Err(error) => Err(error.to_string()),
    }
}

fn counted(body: &str, marker: &str) -> usize {
    body.matches(marker).count()
}

async fn events(inbox: &TurnInbox<OneLog>) -> Result<Vec<LoggedEvent>, String> {
    Ok(inbox
        .log()
        .page(0, 10_000)
        .await
        .map_err(|error| format!("log page: {error}"))?
        .events)
}

fn results(events: &[LoggedEvent], request_id: &str) -> Vec<String> {
    events
        .iter()
        .filter(|event| event.request_id.as_deref() == Some(request_id))
        .map(|event| event.event.get().to_owned())
        .filter(|body| body.contains("\"type\":\"result\""))
        .collect()
}

struct Seen {
    record: Option<TurnRecord>,
    pass: Option<PassRecord>,
    admitted: BTreeMap<String, Option<String>>,
    results: Vec<String>,
    stray: usize,
}

async fn inspect(
    inbox: &TurnInbox<OneLog>,
    request_id: &str,
    passes: &[String],
) -> Result<Seen, String> {
    let record = inbox
        .log()
        .turn(request_id)
        .await
        .map_err(|error| format!("turn: {error}"))?;
    let pass = inbox
        .pass_record(request_id)
        .await
        .map_err(|error| format!("pass record: {error}"))?;
    let mut admitted = BTreeMap::new();
    for id in passes {
        let prompt = inbox
            .admitted_pass(id)
            .await
            .map_err(|error| format!("admitted pass {id}: {error}"))?;
        admitted.insert(id.clone(), prompt.as_ref().map(text_of));
    }
    let events = events(inbox).await?;
    let stray = events
        .iter()
        .filter(|event| {
            event
                .request_id
                .as_ref()
                .is_some_and(|id| passes.contains(id))
        })
        .count();
    Ok(Seen {
        record,
        pass,
        admitted,
        results: results(&events, request_id),
        stray,
    })
}

fn bodies(world: &World) -> Vec<String> {
    world.wire.bodies.read(Vec::clone)
}

fn check_two_passes(
    run: &mut Run,
    world: &World,
    request_id: &str,
    seen: &Seen,
    live: bool,
    retried: bool,
) {
    let bodies = bodies(world);
    let continuation = format!("{request_id}/pass-2");
    run.check(
        seen.stray == 0,
        format!(
            "{} events of the continuation were not attributed to its request",
            seen.stray
        ),
    );
    let first_pass = bodies
        .iter()
        .filter(|body| counted(body, CHILD_B) == 0)
        .count();
    run.check(
        first_pass == 1,
        format!("the first pass reached the model {first_pass} times"),
    );
    run.check(
        retried || bodies.len() == 3,
        format!("the model saw {} requests instead of three", bodies.len()),
    );
    if let Some(first) = bodies.first() {
        run.check(
            counted(first, CHILD_A) == 1 && counted(first, CHILD_B) == 0,
            format!(
                "the first pass carried A {} times and B {} times",
                counted(first, CHILD_A),
                counted(first, CHILD_B)
            ),
        );
    }
    for (index, body) in bodies.iter().enumerate() {
        run.check(
            counted(body, CHILD_A) <= 1 && counted(body, CHILD_B) <= 1,
            format!("model request {index} repeated a child result"),
        );
    }
    run.check(
        bodies.iter().any(|body| counted(body, CHILD_B) == 1),
        "no pass carried the late child result B",
    );
    let (decisions, returned, acked, next_calls) = world.app.db(|db| {
        (
            db.decisions.clone(),
            db.returned.clone(),
            db.acked.clone(),
            world.host.next_calls.load(Ordering::Acquire),
        )
    });
    run.check(
        next_calls == 0,
        format!("InboxHost::next ran {next_calls} times for a policy request"),
    );
    let first = decisions.get(&(request_id.to_owned(), request_id.to_owned()));
    let expected = Saved::Continue {
        pass_id: continuation.clone(),
        prompt: format!("Child results arrived: {CHILD_B}"),
        carries: vec![CHILD_B.to_owned()],
    };
    run.check(
        first == Some(&expected),
        format!("the first pass's saved decision is {first:?}"),
    );
    run.check(
        returned
            .iter()
            .filter(|(pass, _)| pass == request_id)
            .all(|(_, saved)| saved == &expected),
        format!("a replay returned another continuation: {returned:?}"),
    );
    run.check(
        decisions.get(&(request_id.to_owned(), continuation.clone())) == Some(&Saved::Finish),
        "the continuation did not finish the request",
    );
    run.check(
        acked.contains(CHILD_A) && acked.contains(CHILD_B),
        format!("acknowledged {acked:?}"),
    );
    let admitted = seen.admitted.get(&continuation).cloned().flatten();
    run.check(
        admitted
            == Some(text_of(&Prompt::new(format!(
                "Child results arrived: {CHILD_B}"
            )))),
        format!("the runtime admitted {admitted:?} under {continuation}"),
    );
    match &seen.pass {
        Some(pass) => {
            run.check(
                pass.passes == vec![request_id.to_owned(), continuation],
                format!("the pass record lists {:?}", pass.passes),
            );
            run.check(
                pass.phase
                    == PassPhase::Closed {
                        outcome: PassOutcome::Completed {
                            message: "answer-with-B".to_owned(),
                        },
                    },
                format!("the pass record ended {:?}", pass.phase),
            );
        }
        None => run.check(false, "the request has no pass record"),
    }
    if live {
        run.check(
            seen.record.is_none(),
            "a live request created an inbox turn record",
        );
    } else {
        run.check(
            seen.record.as_ref().map(|record| record.state) == Some(TurnState::Completed),
            format!(
                "the inbox record is {:?}",
                seen.record.as_ref().map(|record| record.state)
            ),
        );
        run.check(
            seen.results.len() == 1 && seen.results[0].contains("answer-with-B"),
            format!(
                "the inbox recorded {} results: {:?}",
                seen.results.len(),
                seen.results
            ),
        );
    }
}

fn simulate_case(
    seed: u64,
    mutation: AfterPassMutation,
    case: Case,
    durable: bool,
) -> (Run, u64, u64) {
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let db = Arc::new(Atom::new(Db {
        released: vec![CHILD_A.to_owned()],
        carried: BTreeMap::from([(CHILD_A.to_owned(), "req".to_owned())]),
        ..Db::default()
    }));
    let world = World {
        durable,
        disk: Disk::new(),
        wire: Arc::new(Wire::default()),
        host: Arc::new(Inbox::default()),
        app: App {
            db: Arc::clone(&db),
            mutation,
        },
    };
    let mut run = Run::default();
    let request = "req";
    let continuation = format!("{request}/pass-2");
    match case {
        Case::Continue(crash) => {
            world.app.db(|db| db.crash = Some(crash));
            if crash == Crash::WhileClosing {
                world.host.hang_result.store(true, Ordering::Release);
            }
            if crash == Crash::DuringContinuation {
                world
                    .wire
                    .hang
                    .set(vec![CHILD_B.to_owned(), TOOLED.to_owned()]);
            }
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|number, process, world| {
                    Box::pin(async move {
                        if number == 0 {
                            process
                                .inbox
                                .log()
                                .submit(&submission(request, &first_prompt()))
                                .await
                                .map_err(|error| format!("submit: {error}"))?;
                        } else {
                            world.wire.hang.set(Vec::new());
                        }
                        let result = process.inbox.drive_with_policy(None, &world.app).await;
                        let open = process
                            .inbox
                            .log()
                            .turn(request)
                            .await
                            .map_err(|error| error.to_string())?
                            .is_some_and(|record| !record.state.is_terminal());
                        match (driven(result)?, open) {
                            (Step::Done, true) => Err(format!(
                                "epoch {number}: drive_with_policy returned with the request still open"
                            )),
                            (step, _) => Ok(step),
                        }
                    })
                },
            );
            let seen = verify(
                seed,
                &clock,
                &world,
                case,
                request,
                std::slice::from_ref(&continuation),
                &mut run,
            );
            if let Some(seen) = seen {
                check_two_passes(
                    &mut run,
                    &world,
                    request,
                    &seen,
                    false,
                    matches!(crash, Crash::AfterAdmission | Crash::DuringContinuation),
                );
            }
            if crash != Crash::None {
                run.check(run.crashes >= 1, "the crash never happened");
            }
        }
        Case::LiveRacingDrive => {
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|_, process, world| {
                    Box::pin(async move {
                        let inbox = &process.inbox;
                        let failures = std::cell::RefCell::new(Vec::new());
                        let racing = async {
                            loop {
                                nanocode_platform::sleep(Duration::from_millis(7)).await;
                                if let Err(error) = inbox.drive_with_policy(None, &world.app).await
                                {
                                    failures.borrow_mut().push(error.to_string());
                                }
                            }
                        };
                        let turn = inbox.turn_with_policy(
                            request,
                            PromptRequest::new(first_prompt()).request_id(request),
                            Vec::new(),
                            &world.app,
                        );
                        let ended = alongside(turn, racing).await;
                        let failures = failures.into_inner();
                        match ended {
                            Ok(PolicyTurn::Finished(_)) if failures.is_empty() => Ok(Step::Done),
                            Ok(PolicyTurn::Finished(_)) => {
                                Err(format!("a racing drive failed: {failures:?}"))
                            }
                            Ok(_) => Err("the live request did not finish".to_owned()),
                            Err(error) => Err(error.to_string()),
                        }
                    })
                },
            );
            if let Some(seen) = verify(
                seed,
                &clock,
                &world,
                case,
                request,
                std::slice::from_ref(&continuation),
                &mut run,
            ) {
                check_two_passes(&mut run, &world, request, &seen, true, false);
                let calls = world.app.db(|db| db.after_pass_calls);
                run.check(
                    calls == 2,
                    format!("after_pass ran {calls} times for two passes"),
                );
            }
        }
        Case::Live(crash) => {
            world.app.db(|db| db.crash = Some(crash));
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|number, process, world| {
                    Box::pin(async move {
                        if number == 0 {
                            let request = PromptRequest::new(first_prompt()).request_id(request);
                            return match process
                                .inbox
                                .turn_with_policy(
                                    request.request_id.clone().unwrap_or_default().as_str(),
                                    request,
                                    Vec::new(),
                                    &world.app,
                                )
                                .await
                            {
                                Ok(PolicyTurn::Finished(_)) => Ok(Step::Done),
                                Ok(_) => Ok(Step::Again),
                                Err(error) => Err(error.to_string()),
                            };
                        }
                        driven(process.inbox.drive_with_policy(None, &world.app).await)
                    })
                },
            );
            let seen = verify(
                seed,
                &clock,
                &world,
                case,
                request,
                std::slice::from_ref(&continuation),
                &mut run,
            );
            if let Some(seen) = seen {
                check_two_passes(&mut run, &world, request, &seen, true, false);
            }
        }
        Case::HostError => {
            world.app.db(|db| db.fail_next = true);
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|number, process, world| {
                    Box::pin(async move {
                        if number > 0 {
                            return driven(process.inbox.drive_with_policy(None, &world.app).await);
                        }
                        process
                            .inbox
                            .log()
                            .submit(&submission(request, &first_prompt()))
                            .await
                            .map_err(|error| format!("submit: {error}"))?;
                        let failed = process.inbox.drive_with_policy(None, &world.app).await;
                        let error = matches!(failed, Err(PolicyError::Host(_)));
                        let record = process
                            .inbox
                            .log()
                            .turn(request)
                            .await
                            .map_err(|error| error.to_string())?;
                        let pass = process
                            .inbox
                            .pass_record(request)
                            .await
                            .map_err(|error| error.to_string())?;
                        let calls = bodies(world).len();
                        if !error
                            || record.map(|record| record.state) != Some(TurnState::Running)
                            || !matches!(pass.map(|pass| pass.phase), Some(PassPhase::Ended { .. }))
                        {
                            return Err("a host error closed or lost the open request".to_owned());
                        }
                        let retried =
                            driven(process.inbox.drive_with_policy(None, &world.app).await)?;
                        let more = bodies(world).len() - calls;
                        if more != 2 {
                            return Err(format!(
                                "the retry ran {more} model calls instead of the continuation's two"
                            ));
                        }
                        Ok(retried)
                    })
                },
            );
            if let Some(seen) = verify(
                seed,
                &clock,
                &world,
                case,
                request,
                std::slice::from_ref(&continuation),
                &mut run,
            ) {
                check_two_passes(&mut run, &world, request, &seen, false, false);
            }
        }
        Case::CancelBeforeAdmission => {
            world
                .app
                .db(|db| db.crash = Some(Crash::AfterDecisionSaved));
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|number, process, world| {
                    Box::pin(async move {
                        if number == 0 {
                            process
                                .inbox
                                .log()
                                .submit(&submission(request, &first_prompt()))
                                .await
                                .map_err(|error| format!("submit: {error}"))?;
                            return driven(process.inbox.drive_with_policy(None, &world.app).await);
                        }
                        process
                            .inbox
                            .cancel(request, &Map::new())
                            .await
                            .map_err(|error| format!("cancel: {error}"))?;
                        driven(process.inbox.drive_with_policy(None, &world.app).await)
                    })
                },
            );
            if let Some(seen) = verify(
                seed,
                &clock,
                &world,
                case,
                request,
                std::slice::from_ref(&continuation),
                &mut run,
            ) {
                run.check(
                    seen.record.as_ref().map(|record| record.state) == Some(TurnState::Cancelled),
                    format!(
                        "the request ended {:?}",
                        seen.record.as_ref().map(|record| record.state)
                    ),
                );
                run.check(
                    seen.admitted
                        .get(&continuation)
                        .cloned()
                        .flatten()
                        .is_none(),
                    "a cancelled request admitted its continuation",
                );
                run.check(
                    bodies(&world).len() == 1,
                    "a cancelled request ran another pass",
                );
                run.check(
                    seen.results.len() == 1,
                    format!("{} results", seen.results.len()),
                );
                let acked = world.app.db(|db| db.acked.contains(CHILD_B));
                run.check(!acked, "B was acknowledged without being admitted");
            }
        }
        Case::CancelAfterAdmission => {
            world.app.db(|db| db.cancel_at = Some(continuation.clone()));
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|_, process, world| {
                    Box::pin(async move {
                        process
                            .inbox
                            .log()
                            .submit(&submission(request, &first_prompt()))
                            .await
                            .map_err(|error| format!("submit: {error}"))?;
                        let inbox = &process.inbox;
                        let cancel = async {
                            loop {
                                if world.app.db(|db| std::mem::take(&mut db.cancel_now)) {
                                    drop(inbox.cancel(request, &Map::new()).await);
                                    return;
                                }
                                nanocode_platform::sleep(Duration::from_millis(1)).await;
                            }
                        };
                        driven(alongside(inbox.drive_with_policy(None, &world.app), cancel).await)
                    })
                },
            );
            if let Some(seen) = verify(
                seed,
                &clock,
                &world,
                case,
                request,
                std::slice::from_ref(&continuation),
                &mut run,
            ) {
                run.check(
                    seen.record.as_ref().map(|record| record.state) == Some(TurnState::Cancelled),
                    format!(
                        "the request ended {:?}",
                        seen.record.as_ref().map(|record| record.state)
                    ),
                );
                run.check(
                    seen.admitted
                        .get(&continuation)
                        .cloned()
                        .flatten()
                        .is_some(),
                    "the continuation was never admitted",
                );
                let (calls, continued) = world.app.db(|db| {
                    (
                        db.after_pass_calls,
                        db.decisions.keys().any(|(_, pass)| pass == &continuation),
                    )
                });
                run.check(
                    calls == 1 && !continued,
                    format!("after_pass ran {calls} times"),
                );
                run.check(
                    seen.results.len() == 1,
                    format!("{} results", seen.results.len()),
                );
                run.check(
                    seen.pass.as_ref().map(|pass| pass.passes.len()) == Some(2),
                    "a cancelled request kept continuing",
                );
            }
        }
        Case::CancelAfterAdmittedCrash => {
            world.app.db(|db| db.crash = Some(Crash::AfterAdmission));
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|number, process, world| {
                    Box::pin(async move {
                        if number == 0 {
                            process
                                .inbox
                                .log()
                                .submit(&submission(request, &first_prompt()))
                                .await
                                .map_err(|error| format!("submit: {error}"))?;
                            return driven(process.inbox.drive_with_policy(None, &world.app).await);
                        }
                        process
                            .inbox
                            .cancel(request, &Map::new())
                            .await
                            .map_err(|error| format!("cancel: {error}"))?;
                        driven(process.inbox.drive_with_policy(None, &world.app).await)
                    })
                },
            );
            if let Some(seen) = verify(
                seed,
                &clock,
                &world,
                case,
                request,
                std::slice::from_ref(&continuation),
                &mut run,
            ) {
                run.check(run.crashes == 1, format!("{} crashes", run.crashes));
                run.check(
                    seen.record.as_ref().map(|record| record.state) == Some(TurnState::Cancelled),
                    format!(
                        "the request ended {:?}",
                        seen.record.as_ref().map(|record| record.state)
                    ),
                );
                run.check(
                    seen.admitted
                        .get(&continuation)
                        .cloned()
                        .flatten()
                        .is_some(),
                    "a pass recorded as admitted was dropped instead of admitted and cancelled",
                );
                let (acked, continued) = world.app.db(|db| {
                    (
                        db.acked.contains(CHILD_B),
                        db.decisions.keys().any(|(_, pass)| pass == &continuation),
                    )
                });
                run.check(
                    acked,
                    "the host never learned the continuation was admitted",
                );
                run.check(
                    !continued,
                    "a cancelled request asked after_pass about its continuation",
                );
                run.check(
                    seen.results.len() == 1,
                    format!("{} results", seen.results.len()),
                );
            }
        }
        Case::Orphan => {
            world
                .wire
                .hang
                .set(vec![ORPHAN.to_owned(), TOOLED.to_owned()]);
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|number, process, world| {
                    Box::pin(async move {
                        if number == 0 {
                            let turn = process
                                .inbox
                                .agent()
                                .prompt(PromptRequest::new(ORPHAN).request_id("orphan"))
                                .await
                                .map_err(|error| format!("orphan prompt: {error}"))?;
                            drop(turn.await);
                            return Err("the orphan's model call returned".to_owned());
                        }
                        world.wire.hang.set(Vec::new());
                        let pending = process
                            .inbox
                            .admitted_pass("orphan")
                            .await
                            .map_err(|error| error.to_string())?;
                        if world.durable && pending.is_none() {
                            return Err(
                                "the orphan operation was not durable before the crash".to_owned()
                            );
                        }
                        let before = world.app.db(|db| db.after_pass_calls);
                        let result =
                            driven(process.inbox.drive_with_policy(None, &world.app).await)?;
                        let record = process
                            .inbox
                            .log()
                            .turn("orphan")
                            .await
                            .map_err(|error| error.to_string())?;
                        let pass = process
                            .inbox
                            .pass_record("orphan")
                            .await
                            .map_err(|error| error.to_string())?;
                        let after = world.app.db(|db| db.after_pass_calls);
                        if record.is_some() || pass.is_some() || after != before {
                            return Err(format!(
                                "recovery of an operation without a request created a request or ran after_pass ({} calls)",
                                after - before
                            ));
                        }
                        Ok(result)
                    })
                },
            );
            run.check(run.crashes == 1, format!("{} crashes", run.crashes));
        }
        Case::Legacy => {
            world
                .app
                .db(|db| db.crash = Some(Crash::BeforeDecisionSaved));
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|number, process, world| {
                    Box::pin(async move {
                        let log = process.inbox.log();
                        match number {
                            0 => {
                                log.submit(&submission("old", &first_prompt()))
                                    .await
                                    .map_err(|error| format!("submit: {error}"))?;
                                let old = process
                                    .inbox
                                    .drive(None)
                                    .await
                                    .map_err(|error| error.to_string())?;
                                let record = match old {
                                    Driven::Finished(record) => record,
                                    _ => {
                                        return Err(
                                            "drive did not finish the old request".to_owned()
                                        );
                                    }
                                };
                                let pass = process
                                    .inbox
                                    .pass_record("old")
                                    .await
                                    .map_err(|error| error.to_string())?;
                                process
                                    .inbox
                                    .turn(
                                        "old-live",
                                        PromptRequest::new("plain question").request_id("old-live"),
                                        Vec::new(),
                                    )
                                    .await;
                                let live = process
                                    .inbox
                                    .pass_record("old-live")
                                    .await
                                    .map_err(|error| error.to_string())?;
                                let next = world.host.next_calls.load(Ordering::Acquire);
                                if record.state != TurnState::Completed
                                    || pass.is_some()
                                    || live.is_some()
                                    || next != 1
                                {
                                    return Err(format!(
                                        "drive changed: {:?}, pass record {}, next ran {next} times",
                                        record.state,
                                        pass.is_some()
                                    ));
                                }
                                log.submit(&submission(request, &first_prompt()))
                                    .await
                                    .map_err(|error| format!("submit: {error}"))?;
                                driven(process.inbox.drive_with_policy(None, &world.app).await)
                            }
                            1 => {
                                if process.inbox.drive(None).await.is_ok() {
                                    return Err(
                                        "drive ran a request created with an after-pass policy"
                                            .to_owned(),
                                    );
                                }
                                driven(process.inbox.drive_with_policy(None, &world.app).await)
                            }
                            _ => driven(process.inbox.drive_with_policy(None, &world.app).await),
                        }
                    })
                },
            );
            if let Some(seen) = verify(
                seed,
                &clock,
                &world,
                case,
                request,
                std::slice::from_ref(&continuation),
                &mut run,
            ) {
                run.check(
                    seen.record.as_ref().map(|record| record.state) == Some(TurnState::Completed),
                    "the policy request did not complete",
                );
            }
        }
        Case::Steered => {
            run_epochs(
                seed,
                &clock,
                &world,
                case,
                &mut run,
                &|_, process, world| {
                    Box::pin(async move {
                        process
                            .inbox
                            .log()
                            .submit(&submission(request, &first_prompt()))
                            .await
                            .map_err(|error| format!("submit: {error}"))?;
                        let inbox = &process.inbox;
                        let steer = async {
                            loop {
                                if !bodies(world).is_empty() {
                                    drop(
                                        inbox
                                            .steer(
                                                Some(request),
                                                STEERED,
                                                Prompt::new("one more thing"),
                                            )
                                            .await,
                                    );
                                    return;
                                }
                                nanocode_platform::sleep(Duration::from_millis(1)).await;
                            }
                        };
                        driven(alongside(inbox.drive_with_policy(None, &world.app), steer).await)
                    })
                },
            );
            let seen = world.app.db(|db| db.seen.clone());
            let read = seen
                .first()
                .map(|pass| pass.read.clone())
                .unwrap_or_default();
            let consumed = bodies(&world)
                .iter()
                .any(|body| body.contains("one more thing"));
            run.check(consumed, "the model never saw the steered input");
            run.check(
                read.contains(&STEERED.to_owned()) == consumed,
                format!("the first pass reported read {read:?} while the model saw the steer: {consumed}"),
            );
        }
    }
    let (model_calls, decisions) = (
        u64::try_from(bodies(&world).len()).unwrap_or(u64::MAX),
        world.app.db(|db| db.decisions.len() as u64),
    );
    (run, model_calls, decisions)
}

fn verify(
    seed: u64,
    clock: &Arc<Clock>,
    world: &World,
    case: Case,
    request: &'static str,
    passes: &[String],
    run: &mut Run,
) -> Option<Seen> {
    if !run.wrong.is_empty() {
        return None;
    }
    let result = std::cell::RefCell::new(None);
    let mut executor = Executor::new(
        Arc::clone(clock),
        Rng::derive(seed, &format!("after_pass verify {case:?}")),
    );
    let task: Task<'_> = Box::pin(async {
        let seen = match open(&world.disk, &world.wire, &world.host, world.durable).await {
            Ok(mut process) => {
                let mut pump = std::mem::replace(&mut process.pump, Box::pin(async {}));
                pumped(&mut pump, inspect(&process.inbox, request, passes)).await
            }
            Err(error) => Err(error),
        };
        result.replace(Some(seen));
    });
    let crash_at = clock.now_ns() + u64::try_from(CRASH_AFTER.as_nanos()).unwrap_or(u64::MAX);
    executor.run(vec![task], Some(crash_at));
    drop(executor);
    drop_spawned();
    match result.into_inner() {
        Some(Ok(seen)) => Some(seen),
        Some(Err(error)) => {
            run.wrong.push(format!("verify: {error}"));
            None
        }
        None => {
            run.wrong.push("verify did not finish".to_owned());
            None
        }
    }
}

struct Outcome {
    digest: u64,
    stats: AfterPassStats,
    wrong: Option<String>,
}

fn simulate(seed: u64, mutation: AfterPassMutation) -> Outcome {
    let mut stats = AfterPassStats {
        seeds: 1,
        ..AfterPassStats::default()
    };
    let mut digest = FNV_OFFSET;
    let mut wrong = Vec::new();
    for (case, durable) in CASES
        .into_iter()
        .flat_map(|case| [(case, false), (case, true)])
    {
        let (run, model_calls, decisions) = simulate_case(seed, mutation, case, durable);
        stats.cases += 1;
        stats.epochs += run.epochs;
        stats.crashes += run.crashes;
        stats.model_calls += model_calls;
        stats.decisions += decisions;
        digest = fnv(
            digest,
            format!(
                "{case:?} {durable} {} {} {model_calls} {decisions}",
                run.epochs, run.crashes
            )
            .as_bytes(),
        );
        if !run.wrong.is_empty() {
            let settles = if durable { "durable" } else { "lazy" };
            wrong.push(format!("{case:?} {settles}: {}", run.wrong.join("; ")));
        }
    }
    Outcome {
        digest,
        stats,
        wrong: (!wrong.is_empty()).then(|| wrong.join(" | ")),
    }
}

pub fn judge(
    seed: u64,
    mutation: AfterPassMutation,
    twice: bool,
    verbose: bool,
    totals: &mut AfterPassStats,
) -> Option<String> {
    let first = simulate(seed, mutation);
    totals.add(&first.stats);
    if verbose {
        println!("seed {seed} digest {:016x} {:?}", first.digest, first.stats);
    }
    if first.wrong.is_some() || !twice {
        return first.wrong;
    }
    let second = simulate(seed, mutation);
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "nondeterminism: digest {:016x} then {:016x}",
            first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: AfterPassMutation) -> String {
    let mut command = format!("nanocode-sim --scenario after_pass --seed {seed}");
    if mutation != AfterPassMutation::None {
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
    mutation: AfterPassMutation,
    totals: &AfterPassStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=after_pass mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        AfterPassMutation::None if failed == 0 => ExitCode::SUCCESS,
        AfterPassMutation::None => ExitCode::FAILURE,
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
