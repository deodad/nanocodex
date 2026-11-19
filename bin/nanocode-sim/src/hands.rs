use std::{
    cell::RefCell,
    collections::BTreeMap,
    ops::Range,
    process::ExitCode,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use nanocode::turnlog::{Compaction, LogHost, OneLog, Policy, Rider, Write};
use nanocode_code_mode::contract::DEFAULT_TOOL_OUTPUT_TOKENS;
use nanocode_code_mode::embedded::{CodeModeHostError, HandCalls, HostFuture};
use nanocode_code_mode::remote::{
    Carried, CodeModeExecutors, NOT_RECEIVED, PEER_CLOSED, RemoteBarrier, RemoteCatalog,
    RemoteCodeModeHost, RemoteContext, RemoteDelivery, RemoteFrame, RemoteLink, RemoteOperation,
    RemoteReply, RemoteTransport, output_to_wire,
};
use nanocode_code_mode::{ToolContext, ToolOutput, ToolOutputBody};
use nanocode_durability_core::{
    OwnerId, Settle, StateCommit, StateStore, StoreError, StoreFuture, SummaryUpdate,
};
use nanocode_http::StatusCode;
use nanocode_sync::FutureExt;
use nanocode_sync::{Atom, Owned, mpsc};
use nanocode_tool_runtime::remote::accept::{
    Acceptor, AcceptorOptions, AcceptorRoute, HandRequests, PendingRequest, Refusal,
};
use serde_json::{Value, json};

use crate::{
    executor::{Clock, Ended, Executor, Task},
    rng::{FNV_OFFSET, Rng, fnv},
};

const KEY: &str = "hand";
const SESSION: &str = "session";
const TURN: &str = "turn";
const SCOPE: &str = "hands";
const SENT: &str = "hand_request_sent";
const REPLIED: &str = "hand_request_replied";
const ADMITTED: &str = "admitted";
const ANSWERED: &str = "answered";
const SNAPSHOT_RATIO: usize = 2;
const CALLS_MAX: u64 = 5;
const START_MS_MAX: u64 = 300;
const RUN_MS_MAX: u64 = 400;
const COMMIT_MS_MAX: u64 = 20;
const CRASHES_MAX: u64 = 3;
const DROPS_MAX: u64 = 3;
const EVENT_MS_MAX: u64 = 1_500;
const RESTART_PPM: u64 = 300_000;
const REDIAL_MS_MAX: u64 = 40;
const WRITE_MS_MAX: u64 = 3;
const TICK: Duration = Duration::from_millis(1);
const GRACE: Duration = Duration::from_secs(5);
const SETTLE_HORIZON_MS: u64 = 60_000;
const FRAME_BYTES_MAX: usize = 1 << 20;
const RUNTIMES_MAX: usize = 4;
const ATTACHES_QUEUED_MAX: usize = 64;
const MS: u64 = 1_000_000;
const FLEET_MUTATED: usize = 3;
const FLEET_COMMIT_MS_MAX: u64 = 200;
const UNSENT: [&str; 3] = [
    "not dispatched",
    "never dispatched",
    "nothing was dispatched",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandsMutation {
    None,
    AckBeforeStore,
    SkipRecord,
    ForgetPending,
    CrossReplies,
    MangleRuntime,
    CarryDropsLog,
    CarryDropsRequest,
    DispatchUnsettled,
    RideDropsReply,
    FleetUnbarriered,
    FleetCrossHands,
    FleetRecoverNone,
    FleetSwallowUnsent,
}

impl HandsMutation {
    const ALL: [Self; 14] = [
        Self::None,
        Self::AckBeforeStore,
        Self::SkipRecord,
        Self::ForgetPending,
        Self::CrossReplies,
        Self::MangleRuntime,
        Self::CarryDropsLog,
        Self::CarryDropsRequest,
        Self::DispatchUnsettled,
        Self::RideDropsReply,
        Self::FleetUnbarriered,
        Self::FleetCrossHands,
        Self::FleetRecoverNone,
        Self::FleetSwallowUnsent,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::AckBeforeStore => "ack_before_store",
            Self::SkipRecord => "skip_record",
            Self::ForgetPending => "forget_pending",
            Self::CrossReplies => "cross_replies",
            Self::MangleRuntime => "mangle_runtime",
            Self::CarryDropsLog => "carry_drops_log",
            Self::CarryDropsRequest => "carry_drops_request",
            Self::DispatchUnsettled => "dispatch_unsettled",
            Self::RideDropsReply => "ride_drops_reply",
            Self::FleetUnbarriered => "fleet_unbarriered",
            Self::FleetCrossHands => "fleet_cross_hands",
            Self::FleetRecoverNone => "fleet_recover_none",
            Self::FleetSwallowUnsent => "fleet_swallow_unsent",
        }
    }

    const fn fleet(self, fleet: usize) -> usize {
        match self {
            Self::FleetUnbarriered
            | Self::FleetCrossHands
            | Self::FleetRecoverNone
            | Self::FleetSwallowUnsent
                if fleet == 1 =>
            {
                FLEET_MUTATED
            }
            _ => fleet,
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

#[derive(Debug, Default)]
pub struct HandsStats {
    pub seeds: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub drops: u64,
    pub calls: u64,
    pub recovered: u64,
    pub unknown: u64,
    pub folded: u64,
    pub rode: u64,
    pub replayed: u64,
    pub retained: u64,
}

impl HandsStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.crashes += other.crashes;
        self.restarts += other.restarts;
        self.drops += other.drops;
        self.calls += other.calls;
        self.recovered += other.recovered;
        self.unknown += other.unknown;
        self.folded += other.folded;
        self.rode += other.rode;
        self.replayed += other.replayed;
        self.retained += other.retained;
    }
}

struct Script {
    starts: Vec<u64>,
    runs: Vec<u64>,
    commits: Vec<u64>,
    crashes: Vec<u64>,
    hands: Vec<usize>,
    schedules: Vec<Schedule>,
}

struct Schedule {
    restarts: Vec<u64>,
    drops: Vec<u64>,
}

fn times(rng: &mut Rng, count: u64) -> Vec<u64> {
    let mut times: Vec<u64> = (0..count)
        .map(|_| rng.range(1, EVENT_MS_MAX) * MS)
        .collect();
    times.sort_unstable();
    times
}

impl Script {
    fn of(seed: u64, fleet: usize) -> Self {
        if fleet > 1 {
            return Self::fleet(seed, fleet);
        }
        let mut rng = Rng::derive(seed, "hands script");
        let calls = rng.range(1, CALLS_MAX);
        let starts: Vec<u64> = (0..calls)
            .map(|_| rng.range(0, START_MS_MAX) * MS)
            .collect();
        let runs = (0..calls).map(|_| rng.range(1, RUN_MS_MAX) * MS).collect();
        let commits = (0..calls)
            .map(|_| rng.range(0, COMMIT_MS_MAX) * MS)
            .collect();
        let crashes = rng.range(0, CRASHES_MAX);
        let crashes = times(&mut rng, crashes);
        let restarts = u64::from(rng.chance(RESTART_PPM));
        let restarts = times(&mut rng, restarts);
        let drops = rng.range(0, DROPS_MAX);
        let drops = times(&mut rng, drops);
        let hands = vec![0; starts.len()];
        Self {
            starts,
            runs,
            commits,
            crashes,
            hands,
            schedules: vec![Schedule { restarts, drops }],
        }
    }

    fn fleet(seed: u64, fleet: usize) -> Self {
        let mut rng = Rng::derive(seed, "hands fleet script");
        let width = u64::try_from(fleet).expect("a fleet size fits in 64 bits");
        let calls = rng.range(width, CALLS_MAX * width);
        let starts = (0..calls)
            .map(|_| rng.range(0, START_MS_MAX) * MS)
            .collect();
        let runs = (0..calls).map(|_| rng.range(1, RUN_MS_MAX) * MS).collect();
        let commits = (0..calls)
            .map(|_| rng.range(0, FLEET_COMMIT_MS_MAX) * MS)
            .collect();
        let hands = (0..calls).map(|_| rng.pick(fleet)).collect();
        let crashes = rng.range(0, CRASHES_MAX);
        let crashes = times(&mut rng, crashes);
        let schedules = (0..fleet)
            .map(|_| {
                let restarts = u64::from(rng.chance(RESTART_PPM));
                let restarts = times(&mut rng, restarts);
                let drops = rng.range(0, DROPS_MAX);
                let drops = times(&mut rng, drops);
                Schedule { restarts, drops }
            })
            .collect();
        Self {
            starts,
            runs,
            commits,
            crashes,
            hands,
            schedules,
        }
    }

    fn describe(&self) -> String {
        let restarts: usize = self.schedules.iter().map(|at| at.restarts.len()).sum();
        let drops: usize = self.schedules.iter().map(|at| at.drops.len()).sum();
        let shape = format!(
            "calls={} crashes={} restarts={restarts} drops={drops}",
            self.starts.len(),
            self.crashes.len(),
        );
        match self.schedules.len() {
            1 => shape,
            fleet => format!("{shape} hands={fleet}"),
        }
    }
}

#[derive(Clone)]
struct Row {
    id: u64,
    key: String,
    runtime: String,
    turn_id: Option<String>,
    reply: Option<RemoteReply>,
}

struct Store {
    rows: Atom<BTreeMap<(String, String), Row>>,
    writes: AtomicU64,
    seed: u64,
    mutation: HandsMutation,
}

impl Store {
    async fn write(&self) {
        let count = self.writes.fetch_add(1, Ordering::Relaxed);
        let delay = fnv(self.seed, &count.to_le_bytes()) % WRITE_MS_MAX + 1;
        nanocode_platform::sleep(Duration::from_millis(delay)).await;
    }

    fn insert(&self, request: &PendingRequest) {
        if self.mutation == HandsMutation::SkipRecord {
            return;
        }
        let runtime = if self.mutation == HandsMutation::MangleRuntime {
            String::new()
        } else {
            request.runtime.clone()
        };
        let row = Row {
            id: request.id,
            key: request.key.clone(),
            runtime,
            turn_id: request.turn_id.clone(),
            reply: None,
        };
        let at = (request.session_id.clone(), request.call_id.clone());
        self.rows.update(|rows| {
            rows.insert(at.clone(), row.clone());
        });
    }

    fn recorded(&self, session_id: &str, call_id: &str) -> bool {
        self.rows
            .read(|rows| rows.contains_key(&(session_id.to_owned(), call_id.to_owned())))
    }

    fn answer(&self, key: &str, runtime: &str, id: u64, reply: &RemoteReply) {
        let cross = self.mutation == HandsMutation::CrossReplies;
        let runtime = if self.mutation == HandsMutation::MangleRuntime {
            ""
        } else {
            runtime
        };
        self.rows.update(|rows| {
            for row in rows.values_mut() {
                let same = row.key == key && row.runtime == runtime;
                if same && (row.id == id || (cross && row.reply.is_none())) {
                    row.reply = Some(reply.clone());
                }
            }
        });
    }

    fn unanswered(&self, key: &str, id: u64) -> bool {
        self.rows.read(|rows| {
            rows.values()
                .any(|row| row.key == key && row.id == id && row.reply.is_none())
        })
    }
}

impl HandRequests for Store {
    fn sent<'a>(&'a self, request: &'a PendingRequest) -> HostFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.write().await;
            self.insert(request);
            Ok(())
        })
    }

    fn sent_after<'a>(
        &'a self,
        request: &'a PendingRequest,
        barrier: &'a dyn RemoteBarrier,
    ) -> HostFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let turn =
                serde_json::to_string(&request.turn_id).map_err(|error| error.to_string())?;
            let params = vec![
                request.session_id.clone(),
                request.call_id.clone(),
                request.id.to_string(),
                request.key.clone(),
                request.runtime.clone(),
                turn,
            ];
            let carried = Carried {
                host: std::ptr::from_ref(self) as usize,
                statements: vec![(SENT, params)],
            };
            if barrier.carry(carried).await? {
                return Ok(());
            }
            self.sent(request).await
        })
    }

    fn replied<'a>(
        &'a self,
        key: &'a str,
        runtime: &'a str,
        id: u64,
        reply: &'a RemoteReply,
    ) -> HostFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if self.mutation == HandsMutation::AckBeforeStore {
                return Ok(());
            }
            self.write().await;
            self.answer(key, runtime, id, reply);
            Ok(())
        })
    }

    fn rider(&self, key: &str, runtime: &str, id: u64, reply: &RemoteReply) -> Option<Carried> {
        if self.mutation == HandsMutation::AckBeforeStore {
            return None;
        }
        let reply = serde_json::to_string(reply).ok()?;
        Some(Carried {
            host: std::ptr::from_ref(self) as usize,
            statements: vec![(
                REPLIED,
                vec![key.to_owned(), runtime.to_owned(), id.to_string(), reply],
            )],
        })
    }

    fn pending<'a>(
        &'a self,
        key: &'a str,
        runtime: &'a str,
    ) -> HostFuture<'a, Result<Vec<u64>, String>> {
        Box::pin(async move {
            self.write().await;
            if self.mutation == HandsMutation::ForgetPending {
                return Ok(Vec::new());
            }
            Ok(self.rows.read(|rows| {
                rows.values()
                    .filter(|row| row.key == key && row.runtime == runtime && row.reply.is_none())
                    .map(|row| row.id)
                    .collect()
            }))
        })
    }

    fn find<'a>(
        &'a self,
        session_id: &'a str,
        call_id: &'a str,
    ) -> HostFuture<'a, Result<Option<PendingRequest>, String>> {
        Box::pin(async move {
            self.write().await;
            let at = (session_id.to_owned(), call_id.to_owned());
            Ok(self.rows.read(|rows| {
                rows.get(&at).map(|row| PendingRequest {
                    id: row.id,
                    key: row.key.clone(),
                    runtime: row.runtime.clone(),
                    session_id: session_id.to_owned(),
                    call_id: call_id.to_owned(),
                    turn_id: row.turn_id.clone(),
                    reply: row.reply.clone(),
                })
            }))
        })
    }

    fn settle<'a>(&'a self, session_id: &'a str) -> HostFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.write().await;
            self.rows
                .update(|rows| rows.retain(|(session, _), _| session != session_id));
            Ok(())
        })
    }
}

struct StepHost {
    rows: Atom<BTreeMap<u64, String>>,
    store: Arc<Store>,
    folded: AtomicU64,
    rode: AtomicU64,
}

impl StepHost {
    fn applied(&self, rows: &[(u64, String)], compaction: &Compaction<'_>) {
        self.rows.update(|stored| {
            stored.extend(rows.iter().chain(compaction.archived).cloned());
            for range in std::iter::once(&compaction.dropped).chain(compaction.superseded) {
                for seq in range.clone() {
                    stored.remove(&seq);
                }
            }
        });
    }

    fn carried(&self, statements: &[nanocode_durability_core::SqlStatement<'_>]) {
        for statement in statements {
            if statement.sql == REPLIED {
                let [key, runtime, id, reply] = statement.params[..] else {
                    panic!("the simulated replied statement has four parameters");
                };
                self.rode.fetch_add(1, Ordering::Relaxed);
                if self.store.mutation != HandsMutation::RideDropsReply {
                    let reply = serde_json::from_str(reply).expect("the reply is JSON");
                    let id = id.parse().expect("request ids are numbers");
                    self.store.answer(key, runtime, id, &reply);
                }
                continue;
            }
            let [session_id, call_id, id, key, runtime, turn] = statement.params[..] else {
                panic!("the simulated ledger statement has six parameters");
            };
            assert_eq!(
                statement.sql, SENT,
                "only the ledger's sent statement rides"
            );
            self.store.insert(&PendingRequest {
                id: id.parse().expect("request ids are numbers"),
                key: key.to_owned(),
                runtime: runtime.to_owned(),
                session_id: session_id.to_owned(),
                call_id: call_id.to_owned(),
                turn_id: serde_json::from_str(turn).expect("the turn id is JSON"),
                reply: None,
            });
        }
    }
}

impl LogHost for StepHost {
    fn scan<'a>(
        &'a self,
        _scope: &'a str,
        from: u64,
        to: u64,
    ) -> StoreFuture<'a, Result<Vec<(u64, String)>, StoreError>> {
        let rows = self.rows.read(|rows| {
            rows.range(from..to)
                .map(|(seq, body)| (*seq, body.clone()))
                .collect()
        });
        Box::pin(async move { Ok(rows) })
    }

    fn load<'a>(
        &'a self,
        _scope: &'a str,
    ) -> StoreFuture<'a, Result<Vec<(u64, String)>, StoreError>> {
        let rows = self.rows.read(|rows| {
            let start = rows
                .iter()
                .rev()
                .find(|(_, body)| body.starts_with('s'))
                .map_or(0, |(seq, _)| *seq);
            rows.range(start..)
                .map(|(seq, body)| (*seq, body.clone()))
                .collect()
        });
        Box::pin(async move { Ok(rows) })
    }

    fn append<'a>(
        &'a self,
        _scope: &'a str,
        rows: &'a [(u64, String)],
    ) -> StoreFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.store.write().await;
            self.applied(
                rows,
                &Compaction {
                    archived: &[],
                    dropped: 0..0,
                    superseded: &[],
                },
            );
            Ok(())
        })
    }

    fn append_compacting<'a>(
        &'a self,
        _scope: &'a str,
        rows: &'a [(u64, String)],
        compaction: Compaction<'a>,
    ) -> StoreFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.store.write().await;
            self.applied(rows, &compaction);
            Ok(())
        })
    }

    fn carry<'a>(
        &'a self,
        _scope: &'a str,
        write: Write<'a>,
        rider: Rider<'a>,
    ) -> Option<StoreFuture<'a, Result<(), StoreError>>> {
        if rider.host != Arc::as_ptr(&self.store) as usize {
            return None;
        }
        let Write::Append { rows, compaction } = write else {
            return None;
        };
        Some(Box::pin(async move {
            self.store.write().await;
            if self.store.mutation != HandsMutation::CarryDropsLog {
                self.applied(rows, &compaction);
            }
            if self.store.mutation != HandsMutation::CarryDropsRequest {
                self.carried(rider.statements);
            }
            self.folded.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }))
    }
}

fn policy() -> Policy {
    Policy {
        snapshot_ratio: SNAPSHOT_RATIO,
        ..Policy::default()
    }
}

fn step(index: usize) -> String {
    format!("step-{index}")
}

fn durable_step(host: &Arc<StepHost>, index: usize) -> bool {
    let host = Arc::clone(host) as Arc<dyn LogHost>;
    let read = async move {
        let log = OneLog::open(host, SCOPE, policy())
            .await
            .expect("the simulated turn log opens");
        let mut store = log.store();
        store
            .peek(&step(index))
            .await
            .expect("the simulated turn log reads")
            .payload
            .is_some_and(|payload| payload == ADMITTED || payload == ANSWERED)
    };
    read.now_or_never()
        .expect("the simulated turn log answers at once")
}

struct SimLink {
    incoming: Owned<mpsc::UnboundedReceiver<String>>,
    outgoing: mpsc::UnboundedSender<String>,
    resumes: String,
}

impl RemoteLink for SimLink {
    fn send(
        &self,
        frames: Vec<RemoteFrame>,
        delivery: RemoteDelivery,
    ) -> HostFuture<'_, Result<(), CodeModeHostError>> {
        self.send_encoded(
            frames.into_iter().map(|frame| (frame, None)).collect(),
            delivery,
        )
    }

    fn send_encoded(
        &self,
        frames: Vec<(RemoteFrame, Option<String>)>,
        _: RemoteDelivery,
    ) -> HostFuture<'_, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            for (frame, text) in frames {
                let text = match text {
                    Some(text) => text,
                    None => frame.encode(FRAME_BYTES_MAX)?,
                };
                self.outgoing
                    .send(text)
                    .map_err(|_| CodeModeHostError::new("socket write failed"))?;
            }
            Ok(())
        })
    }

    fn receive(&self) -> HostFuture<'_, Result<Vec<RemoteFrame>, CodeModeHostError>> {
        Box::pin(async move {
            match self.incoming.acquire().await.recv().await {
                Some(text) => Ok(vec![RemoteFrame::decode(&text, FRAME_BYTES_MAX)?]),
                None => Err(CodeModeHostError::new(PEER_CLOSED)),
            }
        })
    }

    fn close<'a>(&'a self, _: &'a str) -> HostFuture<'a, ()> {
        Box::pin(async {})
    }

    fn max_frame_bytes(&self) -> Option<usize> {
        Some(FRAME_BYTES_MAX)
    }

    fn resumes(&self) -> Option<String> {
        Some(self.resumes.clone())
    }
}

#[derive(Clone, Debug)]
enum Outcome {
    Value(Value),
    NotDispatched,
    Unknown,
}

#[derive(Clone, Debug)]
enum CallState {
    Fresh,
    Admitted,
    Committed(Outcome),
}

struct Call {
    state: CallState,
    exposed: bool,
    hand: usize,
}

#[derive(Default)]
struct Hand {
    runtime: u64,
    running: BTreeMap<u64, (usize, u64, Value)>,
    retained: BTreeMap<u64, Value>,
    executions: BTreeMap<usize, Vec<Value>>,
}

struct Slot {
    key: String,
    hand: RefCell<Hand>,
    restarts: RefCell<Vec<u64>>,
    drops: RefCell<Vec<u64>>,
}

enum Via<'a> {
    Route(&'a AcceptorRoute, &'a Arc<dyn RemoteBarrier>),
    Hands(&'a Arc<dyn HandCalls>),
}

struct World {
    script: Script,
    store: Arc<Store>,
    log: Arc<StepHost>,
    slots: Vec<Slot>,
    calls: RefCell<Vec<Call>>,
    rng: RefCell<Rng>,
    clock: Arc<Clock>,
    trace: RefCell<Vec<String>>,
    wrong: RefCell<Vec<String>>,
    stats: RefCell<HandsStats>,
}

impl World {
    fn note(&self, line: String) {
        self.trace.borrow_mut().push(line);
    }

    fn fail(&self, invariant: &str, line: String) {
        self.wrong.borrow_mut().push(format!("{invariant}: {line}"));
    }

    fn done(&self) -> bool {
        self.calls
            .borrow()
            .iter()
            .all(|call| matches!(call.state, CallState::Committed(_)))
    }

    fn now(&self) -> u64 {
        self.clock.now_ns()
    }

    const fn fleet(&self) -> bool {
        self.slots.len() > 1
    }

    fn due(events: &RefCell<Vec<u64>>, now: u64) -> bool {
        let mut events = events.borrow_mut();
        if events.first().is_some_and(|at| *at <= now) {
            events.remove(0);
            return true;
        }
        false
    }

    fn restart_due(&self, at: usize) -> bool {
        let slot = &self.slots[at];
        if !Self::due(&slot.restarts, self.now()) {
            return false;
        }
        let mut hand = slot.hand.borrow_mut();
        hand.runtime += 1;
        hand.running.clear();
        hand.retained.clear();
        for call in self.calls.borrow_mut().iter_mut() {
            if call.hand == at && !matches!(call.state, CallState::Committed(_)) {
                call.exposed = true;
            }
        }
        self.stats.borrow_mut().restarts += 1;
        self.note(format!("{} restarts as runtime {}", slot.key, hand.runtime));
        true
    }

    fn reply(id: u64, value: Value) -> String {
        RemoteFrame::Reply {
            id,
            result: Ok(Some(value)),
        }
        .encode(FRAME_BYTES_MAX)
        .expect("a simulated reply fits its frame")
    }

    fn receive(&self, at: usize, text: &str, out: &mpsc::UnboundedSender<String>) {
        let frame = RemoteFrame::decode(text, FRAME_BYTES_MAX).expect("the brain sends frames");
        let slot = &self.slots[at];
        let mut hand = slot.hand.borrow_mut();
        match frame {
            RemoteFrame::Request {
                id,
                operation: RemoteOperation::Call { context, .. },
                ..
            } => {
                if let Some(value) = hand.retained.get(&id) {
                    let _ = out.send(Self::reply(id, value.clone()));
                    return;
                }
                if hand.running.contains_key(&id) {
                    return;
                }
                let index: usize = context.call_id.parse().expect("calls are numbered");
                if !durable_step(&self.log, index) {
                    self.fail(
                        "folded",
                        format!("call {index} reached the hand before its step was durable"),
                    );
                }
                if !self.store.recorded(&context.session_id, &context.call_id) {
                    self.fail(
                        "folded",
                        format!("call {index} reached the hand before its request was recorded"),
                    );
                }
                let runs = hand.executions.entry(index).or_default();
                let label = format!("call {index} run {}", runs.len());
                runs.push(json!(label));
                let value = if self.fleet() {
                    output_to_wire(ToolOutput::text(label), false)
                        .expect("a simulated output encodes")
                } else {
                    json!(label)
                };
                let done = self.now() + self.script.runs[index];
                hand.running.insert(id, (index, done, value));
                self.note(format!("{} runs call {index} as request {id}", slot.key));
            }
            RemoteFrame::Ack { ids } => {
                for id in ids {
                    if self.store.unanswered(&slot.key, id) {
                        self.fail(
                            "ack_after_store",
                            format!("request {id} was acked before its reply was stored"),
                        );
                    }
                    hand.retained.remove(&id);
                }
            }
            RemoteFrame::Resume { pending } => {
                hand.retained.retain(|id, _| pending.contains(id));
                for id in pending {
                    if let Some(value) = hand.retained.get(&id) {
                        let _ = out.send(Self::reply(id, value.clone()));
                    } else if !hand.running.contains_key(&id) {
                        let refused = RemoteFrame::Reply {
                            id,
                            result: Err(NOT_RECEIVED.to_owned()),
                        };
                        let _ = out.send(refused.encode(FRAME_BYTES_MAX).expect("fits"));
                    }
                }
            }
            _ => {}
        }
    }

    fn finish(&self, at: usize, out: &mpsc::UnboundedSender<String>) {
        let now = self.now();
        let mut hand = self.slots[at].hand.borrow_mut();
        let done: Vec<u64> = hand
            .running
            .iter()
            .filter(|(_, (_, at, _))| *at <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in done {
            let Some((_, _, value)) = hand.running.remove(&id) else {
                continue;
            };
            hand.retained.insert(id, value.clone());
            let _ = out.send(Self::reply(id, value));
        }
    }

    async fn connection(
        &self,
        at: usize,
        out: mpsc::UnboundedSender<String>,
        mut incoming: mpsc::UnboundedReceiver<String>,
    ) {
        loop {
            if self.done() || self.restart_due(at) {
                return;
            }
            if Self::due(&self.slots[at].drops, self.now()) {
                self.stats.borrow_mut().drops += 1;
                if self.fleet() {
                    self.note(format!("{} socket drops", self.slots[at].key));
                } else {
                    self.note("socket drops".to_owned());
                }
                return;
            }
            self.finish(at, &out);
            nanocode_sync::select! {
                text = incoming.recv() => match text {
                    Some(text) => self.receive(at, &text, &out),
                    None => return,
                },
                () = self.clock.sleep(TICK) => {}
            }
        }
    }

    async fn hand(&self, at: usize, acceptor: &Arc<Acceptor>) {
        let slot = &self.slots[at];
        loop {
            let redial = self.rng.borrow_mut().range(1, REDIAL_MS_MAX);
            let until = self.now() + redial * MS;
            while self.now() < until {
                if self.done() {
                    return;
                }
                self.restart_due(at);
                self.clock.sleep(TICK).await;
            }
            let runtime = if self.fleet() {
                format!("{}-runtime-{}", slot.key, slot.hand.borrow().runtime)
            } else {
                format!("runtime-{}", slot.hand.borrow().runtime)
            };
            let catalog = json!({"type": "catalog", "definitions": [], "runtime_id": runtime, "resumable": true});
            let catalog = RemoteCatalog::decode(&catalog.to_string(), FRAME_BYTES_MAX)
                .expect("the simulated catalog decodes");
            let (to_brain, from_hand) = mpsc::unbounded_channel();
            let (to_hand, from_brain) = mpsc::unbounded_channel();
            let link = SimLink {
                incoming: Owned::new(from_hand),
                outgoing: to_hand,
                resumes: runtime,
            };
            let brain = async {
                match acceptor.attach(slot.key.clone(), catalog, link).await {
                    Ok(attached) => attached.ended().await,
                    Err(error) => self.note(format!("attach refused: {error}")),
                }
            };
            nanocode_sync::future::join(brain, self.connection(at, to_brain, from_brain)).await;
        }
    }

    async fn admit(&self, log: &Arc<OneLog>, index: usize) -> bool {
        self.step(log, index, ADMITTED).await
    }

    async fn step(&self, log: &Arc<OneLog>, index: usize, payload: &str) -> bool {
        let mut store = log.store();
        let step = step(index);
        let result = async {
            let owned = store.acquire(&step, OwnerId::new()).await?;
            let commit = StateCommit {
                payload,
                records: &[],
                events: &[],
                summary: &SummaryUpdate::Unchanged,
                settle: Settle::Durable,
            };
            store
                .commit(&step, &owned.owner, owned.state.revision, commit)
                .await?;
            if payload == ANSWERED {
                store.flush().await?;
            }
            Ok::<_, StoreError>(())
        };
        match result.await {
            Ok(_) => true,
            Err(error) => {
                self.note(format!("call {index} step not committed: {error}"));
                false
            }
        }
    }

    fn named(&self, index: usize) -> &str {
        let hand = self.calls.borrow()[index].hand;
        let hand = if self.store.mutation == HandsMutation::FleetCrossHands {
            (hand + 1) % self.slots.len()
        } else {
            hand
        };
        &self.slots[hand].key
    }

    fn output(output: &ToolOutput) -> Outcome {
        let ToolOutputBody::Text(text) = &output.output else {
            return Outcome::Unknown;
        };
        if output.success {
            Outcome::Value(json!(text))
        } else if UNSENT.iter().any(|unsent| text.contains(unsent)) {
            Outcome::NotDispatched
        } else {
            Outcome::Unknown
        }
    }

    async fn dispatch(&self, via: &Via<'_>, index: usize) -> Outcome {
        let call_id = index.to_string();
        match via {
            Via::Route(route, barrier) => {
                let context =
                    RemoteContext::new(SESSION.to_owned(), call_id, Some(TURN.to_owned()));
                let operation = RemoteOperation::Call {
                    name: "run".to_owned(),
                    input: json!({}),
                    context,
                    selected_tools: None,
                    run_time_ms: None,
                };
                let result = if self.store.mutation == HandsMutation::DispatchUnsettled {
                    route.call(operation).await
                } else {
                    route.call_after(operation, Arc::clone(barrier)).await
                };
                match result {
                    Ok(value) => Outcome::Value(value),
                    Err(error) if error.is_unsent() => Outcome::NotDispatched,
                    Err(_) => Outcome::Unknown,
                }
            }
            Via::Hands(calls) => {
                let context =
                    ToolContext::new("", SESSION, &call_id, &[], DEFAULT_TOOL_OUTPUT_TOKENS)
                        .with_turn_id(Some(TURN));
                let output = calls
                    .call(self.named(index), "run", json!({}), context)
                    .await;
                Self::output(&output)
            }
        }
    }

    async fn recover(&self, via: &Via<'_>, index: usize) -> Outcome {
        self.stats.borrow_mut().recovered += 1;
        let call_id = index.to_string();
        match via {
            Via::Route(route, _) => {
                let context =
                    RemoteContext::new(SESSION.to_owned(), call_id, Some(TURN.to_owned()));
                let recovered = route
                    .recover(&context)
                    .expect("the acceptor's route recovers")
                    .await;
                match recovered {
                    Ok(Some(value)) => Outcome::Value(value),
                    Ok(None) => Outcome::Unknown,
                    Err(error) if error.is_unsent() => self.dispatch(via, index).await,
                    Err(_) => Outcome::Unknown,
                }
            }
            Via::Hands(_) if self.store.mutation == HandsMutation::FleetRecoverNone => {
                Outcome::Unknown
            }
            Via::Hands(calls) => {
                let context =
                    ToolContext::new("", SESSION, &call_id, &[], DEFAULT_TOOL_OUTPUT_TOKENS)
                        .with_turn_id(Some(TURN));
                let outcome = calls
                    .recover(self.named(index), context)
                    .await
                    .map_or(Outcome::Unknown, |output| Self::output(&output));
                match outcome {
                    Outcome::NotDispatched
                        if self.store.mutation == HandsMutation::FleetSwallowUnsent =>
                    {
                        Outcome::Unknown
                    }
                    Outcome::Value(value) => {
                        if self.calls.borrow()[index].hand > 0 {
                            self.stats.borrow_mut().retained += 1;
                        }
                        Outcome::Value(value)
                    }
                    outcome => outcome,
                }
            }
        }
    }

    async fn session_call(&self, via: &Via<'_>, log: &Arc<OneLog>, index: usize) {
        let state = self.calls.borrow()[index].state.clone();
        let outcome = match state {
            CallState::Committed(_) => return,
            CallState::Fresh => {
                let start = self.script.starts[index];
                let now = self.now();
                if start > now {
                    self.clock.sleep(Duration::from_nanos(start - now)).await;
                }
                if !self.admit(log, index).await {
                    return;
                }
                self.calls.borrow_mut()[index].state = CallState::Admitted;
                self.dispatch(via, index).await
            }
            CallState::Admitted => self.recover(via, index).await,
        };
        self.clock
            .sleep(Duration::from_nanos(self.script.commits[index]))
            .await;
        if !self.step(log, index, ANSWERED).await {
            return;
        }
        self.note(format!("call {index} commits {outcome:?}"));
        self.calls.borrow_mut()[index].state = CallState::Committed(outcome);
    }

    fn check(&self) {
        for (index, call) in self.calls.borrow().iter().enumerate() {
            for (at, slot) in self.slots.iter().enumerate() {
                if at != call.hand && slot.hand.borrow().executions.contains_key(&index) {
                    self.fail(
                        "routed",
                        format!(
                            "call {index} for {} ran on {}",
                            self.slots[call.hand].key, slot.key
                        ),
                    );
                }
            }
            let hand = self.slots[call.hand].hand.borrow();
            let runs = hand.executions.get(&index).map_or(&[][..], Vec::as_slice);
            if runs.len() > 1 {
                self.fail("once", format!("call {index} ran {} times", runs.len()));
            }
            match &call.state {
                CallState::Committed(Outcome::Value(value)) if runs != [value.clone()] => {
                    self.fail(
                        "real",
                        format!("call {index} committed {value} but ran {runs:?}"),
                    );
                }
                CallState::Committed(Outcome::NotDispatched) if !runs.is_empty() => {
                    self.fail(
                        "real",
                        format!("call {index} committed not dispatched but ran"),
                    );
                }
                CallState::Committed(Outcome::Unknown) if !call.exposed => {
                    self.fail(
                        "honest",
                        format!("call {index} committed unknown though its hand never restarted"),
                    );
                }
                CallState::Committed(Outcome::Unknown) => {
                    self.stats.borrow_mut().unknown += 1;
                }
                CallState::Fresh | CallState::Admitted => {
                    self.fail("settles", format!("call {index} never settled"));
                }
                CallState::Committed(_) => {}
            }
        }
    }
}

fn options(fleet: usize) -> AcceptorOptions {
    AcceptorOptions {
        path: "/".to_owned(),
        frame_bytes_max: FRAME_BYTES_MAX,
        head_timeout: GRACE,
        catalog_timeout: GRACE,
        ping: GRACE,
        heartbeat_timeout: GRACE,
        operation_timeout: None,
        reconnect_grace: GRACE,
        runtimes_max: RUNTIMES_MAX.max(fleet),
        attaches_queued_max: ATTACHES_QUEUED_MAX,
    }
}

async fn process(world: &World) {
    let host = Arc::clone(&world.log) as Arc<dyn LogHost>;
    let log = OneLog::open(host, SCOPE, policy())
        .await
        .expect("the simulated turn log opens");
    log.settle_durably();
    let barrier = log.barrier();
    for (index, call) in world.calls.borrow_mut().iter_mut().enumerate() {
        if matches!(call.state, CallState::Admitted) && !durable_step(&world.log, index) {
            world.note(format!("call {index} step was lost; the model asks again"));
            world.stats.borrow_mut().replayed += 1;
            call.state = CallState::Fresh;
        }
    }
    let (acceptor, _attaches) = Acceptor::new(
        options(world.slots.len()),
        Arc::clone(&world.store) as Arc<dyn HandRequests>,
        |_, _| {
            Err(Refusal {
                status: StatusCode::NOT_FOUND,
                body: String::new(),
            })
        },
    );
    if !world.fleet() {
        let route = acceptor.route(Arc::new(|| Box::pin(async { Some(KEY.to_owned()) })));
        let via = Via::Route(&route, &barrier);
        let calls =
            (0..world.script.starts.len()).map(|index| world.session_call(&via, &log, index));
        let sessions = nanocode_sync::join_in_order(calls);
        nanocode_sync::future::join(sessions, world.hand(0, &acceptor)).await;
        return;
    }
    let executors = CodeModeExecutors::new();
    for (at, slot) in world.slots.iter().enumerate() {
        let key = slot.key.clone();
        let route = acceptor.route(Arc::new(move || {
            let key = key.clone();
            Box::pin(async move { Some(key) })
        }));
        let host = RemoteCodeModeHost::new(route, Vec::new());
        let unbarriered = match world.store.mutation {
            HandsMutation::DispatchUnsettled => true,
            HandsMutation::FleetUnbarriered => at > 0,
            _ => false,
        };
        let host = if unbarriered {
            host
        } else {
            host.barrier(Arc::clone(&barrier))
        };
        executors.insert(slot.key.clone(), Arc::new(host));
    }
    let hands = executors.calls();
    let via = Via::Hands(&hands);
    let calls = (0..world.script.starts.len()).map(|index| world.session_call(&via, &log, index));
    let sessions = nanocode_sync::join_in_order(calls);
    let drivers = (0..world.slots.len()).map(|at| world.hand(at, &acceptor));
    nanocode_sync::future::join(sessions, nanocode_sync::join_in_order(drivers)).await;
}

pub struct HandsOutcome {
    pub digest: u64,
    pub wrong: Option<String>,
    pub stats: HandsStats,
    pub shape: String,
}

fn simulate(seed: u64, mutation: HandsMutation, fleet: usize) -> HandsOutcome {
    let fleet = mutation.fleet(fleet);
    let script = Script::of(seed, fleet);
    let shape = script.describe();
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let calls = script
        .hands
        .iter()
        .map(|hand| Call {
            state: CallState::Fresh,
            exposed: false,
            hand: *hand,
        })
        .collect();
    let slots = script
        .schedules
        .iter()
        .enumerate()
        .map(|(at, schedule)| Slot {
            key: if fleet > 1 {
                format!("hand-{at}")
            } else {
                KEY.to_owned()
            },
            hand: RefCell::new(Hand::default()),
            restarts: RefCell::new(schedule.restarts.clone()),
            drops: RefCell::new(schedule.drops.clone()),
        })
        .collect();
    let crashes = script.crashes.clone();
    let store = Arc::new(Store {
        rows: Atom::default(),
        writes: AtomicU64::new(0),
        seed,
        mutation,
    });
    let world = World {
        slots,
        script,
        log: Arc::new(StepHost {
            rows: Atom::default(),
            store: Arc::clone(&store),
            folded: AtomicU64::new(0),
            rode: AtomicU64::new(0),
        }),
        store,
        calls: RefCell::new(calls),
        rng: RefCell::new(Rng::derive(seed, "hands world")),
        clock: Arc::clone(&clock),
        trace: RefCell::new(Vec::new()),
        wrong: RefCell::new(Vec::new()),
        stats: RefCell::new(HandsStats {
            seeds: 1,
            calls: u64::try_from(Script::of(seed, fleet).starts.len()).unwrap_or(u64::MAX),
            ..HandsStats::default()
        }),
    };
    let world = Rc::new(world);
    let mut epoch = 0_u64;
    loop {
        let crash = crashes
            .get(usize::try_from(epoch).unwrap_or(usize::MAX))
            .copied();
        let horizon = crash.unwrap_or_else(|| world.now() + SETTLE_HORIZON_MS * MS);
        let tasks: Vec<Task<'_>> = vec![Box::pin(process(&world))];
        let mut executor = Executor::new(
            Arc::clone(&clock),
            Rng::derive(seed ^ epoch, "hands scheduler"),
        );
        let ended = executor.run(tasks, Some(horizon));
        drop(executor);
        drop(crate::host::spawned());
        match ended {
            Ended::Crashed if crash.is_some() => {
                world.stats.borrow_mut().crashes += 1;
                world.note(format!("host crashes in epoch {epoch}"));
                epoch += 1;
            }
            Ended::Crashed => {
                world.note("the run reached its horizon".to_owned());
                break;
            }
            Ended::Stuck => {
                world.fail("settles", "every task waits and no timer is set".to_owned());
                break;
            }
            Ended::Finished => break,
        }
    }
    world.check();
    world.stats.borrow_mut().folded = world.log.folded.load(Ordering::Relaxed);
    world.stats.borrow_mut().rode = world.log.rode.load(Ordering::Relaxed);
    let trace = world.trace.borrow();
    let mut digest = fnv(FNV_OFFSET, shape.as_bytes());
    for line in trace.iter() {
        digest = fnv(digest, line.as_bytes());
    }
    digest = fnv(digest, &clock.now_ns().to_le_bytes());
    let wrong = world.wrong.borrow().first().cloned();
    let stats = std::mem::take(&mut *world.stats.borrow_mut());
    HandsOutcome {
        digest,
        wrong,
        stats,
        shape,
    }
}

pub fn run_seed(seed: u64, mutation: HandsMutation, fleet: usize) -> HandsOutcome {
    std::panic::catch_unwind(|| simulate(seed, mutation, fleet)).unwrap_or_else(|panic| {
        let message = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
            .unwrap_or_default();
        HandsOutcome {
            digest: 0,
            wrong: Some(format!("panic: {message}")),
            stats: HandsStats::default(),
            shape: String::new(),
        }
    })
}

pub fn judge_seed(
    seed: u64,
    mutation: HandsMutation,
    fleet: usize,
    twice: bool,
    verbose: bool,
    totals: &mut HandsStats,
) -> Option<String> {
    let first = run_seed(seed, mutation, fleet);
    totals.add(&first.stats);
    if verbose {
        println!("seed {seed} digest {:016x} {}", first.digest, first.shape);
    }
    if first.wrong.is_some() || !twice {
        return first
            .wrong
            .map(|message| format!("{} :: {message}", first.shape));
    }
    let second = crate::host::second_pass(|| run_seed(seed, mutation, fleet));
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: trace digest {:016x} then {:016x}",
            first.shape, first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: HandsMutation, fleet: usize) -> String {
    let mut command = format!("nanocode-sim --scenario hands --seed {seed}");
    if fleet > 1 {
        command.push_str(&format!(" --fleet {fleet}"));
    }
    if mutation != HandsMutation::None {
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
    mutation: HandsMutation,
    fleet: usize,
    totals: &HandsStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=hands mutation={} fleet={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name(),
        mutation.fleet(fleet)
    );
    match mutation {
        HandsMutation::None if failed == 0 => ExitCode::SUCCESS,
        HandsMutation::None => ExitCode::FAILURE,
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
