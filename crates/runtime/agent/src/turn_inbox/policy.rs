use std::collections::BTreeSet;

use super::{
    Claim, Delivered, Delivery, Driven, DurableOutcome, HostFuture, InboxError, NanocodeError,
    PassEnd, Prompt, PromptRequest, Run, Started, SteerReceiptStatus, StoreError, TurnControl,
    TurnInbox, TurnLog, TurnState, fields,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PassOutcome {
    Completed { message: String },
    Failed { error: String },
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FinishedPass {
    pub pass_id: String,
    pub outcome: PassOutcome,
    pub read: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PassSpec {
    pub pass_id: String,
    pub prompt: Prompt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PassOrigin {
    Inbox,
    Live,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum PassPhase {
    Running { admitted: bool },
    Ended { pass: FinishedPass },
    Finishing { outcome: PassOutcome },
    Closed { outcome: PassOutcome },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PassRecord {
    pub request_id: String,
    pub origin: PassOrigin,
    pub passes: Vec<String>,
    pub current: PassSpec,
    pub phase: PassPhase,
}

impl PassRecord {
    fn first(request_id: &str, origin: PassOrigin, current: PassSpec) -> Self {
        Self {
            request_id: request_id.to_owned(),
            origin,
            passes: vec![current.pass_id.clone()],
            current,
            phase: PassPhase::Running { admitted: false },
        }
    }

    #[must_use]
    pub const fn is_open(&self) -> bool {
        !matches!(self.phase, PassPhase::Closed { .. })
    }
}

pub struct PassContext<'a> {
    pub session_id: &'a str,
    pub request_id: &'a str,
    pub pass: &'a FinishedPass,
}

pub enum AfterPass {
    Finish,
    Continue(PassSpec),
}

pub trait PassPolicy: Send + Sync {
    fn after_pass<'a>(
        &'a self,
        context: PassContext<'a>,
    ) -> HostFuture<'a, Result<AfterPass, String>>;

    fn admitted<'a>(
        &'a self,
        _session_id: &'a str,
        _request_id: &'a str,
        _pass_id: &'a str,
    ) -> HostFuture<'a, ()> {
        Box::pin(async {})
    }
}

#[derive(Debug)]
pub enum PolicyError {
    Inbox(InboxError),
    Host(String),
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inbox(error) => error.fmt(formatter),
            Self::Host(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for PolicyError {}

impl From<InboxError> for PolicyError {
    fn from(error: InboxError) -> Self {
        Self::Inbox(error)
    }
}

impl From<StoreError> for PolicyError {
    fn from(error: StoreError) -> Self {
        Self::Inbox(InboxError::Store(error))
    }
}

impl From<NanocodeError> for PolicyError {
    fn from(error: NanocodeError) -> Self {
        Self::Inbox(InboxError::Agent(error))
    }
}

pub enum PolicyTurn {
    Finished(PassOutcome),
    Stopped {
        reason: String,
        control: TurnControl,
    },
    Left {
        control: TurnControl,
    },
    Open,
}

enum Ran {
    Ended(PassOutcome, Vec<String>),
    Open(Driven),
}

impl<L: TurnLog + ?Sized> TurnInbox<L> {
    pub async fn drive_with_policy(
        &self,
        next: Option<&str>,
        policy: &dyn PassPolicy,
    ) -> Result<Driven, PolicyError> {
        self.drive_with(next, Some(policy)).await
    }

    pub async fn turn_with_policy(
        &self,
        request_id: &str,
        request: PromptRequest,
        read: Vec<String>,
        policy: &dyn PassPolicy,
    ) -> Result<PolicyTurn, PolicyError> {
        let record = match self.load_pass(request_id).await? {
            Some(record) if record.origin == PassOrigin::Inbox => {
                return Err(PolicyError::Host(format!(
                    "request {request_id} is an inbox request; drive it with drive_with_policy"
                )));
            }
            Some(record) => record,
            None => {
                let pass_id = request
                    .request_id
                    .clone()
                    .unwrap_or_else(|| request_id.to_owned());
                let record = PassRecord::first(
                    request_id,
                    PassOrigin::Live,
                    PassSpec {
                        pass_id,
                        prompt: request.prompt,
                    },
                );
                self.save_pass(&record).await?;
                record
            }
        };
        if !self.claim(request_id) {
            return Ok(PolicyTurn::Open);
        }
        let _claim = Claim(&self.shared, request_id);
        self.shared.delivery.update(|delivery| {
            for id in &read {
                delivery.mark(id, Delivered::Read);
            }
        });
        self.live_loop(record, read, policy).await
    }

    async fn live_loop(
        &self,
        mut record: PassRecord,
        mut given: Vec<String>,
        policy: &dyn PassPolicy,
    ) -> Result<PolicyTurn, PolicyError> {
        let request_id = record.request_id.clone();
        let request_id = request_id.as_str();
        loop {
            match record.phase.clone() {
                PassPhase::Closed { outcome } => return Ok(PolicyTurn::Finished(outcome)),
                PassPhase::Finishing { outcome } => {
                    self.shared.state.update(|state| state.finished(request_id));
                    record.phase = PassPhase::Closed { outcome };
                    self.save_pass(&record).await?;
                }
                PassPhase::Ended { pass } => self.decide(&mut record, &pass, policy).await?,
                PassPhase::Running { admitted } => {
                    let pass = record.current.clone();
                    if !admitted
                        && self.request_cancelled(&record).await?
                        && self.admitted_pass(&pass.pass_id).await?.is_none()
                    {
                        self.ended(
                            &mut record,
                            pass.pass_id,
                            PassOutcome::Cancelled,
                            Vec::new(),
                        )
                        .await?;
                        continue;
                    }
                    let before = self.read_now();
                    let session_id = self.session_id();
                    let marked = PassRecord {
                        phase: PassPhase::Running { admitted: true },
                        ..record.clone()
                    };
                    let admitted = self.admitted(&marked, policy);
                    let request =
                        PromptRequest::new(pass.prompt.clone()).request_id(pass.pass_id.clone());
                    let outcome = match self.pass(request_id, request, Some(&admitted)).await {
                        PassEnd::Completed(result) => PassOutcome::Completed {
                            message: result.into_final_message(),
                        },
                        PassEnd::Failed(error) => match self
                            .runtime
                            .durable_outcome(session_id, &pass.pass_id, Err(error))
                            .await
                        {
                            DurableOutcome::Completed(message) => {
                                PassOutcome::Completed { message }
                            }
                            DurableOutcome::Failed(error) => PassOutcome::Failed { error },
                            DurableOutcome::Cancelled => PassOutcome::Cancelled,
                            DurableOutcome::Waiting | DurableOutcome::Interrupted => {
                                return Ok(PolicyTurn::Open);
                            }
                        },
                        PassEnd::Refused(error) => return Err(error.into()),
                        PassEnd::Stopped { reason, control } => {
                            return Ok(PolicyTurn::Stopped { reason, control });
                        }
                        PassEnd::Left { control } => return Ok(PolicyTurn::Left { control }),
                    };
                    let mut delivered = self.delivered(&before);
                    delivered.append(&mut given);
                    self.ended(&mut record, pass.pass_id, outcome, delivered)
                        .await?;
                }
            }
        }
    }

    pub async fn pass_record(&self, request_id: &str) -> Result<Option<PassRecord>, InboxError> {
        let Some(mut record) = self.load_pass(request_id).await? else {
            return Ok(None);
        };
        if record.origin == PassOrigin::Inbox
            && let PassPhase::Finishing { outcome } = &record.phase
            && self
                .shared
                .log
                .turn(request_id)
                .await?
                .is_some_and(|turn| turn.state.is_terminal())
        {
            record.phase = PassPhase::Closed {
                outcome: outcome.clone(),
            };
        }
        Ok(Some(record))
    }

    pub async fn admitted_pass(&self, pass_id: &str) -> Result<Option<Prompt>, InboxError> {
        Ok(self.runtime.durable_prompt(&self.agent, pass_id).await?)
    }

    fn own(&self, record: &PassRecord) {
        self.shared.state.update(|state| {
            for pass in &record.passes {
                state.passes.insert(pass.clone(), record.request_id.clone());
            }
        });
    }

    async fn load_pass(&self, request_id: &str) -> Result<Option<PassRecord>, StoreError> {
        let record = self.shared.log.pass_record(request_id).await?;
        if let Some(record) = &record {
            self.own(record);
        }
        Ok(record)
    }

    fn claim(&self, id: &str) -> bool {
        self.shared.state.update(|state| {
            let request = state.requests.entry(id.to_owned()).or_default();
            if request.running.is_some() {
                return false;
            }
            request.running = Some(Run::Live);
            true
        })
    }

    fn read_now(&self) -> BTreeSet<String> {
        self.shared
            .delivery
            .read(Delivery::read)
            .into_iter()
            .collect()
    }

    fn delivered(&self, before: &BTreeSet<String>) -> Vec<String> {
        self.shared
            .delivery
            .read(Delivery::read)
            .into_iter()
            .filter(|id| !before.contains(id))
            .collect()
    }

    async fn save_pass(&self, record: &PassRecord) -> Result<(), InboxError> {
        self.own(record);
        Ok(self.shared.log.save_pass_record(record).await?)
    }

    fn admitted<'h>(
        &'h self,
        marked: &'h PassRecord,
        policy: &'h dyn PassPolicy,
    ) -> impl Fn() -> HostFuture<'h, ()> + Send + Sync + 'h {
        move || {
            Box::pin(async move {
                if self.save_pass(marked).await.is_ok() {
                    policy
                        .admitted(
                            self.session_id(),
                            &marked.request_id,
                            &marked.current.pass_id,
                        )
                        .await;
                }
            })
        }
    }

    async fn ended(
        &self,
        record: &mut PassRecord,
        pass_id: String,
        outcome: PassOutcome,
        delivered: Vec<String>,
    ) -> Result<(), InboxError> {
        let mut read: BTreeSet<String> = delivered.into_iter().collect();
        if matches!(outcome, PassOutcome::Completed { .. }) {
            read.extend(
                self.runtime
                    .consumed_steers(self.session_id(), &pass_id)
                    .await?,
            );
        }
        record.phase = PassPhase::Ended {
            pass: FinishedPass {
                pass_id,
                outcome,
                read: read.into_iter().collect(),
            },
        };
        self.save_pass(record).await
    }

    pub(super) async fn run_policy(
        &self,
        id: &str,
        policy: &dyn PassPolicy,
    ) -> Result<Driven, PolicyError> {
        let Some(turn) = self.shared.log.turn(id).await? else {
            return Ok(Driven::Idle);
        };
        let mut record = match self.load_pass(id).await? {
            Some(record) if record.origin == PassOrigin::Live => {
                return self.drive_live(id, policy).await;
            }
            Some(record) => record,
            None => {
                match turn.state {
                    TurnState::Queued => {}
                    TurnState::Running => return Ok(self.run(id, false).await?),
                    _ => return Ok(Driven::Finished(turn)),
                }
                let Some(prompt) = turn.prompt.clone() else {
                    return Ok(self.run(id, false).await?);
                };
                if self
                    .agent
                    .steer_receipt(id.to_owned(), prompt.clone())
                    .await?
                    == SteerReceiptStatus::Accepted
                    && let Some(record) = self.withdraw_steered(&turn).await?
                {
                    return Ok(Driven::Finished(record));
                }
                let record = PassRecord::first(
                    id,
                    PassOrigin::Inbox,
                    PassSpec {
                        pass_id: id.to_owned(),
                        prompt,
                    },
                );
                self.save_pass(&record).await?;
                record
            }
        };
        if turn.state.is_terminal() {
            if record.is_open() {
                let outcome = match record.phase {
                    PassPhase::Finishing { outcome } => outcome,
                    _ if turn.state == TurnState::Cancelled => PassOutcome::Cancelled,
                    _ => PassOutcome::Failed {
                        error: format!("the request was closed as {:?}", turn.state),
                    },
                };
                record.phase = PassPhase::Closed { outcome };
                self.save_pass(&record).await?;
            }
            return Ok(Driven::Finished(turn));
        }
        if turn.state == TurnState::Queued || matches!(record.phase, PassPhase::Running { .. }) {
            let Some(Started {
                record: turn,
                event,
                ..
            }) = self.shared.log.start(id).await?
            else {
                return Ok(match self.shared.log.turn(id).await? {
                    Some(turn) => Driven::Finished(turn),
                    None => Driven::Idle,
                });
            };
            self.publish(event).await?;
            self.shared
                .host
                .place(self.session_id(), turn.hand.as_deref());
        }
        self.policy_loop(record, policy).await
    }

    pub(super) async fn recover_pass(
        &self,
        owner: &str,
        pass_id: &str,
        policy: &dyn PassPolicy,
    ) -> Result<Driven, PolicyError> {
        let record = self.load_pass(owner).await?;
        let turn = self.shared.log.turn(owner).await?;
        match &record {
            Some(record) if record.is_open() && record.origin == PassOrigin::Live => {
                return self.drive_live(owner, policy).await;
            }
            Some(record)
                if record.is_open()
                    && turn.as_ref().is_some_and(|turn| !turn.state.is_terminal()) =>
            {
                return self.run_policy(owner, policy).await;
            }
            _ => {}
        }
        let abandoned = turn
            .as_ref()
            .is_none_or(|turn| turn.state != TurnState::Completed);
        Ok(self
            .resume(
                pass_id.to_owned(),
                turn.and_then(|turn| turn.hand),
                abandoned,
            )
            .await?)
    }

    pub(super) async fn drive_live(
        &self,
        id: &str,
        policy: &dyn PassPolicy,
    ) -> Result<Driven, PolicyError> {
        match self.load_pass(id).await? {
            Some(record) if record.origin == PassOrigin::Live && record.is_open() => {
                if !self.claim(id) {
                    return Ok(Driven::Waiting(id.to_owned()));
                }
                let _claim = Claim(&self.shared, id);
                Ok(match self.live_loop(record, Vec::new(), policy).await? {
                    PolicyTurn::Finished(_) => Driven::Resumed(id.to_owned()),
                    PolicyTurn::Open => Driven::Interrupted(id.to_owned()),
                    PolicyTurn::Stopped { .. } | PolicyTurn::Left { .. } => {
                        Driven::Waiting(id.to_owned())
                    }
                })
            }
            _ => Ok(Driven::Idle),
        }
    }

    async fn policy_loop(
        &self,
        mut record: PassRecord,
        policy: &dyn PassPolicy,
    ) -> Result<Driven, PolicyError> {
        let id = record.request_id.clone();
        loop {
            match record.phase.clone() {
                PassPhase::Closed { .. } => {
                    return Ok(match (record.origin, self.shared.log.turn(&id).await?) {
                        (PassOrigin::Inbox, Some(turn)) => Driven::Finished(turn),
                        _ => Driven::Resumed(id),
                    });
                }
                PassPhase::Finishing { outcome } => {
                    let driven = self.close_pass(&id, record.origin, outcome.clone()).await?;
                    record.phase = PassPhase::Closed { outcome };
                    self.save_pass(&record).await?;
                    return Ok(driven);
                }
                PassPhase::Ended { pass } => self.decide(&mut record, &pass, policy).await?,
                PassPhase::Running { .. } => match self.run_pass(&record, policy).await? {
                    Ran::Ended(outcome, delivered) => {
                        let pass_id = record.current.pass_id.clone();
                        self.ended(&mut record, pass_id, outcome, delivered).await?;
                    }
                    Ran::Open(driven) => return Ok(driven),
                },
            }
        }
    }

    async fn run_pass(
        &self,
        record: &PassRecord,
        policy: &dyn PassPolicy,
    ) -> Result<Ran, PolicyError> {
        let id = record.request_id.as_str();
        let pass = &record.current;
        let cancel = self.request_cancelled(record).await?;
        let marked = matches!(record.phase, PassPhase::Running { admitted: true });
        if cancel && !marked && self.admitted_pass(&pass.pass_id).await?.is_none() {
            return Ok(Ran::Ended(PassOutcome::Cancelled, Vec::new()));
        }
        let before = self.read_now();
        let marked = PassRecord {
            phase: PassPhase::Running { admitted: true },
            ..record.clone()
        };
        let admitted = self.admitted(&marked, policy);
        Ok(
            match self
                .execute_as(
                    id,
                    &pass.pass_id,
                    pass.prompt.clone(),
                    cancel,
                    Run::Inbox,
                    Some(&admitted),
                )
                .await
            {
                DurableOutcome::Completed(message) => {
                    Ran::Ended(PassOutcome::Completed { message }, self.delivered(&before))
                }
                DurableOutcome::Failed(error) => {
                    Ran::Ended(PassOutcome::Failed { error }, self.delivered(&before))
                }
                DurableOutcome::Cancelled => {
                    Ran::Ended(PassOutcome::Cancelled, self.delivered(&before))
                }
                DurableOutcome::Waiting => Ran::Open(Driven::Waiting(id.to_owned())),
                DurableOutcome::Interrupted => Ran::Open(Driven::Interrupted(id.to_owned())),
            },
        )
    }

    async fn decide(
        &self,
        record: &mut PassRecord,
        finished: &FinishedPass,
        policy: &dyn PassPolicy,
    ) -> Result<(), PolicyError> {
        if self.request_cancelled(record).await? {
            record.phase = PassPhase::Finishing {
                outcome: PassOutcome::Cancelled,
            };
            return Ok(self.save_pass(record).await?);
        }
        let choice = policy
            .after_pass(PassContext {
                session_id: self.session_id(),
                request_id: &record.request_id,
                pass: finished,
            })
            .await
            .map_err(PolicyError::Host)?;
        match choice {
            AfterPass::Finish => {
                record.phase = PassPhase::Finishing {
                    outcome: finished.outcome.clone(),
                };
            }
            AfterPass::Continue(next) => {
                if let Some(owner) = self.shared.log.pass_owner(&next.pass_id).await? {
                    return Err(PolicyError::Host(format!(
                        "pass {} already belongs to request {owner}",
                        next.pass_id
                    )));
                }
                if self.request_cancelled(record).await? {
                    record.phase = PassPhase::Finishing {
                        outcome: PassOutcome::Cancelled,
                    };
                } else {
                    record.passes.push(next.pass_id.clone());
                    record.current = next;
                    record.phase = PassPhase::Running { admitted: false };
                }
            }
        }
        Ok(self.save_pass(record).await?)
    }

    async fn close_pass(
        &self,
        id: &str,
        origin: PassOrigin,
        outcome: PassOutcome,
    ) -> Result<Driven, InboxError> {
        if origin == PassOrigin::Live {
            self.shared.state.update(|state| state.finished(id));
            return Ok(Driven::Resumed(id.to_owned()));
        }
        let (state, outcome) = match outcome {
            PassOutcome::Completed { message } => {
                let mut ok = serde_json::json!({"ok": {
                    "session_id": self.session_id(),
                    "model": self.model,
                    "message": message,
                }});
                if let Some(usage) = self.shared.state.read(|state| state.usage.get(id).cloned()) {
                    ok["ok"]["usage"] = serde_json::to_value(usage).map_err(|error| {
                        InboxError::Agent(NanocodeError::InvalidRequest(error.to_string()))
                    })?;
                }
                (TurnState::Completed, fields(ok))
            }
            PassOutcome::Failed { error } => (
                TurnState::Failed,
                fields(serde_json::json!({"error": error})),
            ),
            PassOutcome::Cancelled => (
                TurnState::Cancelled,
                fields(serde_json::json!({"cancelled": true})),
            ),
        };
        self.finish(id, state, outcome).await
    }

    async fn request_cancelled(&self, record: &PassRecord) -> Result<bool, InboxError> {
        if self
            .shared
            .state
            .read(|state| state.cancelled(&record.request_id))
        {
            return Ok(true);
        }
        if record.origin == PassOrigin::Live {
            return Ok(false);
        }
        Ok(self
            .shared
            .log
            .turn(&record.request_id)
            .await?
            .is_some_and(|turn| turn.cancel_requested || turn.state == TurnState::Cancelled))
    }
}
