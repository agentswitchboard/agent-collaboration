use fleet_identity_protocol::CreateSessionAttempt;
use state_machine_core::{StateMachine, StateMachineResult, StateTag, Transition, TransitionError};

use crate::{
    CentralStanding, CleanupOutcome, Directed, DirectiveId, DirectiveResult, EnvelopeRefusal,
    FaultContext, FinishedOutcome, NotSentNowReason, OpenedSession, Recording, SecretIssue,
    SnapshotFacts, SnapshotKind, StartAnswer, StartDirective, StartEffect, StartEnvelope,
    StartFacts, StartFault, StartInput, StartInstanceId, StartNotification, StartRefusal,
    StartRevision, StartSnapshot, StopReason, UnknownReason, WaiterSlot,
    transition_table::TRANSITIONS,
};

mod central;
mod harness;
mod records;
#[cfg(test)]
#[path = "machine_test.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartStateKind {
    NotStarted,
    SavingCreate,
    CreateSent,
    CreateUnknown,
    StoringSecret,
    SavingLaunchIntent,
    Launching,
    SavingHeld,
    SettingUp,
    SavingBind,
    BindSent,
    BindUnknown,
    Opening,
    SavingCleanUp,
    Ending,
    Closing,
    SavingOutcome,
    Opened,
    Failed,
    Settled,
    Fenced,
    Unknown,
    Stopped,
    Interrupted,
}

impl StartStateKind {
    pub const fn tag(self) -> &'static str {
        match self {
            Self::NotStarted => "NotStarted",
            Self::SavingCreate => "SavingCreate",
            Self::CreateSent => "CreateSent",
            Self::CreateUnknown => "CreateUnknown",
            Self::StoringSecret => "StoringSecret",
            Self::SavingLaunchIntent => "SavingLaunchIntent",
            Self::Launching => "Launching",
            Self::SavingHeld => "SavingHeld",
            Self::SettingUp => "SettingUp",
            Self::SavingBind => "SavingBind",
            Self::BindSent => "BindSent",
            Self::BindUnknown => "BindUnknown",
            Self::Opening => "Opening",
            Self::SavingCleanUp => "SavingCleanUp",
            Self::Ending => "Ending",
            Self::Closing => "Closing",
            Self::SavingOutcome => "SavingOutcome",
            Self::Opened => "Opened",
            Self::Failed => "Failed",
            Self::Settled => "Settled",
            Self::Fenced => "Fenced",
            Self::Unknown => "Unknown",
            Self::Stopped => "Stopped",
            Self::Interrupted => "Interrupted",
        }
    }

    const fn has_directive(self) -> bool {
        matches!(
            self,
            Self::SavingCreate
                | Self::CreateSent
                | Self::StoringSecret
                | Self::SavingLaunchIntent
                | Self::Launching
                | Self::SavingHeld
                | Self::SettingUp
                | Self::SavingBind
                | Self::BindSent
                | Self::Opening
                | Self::SavingCleanUp
                | Self::Ending
                | Self::Closing
                | Self::SavingOutcome
        )
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Opened
                | Self::Failed
                | Self::Settled
                | Self::Fenced
                | Self::Unknown
                | Self::Stopped
                | Self::Interrupted
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Stage {
    Saving(StartSnapshot),
    CreateSent,
    CreateUnknown,
    StoringSecret(SecretIssue),
    Launching,
    SettingUp,
    BindSent,
    BindUnknown,
    Opening(OpenedSession),
    Ending(CleanupOutcome),
    Closing(AfterClose),
    Opened(OpenedSession),
    Finished(FinishedOutcome),
    Interrupted(StartSnapshot),
}

impl Stage {
    const fn kind(&self) -> StartStateKind {
        match self {
            Self::Saving(snapshot) => match snapshot.kind() {
                SnapshotKind::CreateIntended => StartStateKind::SavingCreate,
                SnapshotKind::LaunchIntended => StartStateKind::SavingLaunchIntent,
                SnapshotKind::Held => StartStateKind::SavingHeld,
                SnapshotKind::BindIntended => StartStateKind::SavingBind,
                SnapshotKind::CleaningUp => StartStateKind::SavingCleanUp,
                SnapshotKind::CreateUnknown
                | SnapshotKind::BindUnknown
                | SnapshotKind::Opened
                | SnapshotKind::Failed
                | SnapshotKind::Settled
                | SnapshotKind::Fenced
                | SnapshotKind::Unknown => StartStateKind::SavingOutcome,
            },
            Self::CreateSent => StartStateKind::CreateSent,
            Self::CreateUnknown => StartStateKind::CreateUnknown,
            Self::StoringSecret(_) => StartStateKind::StoringSecret,
            Self::Launching => StartStateKind::Launching,
            Self::SettingUp => StartStateKind::SettingUp,
            Self::BindSent => StartStateKind::BindSent,
            Self::BindUnknown => StartStateKind::BindUnknown,
            Self::Opening(_) => StartStateKind::Opening,
            Self::Ending(_) => StartStateKind::Ending,
            Self::Closing(_) => StartStateKind::Closing,
            Self::Opened(_) => StartStateKind::Opened,
            Self::Finished(outcome) => match outcome {
                FinishedOutcome::Failed(_) => StartStateKind::Failed,
                FinishedOutcome::Settled => StartStateKind::Settled,
                FinishedOutcome::Fenced => StartStateKind::Fenced,
                FinishedOutcome::Unknown(_) => StartStateKind::Unknown,
                FinishedOutcome::Stopped { .. } => StartStateKind::Stopped,
            },
            Self::Interrupted(_) => StartStateKind::Interrupted,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AfterClose {
    WindDown(WindDown),
    Stop {
        why: StopReason,
        while_saving: SnapshotKind,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WindDown {
    Cleanup(CleanupOutcome),
    Fenced,
    Unknown(UnknownReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordedStop {
    NotCommitted,
    Conflict,
    NotKnown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedStart {
    envelope: StartEnvelope,
    attempt: CreateSessionAttempt,
    facts: StartFacts,
    revision: Option<StartRevision>,
    recording: Recording,
    waiting: Vec<WaiterSlot>,
    stage: Stage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartMachine {
    instance: StartInstanceId,
    sequence: u64,
    accepted: Option<Box<AcceptedStart>>,
}

type Refused = TransitionError<StartRefusal>;
type Reduced = Result<(), Refused>;
const NOT_EXPECTED: Refused = TransitionError::Domain(StartRefusal::NotExpectedHere);

struct Outputs<'a> {
    instance: &'a StartInstanceId,
    sequence: &'a mut u64,
    effects: Vec<StartEffect>,
}

impl Outputs<'_> {
    fn directive(&mut self, directive: StartDirective) -> Reduced {
        *self.sequence = self.sequence.checked_add(1).ok_or(TransitionError::Domain(
            StartRefusal::DirectiveSequenceExhausted,
        ))?;
        self.effects.push(StartEffect::Directive(Directed {
            id: DirectiveId {
                instance: self.instance.clone(),
                sequence: *self.sequence,
            },
            directive,
        }));
        Ok(())
    }

    fn notify(&mut self, notification: StartNotification) {
        self.effects.push(StartEffect::Notification(notification));
    }

    fn tell(&mut self, slot: WaiterSlot, answer: StartAnswer) {
        self.notify(StartNotification::Tell { slot, answer });
    }
}

impl StartMachine {
    pub const fn new(instance: StartInstanceId) -> Self {
        Self {
            instance,
            sequence: 0,
            accepted: None,
        }
    }

    pub fn restored(
        instance: StartInstanceId,
        revision: StartRevision,
        snapshot: StartSnapshot,
    ) -> Self {
        let data = snapshot.start();
        let envelope = data.envelope.clone();
        let attempt = data.attempt;
        let facts = data.facts.clone();
        let stage = match snapshot {
            StartSnapshot::Opened { session, .. } => Stage::Opened(session),
            StartSnapshot::Failed { reason, .. } => {
                Stage::Finished(FinishedOutcome::Failed(reason))
            }
            StartSnapshot::Settled(_) => Stage::Finished(FinishedOutcome::Settled),
            StartSnapshot::Fenced(_) => Stage::Finished(FinishedOutcome::Fenced),
            StartSnapshot::Unknown { reason, .. } => {
                Stage::Finished(FinishedOutcome::Unknown(reason))
            }
            interrupted @ (StartSnapshot::CreateIntended(_)
            | StartSnapshot::CreateUnknown(_)
            | StartSnapshot::LaunchIntended(_)
            | StartSnapshot::Held(_)
            | StartSnapshot::BindIntended(_)
            | StartSnapshot::BindUnknown(_)
            | StartSnapshot::CleaningUp { .. }) => Stage::Interrupted(interrupted),
        };
        Self {
            instance,
            sequence: 0,
            accepted: Some(Box::new(AcceptedStart {
                envelope,
                attempt,
                facts,
                revision: Some(revision),
                recording: Recording::Recorded,
                waiting: Vec::new(),
                stage,
            })),
        }
    }

    pub fn kind(&self) -> StartStateKind {
        self.accepted
            .as_deref()
            .map_or(StartStateKind::NotStarted, |start| start.stage.kind())
    }

    pub fn envelope(&self) -> Option<&StartEnvelope> {
        self.accepted.as_deref().map(|start| &start.envelope)
    }

    pub fn facts(&self) -> Option<&StartFacts> {
        self.accepted.as_deref().map(|start| &start.facts)
    }

    pub fn recording(&self) -> Option<Recording> {
        self.accepted.as_deref().map(|start| start.recording)
    }

    pub fn revision(&self) -> Option<StartRevision> {
        self.accepted.as_deref().and_then(|start| start.revision)
    }

    pub fn attempt(&self) -> Option<CreateSessionAttempt> {
        self.accepted.as_deref().map(|start| start.attempt)
    }

    pub fn interrupted_snapshot(&self) -> Option<&StartSnapshot> {
        let Stage::Interrupted(snapshot) = &self.accepted.as_deref()?.stage else {
            return None;
        };
        Some(snapshot)
    }

    pub fn outstanding_id(&self) -> Option<DirectiveId> {
        self.kind().has_directive().then(|| DirectiveId {
            instance: self.instance.clone(),
            sequence: self.sequence,
        })
    }
}

impl StateTag for StartMachine {
    fn state_tag(&self) -> &'static str {
        self.kind().tag()
    }

    fn is_terminal(&self) -> bool {
        self.kind().is_terminal()
    }
}

impl StateMachine for StartMachine {
    type State = Self;
    type Event = StartInput;
    type Effect = StartEffect;
    type Error = StartRefusal;

    fn transition(state: &Self, input: StartInput) -> StateMachineResult<Self> {
        let mut next = state.clone();
        let mut outputs = Outputs {
            instance: &next.instance,
            sequence: &mut next.sequence,
            effects: Vec::new(),
        };
        match input {
            StartInput::Ask {
                slot,
                candidate,
                central,
            } => {
                ask(&mut next.accepted, slot, candidate, central, &mut outputs)?;
            }
            StartInput::DirectiveResult { id, result } => {
                if state.outstanding_id().as_ref() != Some(&id) {
                    return Err(TransitionError::Domain(
                        StartRefusal::NotTheOutstandingDirective,
                    ));
                }
                let start = next.accepted.as_deref_mut().ok_or(NOT_EXPECTED)?;
                start.complete(result, &mut outputs)?;
            }
        }
        let effects = outputs.effects;
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

fn ask(
    accepted: &mut Option<Box<AcceptedStart>>,
    slot: WaiterSlot,
    candidate: Result<StartEnvelope, EnvelopeRefusal>,
    central: CentralStanding,
    outputs: &mut Outputs<'_>,
) -> Reduced {
    let candidate = match candidate {
        Ok(candidate) => candidate,
        Err(refusal) => {
            let answer = if refusal == EnvelopeRefusal::NoProvableAsker {
                StartAnswer::RefusedNoIdentity
            } else {
                StartAnswer::Refused(refusal)
            };
            outputs.tell(slot, answer);
            return Ok(());
        }
    };
    if let Some(start) = accepted {
        return start.ask(slot, &candidate, central, outputs);
    }
    if central != CentralStanding::Ready {
        outputs.tell(slot, StartAnswer::RefusedNoCentral(central));
        return Ok(());
    }
    let attempt = CreateSessionAttempt::try_from(1).map_err(|_| NOT_EXPECTED)?;
    let mut start = Box::new(AcceptedStart {
        envelope: candidate,
        attempt,
        facts: StartFacts::default(),
        revision: None,
        recording: Recording::NotRecorded,
        waiting: vec![slot],
        stage: Stage::CreateUnknown,
    });
    start.save(
        StartSnapshot::CreateIntended(start.snapshot_facts()),
        outputs,
    )?;
    *accepted = Some(start);
    Ok(())
}

impl AcceptedStart {
    fn ask(
        &mut self,
        slot: WaiterSlot,
        candidate: &StartEnvelope,
        central: CentralStanding,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        if !self.envelope.same_request(candidate) {
            outputs.tell(slot, StartAnswer::RefusedDifferentRequest);
        } else if self.stage.kind().has_directive() {
            self.hold(slot);
        } else if let Some(answer) = self.terminal_answer() {
            outputs.tell(slot, answer);
        } else if central != CentralStanding::Ready {
            outputs.tell(slot, StartAnswer::RefusedNoCentral(central));
        } else if matches!(self.stage, Stage::BindUnknown) {
            self.hold(slot);
            self.save(StartSnapshot::BindIntended(self.snapshot_facts()), outputs)?;
        } else if let Some(attempt) = self.attempt.checked_next() {
            self.hold(slot);
            self.attempt = attempt;
            self.save(
                StartSnapshot::CreateIntended(self.snapshot_facts()),
                outputs,
            )?;
        } else {
            self.hold(slot);
            self.wind_down(WindDown::Unknown(UnknownReason::AttemptCeiling), outputs)?;
        }
        Ok(())
    }

    fn hold(&mut self, slot: WaiterSlot) {
        if !self.waiting.contains(&slot) {
            self.waiting.push(slot);
        }
    }

    fn tell_waiting(&mut self, answer: &StartAnswer, outputs: &mut Outputs<'_>) {
        for slot in self.waiting.drain(..) {
            outputs.tell(slot, answer.clone());
        }
    }

    fn terminal_answer(&self) -> Option<StartAnswer> {
        match &self.stage {
            Stage::Opened(session) => Some(StartAnswer::Started {
                address: session.address.clone(),
                session_id: session.session_id.clone(),
            }),
            Stage::Finished(outcome) => Some(StartAnswer::Finished {
                outcome: outcome.clone(),
                facts: Box::new(self.facts.clone()),
                recording: self.recording,
            }),
            Stage::Interrupted(_) => Some(StartAnswer::Interrupted {
                facts: Box::new(self.facts.clone()),
            }),
            Stage::Saving(_)
            | Stage::CreateSent
            | Stage::CreateUnknown
            | Stage::StoringSecret(_)
            | Stage::Launching
            | Stage::SettingUp
            | Stage::BindSent
            | Stage::BindUnknown
            | Stage::Opening(_)
            | Stage::Ending(_)
            | Stage::Closing(_) => None,
        }
    }

    fn tell_terminal(&mut self, outputs: &mut Outputs<'_>) {
        if let Some(answer) = self.terminal_answer() {
            self.tell_waiting(&answer, outputs);
        }
    }

    fn tell_not_sent(&mut self, why: NotSentNowReason, outputs: &mut Outputs<'_>) {
        self.tell_waiting(
            &StartAnswer::NotSentNow {
                why,
                facts: Box::new(self.facts.clone()),
            },
            outputs,
        );
    }

    fn tell_unknown(&mut self, outputs: &mut Outputs<'_>) {
        self.tell_waiting(
            &StartAnswer::OutcomeNotKnown {
                facts: Box::new(self.facts.clone()),
            },
            outputs,
        );
    }

    fn fault_context(&self) -> FaultContext {
        FaultContext {
            scope: self.envelope.scope().clone(),
            facts: Box::new(self.facts.clone()),
        }
    }

    fn report(fault: StartFault, outputs: &mut Outputs<'_>) {
        outputs.notify(StartNotification::ReportFault(fault));
    }

    fn complete(&mut self, result: DirectiveResult, outputs: &mut Outputs<'_>) -> Reduced {
        match result {
            DirectiveResult::Save(result) => self.saved(result, outputs),
            DirectiveResult::SendCreate(result) => self.create_result(result, outputs),
            DirectiveResult::PutSecret(result) => self.secret_result(result, outputs),
            DirectiveResult::Launch(result) => self.launch_result(result, outputs),
            DirectiveResult::SetUp(result) => self.set_up_result(result, outputs),
            DirectiveResult::SendBind(result) => self.bind_result(result, outputs),
            DirectiveResult::CommitOpening(result) => self.opening_result(result, outputs),
            DirectiveResult::SendEnd(result) => self.end_result(result, outputs),
            DirectiveResult::CloseConversation(result) => self.close_result(result, outputs),
        }
    }
}
