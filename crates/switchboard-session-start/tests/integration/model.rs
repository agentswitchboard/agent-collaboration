//! A separate plan model. The case table lives in rows; this file applies its decisions.

use fleet_identity_protocol::{
    CreateFleetSession, CreateSessionKind, EndFleetSession, EndSessionTarget,
    PresentedSessionSecret, RouterEndCause, SessionCreator,
};
use state_machine_core::TransitionError;
use switchboard_session_start::{
    CentralStanding, CleanupOutcome, ConversationFact, DestroyPendingSecret, Directed, DirectiveId,
    EndLeg, EndNotDue, EndNotSent, EnvelopeRefusal, Exposure, FaultContext, FinishedOutcome,
    IntendedOutcome, LaunchFact, OpenedSession, OpeningCommit, PointerFact, Recording, SecretFact,
    SnapshotFacts, SnapshotKind, SnapshotPhase, StartAnswer, StartAsker, StartDirective,
    StartEnvelope, StartFacts, StartInput, StartInstanceId, StartNotification, StartOutput,
    StartRefusal, StartRevision, StartSnapshot, StartStateKind as State, StopReason, StoredFacts,
    StoredSnapshot, WaiterSlot,
};

#[path = "model_rows.rs"]
mod rows;

use super::fixtures::attempt;
use rows::{Destination, Row};

#[derive(Clone)]
pub(super) struct Model {
    pub state: State,
    pub envelope: Option<StartEnvelope>,
    pub facts: StoredFacts,
    pub attempt: u64,
    pub revision: Option<StartRevision>,
    pub recording: Recording,
    pub pending: Option<DirectiveId>,
    instance: StartInstanceId,
    sequence: u64,
    waiters: Vec<WaiterSlot>,
    save: Option<StartSnapshot>,
    cleanup: Option<CleanupOutcome>,
    close_then: Option<Destination>,
    terminal: Option<StartAnswer>,
    opening: Option<OpenedSession>,
}

impl Model {
    pub(super) fn new(instance: StartInstanceId) -> Self {
        Self {
            state: State::NotStarted,
            envelope: None,
            facts: StoredFacts {
                create_exposure: Exposure::None,
                reservation: None,
                secret: SecretFact::None,
                pointer: PointerFact::None,
                launch: LaunchFact::NotIntended,
                harness_session_id: None,
                conversation: ConversationFact::None,
                bind_exposure: Exposure::None,
                active_proof: None,
                settlement: None,
                end: EndLeg::NotDue(EndNotDue::NothingToEnd),
            },
            attempt: 1,
            revision: None,
            recording: Recording::NotRecorded,
            pending: None,
            instance,
            sequence: 0,
            waiters: Vec::new(),
            save: None,
            cleanup: None,
            close_then: None,
            terminal: None,
            opening: None,
        }
    }

    pub(super) fn interrupted(
        instance: StartInstanceId,
        revision: StartRevision,
        data: &SnapshotFacts,
    ) -> Self {
        let mut model = Self::new(instance);
        model.state = State::Interrupted;
        model.envelope = Some(data.envelope.clone());
        model.facts = data.facts.as_stored().clone();
        model.attempt = data.attempt.get();
        model.revision = Some(revision);
        model.recording = Recording::Recorded;
        model.terminal = Some(StartAnswer::Interrupted {
            facts: Box::new(data.facts.clone()),
        });
        model
    }

    pub(super) fn feed(
        &mut self,
        input: StartInput,
    ) -> Result<Vec<StartOutput>, TransitionError<StartRefusal>> {
        let mut effects = StartOutput::default();
        match input {
            StartInput::Ask {
                slot,
                candidate,
                central,
            } => self.ask(slot, candidate, central, &mut effects),
            StartInput::DirectiveResult { id, result } => {
                if self.pending.as_ref() != Some(&id) {
                    return Err(TransitionError::Domain(
                        StartRefusal::NotTheOutstandingDirective,
                    ));
                }
                let mut proposed = self.clone();
                proposed.pending = None;
                let row = Row::select(&mut proposed, result)
                    .ok_or(TransitionError::Domain(StartRefusal::NotExpectedHere))?;
                proposed.apply(row, &mut effects);
                *self = proposed;
            }
        }
        Ok(vec![effects])
    }

    fn ask(
        &mut self,
        slot: WaiterSlot,
        candidate: Result<StartEnvelope, EnvelopeRefusal>,
        central: CentralStanding,
        effects: &mut StartOutput,
    ) {
        let answer = match (candidate, self.envelope.as_ref(), self.state, central) {
            (Err(EnvelopeRefusal::NoProvableAsker), _, _, _) => StartAnswer::RefusedNoIdentity,
            (Err(other), _, _, _) => StartAnswer::Refused(other),
            (Ok(candidate), Some(held), _, _)
                if compared_fields(&candidate) != compared_fields(held) =>
            {
                StartAnswer::RefusedDifferentRequest
            }
            (Ok(candidate), None, State::NotStarted, CentralStanding::Ready) => {
                self.envelope = Some(candidate);
                self.waiters.push(slot);
                self.write(
                    State::SavingCreate,
                    self.snapshot(SnapshotPhase::CreateIntended),
                    effects,
                );
                return;
            }
            (Ok(_), None, State::NotStarted, other) => StartAnswer::RefusedNoCentral(other),
            (Ok(_), Some(_), _, _) if self.pending.is_some() => {
                if !self.waiters.contains(&slot) {
                    self.waiters.push(slot);
                }
                return;
            }
            (Ok(_), Some(_), State::CreateUnknown, CentralStanding::Ready) => {
                self.waiters.push(slot);
                if self.attempt < 9_007_199_254_740_991 {
                    self.attempt += 1;
                    self.write(
                        State::SavingCreate,
                        self.snapshot(SnapshotPhase::CreateIntended),
                        effects,
                    );
                } else {
                    self.wind_down(
                        Destination::Unknown(
                            switchboard_session_start::UnknownReason::AttemptCeiling,
                        ),
                        effects,
                    );
                }
                return;
            }
            (Ok(_), Some(_), State::BindUnknown, CentralStanding::Ready) => {
                self.waiters.push(slot);
                self.write(
                    State::SavingBind,
                    self.snapshot(SnapshotPhase::BindIntended),
                    effects,
                );
                return;
            }
            (Ok(_), Some(_), State::CreateUnknown | State::BindUnknown, other) => {
                StartAnswer::RefusedNoCentral(other)
            }
            _ => self.terminal.as_ref().unwrap().clone(),
        };
        effects.notifications.push(tell(slot, answer));
    }

    fn apply(&mut self, row: Row, effects: &mut StartOutput) {
        match row {
            Row::Issue(state, directive) => self.issue(state, directive, effects),
            Row::Save(state, snapshot) => self.write(state, snapshot, effects),
            Row::Cleanup(outcome, end) => {
                self.facts.end = end;
                let snapshot = self.snapshot(SnapshotPhase::CleaningUp(outcome));
                self.write(State::SavingCleanUp, snapshot, effects);
            }
            Row::WindDown(destination) => self.wind_down(destination, effects),
            Row::SaveFailed(why, kind) => self.save_failed(why, kind, effects),
            Row::Idle(state, why) => {
                self.state = state;
                let facts = self.checked_facts();
                let answer = why.map_or_else(
                    || StartAnswer::OutcomeNotKnown {
                        facts: facts.clone(),
                    },
                    |why| StartAnswer::NotSentNow {
                        why,
                        facts: facts.clone(),
                    },
                );
                self.tell_waiters(&answer, effects);
            }
            Row::CentralRefused(state, reason) => {
                self.state = state;
                self.tell_waiters(
                    &StartAnswer::RefusedByCentralNow {
                        reason,
                        facts: self.checked_facts(),
                    },
                    effects,
                );
            }
            Row::DestroyPending(issue, next) => {
                effects.pending_secret = Some(DestroyPendingSecret(issue));
                self.apply(*next, effects);
            }
            Row::Finish(destination) => self.finish(destination, effects),
            Row::Notify(notification, next) => {
                effects.notifications.push(notification);
                self.apply(*next, effects);
            }
        }
    }

    fn snapshot(&self, phase: SnapshotPhase) -> StartSnapshot {
        self.snapshot_with(phase, self.facts.clone())
    }

    fn snapshot_with(&self, phase: SnapshotPhase, facts: StoredFacts) -> StartSnapshot {
        let envelope = self.envelope.as_ref().unwrap();
        StartSnapshot::from_stored(
            envelope.scope(),
            StoredSnapshot {
                envelope: envelope.clone(),
                attempt: attempt(self.attempt),
                facts,
                phase,
            },
        )
        .unwrap()
    }

    fn checked_facts(&self) -> Box<StartFacts> {
        Box::new(StartFacts::from_stored(self.facts.clone()).unwrap())
    }

    fn issue(&mut self, state: State, directive: StartDirective, effects: &mut StartOutput) {
        self.state = state;
        self.sequence += 1;
        let id = DirectiveId {
            instance: self.instance.clone(),
            sequence: self.sequence,
        };
        self.pending = Some(id.clone());
        effects.directive = Some(Directed { id, directive });
    }

    fn write(&mut self, state: State, snapshot: StartSnapshot, effects: &mut StartOutput) {
        self.save = Some(snapshot.clone());
        self.issue(
            state,
            StartDirective::Save {
                expected: self.revision,
                snapshot,
            },
            effects,
        );
    }

    fn create(&self) -> StartDirective {
        let envelope = self.envelope.as_ref().unwrap();
        StartDirective::SendCreate(CreateFleetSession {
            operation_id: envelope.scope().operation_id.clone(),
            attempt: attempt(self.attempt),
            creator: match envelope.asker() {
                StartAsker::Operator => SessionCreator::RouterOperator {},
                StartAsker::Session { session_id } => SessionCreator::Session {
                    session_id: session_id.clone(),
                    presented: PresentedSessionSecret::Nothing {},
                },
            },
            agent_program: envelope.agent_program().clone(),
            session_kind: CreateSessionKind::Ordinary {},
        })
    }

    fn bind(&self) -> StartDirective {
        let (ConversationFact::Held, Some(harness_session_id)) =
            (self.facts.conversation, &self.facts.harness_session_id)
        else {
            panic!("missing held id")
        };
        let switchboard_session_start::PointerFact::Committed(issue) = &self.facts.pointer else {
            panic!("missing secret issue")
        };
        StartDirective::SendBind {
            session_id: issue.session_id.clone(),
            harness_session_id: harness_session_id.clone(),
            issue: issue.clone(),
        }
    }

    fn end(&self) -> StartDirective {
        StartDirective::SendEnd(EndFleetSession {
            target: EndSessionTarget::CentralId {
                session_id: self.facts.reservation.as_ref().unwrap().session_id.clone(),
            },
            cause: RouterEndCause::GivenUp {},
        })
    }

    fn open(&mut self, address: workspace_ids::SessionRef) -> StartDirective {
        let StartDirective::SendBind {
            session_id,
            harness_session_id,
            issue,
        } = self.bind()
        else {
            panic!("bind fields")
        };
        let session = OpenedSession {
            address: address.clone(),
            session_id,
            harness_session_id,
            issue,
        };
        let opening = OpeningCommit::new(
            self.envelope.as_ref().unwrap(),
            &self.checked_facts(),
            address,
        )
        .unwrap();
        assert_eq!(opening.association(), &session);
        self.opening = Some(session);
        StartDirective::CommitOpening {
            expected: self.revision.unwrap(),
            opening: Box::new(opening),
        }
    }

    fn fault_context(&self) -> FaultContext {
        FaultContext {
            scope: self.envelope.as_ref().unwrap().scope().clone(),
            facts: self.checked_facts(),
        }
    }

    fn wind_down(&mut self, destination: Destination, effects: &mut StartOutput) {
        use switchboard_session_start::ConversationFact;
        if self.facts.conversation == ConversationFact::Held {
            self.close_then = Some(destination);
            self.issue(State::Closing, StartDirective::CloseConversation, effects);
            return;
        }
        let target = if self.facts.settlement.is_some() {
            SnapshotPhase::Settled
        } else {
            match destination {
                Destination::Failed(reason) => SnapshotPhase::Failed(reason),
                Destination::Settled => SnapshotPhase::Settled,
                Destination::Fenced => SnapshotPhase::Fenced,
                Destination::Unknown(reason) => SnapshotPhase::Unknown(reason),
                Destination::Opened | Destination::Stopped(..) => {
                    panic!("invalid wind-down destination")
                }
            }
        };
        self.write(State::SavingOutcome, self.snapshot(target), effects);
    }

    fn save_failed(
        &mut self,
        why: Recording,
        while_saving: SnapshotKind,
        effects: &mut StartOutput,
    ) {
        use switchboard_session_start::{ConversationFact, FailureReason, PointerFact};
        self.recording = why;
        if while_saving == SnapshotKind::LaunchIntended && why != Recording::NotRecorded {
            let PointerFact::Committed(issue) = &self.save.as_ref().unwrap().start().facts.pointer
            else {
                panic!("launch pointer")
            };
            self.facts.pointer = PointerFact::PossiblyCommitted(issue.clone());
        }
        if self.facts.end == EndLeg::Due {
            self.facts.end = EndLeg::NotSent(EndNotSent::RecordUnavailable);
        }
        let stop = match why {
            Recording::NotRecorded => StopReason::RecordNotCommitted,
            Recording::Conflict => StopReason::RecordConflict,
            Recording::NotKnown => StopReason::RecordNotKnown,
            Recording::Recorded => panic!("confirmed record passed to failure handler"),
        };
        let intended = if while_saving == SnapshotKind::Opened {
            None
        } else {
            self.save.as_ref().and_then(|snapshot| {
                if let Some((_, issue)) = snapshot.reserved() {
                    effects.pending_secret = Some(DestroyPendingSecret(issue.clone()));
                }
                match snapshot.phase() {
                    SnapshotPhase::Failed(reason) => Some(IntendedOutcome::Failed(reason.clone())),
                    SnapshotPhase::CleaningUp(CleanupOutcome::Failed(reason)) => {
                        Some(IntendedOutcome::CleaningUp(reason.clone()))
                    }
                    SnapshotPhase::Settled | SnapshotPhase::CleaningUp(CleanupOutcome::Settled) => {
                        Some(IntendedOutcome::Settled)
                    }
                    SnapshotPhase::Fenced => Some(IntendedOutcome::Fenced),
                    SnapshotPhase::Unknown(reason) => Some(IntendedOutcome::Unknown(*reason)),
                    _ => None,
                }
            })
        };
        let destination = Destination::Stopped(stop, while_saving, intended);
        if while_saving == SnapshotKind::CreateIntended
            && self.revision.is_none()
            && why == Recording::NotRecorded
        {
            self.finish(Destination::Failed(FailureReason::CouldNotSave), effects);
        } else if self.facts.conversation == ConversationFact::Held {
            self.close_then = Some(destination);
            self.issue(State::Closing, StartDirective::CloseConversation, effects);
        } else {
            self.finish(destination, effects);
        }
    }

    fn finish(&mut self, destination: Destination, effects: &mut StartOutput) {
        let (state, outcome) = match destination {
            Destination::Opened => {
                let session = self.opening.as_ref().unwrap();
                let answer = StartAnswer::Started {
                    address: session.address.clone(),
                    session_id: session.session_id.clone(),
                };
                self.state = State::Opened;
                self.tell_waiters(&answer, effects);
                self.terminal = Some(answer);
                return;
            }
            Destination::Failed(reason) => (State::Failed, FinishedOutcome::Failed(reason)),
            Destination::Settled => (State::Settled, FinishedOutcome::Settled),
            Destination::Fenced => (State::Fenced, FinishedOutcome::Fenced),
            Destination::Unknown(reason) => (State::Unknown, FinishedOutcome::Unknown(reason)),
            Destination::Stopped(why, while_saving, intended) => (
                State::Stopped,
                FinishedOutcome::Stopped {
                    why,
                    while_saving,
                    intended,
                },
            ),
        };
        self.state = state;
        let answer = StartAnswer::Finished {
            outcome,
            facts: self.checked_facts(),
            recording: self.recording,
        };
        self.tell_waiters(&answer, effects);
        self.terminal = Some(answer);
    }

    fn tell_waiters(&mut self, answer: &StartAnswer, effects: &mut StartOutput) {
        effects.notifications.extend(
            self.waiters
                .drain(..)
                .map(|slot| tell(slot, answer.clone())),
        );
    }
}

fn tell(slot: WaiterSlot, answer: StartAnswer) -> StartNotification {
    StartNotification::Tell { slot, answer }
}

fn compared_fields(
    envelope: &StartEnvelope,
) -> (
    &switchboard_session_start::StartScope,
    &workspace_ids::MachineId,
    &StartAsker,
    &fleet_identity_protocol::HarnessKind,
    switchboard_session_start::LocalStartDigest,
    &fleet_identity_protocol::CreateSessionDigest,
) {
    (
        envelope.scope(),
        envelope.machine(),
        envelope.asker(),
        envelope.agent_program(),
        envelope.local_digest(),
        envelope.central_digest(),
    )
}
