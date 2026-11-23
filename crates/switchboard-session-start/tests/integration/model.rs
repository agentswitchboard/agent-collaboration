//! A separate plan model. The case table lives in rows; this file applies its decisions.

use fleet_identity_protocol::{
    CreateFleetSession, CreateSessionKind, EndFleetSession, EndSessionTarget,
    PresentedSessionSecret, RouterEndCause, SessionCreator,
};
use state_machine_core::TransitionError;
use switchboard_session_start::{
    CentralStanding, CleanupOutcome, ConversationFact, Directed, DirectiveId, EndLeg, EndNotDue,
    EndNotSent, EnvelopeRefusal, Exposure, FaultContext, FinishedOutcome, LaunchFact,
    OpenedSession, OpeningCommit, PointerFact, Recording, SecretFact, SnapshotFacts, SnapshotKind,
    StartAnswer, StartAsker, StartDirective, StartEffect, StartEnvelope, StartFacts, StartInput,
    StartInstanceId, StartNotification, StartRefusal, StartRevision, StartSnapshot,
    StartStateKind as State, StopReason, WaiterSlot,
};

#[path = "model_rows.rs"]
mod rows;

use super::fixtures::attempt;
use rows::{Destination, Row};

#[derive(Clone)]
pub(super) struct Model {
    pub state: State,
    pub envelope: Option<StartEnvelope>,
    pub facts: StartFacts,
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
            facts: StartFacts {
                create_exposure: Exposure::None,
                reservation: None,
                secret: SecretFact::None,
                pointer: PointerFact::None,
                launch: LaunchFact::NotSent,
                conversation: ConversationFact::None,
                bind_exposure: Exposure::None,
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
        model.facts = data.facts.clone();
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
    ) -> Result<Vec<StartEffect>, TransitionError<StartRefusal>> {
        let mut effects = Vec::new();
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
        Ok(effects)
    }

    fn ask(
        &mut self,
        slot: WaiterSlot,
        candidate: Result<StartEnvelope, EnvelopeRefusal>,
        central: CentralStanding,
        effects: &mut Vec<StartEffect>,
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
                    StartSnapshot::CreateIntended(self.data()),
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
                        StartSnapshot::CreateIntended(self.data()),
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
                    StartSnapshot::BindIntended(self.data()),
                    effects,
                );
                return;
            }
            (Ok(_), Some(_), State::CreateUnknown | State::BindUnknown, other) => {
                StartAnswer::RefusedNoCentral(other)
            }
            _ => self.terminal.as_ref().unwrap().clone(),
        };
        effects.push(tell(slot, answer));
    }

    fn apply(&mut self, row: Row, effects: &mut Vec<StartEffect>) {
        match row {
            Row::Issue(state, directive) => self.issue(state, directive, effects),
            Row::Save(state, snapshot) => self.write(state, snapshot, effects),
            Row::Cleanup(outcome, end) => {
                self.facts.end = end;
                let snapshot = StartSnapshot::CleaningUp {
                    start: self.data(),
                    outcome,
                };
                self.write(State::SavingCleanUp, snapshot, effects);
            }
            Row::WindDown(destination) => self.wind_down(destination, effects),
            Row::SaveFailed(why, kind) => self.save_failed(why, kind, effects),
            Row::Idle(state, why) => {
                self.state = state;
                let facts = Box::new(self.facts.clone());
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
            Row::Finish(destination) => self.finish(destination, effects),
            Row::WithFault(fault, next) => {
                effects.push(StartEffect::Notification(StartNotification::ReportFault(
                    fault,
                )));
                self.apply(*next, effects);
            }
        }
    }

    fn data(&self) -> Box<SnapshotFacts> {
        Box::new(SnapshotFacts {
            envelope: self.envelope.as_ref().unwrap().clone(),
            attempt: attempt(self.attempt),
            facts: self.facts.clone(),
        })
    }

    fn issue(&mut self, state: State, directive: StartDirective, effects: &mut Vec<StartEffect>) {
        self.state = state;
        self.sequence += 1;
        let id = DirectiveId {
            instance: self.instance.clone(),
            sequence: self.sequence,
        };
        self.pending = Some(id.clone());
        effects.push(StartEffect::Directive(Directed { id, directive }));
    }

    fn write(&mut self, state: State, snapshot: StartSnapshot, effects: &mut Vec<StartEffect>) {
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
        let switchboard_session_start::ConversationFact::Held(Some(harness_session_id)) =
            &self.facts.conversation
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
            session_id: session_id.clone(),
        };
        self.opening = Some(session.clone());
        StartDirective::CommitOpening {
            expected: self.revision.unwrap(),
            opening: Box::new(OpeningCommit {
                address,
                session_id,
                harness_session_id,
                issue,
                opened: StartSnapshot::Opened {
                    start: self.data(),
                    session,
                },
            }),
        }
    }

    fn fault_context(&self) -> FaultContext {
        FaultContext {
            scope: self.envelope.as_ref().unwrap().scope().clone(),
            facts: Box::new(self.facts.clone()),
        }
    }

    fn wind_down(&mut self, destination: Destination, effects: &mut Vec<StartEffect>) {
        use switchboard_session_start::ConversationFact;
        if matches!(self.facts.conversation, ConversationFact::Held(_)) {
            self.close_then = Some(destination);
            self.issue(State::Closing, StartDirective::CloseConversation, effects);
            return;
        }
        let data = self.data();
        let target = if self.facts.settlement.is_some() {
            StartSnapshot::Settled(data)
        } else {
            match destination {
                Destination::Failed(reason) => StartSnapshot::Failed {
                    start: data,
                    reason,
                },
                Destination::Settled => StartSnapshot::Settled(data),
                Destination::Fenced => StartSnapshot::Fenced(data),
                Destination::Unknown(reason) => StartSnapshot::Unknown {
                    start: data,
                    reason,
                },
                Destination::Opened | Destination::Stopped(..) => {
                    panic!("invalid wind-down destination")
                }
            }
        };
        self.write(State::SavingOutcome, target, effects);
    }

    fn save_failed(
        &mut self,
        why: Recording,
        while_saving: SnapshotKind,
        effects: &mut Vec<StartEffect>,
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
        let destination = Destination::Stopped(stop, while_saving);
        if while_saving == SnapshotKind::CreateIntended
            && self.revision.is_none()
            && why == Recording::NotRecorded
        {
            self.finish(Destination::Failed(FailureReason::CouldNotSave), effects);
        } else if matches!(self.facts.conversation, ConversationFact::Held(_)) {
            self.close_then = Some(destination);
            self.issue(State::Closing, StartDirective::CloseConversation, effects);
        } else {
            self.finish(destination, effects);
        }
    }

    fn finish(&mut self, destination: Destination, effects: &mut Vec<StartEffect>) {
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
            Destination::Stopped(why, while_saving) => (
                State::Stopped,
                FinishedOutcome::Stopped { why, while_saving },
            ),
        };
        self.state = state;
        let answer = StartAnswer::Finished {
            outcome,
            facts: Box::new(self.facts.clone()),
            recording: self.recording,
        };
        self.tell_waiters(&answer, effects);
        self.terminal = Some(answer);
    }

    fn tell_waiters(&mut self, answer: &StartAnswer, effects: &mut Vec<StartEffect>) {
        effects.extend(
            self.waiters
                .drain(..)
                .map(|slot| tell(slot, answer.clone())),
        );
    }
}

fn tell(slot: WaiterSlot, answer: StartAnswer) -> StartEffect {
    StartEffect::Notification(StartNotification::Tell { slot, answer })
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
