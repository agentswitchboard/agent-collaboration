use std::{collections::BTreeSet, num::NonZeroU64, time::Duration};

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::{
    ActiveEnrollment, Effect, EnrollmentDuringDrain, EnrollmentId, EnrollmentPair, Event, Fault,
    Front, FrontRefusal, Generation, LeaseStopResult, Metadata, QuiesceReason, Quiescing, ReadId,
    StopBudgets, StopReport, StopResult, StopTarget, teardown::Step, transition_table::TRANSITIONS,
};

const METADATA_POLL: Duration = Duration::from_secs(15);
const RETRY: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationPhase {
    Polling(FrontRefusal),
    RestartCooldown { read: ReadId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum State {
    Observing {
        phase: ObservationPhase,
    },
    Opening {
        generation: Generation,
        pair: EnrollmentPair,
    },
    Running {
        generation: Generation,
        pair: EnrollmentPair,
    },
    Quiescing(Quiescing),
    Faulted {
        pinned: EnrollmentPair,
        observed: EnrollmentPair,
    },
    ClosedWithOutstanding {
        pieces: BTreeSet<StopTarget>,
    },
    Closed,
}

impl State {
    pub const fn front(&self) -> Front {
        match self {
            Self::Running { generation, .. } => Front::Forwarding(*generation),
            Self::Observing {
                phase: ObservationPhase::Polling(reason),
            } => Front::Refusing(*reason),
            Self::Observing {
                phase: ObservationPhase::RestartCooldown { .. },
            } => Front::Refusing(FrontRefusal::Restarting),
            Self::Quiescing(cleanup) => Front::Refusing(cleanup.reason.refusal()),
            Self::Opening { .. } => Front::Refusing(FrontRefusal::Starting),
            Self::Faulted { .. } => Front::Refusing(FrontRefusal::EnrollmentMismatch),
            Self::ClosedWithOutstanding { .. } | Self::Closed => {
                Front::Refusing(FrontRefusal::ShuttingDown)
            }
        }
    }

    const fn observing_metadata(&self) -> bool {
        match self {
            Self::Observing { .. } | Self::Opening { .. } | Self::Running { .. } => true,
            Self::Quiescing(cleanup) => !matches!(
                cleanup.reason,
                QuiesceReason::Closing | QuiesceReason::DifferentPair { .. }
            ),
            Self::Faulted { .. } | Self::ClosedWithOutstanding { .. } | Self::Closed => false,
        }
    }

    fn owes_store_join(&self) -> bool {
        match self {
            Self::Closed => false,
            Self::ClosedWithOutstanding { pieces } => pieces.contains(&StopTarget::EnrollmentStore),
            Self::Observing { .. }
            | Self::Opening { .. }
            | Self::Running { .. }
            | Self::Quiescing(_)
            | Self::Faulted { .. } => true,
        }
    }
}

impl StateTag for State {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Observing { .. } => "Observing",
            Self::Opening { .. } => "Opening",
            Self::Running { .. } => "Running",
            Self::Quiescing(_) => "Quiescing",
            Self::Faulted { .. } => "Faulted",
            Self::ClosedWithOutstanding { .. } => "ClosedWithOutstanding",
            Self::Closed => "Closed",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Faulted { .. } | Self::ClosedWithOutstanding { .. } | Self::Closed
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SupervisorError {
    #[error("supervisor generation exhausted")]
    GenerationExhausted,
    #[error("metadata read token exhausted")]
    ReadTokenExhausted,
}

impl RefusalTag for SupervisorError {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::GenerationExhausted => "GenerationExhausted",
            Self::ReadTokenExhausted => "ReadTokenExhausted",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Supervisor {
    state: State,
    budgets: StopBudgets,
    pinned: Option<EnrollmentPair>,
    last_generation: Option<Generation>,
    latest_read: ReadId,
    unsettled_retirements: BTreeSet<EnrollmentId>,
    store_exit_reported: bool,
}

type Refused = TransitionError<SupervisorError>;

impl Supervisor {
    /// Creates the supervisor and returns its initial front and read effects.
    pub fn new(budgets: StopBudgets) -> (Self, Vec<Effect>) {
        let read = ReadId(NonZeroU64::MIN);
        let state = State::Observing {
            phase: ObservationPhase::Polling(FrontRefusal::AwaitingFirstRead),
        };
        let effects = vec![
            Effect::PublishFront(state.front()),
            Effect::ScheduleMetadataRead {
                read,
                after: Duration::ZERO,
            },
        ];
        (
            Self {
                state,
                budgets,
                pinned: None,
                last_generation: None,
                latest_read: read,
                unsettled_retirements: BTreeSet::new(),
                store_exit_reported: false,
            },
            effects,
        )
    }

    pub const fn state(&self) -> &State {
        &self.state
    }

    /// High-water mark of issued reads. Its authority ends when observation stops.
    pub const fn latest_read(&self) -> ReadId {
        self.latest_read
    }

    /// Whether the current timer can still read enrollment metadata.
    #[must_use]
    pub fn wants_metadata_read(&self, read: ReadId) -> bool {
        read == self.latest_read && self.state.observing_metadata()
    }

    fn schedule(
        &mut self,
        after: Duration,
        effects: &mut Vec<Effect>,
    ) -> Result<ReadId, SupervisorError> {
        let read = ReadId(
            self.latest_read
                .0
                .checked_add(1)
                .ok_or(SupervisorError::ReadTokenExhausted)?,
        );
        self.latest_read = read;
        effects.push(Effect::ScheduleMetadataRead { read, after });
        Ok(read)
    }

    fn start(
        &mut self,
        pair: EnrollmentPair,
        effects: &mut Vec<Effect>,
    ) -> Result<(), SupervisorError> {
        let generation = match self.last_generation {
            Some(previous) => Generation(
                previous
                    .0
                    .checked_add(1)
                    .ok_or(SupervisorError::GenerationExhausted)?,
            ),
            None => Generation(NonZeroU64::MIN),
        };
        self.last_generation = Some(generation);
        self.state = State::Opening {
            generation,
            pair: pair.clone(),
        };
        effects.push(Effect::StartUnit {
            generation,
            pair,
            retired_enrollments: self.unsettled_retirements.clone(),
        });
        Ok(())
    }

    fn reduce(&mut self, event: Event, effects: &mut Vec<Effect>) -> Result<(), Refused> {
        let not_allowed = Refused::NotAllowed {
            state: self.state.state_tag(),
            event: event.event_tag(),
        };
        match event {
            Event::EnrollmentMayHaveChanged
                if matches!(
                    self.state,
                    State::Observing {
                        phase: ObservationPhase::RestartCooldown { .. }
                    }
                ) => {}
            Event::EnrollmentMayHaveChanged if self.state.observing_metadata() => {
                self.schedule(Duration::ZERO, effects)
                    .map_err(Refused::Domain)?;
            }
            Event::EnrollmentMayHaveChanged => return Err(not_allowed),
            Event::MetadataObserved { read, .. } if read > self.latest_read => {
                return Err(not_allowed);
            }
            Event::MetadataObserved { read, .. } if !self.wants_metadata_read(read) => {
                return Err(Refused::Stale);
            }
            Event::MetadataObserved { metadata, .. } => {
                self.observe(metadata, effects).map_err(Refused::Domain)?;
            }
            Event::BuildFinished { generation } => {
                if let State::Opening {
                    generation: current,
                    pair,
                } = &self.state
                    && *current == generation
                {
                    self.state = State::Running {
                        generation,
                        pair: pair.clone(),
                    };
                } else {
                    return Err(self.inapplicable_generation(generation, not_allowed));
                }
            }
            Event::BuildFailed { generation } => {
                if matches!(self.state, State::Opening { generation: current, .. } if current == generation)
                {
                    self.begin_quiescing(Some(generation), QuiesceReason::BuildFailed, effects);
                    effects.push(Effect::ReportFault(Fault::BuildFailed { generation }));
                } else {
                    return Err(self.inapplicable_generation(generation, not_allowed));
                }
            }
            Event::PieceExited { generation, piece } => {
                if matches!(self.state,
                    State::Opening { generation: current, .. } | State::Running { generation: current, .. }
                    if current == generation
                ) {
                    self.begin_quiescing(Some(generation), QuiesceReason::PieceExited, effects);
                    effects.push(Effect::ReportFault(Fault::PieceExited {
                        generation,
                        piece,
                    }));
                } else {
                    return Err(self.inapplicable_generation(generation, not_allowed));
                }
            }
            Event::EnrollmentStoreExited => {
                if self.store_exit_reported || !self.state.owes_store_join() {
                    return Err(Refused::Stale);
                }
                self.store_exit_reported = true;
                effects.push(Effect::ReportFault(Fault::EnrollmentStoreExited));
                if !self.is_closing() {
                    self.close(effects);
                }
            }
            Event::StopFinished(report) => self.stop_finished(report, effects)?,
            Event::Close => self.close(effects),
        }
        Ok(())
    }

    fn inapplicable_generation(&self, generation: Generation, not_allowed: Refused) -> Refused {
        if self
            .last_generation
            .is_some_and(|latest| generation <= latest)
        {
            Refused::Stale
        } else {
            not_allowed
        }
    }

    fn observe(
        &mut self,
        metadata: Metadata,
        effects: &mut Vec<Effect>,
    ) -> Result<(), SupervisorError> {
        match metadata {
            Metadata::ReadFailed => {
                if let State::Observing { phase } = &mut self.state {
                    *phase = ObservationPhase::Polling(FrontRefusal::MetadataUnreadable);
                }
                return self.schedule(RETRY, effects).map(|_| ());
            }
            Metadata::Empty => self.enrollment_lost(EnrollmentDuringDrain::Empty, effects),
            Metadata::Ended { .. } => self.enrollment_lost(EnrollmentDuringDrain::Ended, effects),
            Metadata::Active(ActiveEnrollment { pair, enrollment }) => {
                if let Some(pinned) = self.pinned.as_ref()
                    && *pinned != pair
                {
                    self.different_pair(pinned.clone(), pair, effects);
                    return Ok(());
                }
                self.pinned.get_or_insert_with(|| pair.clone());
                if self.unsettled_retirements.contains(&enrollment) {
                    self.enrollment_lost(EnrollmentDuringDrain::Ended, effects);
                } else if matches!(self.state, State::Observing { .. }) {
                    self.start(pair, effects)?;
                } else if let State::Quiescing(cleanup) = &mut self.state
                    && let QuiesceReason::EnrollmentLost(latest) = &mut cleanup.reason
                {
                    *latest = EnrollmentDuringDrain::Active;
                }
            }
        }
        self.schedule(METADATA_POLL, effects).map(|_| ())
    }

    fn enrollment_lost(&mut self, observed: EnrollmentDuringDrain, effects: &mut Vec<Effect>) {
        match &mut self.state {
            State::Observing { phase } => *phase = ObservationPhase::Polling(observed.refusal()),
            State::Opening { generation, .. } | State::Running { generation, .. } => {
                let generation = *generation;
                self.begin_quiescing(
                    Some(generation),
                    QuiesceReason::EnrollmentLost(observed),
                    effects,
                );
            }
            State::Quiescing(cleanup) => {
                if let QuiesceReason::EnrollmentLost(latest) = &mut cleanup.reason {
                    *latest = observed;
                }
            }
            State::Faulted { .. } | State::ClosedWithOutstanding { .. } | State::Closed => {}
        }
    }

    fn different_pair(
        &mut self,
        pinned: EnrollmentPair,
        observed: EnrollmentPair,
        effects: &mut Vec<Effect>,
    ) {
        effects.push(Effect::ReportFault(Fault::DifferentPair {
            pinned: pinned.clone(),
            observed: observed.clone(),
        }));
        match &mut self.state {
            State::Opening { generation, .. } | State::Running { generation, .. } => {
                let generation = *generation;
                self.begin_quiescing(
                    Some(generation),
                    QuiesceReason::DifferentPair { pinned, observed },
                    effects,
                );
            }
            State::Quiescing(cleanup) => {
                cleanup.reason = QuiesceReason::DifferentPair { pinned, observed }
            }
            State::Observing { .. } => self.state = State::Faulted { pinned, observed },
            State::Faulted { .. } | State::ClosedWithOutstanding { .. } | State::Closed => {}
        }
    }

    fn is_closing(&self) -> bool {
        matches!(
            self.state,
            State::Closed | State::ClosedWithOutstanding { .. }
        ) || matches!(&self.state, State::Quiescing(cleanup) if cleanup.reason == QuiesceReason::Closing)
    }

    fn begin_quiescing(
        &mut self,
        generation: Option<Generation>,
        reason: QuiesceReason,
        effects: &mut Vec<Effect>,
    ) {
        let cleanup = Quiescing::new(generation, reason);
        if let Some(target) = cleanup.joining {
            effects.push(Self::join(self.budgets, target));
        }
        self.state = State::Quiescing(cleanup);
    }

    fn close(&mut self, effects: &mut Vec<Effect>) {
        match &mut self.state {
            State::Observing { .. } | State::Faulted { .. } => {
                self.begin_quiescing(None, QuiesceReason::Closing, effects)
            }
            State::Opening { generation, .. } | State::Running { generation, .. } => {
                let generation = *generation;
                self.begin_quiescing(Some(generation), QuiesceReason::Closing, effects);
            }
            State::Quiescing(cleanup) => {
                if let Some(target) = cleanup.close() {
                    effects.push(Self::join(self.budgets, target));
                }
            }
            State::Closed | State::ClosedWithOutstanding { .. } => {}
        }
    }

    fn stop_finished(
        &mut self,
        report: StopReport,
        effects: &mut Vec<Effect>,
    ) -> Result<(), Refused> {
        let target = report.target();
        let result = report.result();
        if let State::ClosedWithOutstanding { pieces } = &mut self.state {
            if result != StopResult::Joined || !pieces.remove(&target) {
                return Err(Refused::Stale);
            }
            if pieces.is_empty() {
                self.state = State::Closed;
            }
            return Ok(());
        }
        let State::Quiescing(cleanup) = &mut self.state else {
            return Err(Refused::Stale);
        };
        let step = cleanup.accept(target, result);
        if step == Step::Ignored {
            return Err(Refused::Stale);
        }
        if let StopReport::LeaseAdapter {
            result: LeaseStopResult::Joined {
                unsettled_retirements,
            },
            ..
        } = report
        {
            self.unsettled_retirements.extend(unsettled_retirements);
        }
        if result == StopResult::TimedOut {
            effects.push(Effect::ReportFault(Fault::StopTimedOut { target }));
        }
        if let Some(next) = cleanup.joining {
            if step == Step::Advanced {
                effects.push(Self::join(self.budgets, next));
            }
            return Ok(());
        }
        if matches!(cleanup.reason, QuiesceReason::Closing) {
            let pieces = std::mem::take(&mut cleanup.outstanding);
            self.state = if pieces.is_empty() {
                State::Closed
            } else {
                State::ClosedWithOutstanding { pieces }
            };
            effects.push(Effect::ReleaseInputs);
        } else if cleanup.outstanding.is_empty() {
            match &cleanup.reason {
                QuiesceReason::DifferentPair { pinned, observed } => {
                    self.state = State::Faulted {
                        pinned: pinned.clone(),
                        observed: observed.clone(),
                    };
                }
                QuiesceReason::EnrollmentLost(latest) => {
                    self.state = State::Observing {
                        phase: ObservationPhase::Polling(latest.refusal()),
                    };
                    self.schedule(Duration::ZERO, effects)
                        .map_err(Refused::Domain)?;
                }
                QuiesceReason::BuildFailed | QuiesceReason::PieceExited => {
                    let read = self.schedule(RETRY, effects).map_err(Refused::Domain)?;
                    self.state = State::Observing {
                        phase: ObservationPhase::RestartCooldown { read },
                    };
                }
                QuiesceReason::Closing => {}
            }
        }
        Ok(())
    }

    const fn join(budgets: StopBudgets, target: StopTarget) -> Effect {
        Effect::StopAndJoin {
            target,
            budget: budgets.for_target(target),
        }
    }
}

impl StateMachine for Supervisor {
    type State = Self;
    type Event = Event;
    type Effect = Effect;
    type Error = SupervisorError;

    fn transition(state: &Self, event: Event) -> StateMachineResult<Self> {
        let mut next = state.clone();
        let mut effects = Vec::new();
        let store_exited = event == Event::EnrollmentStoreExited;
        next.reduce(event, &mut effects)?;
        if next.state.front() != state.state.front() {
            effects.insert(
                usize::from(store_exited),
                Effect::PublishFront(next.state.front()),
            );
        }
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

impl StateTag for Supervisor {
    fn state_tag(&self) -> &'static str {
        self.state.state_tag()
    }

    fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}
