use std::{collections::BTreeSet, num::NonZeroU64, time::Duration};

use state_machine_core::{
    RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::{
    ActiveEnrollment, Effect, EnrollmentId, EnrollmentPair, Event, Fault, Front, FrontRefusal,
    Generation, Metadata, QuiesceReason, Quiescing, ReadId, StopBudgets, StopReport, StopResult,
    StopTarget, UnitPiece, teardown::Step, transition_table::TRANSITIONS,
};

const METADATA_POLL: Duration = Duration::from_secs(15);
const RETRY: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum State {
    Observing {
        reason: FrontRefusal,
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
            Self::Observing { reason } => Front::Refusing(*reason),
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
}

impl Supervisor {
    /// Creates the supervisor and returns its initial front and read effects.
    pub fn new(budgets: StopBudgets) -> (Self, Vec<Effect>) {
        let read = ReadId(NonZeroU64::MIN);
        let state = State::Observing {
            reason: FrontRefusal::NotEnrolled,
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
            },
            effects,
        )
    }

    pub const fn state(&self) -> &State {
        &self.state
    }

    pub const fn latest_read(&self) -> ReadId {
        self.latest_read
    }

    fn schedule(
        &mut self,
        after: Duration,
        effects: &mut Vec<Effect>,
    ) -> Result<(), SupervisorError> {
        let read = ReadId(
            self.latest_read
                .0
                .checked_add(1)
                .ok_or(SupervisorError::ReadTokenExhausted)?,
        );
        self.latest_read = read;
        effects.push(Effect::ScheduleMetadataRead { read, after });
        Ok(())
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
        effects.push(Effect::StartUnit { generation, pair });
        Ok(())
    }

    fn reduce(&mut self, event: Event, effects: &mut Vec<Effect>) -> Result<(), SupervisorError> {
        match event {
            Event::EnrollmentMayHaveChanged if self.state.observing_metadata() => {
                self.schedule(Duration::ZERO, effects)?;
            }
            Event::EnrollmentMayHaveChanged => {}
            Event::MetadataObserved { read, metadata }
                if read == self.latest_read && self.state.observing_metadata() =>
            {
                self.observe(metadata, effects)?;
            }
            Event::MetadataObserved { .. } => {}
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
                }
            }
            Event::BuildFailed { generation } => {
                if matches!(self.state, State::Opening { generation: current, .. } if current == generation)
                {
                    self.begin_quiescing(Some(generation), QuiesceReason::BuildFailed, effects);
                    effects.push(Effect::ReportFault(Fault::BuildFailed { generation }));
                }
            }
            Event::PieceExited { generation, piece } => {
                self.piece_exited(generation, piece, effects)
            }
            Event::StopFinished(report) => self.stop_finished(report, effects)?,
            Event::Close => self.close(effects),
        }
        Ok(())
    }

    fn observe(
        &mut self,
        metadata: Metadata,
        effects: &mut Vec<Effect>,
    ) -> Result<(), SupervisorError> {
        match metadata {
            Metadata::ReadFailed => {
                if let State::Observing { reason } = &mut self.state {
                    *reason = FrontRefusal::MetadataUnreadable;
                }
                return self.schedule(RETRY, effects);
            }
            Metadata::Empty => self.enrollment_lost(QuiesceReason::NotEnrolled, effects),
            Metadata::Ended => self.enrollment_lost(QuiesceReason::EnrollmentEnded, effects),
            Metadata::Active(ActiveEnrollment { pair, enrollment }) => {
                if let Some(pinned) = self.pinned.as_ref()
                    && *pinned != pair
                {
                    self.different_pair(pinned.clone(), pair, effects);
                    return Ok(());
                }
                self.pinned.get_or_insert_with(|| pair.clone());
                if self.unsettled_retirements.contains(&enrollment) {
                    self.enrollment_lost(QuiesceReason::EnrollmentEnded, effects);
                } else if matches!(self.state, State::Observing { .. }) {
                    self.start(pair, effects)?;
                }
            }
        }
        self.schedule(METADATA_POLL, effects)
    }

    fn enrollment_lost(&mut self, why: QuiesceReason, effects: &mut Vec<Effect>) {
        match &mut self.state {
            State::Observing { reason } => *reason = why.refusal(),
            State::Opening { generation, .. } | State::Running { generation, .. } => {
                let generation = *generation;
                self.begin_quiescing(Some(generation), why, effects);
            }
            State::Quiescing(_)
            | State::Faulted { .. }
            | State::ClosedWithOutstanding { .. }
            | State::Closed => {}
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

    fn piece_exited(
        &mut self,
        generation: Generation,
        piece: UnitPiece,
        effects: &mut Vec<Effect>,
    ) {
        if matches!(self.state,
            State::Opening { generation: current, .. } | State::Running { generation: current, .. }
                if current == generation)
        {
            self.begin_quiescing(Some(generation), QuiesceReason::PieceExited, effects);
            effects.push(Effect::ReportFault(Fault::PieceExited {
                generation,
                piece,
            }));
        }
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
    ) -> Result<(), SupervisorError> {
        let target = report.target();
        let result = report.result();
        if let State::ClosedWithOutstanding { pieces } = &mut self.state {
            if result == StopResult::Joined && pieces.remove(&target) && pieces.is_empty() {
                self.state = State::Closed;
            }
            return Ok(());
        }
        let State::Quiescing(cleanup) = &mut self.state else {
            return Ok(());
        };
        let step = cleanup.accept(target, result);
        if step == Step::Ignored {
            return Ok(());
        }
        if let StopReport::LeaseAdapter {
            unsettled_retirements,
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
                QuiesceReason::NotEnrolled
                | QuiesceReason::EnrollmentEnded
                | QuiesceReason::BuildFailed
                | QuiesceReason::PieceExited
                | QuiesceReason::Closing => {
                    let reason = cleanup.reason.refusal();
                    let after = if matches!(
                        cleanup.reason,
                        QuiesceReason::BuildFailed | QuiesceReason::PieceExited
                    ) {
                        RETRY
                    } else {
                        Duration::ZERO
                    };
                    self.state = State::Observing { reason };
                    self.schedule(after, effects)?;
                }
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
        next.reduce(event, &mut effects)
            .map_err(TransitionError::Domain)?;
        if next.state.front() != state.state.front() {
            effects.insert(0, Effect::PublishFront(next.state.front()));
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
