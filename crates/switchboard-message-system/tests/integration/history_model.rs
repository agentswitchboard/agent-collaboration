use std::{
    collections::{BTreeSet, VecDeque},
    num::NonZeroU64,
    time::Duration,
};

use state_machine_core::{EventTag, TransitionError};
use switchboard_message_system::{
    Effect, EnrollmentId, EnrollmentPair, Event, Fault, Front, FrontRefusal, Generation,
    LeaseStopResult, Metadata, ReadId, StopReport, StopResult, StopTarget, SupervisorError,
    UnitPiece,
};

use super::{BUDGETS, PIECES, target};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Polling,
    Cooldown,
    Building,
    Serving,
    Draining,
    Faulted,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Finish {
    Loss,
    Retry,
    Fault,
    Close,
}

pub(super) struct Model {
    phase: Phase,
    pub front: Front,
    pub read: ReadId,
    pub last_generation: Option<Generation>,
    pinned: Option<EnrollmentPair>,
    ending: Option<Finish>,
    queue: VecDeque<StopTarget>,
    pub overdue: BTreeSet<StopTarget>,
    excluded: BTreeSet<EnrollmentId>,
    store_joined: bool,
    store_exit_seen: bool,
}

impl Model {
    pub fn new() -> Self {
        Self {
            phase: Phase::Polling,
            front: Front::Refusing(FrontRefusal::AwaitingFirstRead),
            read: ReadId(NonZeroU64::MIN),
            last_generation: None,
            pinned: None,
            ending: None,
            queue: VecDeque::new(),
            overdue: BTreeSet::new(),
            excluded: BTreeSet::new(),
            store_joined: false,
            store_exit_seen: false,
        }
    }

    pub fn initial(&self) -> [Effect; 2] {
        [
            Effect::PublishFront(self.front),
            Effect::ScheduleMetadataRead {
                read: self.read,
                after: Duration::ZERO,
            },
        ]
    }

    pub fn tag(&self) -> &'static str {
        match self.phase {
            Phase::Polling | Phase::Cooldown => "Observing",
            Phase::Building => "Opening",
            Phase::Serving => "Running",
            Phase::Draining => "Quiescing",
            Phase::Faulted => "Faulted",
            Phase::Closed if self.overdue.is_empty() => "Closed",
            Phase::Closed => "ClosedWithOutstanding",
        }
    }

    pub fn joining(&self) -> Option<StopTarget> {
        self.queue.front().copied()
    }

    fn unit_generation(&self) -> Option<Generation> {
        match self.phase {
            Phase::Building | Phase::Serving => self.last_generation,
            Phase::Polling | Phase::Cooldown | Phase::Draining | Phase::Faulted | Phase::Closed => {
                None
            }
        }
    }

    fn reads_metadata(&self) -> bool {
        match self.phase {
            Phase::Polling | Phase::Cooldown | Phase::Building | Phase::Serving => true,
            Phase::Draining => matches!(self.ending, Some(Finish::Loss | Finish::Retry)),
            Phase::Faulted | Phase::Closed => false,
        }
    }

    fn read_after(&mut self, seconds: u64, effects: &mut Vec<Effect>) {
        self.read = ReadId(self.read.0.checked_add(1).unwrap());
        effects.push(Effect::ScheduleMetadataRead {
            read: self.read,
            after: Duration::from_secs(seconds),
        });
    }

    fn refuse(&mut self, reason: FrontRefusal) {
        self.front = Front::Refusing(reason);
    }

    fn request_join(&self, effects: &mut Vec<Effect>) {
        if let Some(target) = self.joining() {
            let budget = match target {
                StopTarget::Unit { piece, .. } => match piece {
                    UnitPiece::Driver => BUDGETS.driver,
                    UnitPiece::Custody => BUDGETS.custody,
                    UnitPiece::Settings => BUDGETS.settings,
                    UnitPiece::LeaseAdapter => BUDGETS.lease_adapter,
                },
                StopTarget::EnrollmentStore => BUDGETS.enrollment_store,
            };
            effects.push(Effect::StopAndJoin { target, budget });
        }
    }

    fn start_drain(
        &mut self,
        outcome: Finish,
        generation: Option<Generation>,
        effects: &mut Vec<Effect>,
    ) {
        self.phase = Phase::Draining;
        self.ending = Some(outcome);
        if let Some(generation) = generation {
            self.queue
                .extend(PIECES.map(|piece| target(generation, piece)));
        }
        if outcome == Finish::Close {
            self.queue.push_back(StopTarget::EnrollmentStore);
        }
        self.request_join(effects);
    }

    fn close(&mut self, effects: &mut Vec<Effect>) {
        if self.phase == Phase::Closed || self.ending == Some(Finish::Close) {
            return;
        }
        self.refuse(FrontRefusal::ShuttingDown);
        if self.phase == Phase::Draining {
            self.ending = Some(Finish::Close);
            let had_join = self.joining().is_some();
            self.queue.push_back(StopTarget::EnrollmentStore);
            if !had_join {
                self.request_join(effects);
            }
        } else {
            self.start_drain(Finish::Close, self.unit_generation(), effects);
        }
    }

    fn observe(&mut self, metadata: Metadata, effects: &mut Vec<Effect>) {
        let loss = match metadata {
            Metadata::ReadFailed => {
                if matches!(self.phase, Phase::Polling | Phase::Cooldown) {
                    self.phase = Phase::Polling;
                    self.refuse(FrontRefusal::MetadataUnreadable);
                }
                self.read_after(5, effects);
                return;
            }
            Metadata::Empty => Some(FrontRefusal::NotEnrolled),
            Metadata::Ended => Some(FrontRefusal::EnrollmentEnded),
            Metadata::Active(active) => {
                if let Some(pinned) = &self.pinned
                    && *pinned != active.pair
                {
                    effects.push(Effect::ReportFault(Fault::DifferentPair {
                        pinned: pinned.clone(),
                        observed: active.pair,
                    }));
                    self.refuse(FrontRefusal::EnrollmentMismatch);
                    match self.phase {
                        Phase::Building | Phase::Serving => {
                            self.start_drain(Finish::Fault, self.last_generation, effects)
                        }
                        Phase::Draining => self.ending = Some(Finish::Fault),
                        Phase::Polling | Phase::Cooldown => self.phase = Phase::Faulted,
                        Phase::Closed | Phase::Faulted => {}
                    }
                    return;
                }
                self.pinned = Some(active.pair.clone());
                if self.excluded.contains(&active.enrollment) {
                    Some(FrontRefusal::EnrollmentEnded)
                } else {
                    if matches!(self.phase, Phase::Polling | Phase::Cooldown) {
                        let generation = self
                            .last_generation
                            .map_or(Generation(NonZeroU64::MIN), |previous| {
                                Generation(previous.0.checked_add(1).unwrap())
                            });
                        self.last_generation = Some(generation);
                        self.phase = Phase::Building;
                        self.refuse(FrontRefusal::Starting);
                        effects.push(Effect::StartUnit {
                            generation,
                            pair: active.pair,
                            retired_enrollments: self.excluded.clone(),
                        });
                    } else if self.ending == Some(Finish::Loss) {
                        self.refuse(FrontRefusal::Restarting);
                    }
                    None
                }
            }
        };
        if let Some(reason) = loss {
            match self.phase {
                Phase::Building | Phase::Serving => {
                    self.refuse(reason);
                    self.start_drain(Finish::Loss, self.last_generation, effects);
                }
                Phase::Polling | Phase::Cooldown => {
                    self.phase = Phase::Polling;
                    self.refuse(reason);
                }
                Phase::Draining if self.ending == Some(Finish::Loss) => self.refuse(reason),
                Phase::Draining | Phase::Closed | Phase::Faulted => {}
            }
        }
        self.read_after(15, effects);
    }

    fn joined(&mut self, report: StopReport, effects: &mut Vec<Effect>) -> bool {
        let stopped = report.target();
        let timed_out = report.result() == StopResult::TimedOut;
        if self.phase == Phase::Closed {
            let accepted = !timed_out && self.overdue.remove(&stopped);
            self.store_joined |= accepted && stopped == StopTarget::EnrollmentStore;
            return accepted;
        }
        if self.phase != Phase::Draining {
            return false;
        }
        if self.joining() == Some(stopped) {
            self.queue.pop_front();
            if timed_out {
                self.overdue.insert(stopped);
                effects.push(Effect::ReportFault(Fault::StopTimedOut { target: stopped }));
            }
            self.request_join(effects);
        } else if timed_out || !self.overdue.remove(&stopped) {
            return false;
        }
        if let StopReport::LeaseAdapter {
            result: LeaseStopResult::Joined {
                unsettled_retirements,
            },
            ..
        } = report
        {
            self.excluded.extend(unsettled_retirements);
        }
        self.store_joined |= !timed_out && stopped == StopTarget::EnrollmentStore;
        if self.queue.is_empty() {
            if self.ending == Some(Finish::Close) {
                self.phase = Phase::Closed;
                effects.push(Effect::ReleaseInputs);
            } else if self.overdue.is_empty() {
                match self.ending.take() {
                    Some(Finish::Loss) => {
                        self.phase = Phase::Polling;
                        self.read_after(0, effects);
                    }
                    Some(Finish::Retry) => {
                        self.phase = Phase::Cooldown;
                        self.read_after(5, effects);
                    }
                    Some(Finish::Fault) => self.phase = Phase::Faulted,
                    Some(Finish::Close) | None => {}
                }
            }
        }
        true
    }

    pub fn apply(&mut self, event: Event) -> Result<Vec<Effect>, TransitionError<SupervisorError>> {
        let old_front = self.front;
        let not_allowed = TransitionError::NotAllowed {
            state: self.tag(),
            event: event.event_tag(),
        };
        let mut effects = Vec::new();
        let store_exited = event == Event::EnrollmentStoreExited;
        match event {
            Event::EnrollmentMayHaveChanged if self.phase == Phase::Cooldown => {}
            Event::EnrollmentMayHaveChanged if self.reads_metadata() => {
                self.read_after(0, &mut effects)
            }
            Event::MetadataObserved { read, metadata } => {
                if read == self.read && self.reads_metadata() {
                    self.observe(metadata, &mut effects);
                } else if read <= self.read {
                    return Err(TransitionError::Stale);
                } else {
                    return Err(not_allowed);
                }
            }
            Event::BuildFinished { generation }
                if self.phase == Phase::Building && self.last_generation == Some(generation) =>
            {
                self.phase = Phase::Serving;
                self.front = Front::Forwarding(generation);
            }
            Event::BuildFailed { generation }
                if self.phase == Phase::Building && self.last_generation == Some(generation) =>
            {
                self.refuse(FrontRefusal::Restarting);
                self.start_drain(Finish::Retry, Some(generation), &mut effects);
                effects.push(Effect::ReportFault(Fault::BuildFailed { generation }));
            }
            Event::PieceExited { generation, piece }
                if matches!(self.phase, Phase::Building | Phase::Serving)
                    && self.last_generation == Some(generation) =>
            {
                self.refuse(FrontRefusal::Restarting);
                self.start_drain(Finish::Retry, Some(generation), &mut effects);
                effects.push(Effect::ReportFault(Fault::PieceExited {
                    generation,
                    piece,
                }));
            }
            Event::Close => self.close(&mut effects),
            Event::EnrollmentStoreExited => {
                if self.store_joined || self.store_exit_seen {
                    return Err(TransitionError::Stale);
                }
                self.store_exit_seen = true;
                effects.push(Effect::ReportFault(Fault::EnrollmentStoreExited));
                self.close(&mut effects);
            }
            Event::StopFinished(report) => {
                if !self.joined(report, &mut effects) {
                    return Err(TransitionError::Stale);
                }
            }
            Event::BuildFinished { generation }
            | Event::BuildFailed { generation }
            | Event::PieceExited { generation, .. } => {
                if self
                    .last_generation
                    .is_some_and(|issued| issued >= generation)
                {
                    return Err(TransitionError::Stale);
                }
                return Err(not_allowed);
            }
            Event::EnrollmentMayHaveChanged => return Err(not_allowed),
        }
        if old_front != self.front {
            effects.insert(usize::from(store_exited), Effect::PublishFront(self.front));
        }
        Ok(effects)
    }
}
