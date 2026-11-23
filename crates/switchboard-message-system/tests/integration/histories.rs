use std::{collections::BTreeSet, num::NonZeroU64};

use hegel::generators as gs;
use state_machine_core::{EventTag, StateMachine, StateTag};
use switchboard_message_system::{
    Effect, Event, Front, Generation, Metadata, ReadId, State, StopReport, StopResult, StopTarget,
    Supervisor, UnitPiece,
};

use super::{BUDGETS, FIRST, PIECES, active, enrollment, report, send, target};

struct History {
    supervisor: Supervisor,
    front: Front,
    latest_read: ReadId,
    close_requested: bool,
    current_join: Option<StopTarget>,
    live: BTreeSet<StopTarget>,
    reported_timeouts: BTreeSet<StopTarget>,
    last_unit_join_read: Option<ReadId>,
    last_started: Option<Generation>,
}

impl History {
    fn new() -> Self {
        let (supervisor, initial) = Supervisor::new(BUDGETS);
        let mut history = Self {
            supervisor,
            front: Front::Refusing(switchboard_message_system::FrontRefusal::NotEnrolled),
            latest_read: ReadId(NonZeroU64::MIN),
            close_requested: false,
            current_join: None,
            live: [StopTarget::EnrollmentStore].into(),
            reported_timeouts: BTreeSet::new(),
            last_unit_join_read: None,
            last_started: None,
        };
        for effect in initial {
            history.record(effect, None);
        }
        history
    }

    fn apply(&mut self, event: Event) {
        if event == Event::Close {
            self.close_requested = true;
        }
        if let Event::StopFinished(report) = &event {
            let target = report.target();
            let is_current = self.current_join == Some(target);
            if is_current {
                self.current_join = None;
                if report.result() == StopResult::TimedOut {
                    self.reported_timeouts.insert(target);
                }
            }
            if report.result() == StopResult::Joined
                && (is_current || self.reported_timeouts.remove(&target))
            {
                assert!(self.live.remove(&target));
                if matches!(target, StopTarget::Unit { .. }) {
                    self.last_unit_join_read = Some(self.latest_read);
                }
            }
        }

        let before = self.supervisor.state_tag();
        let effects = send(&mut self.supervisor, event.clone());
        for effect in effects.iter().cloned() {
            self.record(effect, Some(&event));
        }
        let after = self.supervisor.state_tag();
        if before != after || !effects.is_empty() {
            assert!(
                Supervisor::transitions()
                    .iter()
                    .any(|row| row.from == before
                        && row.event == event.event_tag()
                        && row.to == after
                        && row.terminal == self.supervisor.is_terminal())
            );
        }
        assert_eq!(self.front, self.supervisor.state().front());
        if let Front::Forwarding(generation) = self.front {
            assert!(
                matches!(self.supervisor.state(), State::Running { generation: active, .. } if *active == generation)
            );
        }
        match self.supervisor.state() {
            State::Opening { generation, .. } | State::Running { generation, .. } => {
                let owned: BTreeSet<_> = self
                    .live
                    .iter()
                    .filter_map(|target| match target {
                        StopTarget::Unit { generation, .. } => Some(*generation),
                        StopTarget::EnrollmentStore => None,
                    })
                    .collect();
                assert_eq!(owned, [*generation].into());
            }
            State::Closed => assert!(self.live.is_empty()),
            State::ClosedWithOutstanding { pieces } => {
                assert_eq!(*pieces, self.live);
                assert_eq!(self.reported_timeouts, self.live);
            }
            State::Observing { .. } | State::Quiescing(_) | State::Faulted { .. } => {}
        }
    }

    fn record(&mut self, effect: Effect, event: Option<&Event>) {
        match effect {
            Effect::StartUnit { generation, pair } => {
                assert!(!self.close_requested);
                assert!(self.last_started.is_none_or(|prior| generation > prior));
                assert!(
                    self.live
                        .iter()
                        .all(|target| *target == StopTarget::EnrollmentStore)
                );
                assert!(matches!(event, Some(Event::MetadataObserved {
                    read, metadata: Metadata::Active(active),
                }) if *read == self.latest_read && active.pair == pair
                    && self.last_unit_join_read.is_none_or(|floor| *read > floor)));
                self.last_started = Some(generation);
                for piece in PIECES {
                    assert!(self.live.insert(target(generation, piece)));
                }
            }
            Effect::StopAndJoin { target, .. } => {
                assert!(self.live.contains(&target));
                assert!(self.current_join.replace(target).is_none());
            }
            Effect::ScheduleMetadataRead { read, .. } => {
                if event.is_some() {
                    assert!(read > self.latest_read && !self.close_requested);
                }
                self.latest_read = read;
            }
            Effect::PublishFront(front) => self.front = front,
            Effect::ReleaseInputs => assert!(self.close_requested),
            Effect::ReportFault(_) => {}
        }
    }

    fn generated_event(&self, case: &hegel::TestCase) -> Event {
        let natural = case.draw(gs::booleans());
        let num = case.draw(gs::integers::<u8>().min_value(1).max_value(7));
        let generated = NonZeroU64::new(u64::from(num)).unwrap();
        let generation = if natural {
            self.last_started.unwrap_or(FIRST)
        } else {
            Generation(generated)
        };
        let read = if natural {
            self.latest_read
        } else {
            ReadId(generated)
        };
        let piece = case.draw(gs::sampled_from(PIECES.to_vec()));
        let selected = if natural {
            self.current_join
                .or_else(|| self.reported_timeouts.iter().next().copied())
                .unwrap_or_else(|| target(generation, piece))
        } else if num == 1 {
            StopTarget::EnrollmentStore
        } else {
            target(generation, piece)
        };
        let result = if case.draw(gs::booleans()) {
            StopResult::Joined
        } else {
            StopResult::TimedOut
        };
        match case.draw(gs::integers::<u8>().max_value(19)) {
            0 => Event::EnrollmentMayHaveChanged,
            1 => Event::MetadataObserved {
                read,
                metadata: Metadata::Empty,
            },
            2 => Event::MetadataObserved {
                read,
                metadata: Metadata::Ended,
            },
            3 => Event::MetadataObserved {
                read,
                metadata: Metadata::ReadFailed,
            },
            4 => Event::MetadataObserved {
                read,
                metadata: active(2, 1, 2),
            },
            5 => Event::MetadataObserved {
                read,
                metadata: active(1, 2, 3),
            },
            6 | 7 => Event::BuildFinished { generation },
            8 => Event::BuildFailed { generation },
            9 => Event::PieceExited { generation, piece },
            10 => Event::Close,
            11..=14 => {
                let completion = match selected {
                    StopTarget::Unit {
                        generation,
                        piece: UnitPiece::LeaseAdapter,
                    } => StopReport::LeaseAdapter {
                        generation,
                        result,
                        unsettled_retirements: vec![enrollment(1)],
                    },
                    StopTarget::Unit { .. } | StopTarget::EnrollmentStore => {
                        report(selected, result)
                    }
                };
                Event::StopFinished(completion)
            }
            _ => Event::MetadataObserved {
                read,
                metadata: active(1, 1, num),
            },
        }
    }
}

#[hegel::test(test_cases = 450)]
fn generated_histories_preserve_admission_scan_order_and_join_ownership(case: hegel::TestCase) {
    let mut history = History::new();
    for _ in 0..case.draw(gs::integers::<u8>().min_value(8).max_value(64)) {
        history.apply(history.generated_event(&case));
    }
    history.apply(Event::Close);
    while let Some(joining) = history.current_join {
        let outcome = if case.draw(gs::booleans()) {
            StopResult::Joined
        } else {
            StopResult::TimedOut
        };
        history.apply(Event::StopFinished(report(joining, outcome)));
    }
    assert!(matches!(
        history.supervisor.state(),
        State::Closed | State::ClosedWithOutstanding { .. }
    ));
    history.apply(Event::MetadataObserved {
        read: history.latest_read,
        metadata: active(1, 1, 1),
    });
}
