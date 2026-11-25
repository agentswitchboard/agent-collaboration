use std::{collections::BTreeSet, num::NonZeroU64};

use hegel::generators as gs;
use state_machine_core::{StateMachine, StateTag};
use switchboard_message_system::{
    Effect, Event, Front, Generation, LeaseStopResult, Metadata, ReadId, State, StopReport,
    StopResult, StopTarget, Supervisor, UnitPiece,
};

use super::{BUDGETS, FIRST, PIECES, active, enrollment, history_model::Model, report, target};

struct History {
    supervisor: Supervisor,
    model: Model,
    close_requested: bool,
    live: BTreeSet<StopTarget>,
    last_unit_join_read: Option<ReadId>,
    last_join: Option<StopReport>,
}

impl History {
    fn new() -> Self {
        let (supervisor, initial) = Supervisor::new(BUDGETS);
        let model = Model::new();
        assert_eq!(initial, model.initial());
        Self {
            supervisor,
            model,
            close_requested: false,
            live: [StopTarget::EnrollmentStore].into(),
            last_unit_join_read: None,
            last_join: None,
        }
    }

    fn apply(&mut self, event: Event) {
        let prior_join = self.model.joining();
        let prior_read = self.model.read;
        let before = self.supervisor.clone();
        let expected = self.model.apply(event.clone());
        match (
            expected,
            Supervisor::transition(&self.supervisor, event.clone()),
        ) {
            (Err(expected), actual) => {
                assert_eq!(actual, Err(expected), "{event:?}");
                assert_eq!(self.supervisor, before);
            }
            (Ok(expected), actual) => {
                let (next, effects) = actual.unwrap();
                assert_eq!(effects, expected, "{event:?} from {}", before.state_tag());
                self.close_requested |=
                    matches!(event, Event::Close | Event::EnrollmentStoreExited);
                if let Event::StopFinished(report) = &event {
                    let stopped = report.target();
                    if report.result() == StopResult::Joined {
                        assert!(self.live.remove(&stopped));
                        if matches!(stopped, StopTarget::Unit { .. }) {
                            self.last_unit_join_read = Some(prior_read);
                        }
                    } else {
                        assert_eq!(prior_join, Some(stopped));
                        assert!(self.live.contains(&stopped));
                    }
                    self.last_join = Some(report.clone());
                }
                for effect in &effects {
                    if let Effect::StartUnit {
                        generation, pair, ..
                    } = effect
                    {
                        assert!(!self.close_requested);
                        assert_eq!(self.live, [StopTarget::EnrollmentStore].into());
                        assert!(matches!(&event, Event::MetadataObserved {
                            read, metadata: Metadata::Active(active),
                        } if *read == prior_read && &active.pair == pair
                            && self.last_unit_join_read.is_none_or(|floor| *read > floor)));
                        for piece in PIECES {
                            assert!(self.live.insert(target(*generation, piece)));
                        }
                    }
                }
                self.supervisor = next;
            }
        }

        assert_eq!(self.supervisor.state_tag(), self.model.tag());
        assert_eq!(self.supervisor.latest_read(), self.model.read);
        assert_eq!(self.supervisor.state().front(), self.model.front);
        if let Front::Forwarding(generation) = self.model.front {
            assert!(
                matches!(self.supervisor.state(), State::Running { generation: current, .. }
                if *current == generation)
            );
        }
        match self.supervisor.state() {
            State::Opening { generation, .. } | State::Running { generation, .. } => {
                assert_eq!(
                    self.live,
                    PIECES
                        .map(|piece| target(*generation, piece))
                        .into_iter()
                        .chain([StopTarget::EnrollmentStore])
                        .collect()
                );
            }
            State::Closed => assert!(self.live.is_empty()),
            State::ClosedWithOutstanding { pieces } => {
                assert_eq!(*pieces, self.live);
                assert_eq!(*pieces, self.model.overdue);
            }
            State::Observing { .. } | State::Quiescing(_) | State::Faulted { .. } => {}
        }
    }

    fn generated_event(&self, case: &hegel::TestCase) -> Event {
        let current = case.draw(gs::booleans());
        let number = case.draw(gs::integers::<u8>().min_value(1).max_value(7));
        let arbitrary = NonZeroU64::new(u64::from(number)).unwrap();
        let generation = if current {
            self.model.last_generation.unwrap_or(FIRST)
        } else {
            Generation(arbitrary)
        };
        let read = if current {
            self.model.read
        } else {
            ReadId(arbitrary)
        };
        let piece = case.draw(gs::sampled_from(PIECES.to_vec()));
        let selected = if current {
            self.model
                .joining()
                .or_else(|| self.model.overdue.iter().next().copied())
                .unwrap_or_else(|| target(generation, piece))
        } else if number == 1 {
            StopTarget::EnrollmentStore
        } else {
            target(generation, piece)
        };
        let result = if case.draw(gs::booleans()) {
            StopResult::Joined
        } else {
            StopResult::TimedOut
        };
        match case.draw(gs::integers::<u8>().max_value(23)) {
            0 => Event::EnrollmentMayHaveChanged,
            1 => Event::MetadataObserved {
                read,
                metadata: Metadata::Empty,
            },
            2 => Event::MetadataObserved {
                read,
                metadata: super::ended_metadata(),
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
            11..=14 => Event::StopFinished(match selected {
                StopTarget::Unit {
                    generation,
                    piece: UnitPiece::LeaseAdapter,
                } => StopReport::LeaseAdapter {
                    generation,
                    result: match result {
                        StopResult::Joined => LeaseStopResult::Joined {
                            unsettled_retirements: vec![enrollment(1)],
                        },
                        StopResult::TimedOut => LeaseStopResult::TimedOut,
                    },
                },
                StopTarget::Unit { .. } | StopTarget::EnrollmentStore => report(selected, result),
            }),
            15 => Event::EnrollmentStoreExited,
            16 => self.last_join.clone().map_or(
                Event::StopFinished(report(selected, result)),
                Event::StopFinished,
            ),
            _ => Event::MetadataObserved {
                read,
                metadata: active(1, 1, number),
            },
        }
    }

    fn seed_restart(&mut self, case: &hegel::TestCase, exited: bool) {
        self.apply(Event::MetadataObserved {
            read: self.model.read,
            metadata: active(1, 1, 1),
        });
        if exited {
            self.apply(Event::BuildFinished { generation: FIRST });
            let piece = case.draw(gs::sampled_from(PIECES.to_vec()));
            self.apply(Event::PieceExited {
                generation: FIRST,
                piece,
            });
        } else {
            self.apply(Event::BuildFailed { generation: FIRST });
        }
        let read_before_drain = self.model.read;
        for piece in PIECES {
            self.apply(Event::StopFinished(report(
                target(FIRST, piece),
                StopResult::Joined,
            )));
        }
        self.apply(Event::EnrollmentMayHaveChanged);
        self.apply(Event::MetadataObserved {
            read: read_before_drain,
            metadata: active(1, 1, 1),
        });
    }

    fn finish(&mut self, case: &hegel::TestCase) {
        self.apply(Event::Close);
        // The model queue comes only from the five possible stop slots.
        for _ in 0..=PIECES.len() {
            let Some(joining) = self.model.joining() else {
                break;
            };
            if let Some(previous) = self.last_join.clone() {
                self.apply(Event::StopFinished(previous));
            }
            let wrong_generation = Generation(FIRST.0.checked_add(999).unwrap());
            self.apply(Event::StopFinished(report(
                target(wrong_generation, UnitPiece::Driver),
                StopResult::Joined,
            )));
            let outcome = if case.draw(gs::booleans()) {
                StopResult::Joined
            } else {
                StopResult::TimedOut
            };
            self.apply(Event::StopFinished(report(joining, outcome)));
        }
        assert!(self.model.joining().is_none());
        assert!(matches!(
            self.supervisor.state(),
            State::Closed | State::ClosedWithOutstanding { .. }
        ));
        let overdue = self.model.overdue.clone();
        for piece in overdue {
            let completion = report(piece, StopResult::Joined);
            self.apply(Event::StopFinished(completion.clone()));
            self.apply(Event::StopFinished(completion));
        }
        assert_eq!(self.supervisor.state(), &State::Closed);
        self.apply(Event::MetadataObserved {
            read: self.model.read,
            metadata: active(1, 1, 1),
        });
    }
}

#[hegel::test(test_cases = 450)]
fn generated_histories_preserve_admission_scan_order_and_join_ownership(case: hegel::TestCase) {
    let mut history = History::new();
    // Short random histories often close before a complete ordinary drain.
    // Exercise change-signal/late-response order inside one generated history
    // even when the remaining random suffix never reaches a restart.
    match case.draw(gs::integers::<u8>().max_value(2)) {
        0 => {}
        1 => history.seed_restart(&case, false),
        _ => history.seed_restart(&case, true),
    }
    for _ in 0..case.draw(gs::integers::<u8>().min_value(8).max_value(64)) {
        history.apply(history.generated_event(&case));
    }
    history.finish(&case);
}
