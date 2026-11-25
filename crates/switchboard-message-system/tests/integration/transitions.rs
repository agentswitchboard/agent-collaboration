use std::time::Duration;

use state_machine_core::{StateMachine, TransitionError, check_state_machine_transitions};
use switchboard_message_system::{
    Effect, Event, Fault, Front, FrontRefusal, LeaseStopResult, Metadata, ObservationPhase, ReadId,
    State, StopReport, StopResult, StopTarget, Supervisor, UnitPiece,
};

use super::{
    BUDGETS, FIRST, PIECES, active, enrollment, observe, pair, report, send, stop, target,
};

pub(super) fn current_read(supervisor: &Supervisor, metadata: Metadata) -> Event {
    Event::MetadataObserved {
        read: supervisor.latest_read(),
        metadata,
    }
}

fn observing(supervisor: &Supervisor, why: FrontRefusal) {
    assert_eq!(
        supervisor.state(),
        &State::Observing {
            phase: ObservationPhase::Polling(why)
        }
    );
}

pub(super) fn scheduled(supervisor: &Supervisor, effects: &[Effect], delay: u64) {
    assert!(effects.contains(&Effect::ScheduleMetadataRead {
        read: supervisor.latest_read(),
        after: Duration::from_secs(delay)
    }));
}

pub(super) fn start() -> Supervisor {
    let (mut supervisor, _) = Supervisor::new(BUDGETS);
    let effects = observe(&mut supervisor, active(1, 1, 1));
    assert_eq!(
        effects.first(),
        Some(&Effect::PublishFront(Front::Refusing(
            FrontRefusal::Starting
        )))
    );
    assert!(effects.contains(&Effect::StartUnit {
        generation: FIRST,
        pair: pair(1, 1),
        retired_enrollments: [].into(),
    }));
    supervisor
}

pub(super) fn running() -> Supervisor {
    let mut supervisor = start();
    assert_eq!(
        send(&mut supervisor, Event::BuildFinished { generation: FIRST }),
        [Effect::PublishFront(Front::Forwarding(FIRST))]
    );
    supervisor
}

fn stop_unit(supervisor: &mut Supervisor) -> Vec<Effect> {
    PIECES.into_iter().fold(Vec::new(), |_, piece| {
        stop(supervisor, target(FIRST, piece), StopResult::Joined)
    })
}

#[test]
fn bootstrap_refusal_is_distinct_from_a_running_supervisors_first_read() {
    let unavailable = Front::Refusing(FrontRefusal::CouldNotStart);
    let (supervisor, initial) = Supervisor::new(BUDGETS);

    assert_eq!(
        supervisor.state().front(),
        Front::Refusing(FrontRefusal::AwaitingFirstRead)
    );
    assert!(!initial.contains(&Effect::PublishFront(unavailable)));
}

#[test]
fn observation_only_until_an_active_row_and_read_changes_replace_the_timer() {
    let (mut supervisor, initial) = Supervisor::new(BUDGETS);
    observing(&supervisor, FrontRefusal::AwaitingFirstRead);
    assert_eq!(
        initial.first(),
        Some(&Effect::PublishFront(Front::Refusing(
            FrontRefusal::AwaitingFirstRead
        )))
    );
    scheduled(&supervisor, &initial, 0);
    let mut already_enrolled = supervisor.clone();
    assert_eq!(
        observe(&mut already_enrolled, active(1, 1, 1)).first(),
        Some(&Effect::PublishFront(Front::Refusing(
            FrontRefusal::Starting
        )))
    );
    for (metadata, why, delay) in [
        (Metadata::Empty, FrontRefusal::NotEnrolled, 15),
        (super::ended_metadata(), FrontRefusal::EnrollmentEnded, 15),
        (Metadata::ReadFailed, FrontRefusal::MetadataUnreadable, 5),
    ] {
        let effects = observe(&mut supervisor, metadata);
        observing(&supervisor, why);
        assert!(effects.iter().all(|effect| matches!(
            effect,
            Effect::ScheduleMetadataRead { .. } | Effect::PublishFront(_)
        )));
        scheduled(&supervisor, &effects, delay);
    }
    let old_read = supervisor.latest_read();
    let effects = send(&mut supervisor, Event::EnrollmentMayHaveChanged);
    scheduled(&supervisor, &effects, 0);
    assert!(supervisor.latest_read() > old_read);
    assert_eq!(
        Supervisor::transition(
            &supervisor,
            Event::MetadataObserved {
                read: old_read,
                metadata: active(1, 1, 1)
            }
        ),
        Err(TransitionError::Stale)
    );
    let effects = observe(&mut supervisor, active(1, 1, 1));
    assert!(effects.contains(&Effect::StartUnit {
        generation: FIRST,
        pair: pair(1, 1),
        retired_enrollments: [].into()
    }));
    scheduled(&supervisor, &effects, 15);
}

#[test]
fn empty_and_ended_withdraw_immediately_during_opening_and_running() {
    for setup in [start, running] {
        for (metadata, why) in [
            (Metadata::Empty, FrontRefusal::NotEnrolled),
            (super::ended_metadata(), FrontRefusal::EnrollmentEnded),
        ] {
            let mut supervisor = setup();
            let effects = observe(&mut supervisor, metadata);
            assert_eq!(
                effects.first(),
                Some(&Effect::PublishFront(Front::Refusing(why)))
            );
            assert!(effects.contains(&Effect::StopAndJoin {
                target: target(FIRST, UnitPiece::Driver),
                budget: BUDGETS.driver,
            }));
            assert_eq!(
                Supervisor::transition(&supervisor, Event::BuildFinished { generation: FIRST }),
                Err(TransitionError::Stale)
            );
            let after_drain = stop_unit(&mut supervisor);
            observing(&supervisor, why);
            scheduled(&supervisor, &after_drain, 0);
        }
    }
}

#[test]
fn failed_metadata_reads_and_same_pair_active_rows_leave_opening_and_running_untouched() {
    for setup in [start, running] {
        let mut supervisor = setup();
        let before = supervisor.state().clone();
        let failure = observe(&mut supervisor, Metadata::ReadFailed);
        assert_eq!(supervisor.state(), &before);
        scheduled(&supervisor, &failure, 5);
        assert_eq!(failure.len(), 1);
        let same_pair_new_id = observe(&mut supervisor, active(1, 1, 2));
        assert_eq!(supervisor.state(), &before);
        scheduled(&supervisor, &same_pair_new_id, 15);
        assert_eq!(same_pair_new_id.len(), 1);
    }
}

#[test]
fn foreign_organization_or_deployment_stops_and_faults_even_after_unit_turnover() {
    for (organization, deployment) in [(2, 1), (1, 2)] {
        let mut supervisor = running();
        let effects = observe(&mut supervisor, active(organization, deployment, 2));
        assert_eq!(
            effects.first(),
            Some(&Effect::PublishFront(Front::Refusing(
                FrontRefusal::EnrollmentMismatch
            )))
        );
        assert!(effects.contains(&Effect::ReportFault(Fault::DifferentPair {
            pinned: pair(1, 1),
            observed: pair(organization, deployment),
        })));
        let finished = stop_unit(&mut supervisor);
        assert_eq!(
            supervisor.state(),
            &State::Faulted {
                pinned: pair(1, 1),
                observed: pair(organization, deployment),
            }
        );
        assert_eq!(
            supervisor.state().front(),
            Front::Refusing(FrontRefusal::EnrollmentMismatch)
        );
        assert!(finished.is_empty());
        assert_eq!(
            Supervisor::transition(&supervisor, current_read(&supervisor, active(1, 1, 3))),
            Err(TransitionError::Stale)
        );
        assert_eq!(
            send(&mut supervisor, Event::Close),
            [
                Effect::PublishFront(Front::Refusing(FrontRefusal::ShuttingDown)),
                Effect::StopAndJoin {
                    target: StopTarget::EnrollmentStore,
                    budget: BUDGETS.enrollment_store,
                },
            ]
        );
        let mut supervisor = running();
        observe(&mut supervisor, Metadata::Empty);
        stop_unit(&mut supervisor);
        observing(&supervisor, FrontRefusal::NotEnrolled);
        observe(&mut supervisor, active(organization, deployment, 2));
        assert!(matches!(supervisor.state(), State::Faulted { .. }));
    }
}

#[test]
fn failed_and_exited_units_join_every_piece_and_require_a_new_read_after_drain() {
    for (event, setup) in [
        (
            Event::BuildFailed { generation: FIRST },
            start as fn() -> Supervisor,
        ),
        (
            Event::PieceExited {
                generation: FIRST,
                piece: UnitPiece::Custody,
            },
            running,
        ),
    ] {
        let mut supervisor = setup();
        let mut effects = send(&mut supervisor, event);
        assert_eq!(
            effects.first(),
            Some(&Effect::PublishFront(Front::Refusing(
                FrontRefusal::Restarting
            )))
        );
        let during_drain = supervisor.latest_read();
        assert!(
            !observe(&mut supervisor, active(1, 1, 1))
                .iter()
                .any(|effect| matches!(effect, Effect::StartUnit { .. }))
        );
        let crossing = supervisor.latest_read();
        for piece in PIECES {
            assert!(effects.iter().any(|effect| matches!(effect, Effect::StopAndJoin { target: join, .. } if *join == target(FIRST, piece))));
            effects = stop(&mut supervisor, target(FIRST, piece), StopResult::Joined);
        }
        let witness = supervisor.latest_read();
        assert_eq!(
            supervisor.state(),
            &State::Observing {
                phase: ObservationPhase::RestartCooldown { read: witness }
            }
        );
        scheduled(&supervisor, &effects, 5);
        assert!(send(&mut supervisor, Event::EnrollmentMayHaveChanged).is_empty());
        assert_eq!(supervisor.latest_read(), witness);
        for token in [during_drain, crossing] {
            assert_eq!(
                Supervisor::transition(
                    &supervisor,
                    Event::MetadataObserved {
                        read: token,
                        metadata: active(1, 1, 1),
                    }
                ),
                Err(TransitionError::Stale)
            );
        }
        let reopened = observe(&mut supervisor, active(1, 1, 1));
        assert_eq!(
            reopened.first(),
            Some(&Effect::PublishFront(Front::Refusing(
                FrontRefusal::Starting
            )))
        );
        assert!(reopened.iter().any(
            |effect| matches!(effect, Effect::StartUnit { generation, .. } if *generation > FIRST)
        ));
        for old in [
            Event::BuildFinished { generation: FIRST },
            Event::BuildFailed { generation: FIRST },
            Event::PieceExited {
                generation: FIRST,
                piece: UnitPiece::Driver,
            },
        ] {
            assert_eq!(
                Supervisor::transition(&supervisor, old),
                Err(TransitionError::Stale)
            );
        }
        assert!(matches!(supervisor.state(), State::Opening { .. }));
    }
}

#[test]
fn normal_drain_waits_for_timed_out_owner_and_fences_unsettled_enrollments() {
    let mut supervisor = running();
    observe(&mut supervisor, super::ended_metadata());
    let driver = target(FIRST, UnitPiece::Driver);
    let effects = stop(&mut supervisor, driver, StopResult::TimedOut);
    assert!(effects.contains(&Effect::ReportFault(Fault::StopTimedOut { target: driver })));
    assert!(effects.contains(&Effect::StopAndJoin {
        target: target(FIRST, UnitPiece::Custody),
        budget: BUDGETS.custody,
    }));
    for piece in [UnitPiece::Custody, UnitPiece::Settings] {
        stop(&mut supervisor, target(FIRST, piece), StopResult::Joined);
    }
    let effects = send(
        &mut supervisor,
        Event::StopFinished(StopReport::LeaseAdapter {
            generation: FIRST,
            result: LeaseStopResult::Joined {
                unsettled_retirements: vec![enrollment(1)],
            },
        }),
    );
    assert!(effects.is_empty());
    assert!(matches!(supervisor.state(), State::Quiescing(cleanup)
        if cleanup.joining().is_none() && cleanup.outstanding().contains(&driver)));
    observe(&mut supervisor, active(1, 1, 2));
    assert!(matches!(supervisor.state(), State::Quiescing(_)));
    let post_drain = stop(&mut supervisor, driver, StopResult::Joined);
    scheduled(&supervisor, &post_drain, 0);
    let effects = observe(&mut supervisor, active(1, 1, 1));
    observing(&supervisor, FrontRefusal::EnrollmentEnded);
    scheduled(&supervisor, &effects, 15);
    assert!(
        observe(&mut supervisor, active(1, 1, 2)).contains(&Effect::StartUnit {
            generation: switchboard_message_system::Generation(FIRST.0.checked_add(1).unwrap()),
            pair: pair(1, 1),
            retired_enrollments: [enrollment(1)].into(),
        })
    );
}

#[test]
fn close_joins_overtaken_opening_in_order_then_closes_store_and_releases_inputs() {
    let mut supervisor = start();
    let closing = send(&mut supervisor, Event::Close);
    assert_eq!(
        closing.first(),
        Some(&Effect::PublishFront(Front::Refusing(
            FrontRefusal::ShuttingDown
        )))
    );
    let mut effects = closing.into_iter().skip(1).collect::<Vec<_>>();
    assert_eq!(
        Supervisor::transition(&supervisor, Event::BuildFinished { generation: FIRST }),
        Err(TransitionError::Stale)
    );
    assert!(send(&mut supervisor, Event::Close).is_empty());
    for (piece, budget) in [
        (UnitPiece::Driver, BUDGETS.driver),
        (UnitPiece::Custody, BUDGETS.custody),
        (UnitPiece::Settings, BUDGETS.settings),
        (UnitPiece::LeaseAdapter, BUDGETS.lease_adapter),
    ] {
        let join = target(FIRST, piece);
        assert_eq!(
            effects,
            [Effect::StopAndJoin {
                target: join,
                budget
            }]
        );
        effects = stop(&mut supervisor, join, StopResult::Joined);
    }
    assert_eq!(
        effects,
        [Effect::StopAndJoin {
            target: StopTarget::EnrollmentStore,
            budget: BUDGETS.enrollment_store
        }]
    );
    assert_eq!(
        stop(
            &mut supervisor,
            StopTarget::EnrollmentStore,
            StopResult::Joined
        ),
        [Effect::ReleaseInputs]
    );
    assert_eq!(supervisor.state(), &State::Closed);
    assert_eq!(
        supervisor.state().front(),
        Front::Refusing(FrontRefusal::ShuttingDown)
    );
    assert_eq!(
        Supervisor::transition(&supervisor, current_read(&supervisor, active(1, 1, 1))),
        Err(TransitionError::Stale)
    );
}

#[test]
fn final_timeouts_name_all_live_owners_and_late_joins_settle_without_effects() {
    let mut supervisor = running();
    observe(&mut supervisor, Metadata::Empty);
    let driver = target(FIRST, UnitPiece::Driver);
    stop(&mut supervisor, driver, StopResult::TimedOut);
    assert_eq!(
        send(&mut supervisor, Event::Close),
        [Effect::PublishFront(Front::Refusing(
            FrontRefusal::ShuttingDown
        ))]
    );
    for piece in [
        UnitPiece::Custody,
        UnitPiece::Settings,
        UnitPiece::LeaseAdapter,
    ] {
        stop(&mut supervisor, target(FIRST, piece), StopResult::Joined);
    }
    let store = StopTarget::EnrollmentStore;
    assert_eq!(
        stop(&mut supervisor, store, StopResult::TimedOut),
        [
            Effect::ReportFault(Fault::StopTimedOut { target: store }),
            Effect::ReleaseInputs
        ]
    );
    assert_eq!(
        supervisor.state(),
        &State::ClosedWithOutstanding {
            pieces: [driver, store].into()
        }
    );
    assert_eq!(
        supervisor.state().front(),
        Front::Refusing(FrontRefusal::ShuttingDown)
    );
    assert_eq!(
        Supervisor::transition(&supervisor, current_read(&supervisor, active(1, 1, 1))),
        Err(TransitionError::Stale)
    );
    assert!(stop(&mut supervisor, driver, StopResult::Joined).is_empty());
    assert_eq!(
        supervisor.state(),
        &State::ClosedWithOutstanding {
            pieces: [store].into()
        }
    );
    assert!(stop(&mut supervisor, store, StopResult::Joined).is_empty());
    assert_eq!(supervisor.state(), &State::Closed);
    assert_eq!(
        supervisor.state().front(),
        Front::Refusing(FrontRefusal::ShuttingDown)
    );
}

#[test]
fn loss_drain_shows_the_latest_enrollment_evidence_until_a_fresh_read() {
    for (later, expected) in [
        (
            vec![(super::ended_metadata(), FrontRefusal::EnrollmentEnded)],
            FrontRefusal::EnrollmentEnded,
        ),
        (
            vec![
                (super::ended_metadata(), FrontRefusal::EnrollmentEnded),
                (active(1, 1, 2), FrontRefusal::Restarting),
            ],
            FrontRefusal::Restarting,
        ),
        (
            vec![
                (super::ended_metadata(), FrontRefusal::EnrollmentEnded),
                (Metadata::Empty, FrontRefusal::NotEnrolled),
            ],
            FrontRefusal::NotEnrolled,
        ),
    ] {
        let mut supervisor = running();
        observe(&mut supervisor, Metadata::Empty);
        for (metadata, reason) in later {
            let effects = observe(&mut supervisor, metadata);
            assert_eq!(
                effects.first(),
                Some(&Effect::PublishFront(Front::Refusing(reason)))
            );
            assert!(matches!(supervisor.state(), State::Quiescing(_)));
        }
        let effects = stop_unit(&mut supervisor);
        observing(&supervisor, expected);
        scheduled(&supervisor, &effects, 0);
        assert!(
            observe(&mut supervisor, active(1, 1, 2))
                .iter()
                .any(|effect| matches!(effect, Effect::StartUnit { .. }))
        );
    }
}

#[test]
fn store_exit_reports_its_fault_and_closes_the_lifetime_in_order() {
    let mut supervisor = running();
    observe(&mut supervisor, Metadata::ReadFailed);
    assert_eq!(supervisor.state().front(), Front::Forwarding(FIRST));
    let exit = send(&mut supervisor, Event::EnrollmentStoreExited);
    let mut closing = running();
    observe(&mut closing, Metadata::ReadFailed);
    let requested_close = send(&mut closing, Event::Close);
    assert_eq!(
        exit,
        [
            vec![Effect::ReportFault(Fault::EnrollmentStoreExited)],
            requested_close,
        ]
        .concat()
    );
    assert_eq!(supervisor.state(), closing.state());
    assert_eq!(
        Supervisor::transition(&supervisor, Event::EnrollmentStoreExited),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        stop_unit(&mut supervisor),
        [Effect::StopAndJoin {
            target: StopTarget::EnrollmentStore,
            budget: BUDGETS.enrollment_store,
        }]
    );
    assert_eq!(
        stop(
            &mut supervisor,
            StopTarget::EnrollmentStore,
            StopResult::Joined
        ),
        [Effect::ReleaseInputs]
    );
    assert_eq!(supervisor.state(), &State::Closed);
    assert_eq!(
        Supervisor::transition(&supervisor, Event::EnrollmentStoreExited),
        Err(TransitionError::Stale)
    );
}

#[test]
fn rejected_events_distinguish_stale_ownership_from_inapplicable_or_future_input() {
    let (mut supervisor, _) = Supervisor::new(BUDGETS);
    let initial = supervisor.clone();
    let fabricated = ReadId(supervisor.latest_read().0.checked_add(1).unwrap());
    for event in [
        Event::MetadataObserved {
            read: fabricated,
            metadata: active(1, 1, 1),
        },
        Event::BuildFinished { generation: FIRST },
    ] {
        let expected_tag = state_machine_core::EventTag::event_tag(&event);
        assert_eq!(
            Supervisor::transition(&supervisor, event),
            Err(TransitionError::NotAllowed {
                state: "Observing",
                event: expected_tag,
            })
        );
    }
    assert_eq!(supervisor, initial);
    let no_join = Event::StopFinished(report(target(FIRST, UnitPiece::Driver), StopResult::Joined));
    assert_eq!(
        Supervisor::transition(&supervisor, no_join),
        Err(TransitionError::Stale)
    );
    supervisor = running();
    observe(&mut supervisor, super::ended_metadata());
    let driver = target(FIRST, UnitPiece::Driver);
    stop(&mut supervisor, driver, StopResult::TimedOut);
    let duplicate = Event::StopFinished(report(driver, StopResult::TimedOut));
    assert_eq!(
        Supervisor::transition(&supervisor, duplicate),
        Err(TransitionError::Stale)
    );
    for piece in [
        UnitPiece::Custody,
        UnitPiece::Settings,
        UnitPiece::LeaseAdapter,
    ] {
        stop(&mut supervisor, target(FIRST, piece), StopResult::Joined);
    }
    stop(&mut supervisor, driver, StopResult::Joined);
    assert_eq!(
        Supervisor::transition(&supervisor, Event::BuildFinished { generation: FIRST }),
        Err(TransitionError::Stale)
    );
    let mut opening = start();
    let future = switchboard_message_system::Generation(FIRST.0.checked_add(1).unwrap());
    assert_eq!(
        Supervisor::transition(&opening, Event::BuildFinished { generation: future }),
        Err(TransitionError::NotAllowed {
            state: "Opening",
            event: "BuildFinished",
        })
    );
    send(&mut opening, Event::Close);
    assert!(send(&mut opening, Event::Close).is_empty());
}

#[test]
fn shared_checker_reaches_the_full_declared_table() {
    let (initial, _) = Supervisor::new(BUDGETS);
    let mut cooldown = start();
    send(&mut cooldown, Event::BuildFailed { generation: FIRST });
    stop_unit(&mut cooldown);
    let mut lost = running();
    observe(&mut lost, Metadata::Empty);
    let mut last_normal = lost.clone();
    for piece in [UnitPiece::Driver, UnitPiece::Custody, UnitPiece::Settings] {
        stop(&mut last_normal, target(FIRST, piece), StopResult::Joined);
    }
    let mut after_unit = last_normal.clone();
    stop(
        &mut after_unit,
        target(FIRST, UnitPiece::LeaseAdapter),
        StopResult::Joined,
    );
    let mut mismatch = last_normal.clone();
    observe(&mut mismatch, active(2, 1, 2));
    let mut faulted = mismatch.clone();
    stop(
        &mut faulted,
        target(FIRST, UnitPiece::LeaseAdapter),
        StopResult::Joined,
    );
    let mut store_only = initial.clone();
    send(&mut store_only, Event::Close);
    let mut outstanding = store_only.clone();
    stop(
        &mut outstanding,
        StopTarget::EnrollmentStore,
        StopResult::TimedOut,
    );
    let mut closed = store_only.clone();
    stop(&mut closed, StopTarget::EnrollmentStore, StopResult::Joined);
    let mut two_outstanding = running();
    send(&mut two_outstanding, Event::Close);
    for piece in PIECES {
        stop(
            &mut two_outstanding,
            target(FIRST, piece),
            if piece == UnitPiece::Driver {
                StopResult::TimedOut
            } else {
                StopResult::Joined
            },
        );
    }
    stop(
        &mut two_outstanding,
        StopTarget::EnrollmentStore,
        StopResult::TimedOut,
    );
    let fixtures = [
        initial,
        start(),
        running(),
        lost,
        last_normal,
        after_unit,
        mismatch,
        faulted,
        cooldown,
        store_only,
        outstanding,
        two_outstanding,
        closed,
    ];
    let cases = fixtures.into_iter().flat_map(|supervisor| {
        let mut events = vec![
            Event::Close,
            Event::EnrollmentStoreExited,
            Event::EnrollmentMayHaveChanged,
            Event::BuildFinished { generation: FIRST },
            Event::BuildFailed { generation: FIRST },
            Event::PieceExited {
                generation: FIRST,
                piece: UnitPiece::Driver,
            },
        ];
        for metadata in [
            Metadata::ReadFailed,
            Metadata::Empty,
            super::ended_metadata(),
            active(1, 1, 1),
            active(2, 1, 2),
        ] {
            events.push(current_read(&supervisor, metadata));
        }
        for target in PIECES
            .map(|piece| target(FIRST, piece))
            .into_iter()
            .chain([StopTarget::EnrollmentStore])
        {
            for result in [StopResult::Joined, StopResult::TimedOut] {
                events.push(Event::StopFinished(report(target, result)));
            }
        }
        events
            .into_iter()
            .map(move |event| (supervisor.clone(), event))
    });
    check_state_machine_transitions::<Supervisor>(cases).unwrap();
}
