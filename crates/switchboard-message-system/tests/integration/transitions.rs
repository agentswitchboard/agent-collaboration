use std::time::Duration;

use switchboard_message_system::{
    Effect, Event, Fault, Front, FrontRefusal, Metadata, State, StopReport, StopResult, StopTarget,
    Supervisor, UnitPiece,
};

use super::{BUDGETS, FIRST, PIECES, active, enrollment, observe, pair, read, send, stop, target};

fn observing(supervisor: &Supervisor, why: FrontRefusal) {
    assert_eq!(supervisor.state(), &State::Observing { reason: why });
}

fn scheduled(supervisor: &Supervisor, effects: &[Effect], delay: u64) {
    assert!(effects.contains(&Effect::ScheduleMetadataRead {
        read: supervisor.latest_read(),
        after: Duration::from_secs(delay)
    }));
}

fn start() -> Supervisor {
    let (mut supervisor, _) = Supervisor::new(BUDGETS);
    assert!(
        observe(&mut supervisor, active(1, 1, 1)).contains(&Effect::StartUnit {
            generation: FIRST,
            pair: pair(1, 1),
        })
    );
    supervisor
}

fn running() -> Supervisor {
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
fn observation_only_until_an_active_row_and_read_changes_replace_the_timer() {
    let (mut supervisor, initial) = Supervisor::new(BUDGETS);
    observing(&supervisor, FrontRefusal::NotEnrolled);
    assert_eq!(
        initial.first(),
        Some(&Effect::PublishFront(supervisor.state().front()))
    );
    scheduled(&supervisor, &initial, 0);
    for (metadata, why, delay) in [
        (Metadata::Empty, FrontRefusal::NotEnrolled, 15),
        (Metadata::Ended, FrontRefusal::EnrollmentEnded, 15),
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
    assert!(read(&mut supervisor, old_read, active(1, 1, 1)).is_empty());
    let effects = observe(&mut supervisor, active(1, 1, 1));
    assert!(effects.contains(&Effect::StartUnit {
        generation: FIRST,
        pair: pair(1, 1)
    }));
    scheduled(&supervisor, &effects, 15);
}

#[test]
fn empty_and_ended_withdraw_immediately_during_opening_and_running() {
    for setup in [start, running] {
        for (metadata, why) in [
            (Metadata::Empty, FrontRefusal::NotEnrolled),
            (Metadata::Ended, FrontRefusal::EnrollmentEnded),
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
            assert!(send(&mut supervisor, Event::BuildFinished { generation: FIRST }).is_empty());
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
                FrontRefusal::Restarting
            )))
        );
        assert!(effects.contains(&Effect::ReportFault(Fault::DifferentPair {
            pinned: pair(1, 1),
            observed: pair(organization, deployment),
        })));
        assert!(stop_unit(&mut supervisor).is_empty());
        assert_eq!(
            supervisor.state(),
            &State::Faulted {
                pinned: pair(1, 1),
                observed: pair(organization, deployment),
            }
        );
        assert!(observe(&mut supervisor, active(1, 1, 3)).is_empty());
        assert_eq!(
            send(&mut supervisor, Event::Close),
            [Effect::StopAndJoin {
                target: StopTarget::EnrollmentStore,
                budget: BUDGETS.enrollment_store,
            }]
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
    for (event, delay) in [
        (Event::BuildFailed { generation: FIRST }, 5),
        (
            Event::PieceExited {
                generation: FIRST,
                piece: UnitPiece::Custody,
            },
            0,
        ),
    ] {
        let mut supervisor = if delay == 5 { start() } else { running() };
        let mut effects = send(&mut supervisor, event);
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
        observing(&supervisor, FrontRefusal::Restarting);
        scheduled(&supervisor, &effects, delay);
        assert!(read(&mut supervisor, during_drain, active(1, 1, 1)).is_empty());
        assert!(read(&mut supervisor, crossing, active(1, 1, 1)).is_empty());
        let reopened = observe(&mut supervisor, active(1, 1, 1));
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
            assert!(send(&mut supervisor, old).is_empty());
        }
        assert!(matches!(supervisor.state(), State::Opening { .. }));
    }
}

#[test]
fn normal_drain_waits_for_timed_out_owner_and_fences_unsettled_enrollments() {
    let mut supervisor = running();
    observe(&mut supervisor, Metadata::Ended);
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
            result: StopResult::Joined,
            unsettled_retirements: vec![enrollment(1)],
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
        observe(&mut supervisor, active(1, 1, 2))
            .iter()
            .any(|effect| matches!(effect, Effect::StartUnit { .. }))
    );
}

#[test]
fn close_joins_overtaken_opening_in_order_then_closes_store_and_releases_inputs() {
    let mut supervisor = start();
    let mut effects = send(&mut supervisor, Event::Close);
    assert!(send(&mut supervisor, Event::BuildFinished { generation: FIRST }).is_empty());
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
    assert!(observe(&mut supervisor, active(1, 1, 1)).is_empty());
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
            FrontRefusal::Restarting
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
    assert!(observe(&mut supervisor, active(1, 1, 1)).is_empty());
    assert!(stop(&mut supervisor, driver, StopResult::Joined).is_empty());
    assert_eq!(
        supervisor.state(),
        &State::ClosedWithOutstanding {
            pieces: [store].into()
        }
    );
    assert!(stop(&mut supervisor, store, StopResult::Joined).is_empty());
    assert_eq!(supervisor.state(), &State::Closed);
}
