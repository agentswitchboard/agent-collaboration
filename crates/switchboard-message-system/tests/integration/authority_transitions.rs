use state_machine_core::{StateMachine, TransitionError};
use switchboard_message_system::{
    Effect, Event, Fault, Front, FrontRefusal, Generation, LeaseStopResult, Metadata, ReadId,
    State, StopReport, StopResult, StopTarget, Supervisor, UnitPiece,
};

use super::{
    BUDGETS, FIRST, PIECES, active, enrollment, observe, pair, send, stop, target,
    transitions::{current_read, running, scheduled, start},
};

#[test]
fn late_lease_join_uses_only_the_final_retirement_snapshot() {
    for pending_at_join in [false, true] {
        let mut supervisor = running();
        let enrollment_a = enrollment(1);
        // The executor's provisional fence has A when the worker times out.
        let mut pending = vec![enrollment_a];
        observe(&mut supervisor, super::ended_metadata());
        for piece in [UnitPiece::Driver, UnitPiece::Custody, UnitPiece::Settings] {
            stop(&mut supervisor, target(FIRST, piece), StopResult::Joined);
        }
        let lease = target(FIRST, UnitPiece::LeaseAdapter);
        assert_eq!(
            send(
                &mut supervisor,
                Event::StopFinished(StopReport::LeaseAdapter {
                    generation: FIRST,
                    result: LeaseStopResult::TimedOut,
                })
            ),
            [Effect::ReportFault(Fault::StopTimedOut { target: lease })]
        );
        assert!(matches!(supervisor.state(), State::Quiescing(cleanup)
            if cleanup.joining().is_none() && cleanup.outstanding().contains(&lease)));
        assert!(
            observe(&mut supervisor, active(1, 1, 1))
                .iter()
                .all(|effect| !matches!(effect, Effect::StartUnit { .. }))
        );

        if !pending_at_join {
            pending.clear();
        }
        let effects = send(
            &mut supervisor,
            Event::StopFinished(StopReport::LeaseAdapter {
                generation: FIRST,
                result: LeaseStopResult::Joined {
                    unsettled_retirements: pending,
                },
            }),
        );
        scheduled(&supervisor, &effects, 0);
        let active_a = observe(&mut supervisor, active(1, 1, 1));
        let next_generation = Generation(FIRST.0.checked_add(1).unwrap());
        if pending_at_join {
            assert_eq!(
                supervisor.state().front(),
                Front::Refusing(FrontRefusal::EnrollmentEnded)
            );
            assert!(
                !active_a
                    .iter()
                    .any(|effect| matches!(effect, Effect::StartUnit { .. }))
            );
            assert!(
                observe(&mut supervisor, active(1, 1, 2)).contains(&Effect::StartUnit {
                    generation: next_generation,
                    pair: pair(1, 1),
                    retired_enrollments: [enrollment_a].into(),
                })
            );
        } else {
            assert!(active_a.contains(&Effect::StartUnit {
                generation: next_generation,
                pair: pair(1, 1),
                retired_enrollments: [].into(),
            }));
        }
    }
}

#[test]
fn the_last_issued_read_is_stale_once_close_store_exit_or_mismatch_retires_it() {
    let triggers: [fn(&Supervisor) -> Event; 3] = [
        |_| Event::Close,
        |_| Event::EnrollmentStoreExited,
        |supervisor| current_read(supervisor, active(2, 1, 2)),
    ];
    for trigger in triggers {
        let mut supervisor = running();
        let in_flight = current_read(&supervisor, active(1, 1, 1));
        let future = ReadId(supervisor.latest_read().0.checked_add(1).unwrap());
        let cause = trigger(&supervisor);
        send(&mut supervisor, cause);
        assert_eq!(
            Supervisor::transition(&supervisor, in_flight),
            Err(TransitionError::Stale)
        );
        assert_eq!(
            Supervisor::transition(
                &supervisor,
                Event::MetadataObserved {
                    read: future,
                    metadata: super::ended_metadata()
                }
            ),
            Err(TransitionError::NotAllowed {
                state: "Quiescing",
                event: "MetadataObserved"
            })
        );
        assert_eq!(
            Supervisor::transition(&supervisor, Event::EnrollmentMayHaveChanged),
            Err(TransitionError::NotAllowed {
                state: "Quiescing",
                event: "EnrollmentMayHaveChanged"
            })
        );
    }
}

#[test]
fn issued_generation_loses_delivery_authority_before_it_loses_join_ownership() {
    let built = Event::BuildFinished { generation: FIRST };
    let failed = Event::BuildFailed { generation: FIRST };
    let exited = Event::PieceExited {
        generation: FIRST,
        piece: UnitPiece::Driver,
    };
    let mut running_unit = running();
    for event in [built.clone(), failed.clone()] {
        assert_eq!(
            Supervisor::transition(&running_unit, event),
            Err(TransitionError::Stale)
        );
    }
    assert!(!send(&mut running_unit, exited.clone()).is_empty());

    let mut overtaken = start();
    observe(&mut overtaken, Metadata::Empty);
    for (supervisor, events) in [
        (
            running_unit,
            [built.clone(), failed.clone(), exited.clone()],
        ),
        (overtaken, [built, failed, exited]),
    ] {
        assert!(matches!(supervisor.state(), State::Quiescing(cleanup)
            if cleanup.joining() == Some(target(FIRST, UnitPiece::Driver))));
        for event in events {
            assert_eq!(
                Supervisor::transition(&supervisor, event),
                Err(TransitionError::Stale)
            );
        }
    }
}

#[test]
fn explicit_close_still_reports_the_first_autonomous_store_exit_during_the_driver_leg() {
    let mut supervisor = running();
    send(&mut supervisor, Event::Close);
    let before = supervisor.state().clone();
    assert_eq!(
        send(&mut supervisor, Event::EnrollmentStoreExited),
        [Effect::ReportFault(Fault::EnrollmentStoreExited)]
    );
    assert_eq!(supervisor.state(), &before);
    assert_eq!(
        Supervisor::transition(&supervisor, Event::EnrollmentStoreExited),
        Err(TransitionError::Stale)
    );

    let mut targets = PIECES
        .into_iter()
        .skip(1)
        .map(|piece| target(FIRST, piece))
        .chain([StopTarget::EnrollmentStore]);
    for piece in PIECES {
        let effects = stop(&mut supervisor, target(FIRST, piece), StopResult::Joined);
        let next = targets.next().unwrap();
        let budget = match next {
            StopTarget::Unit {
                piece: UnitPiece::Custody,
                ..
            } => BUDGETS.custody,
            StopTarget::Unit {
                piece: UnitPiece::Settings,
                ..
            } => BUDGETS.settings,
            StopTarget::Unit {
                piece: UnitPiece::LeaseAdapter,
                ..
            } => BUDGETS.lease_adapter,
            StopTarget::Unit {
                piece: UnitPiece::Driver,
                ..
            } => BUDGETS.driver,
            StopTarget::EnrollmentStore => BUDGETS.enrollment_store,
        };
        assert_eq!(
            effects,
            [Effect::StopAndJoin {
                target: next,
                budget
            }]
        );
    }
    assert_eq!(
        stop(
            &mut supervisor,
            StopTarget::EnrollmentStore,
            StopResult::Joined
        ),
        [Effect::ReleaseInputs]
    );
    assert_eq!(supervisor.state(), &State::Closed);
}

#[test]
fn final_timeout_can_report_an_autonomous_exit_only_while_store_join_is_owed() {
    for store_result in [StopResult::Joined, StopResult::TimedOut] {
        let mut supervisor = running();
        send(&mut supervisor, Event::Close);
        stop(
            &mut supervisor,
            target(FIRST, UnitPiece::Driver),
            StopResult::TimedOut,
        );
        for piece in [
            UnitPiece::Custody,
            UnitPiece::Settings,
            UnitPiece::LeaseAdapter,
        ] {
            stop(&mut supervisor, target(FIRST, piece), StopResult::Joined);
        }
        stop(&mut supervisor, StopTarget::EnrollmentStore, store_result);
        assert!(matches!(
            supervisor.state(),
            State::ClosedWithOutstanding { .. }
        ));
        let before = supervisor.state().clone();
        if store_result == StopResult::TimedOut {
            assert_eq!(
                send(&mut supervisor, Event::EnrollmentStoreExited),
                [Effect::ReportFault(Fault::EnrollmentStoreExited)]
            );
            assert_eq!(supervisor.state(), &before);
            assert!(
                stop(
                    &mut supervisor,
                    StopTarget::EnrollmentStore,
                    StopResult::Joined
                )
                .is_empty()
            );
        }
        assert_eq!(
            Supervisor::transition(&supervisor, Event::EnrollmentStoreExited),
            Err(TransitionError::Stale)
        );
        assert!(matches!(
            supervisor.state(),
            State::ClosedWithOutstanding { .. }
        ));
    }
}
