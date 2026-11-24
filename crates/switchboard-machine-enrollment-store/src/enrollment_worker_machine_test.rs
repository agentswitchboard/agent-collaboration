use hegel::generators as gs;
use state_machine_core::{
    TransitionCheckError, check_declared_transitions, check_state_machine_transitions,
};

use super::*;

fn close_error() -> Arc<MachineLocalStoreError> {
    Arc::new(MachineLocalStoreError::Database(sqlx::Error::WorkerCrashed))
}

fn advance(state: &WorkerState, event: WorkerEvent) -> WorkerState {
    EnrollmentWorkerMachine::transition(state, event).unwrap().0
}

#[test]
fn worker_table_has_every_taken_edge_and_no_unwitnessed_edges() {
    use WorkerEvent as Event;
    use WorkerState as State;

    let open = State::Running(WorkerAdmission::Open);
    let closing = State::Running(WorkerAdmission::Closing);
    let exited = advance(&open, Event::TaskExited(WorkerTaskEnd::Failed));
    let joined = advance(&exited, Event::SqliteCloseFinished(Ok(())));
    let close_unconfirmed = advance(&exited, Event::SqliteCloseFinished(Err(close_error())));
    let events = [
        Event::RequestClose,
        Event::Enqueue,
        Event::BeginRequest,
        Event::TaskExited(WorkerTaskEnd::Failed),
        Event::SqliteCloseFinished(Ok(())),
        Event::SqliteCloseFinished(Err(close_error())),
    ];
    let cases: Vec<_> = [open, closing, exited, joined, close_unconfirmed]
        .into_iter()
        .flat_map(|state| {
            events
                .iter()
                .cloned()
                .map(move |event| (state.clone(), event))
        })
        .collect();

    check_state_machine_transitions::<EnrollmentWorkerMachine>(cases.clone())
        .expect("the worker takes exactly the declared transitions");

    let declared = EnrollmentWorkerMachine::transitions();
    let taken = Transition {
        from: "runningOpen",
        event: "beginRequest",
        to: "runningOpen",
        terminal: false,
    };
    let missing: Vec<_> = declared
        .iter()
        .copied()
        .filter(|edge| *edge != taken)
        .collect();
    let unwitnessed = Transition {
        from: "runningClosing",
        event: "beginRequest",
        to: "runningClosing",
        terminal: false,
    };
    let mut extra = declared.to_vec();
    extra.push(unwitnessed);
    let apply = |state: &WorkerState, event| {
        EnrollmentWorkerMachine::transition(state, event)
            .ok()
            .map(|(next, _)| next)
    };
    assert_eq!(
        check_declared_transitions(cases.clone(), &missing, apply),
        Err(TransitionCheckError::Undeclared(taken))
    );
    assert_eq!(
        check_declared_transitions(cases, &extra, apply),
        Err(TransitionCheckError::Unreachable(unwitnessed))
    );
}

#[hegel::test(test_cases = 48)]
fn generated_worker_histories_distinguish_exit_cleanup_and_receipts(case: hegel::TestCase) {
    use EnrollmentWorkerExit as Exit;
    use WorkerEvent as Event;
    use WorkerState as State;

    let close_before_failure = case.draw(gs::booleans());
    let observations_while_key_held = case.draw(gs::integers::<u8>().min_value(1).max_value(5));
    let repeated_stops = case.draw(gs::integers::<u8>().min_value(1).max_value(4));
    let close_acknowledged = case.draw(gs::booleans());
    let close_fault = close_error();

    // A normal return, panic and cancellation are inputs from the driver.
    // Panic and cancellation both lose the loop's own return value.
    for (task_end, close_requested, expected_exit, task_failed) in [
        (
            WorkerTaskEnd::Returned(Exit::Requested),
            true,
            Exit::Requested,
            false,
        ),
        (
            WorkerTaskEnd::Returned(Exit::Autonomous),
            false,
            Exit::Autonomous,
            false,
        ),
        (
            WorkerTaskEnd::Failed,
            close_before_failure,
            Exit::Autonomous,
            true,
        ),
        (WorkerTaskEnd::Failed, true, Exit::Autonomous, true),
    ] {
        let mut state = State::Running(WorkerAdmission::Open);
        state = advance(&state, Event::Enqueue);
        state = advance(&state, Event::BeginRequest);
        if close_requested {
            for _ in 0..repeated_stops {
                state = advance(&state, Event::RequestClose);
            }
            for event in [Event::Enqueue, Event::BeginRequest] {
                assert!(matches!(
                    EnrollmentWorkerMachine::transition(&state, event),
                    Err(TransitionError::NotAllowed {
                        state: "runningClosing",
                        ..
                    })
                ));
            }
        }

        let exited = advance(&state, Event::TaskExited(task_end));
        let State::Progress(progress) = &exited else {
            panic!("worker exit must be observable before cleanup");
        };
        // While a blocking Keychain read is held, the driver has no SQLite
        // acknowledgement to advance the machine. Observations stay repeatable.
        for _ in 0..observations_while_key_held {
            assert_eq!(progress.exit(), expected_exit);
            assert!(progress.report().is_none());
        }

        let connection = if close_acknowledged {
            Ok(())
        } else {
            Err(close_fault.clone())
        };
        let reported = advance(&exited, Event::SqliteCloseFinished(connection));
        assert!(reported.is_terminal());
        let State::Progress(progress) = &reported else {
            panic!("SQLite result must produce a report");
        };
        let report = progress.report().expect("sticky cleanup report");
        for _ in 0..2 {
            assert_eq!(progress.exit(), expected_exit);
            match (close_acknowledged, task_failed, report.join()) {
                (true, false, Ok(EnrollmentJoin { outcome: Ok(()) }))
                | (
                    true,
                    true,
                    Ok(EnrollmentJoin {
                        outcome: Err(EnrollmentStoreCloseError::WorkerStopped),
                    }),
                ) => {}
                (
                    false,
                    _,
                    Err(EnrollmentObservationError::ConnectionCloseUnconfirmed { exit }),
                ) => {
                    assert_eq!(exit, expected_exit);
                }
                (_, _, observed) => panic!("unexpected join receipt: {observed:?}"),
            }
            match (close_acknowledged, task_failed, report.close_outcome()) {
                (true, false, Ok(()))
                | (_, true, Err(EnrollmentStoreCloseError::WorkerStopped)) => {}
                (false, false, Err(EnrollmentStoreCloseError::Connection(error))) => {
                    assert!(Arc::ptr_eq(&error, &close_fault));
                }
                (_, _, observed) => panic!("unexpected close result: {observed:?}"),
            }
        }
    }
}
