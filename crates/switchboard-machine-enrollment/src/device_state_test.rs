use hegel::generators as gs;
use state_machine_core::{
    TransitionCheckError, check_declared_transitions, check_state_machine_transitions,
};

use crate::{
    BindingGeneration, CredentialStoreError, EnrollmentSaveFailure, RefreshCredential, SecretValue,
    TokenGrant,
};

use super::*;

fn elapsed(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

fn started(expires: u64) -> DeviceSignInEvent<'static> {
    DeviceSignInEvent::Started {
        dispatched: elapsed(0),
        arrived: elapsed(0),
        interval: Duration::from_secs(5),
        expires_in: Duration::from_secs(expires),
    }
}

fn step(state: DeviceSignInState, event: DeviceSignInEvent<'_>) -> DeviceSignInState {
    DeviceSignInMachine::transition(&state, event).unwrap().0
}

fn start(expires: u64) -> DeviceSignInState {
    step(DeviceSignInState::Starting, started(expires))
}

fn pending_window(state: DeviceSignInState) -> DevicePollWindow {
    let DeviceSignInState::Waiting(window) = state else {
        panic!("expected a waiting device code, saw {state:?}");
    };
    window
}

fn poll_at(state: DeviceSignInState, seconds: u64) -> DeviceSignInState {
    let polling = step(state, DeviceSignInEvent::Woke(elapsed(seconds)));
    assert!(matches!(polling, DeviceSignInState::Polling(_)));
    polling
}

fn reply_at<'reply>(seconds: u64, reply: &'reply PollAnswer) -> DeviceSignInEvent<'reply> {
    DeviceSignInEvent::PollSettled {
        now: elapsed(seconds),
        reply: Some(reply),
    }
}

fn receipt() -> EnrollmentReceipt {
    EnrollmentReceipt {
        generation: BindingGeneration::from_persisted(1).unwrap(),
        enrollment_id: "019a0000-0000-7000-8000-000000000002".parse().unwrap(),
    }
}

fn approval() -> PollAnswer {
    PollAnswer::Enrolled(TokenGrant {
        credential: RefreshCredential {
            enrollment_id: receipt().enrollment_id,
            refresh_token: SecretValue::new("fictional-refresh".into()).unwrap(),
        },
        access_token: SecretValue::new("fictional-access".into()).unwrap(),
        expires_in: Duration::from_secs(60),
    })
}

#[test]
fn declared_device_edges_are_witnessed_and_the_checker_rejects_both_bad_tables() {
    let waiting = start(6);
    let polling = poll_at(waiting, 5);
    let approved = approval();
    let saving = step(polling, reply_at(6, &approved));
    let cases = [
        (DeviceSignInState::Starting, started(6)),
        (waiting, DeviceSignInEvent::Woke(elapsed(4))),
        (waiting, DeviceSignInEvent::Woke(elapsed(5))),
        (waiting, DeviceSignInEvent::Woke(elapsed(6))),
        (polling, reply_at(5, &PollAnswer::Pending)),
        (polling, reply_at(6, &approved)),
        (polling, reply_at(5, &PollAnswer::Denied)),
        (saving, DeviceSignInEvent::SaveSettled(Ok(receipt()))),
        (
            saving,
            DeviceSignInEvent::SaveSettled(Err(ApprovedEnrollmentSaveFailure::NotSaved(
                EnrollmentSaveFailure::Store(CredentialStoreError::Replace),
            ))),
        ),
    ];
    check_state_machine_transitions::<DeviceSignInMachine<'_>>(cases).unwrap();
    let omitted = edge("polling", "pollSettled", "saving", false);
    let mut table = DeviceSignInMachine::transitions().to_vec();
    table.retain(|candidate| *candidate != omitted);
    let apply = |state: &DeviceSignInState, event| {
        DeviceSignInMachine::transition(state, event)
            .ok()
            .map(|(next, _)| next)
    };
    assert_eq!(
        check_declared_transitions(cases, &table, apply),
        Err(TransitionCheckError::Undeclared(omitted))
    );
    table.push(omitted);
    let unwitnessed = edge("waiting", "woke", "saving", false);
    table.push(unwitnessed);
    assert_eq!(
        check_declared_transitions(cases, &table, apply),
        Err(TransitionCheckError::Unreachable(unwitnessed))
    );
}

#[test]
fn a_final_poll_answer_never_reopens_the_attempt() {
    for (answer, expected) in [
        (PollAnswer::Denied, EnrollmentEnd::Denied),
        (PollAnswer::Expired, EnrollmentEnd::Expired),
        (
            PollAnswer::Invalid {
                reason: crate::InvalidReason::UnknownError,
            },
            EnrollmentEnd::Invalid(crate::InvalidReason::UnknownError),
        ),
    ] {
        let stopped = step(poll_at(start(180), 5), reply_at(5, &answer));
        assert_eq!(stopped, ended(expected));
        assert!(matches!(
            DeviceSignInMachine::transition(&stopped, reply_at(10, &PollAnswer::Pending)),
            Err(TransitionError::Stale)
        ));
    }
}

#[test]
fn poll_waits_follow_the_server_and_stop_at_the_local_deadline() {
    let state = start(30);
    assert_eq!(pending_window(state).wake_at(), elapsed(5));
    let state = step(poll_at(state, 5), reply_at(5, &PollAnswer::SlowDown));
    assert_eq!(pending_window(state).wake_at(), elapsed(15));
    let state = step(
        poll_at(state, 15),
        reply_at(
            15,
            &PollAnswer::Unavailable {
                retry_after: Some(Duration::from_secs(2)),
            },
        ),
    );
    assert_eq!(pending_window(state).wake_at(), elapsed(17));
    let state = step(
        poll_at(state, 17),
        reply_at(17, &PollAnswer::Unavailable { retry_after: None }),
    );
    assert_eq!(pending_window(state).wake_at(), elapsed(27));
    let state = step(poll_at(state, 27), reply_at(27, &PollAnswer::Pending));
    assert_eq!(pending_window(state).wake_at(), elapsed(30));
    assert_eq!(
        step(state, DeviceSignInEvent::Woke(elapsed(30))),
        ended(EnrollmentEnd::Expired)
    );
}

#[hegel::test(seed = Some(1350), test_cases = 100)]
fn generated_poll_and_clock_orders_match_the_device_attempt_model(case: hegel::TestCase) {
    let lifetime = case.draw(gs::integers::<u64>().min_value(6).max_value(220));
    let steps = case.draw(gs::integers::<u8>().min_value(1).max_value(40));
    let mut state = start(lifetime);
    let (mut seconds, mut cadence, mut next_poll) = (0, 5, 5);

    for _ in 0..steps {
        if state.is_terminal() {
            break;
        }
        seconds += case.draw(gs::integers::<u64>().max_value(20));
        let next = step(state, DeviceSignInEvent::Woke(elapsed(seconds)));
        if seconds >= lifetime {
            assert_eq!(next, ended(EnrollmentEnd::Expired));
            break;
        }
        if seconds < next_poll {
            assert_eq!(next, state);
            continue;
        }
        assert!(matches!(next, DeviceSignInState::Polling(_)));

        let (answer, delay, ending) = match case.draw(gs::integers::<u8>().max_value(9)) {
            0 | 1 => (PollAnswer::Pending, cadence, None),
            2 | 3 => {
                cadence += 5;
                (PollAnswer::SlowDown, cadence, None)
            }
            4 => (PollAnswer::Unavailable { retry_after: None }, cadence, None),
            5 | 6 => {
                let delay = case.draw(gs::integers::<u64>().max_value(40));
                (
                    PollAnswer::Unavailable {
                        retry_after: Some(Duration::from_secs(delay)),
                    },
                    delay,
                    None,
                )
            }
            7 => (PollAnswer::Denied, 0, Some(EnrollmentEnd::Denied)),
            8 => (PollAnswer::Expired, 0, Some(EnrollmentEnd::Expired)),
            _ => (
                PollAnswer::Invalid {
                    reason: crate::InvalidReason::UnknownError,
                },
                0,
                Some(EnrollmentEnd::Invalid(crate::InvalidReason::UnknownError)),
            ),
        };
        state = step(next, reply_at(seconds, &answer));
        if let Some(why) = ending {
            assert_eq!(state, ended(why));
        } else {
            next_poll = seconds + delay;
            assert_eq!(
                pending_window(state).wake_at(),
                elapsed(next_poll.min(lifetime))
            );
        }
    }

    // Every history drives the exact deadline, both grace seconds and the
    // first instant after grace. No save can happen unless a poll was started.
    let polling = poll_at(start(lifetime), lifetime - 1);
    let grant = approval();
    for seconds_after_expiry in 0..=3 {
        let now = lifetime + seconds_after_expiry;
        let (state, effects) =
            DeviceSignInMachine::transition(&polling, reply_at(now, &grant)).unwrap();
        assert_eq!(
            step(polling, reply_at(now, &PollAnswer::Denied)),
            ended(EnrollmentEnd::Expired)
        );
        if seconds_after_expiry <= 2 {
            assert_eq!(state, DeviceSignInState::Saving);
            assert_eq!(effects.len(), 1);
            assert_eq!(
                step(state, DeviceSignInEvent::SaveSettled(Ok(receipt()))),
                DeviceSignInState::Enrolled(receipt())
            );
        } else {
            assert_eq!(state, ended(EnrollmentEnd::Expired));
            assert!(effects.is_empty());
        }
    }
}
