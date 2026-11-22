use std::time::Duration;

use hegel::generators as gs;
use router_clock::ClockInstant;

use crate::{InvalidReason, decode_device_authorization};

use super::{DeviceEnrollment, DeviceEnrollmentInput, DevicePollSchedule, EnrollmentEnd};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelAnswer {
    Pending,
    SlowDown,
    Unavailable(Option<u64>),
    Denied,
    Expired,
    Invalid,
}

fn elapsed(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

fn start(expires: u64) -> DeviceEnrollment {
    let body = format!(
        r#"{{"device_code":"abc-private","user_code":"AB-CD",
        "verification_uri":"https://signin.example/device",
        "verification_uri_complete":"https://signin.example/device?user_code=AB-CD",
        "expires_in":{expires},"interval":5}}"#
    );
    let challenge = decode_device_authorization(body.as_bytes()).expect("device fixture");
    DeviceEnrollment::NotEnrolled.step(elapsed(0), DeviceEnrollmentInput::Started(challenge))
}

#[test]
fn a_final_poll_answer_never_reopens_the_attempt() {
    for (answer, expected) in [
        (DeviceEnrollmentInput::Denied, EnrollmentEnd::Denied),
        (DeviceEnrollmentInput::Expired, EnrollmentEnd::Expired),
        (
            DeviceEnrollmentInput::Invalid {
                reason: InvalidReason::UnknownError,
            },
            EnrollmentEnd::Invalid(InvalidReason::UnknownError),
        ),
    ] {
        let stopped = start(180).step(elapsed(5), answer);
        assert!(matches!(stopped, DeviceEnrollment::Ended { why } if why == expected));
        let unchanged = stopped.step(elapsed(10), DeviceEnrollmentInput::Pending);
        assert_eq!(
            unchanged.poll_schedule(elapsed(15)),
            DevicePollSchedule::Inactive
        );
    }
}

#[test]
fn poll_waits_follow_the_server_and_stop_at_the_local_deadline() {
    let state = start(30);
    assert_eq!(
        state.poll_schedule(elapsed(4)),
        DevicePollSchedule::WaitUntil(elapsed(5))
    );
    assert_eq!(state.poll_schedule(elapsed(5)), DevicePollSchedule::Due);
    let state = state.step(elapsed(5), DeviceEnrollmentInput::SlowDown);
    assert_eq!(
        state.poll_schedule(elapsed(6)),
        DevicePollSchedule::WaitUntil(elapsed(15))
    );
    let state = state.step(
        elapsed(15),
        DeviceEnrollmentInput::Unavailable {
            retry_after: Some(Duration::from_secs(2)),
        },
    );
    assert_eq!(
        state.poll_schedule(elapsed(16)),
        DevicePollSchedule::WaitUntil(elapsed(17))
    );
    let state = state.step(
        elapsed(17),
        DeviceEnrollmentInput::Unavailable { retry_after: None },
    );
    assert_eq!(
        state.poll_schedule(elapsed(18)),
        DevicePollSchedule::WaitUntil(elapsed(27))
    );
    let state = state.step(elapsed(27), DeviceEnrollmentInput::Pending);
    assert_eq!(
        state.poll_schedule(elapsed(29)),
        DevicePollSchedule::WaitUntil(elapsed(30))
    );
    assert_eq!(
        state.poll_schedule(elapsed(30)),
        DevicePollSchedule::DeadlineReached
    );
    assert!(matches!(
        state.step(elapsed(30), DeviceEnrollmentInput::ClockReached),
        DeviceEnrollment::Ended {
            why: EnrollmentEnd::Expired
        }
    ));
}

#[hegel::test(seed = Some(1350), test_cases = 100)]
fn generated_poll_and_clock_orders_match_the_device_attempt_model(case: hegel::TestCase) {
    let lifetime = case.draw(gs::integers::<u64>().min_value(6).max_value(220));
    let steps = case.draw(gs::integers::<u8>().min_value(1).max_value(40));
    let mut state = start(lifetime);
    let (mut seconds, mut cadence, mut next) = (0, 5, 5);
    let mut final_answer = None;

    for _ in 0..steps {
        seconds += case.draw(gs::integers::<u64>().max_value(20));
        let answer = match case.draw(gs::integers::<u8>().max_value(9)) {
            0 | 1 => ModelAnswer::Pending,
            2 | 3 => ModelAnswer::SlowDown,
            4 => ModelAnswer::Unavailable(None),
            5 | 6 => ModelAnswer::Unavailable(Some(case.draw(gs::integers::<u64>().max_value(40)))),
            7 => ModelAnswer::Denied,
            8 => ModelAnswer::Expired,
            _ => ModelAnswer::Invalid,
        };
        let schedule = if final_answer.is_some() {
            DevicePollSchedule::Inactive
        } else if seconds >= lifetime {
            DevicePollSchedule::DeadlineReached
        } else if seconds >= next {
            DevicePollSchedule::Due
        } else {
            DevicePollSchedule::WaitUntil(elapsed(std::cmp::min(next, lifetime)))
        };
        assert_eq!(
            state.poll_schedule(elapsed(seconds)),
            schedule,
            "poll schedule at {seconds}"
        );

        if schedule == DevicePollSchedule::DeadlineReached {
            state = state.step(elapsed(seconds), DeviceEnrollmentInput::ClockReached);
            final_answer = Some(EnrollmentEnd::Expired);
        } else if schedule == DevicePollSchedule::Due {
            let input = match answer {
                ModelAnswer::Pending => DeviceEnrollmentInput::Pending,
                ModelAnswer::SlowDown => {
                    cadence += 5;
                    DeviceEnrollmentInput::SlowDown
                }
                ModelAnswer::Unavailable(retry_after) => DeviceEnrollmentInput::Unavailable {
                    retry_after: retry_after.map(Duration::from_secs),
                },
                ModelAnswer::Denied => {
                    final_answer = Some(EnrollmentEnd::Denied);
                    DeviceEnrollmentInput::Denied
                }
                ModelAnswer::Expired => {
                    final_answer = Some(EnrollmentEnd::Expired);
                    DeviceEnrollmentInput::Expired
                }
                ModelAnswer::Invalid => {
                    final_answer = Some(EnrollmentEnd::Invalid(InvalidReason::UnknownError));
                    DeviceEnrollmentInput::Invalid {
                        reason: InvalidReason::UnknownError,
                    }
                }
            };
            next = seconds
                + match answer {
                    ModelAnswer::Unavailable(Some(delay)) => delay,
                    _ => cadence,
                };
            state = state.step(elapsed(seconds), input);
        }
        let observed = match state {
            DeviceEnrollment::Ended { why } => Some(why),
            _ => None,
        };
        assert_eq!(observed, final_answer, "terminal result at {seconds}");
    }
}
