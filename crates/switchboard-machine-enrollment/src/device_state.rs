use std::time::Duration;

use router_clock::ClockInstant;

use crate::{DeviceAuthorization, InvalidReason};

/// An enrollment owner uses this state to track one sign-in attempt.
#[derive(Debug)]
pub enum DeviceEnrollment {
    NotEnrolled,
    Asking {
        authorization: Box<DeviceAuthorization>,
        interval: Duration,
        next_poll: ClockInstant,
        deadline: ClockInstant,
    },
    Enrolled,
    Ended {
        why: EnrollmentEnd,
    },
}

/// The caller's clock and the sign-in decoder supply inputs to one enrollment attempt.
#[derive(Debug)]
pub enum DeviceEnrollmentInput {
    Started(DeviceAuthorization),
    ClockReached,
    Pending,
    SlowDown,
    Unavailable { retry_after: Option<Duration> },
    Denied,
    Expired,
    Invalid { reason: InvalidReason },
    Committed,
}

/// The enrollment reducer or refresh owner reports one of these final reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnrollmentEnd {
    Denied,
    Expired,
    Invalid(InvalidReason),
    RefreshRefused,
    LocalCredentialLost,
    Revoked,
    Inactive,
}

/// The reducer tells the caller whether and when to poll sign-in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevicePollSchedule {
    Inactive,
    WaitUntil(ClockInstant),
    Due,
    DeadlineReached,
}

impl DeviceEnrollment {
    #[must_use]
    pub fn poll_schedule(&self, now: ClockInstant) -> DevicePollSchedule {
        let Self::Asking {
            deadline,
            next_poll,
            ..
        } = self
        else {
            return DevicePollSchedule::Inactive;
        };
        if now >= *deadline {
            DevicePollSchedule::DeadlineReached
        } else if now >= *next_poll {
            DevicePollSchedule::Due
        } else {
            DevicePollSchedule::WaitUntil((*next_poll).min(*deadline))
        }
    }

    #[must_use]
    pub fn step(self, now: ClockInstant, input: DeviceEnrollmentInput) -> Self {
        use DeviceEnrollmentInput as Input;

        match (self, input) {
            (Self::NotEnrolled, Input::Started(authorization)) => Self::Asking {
                interval: authorization.interval,
                next_poll: now.after(authorization.interval),
                deadline: now.after(authorization.expires_in),
                authorization: Box::new(authorization),
            },
            (Self::Asking { .. }, Input::Committed) => Self::Enrolled,
            (Self::Asking { deadline, .. }, _) if now >= deadline => Self::Ended {
                why: EnrollmentEnd::Expired,
            },
            (Self::Asking { .. }, Input::Denied) => Self::Ended {
                why: EnrollmentEnd::Denied,
            },
            (Self::Asking { .. }, Input::Expired) => Self::Ended {
                why: EnrollmentEnd::Expired,
            },
            (Self::Asking { .. }, Input::Invalid { reason }) => Self::Ended {
                why: EnrollmentEnd::Invalid(reason),
            },
            (
                Self::Asking {
                    authorization,
                    interval,
                    deadline,
                    ..
                },
                input @ (Input::Pending | Input::SlowDown | Input::Unavailable { .. }),
            ) => {
                let interval = if matches!(input, Input::SlowDown) {
                    interval.saturating_add(Duration::from_secs(5))
                } else {
                    interval
                };
                let delay = match input {
                    Input::Unavailable {
                        retry_after: Some(delay),
                    } => delay,
                    _ => interval,
                };
                Self::Asking {
                    authorization,
                    interval,
                    next_poll: now.after(delay),
                    deadline,
                }
            }
            (unchanged, _) => unchanged,
        }
    }
}

#[cfg(test)]
#[path = "device_state_test.rs"]
mod tests;
