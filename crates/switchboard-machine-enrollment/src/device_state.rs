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
    Revoked,
    Inactive,
}
