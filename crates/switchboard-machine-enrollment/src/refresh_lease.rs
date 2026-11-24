//! Caller-facing access lease and renewal errors.

use std::time::Duration;

use router_clock::ClockInstant;

use crate::{
    BindingGeneration, CredentialStoreError, EnrollmentEnd, EnrollmentKey, InvalidReason,
    SecretValue,
};

/// The refresh owner issues this memory-only lease after the new binding commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccessLease {
    pub token: SecretValue,
    pub usable_until: ClockInstant,
    pub binding_generation: BindingGeneration,
    pub key: EnrollmentKey,
}

/// A link caller requests any usable lease or one newer than a rejected lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessRequest {
    Usable,
    NewerThan(BindingGeneration),
}

impl AccessRequest {
    pub(super) fn accepts(self, generation: BindingGeneration) -> bool {
        match self {
            Self::Usable => true,
            Self::NewerThan(rejected) => generation > rejected,
        }
    }
}

/// The refresh owner reports why a caller did not receive a lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LeaseUnavailable {
    #[error("this Mac is not enrolled")]
    NotEnrolled,
    #[error("the requested enrollment is no longer current")]
    DifferentEnrollment,
    #[error("enrollment ended, enroll this Mac again")]
    Ended { why: EnrollmentEnd },
    #[error("sign-in returned access grants that expired before delivery")]
    StaleAccessGrant,
    #[error("sign-in's refresh response exceeded the size limit")]
    OversizedRefreshReply,
    #[error("sign-in is temporarily unreachable")]
    SignInUnreachable { retry_after: Option<Duration> },
    #[error(transparent)]
    Store(#[from] CredentialStoreError),
    #[error(transparent)]
    Configuration(#[from] LeaseConfigurationFault),
}

/// The owner rejects incompatible binding and sign-in configuration as a distinct fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LeaseConfigurationFault {
    #[error(
        "the saved enrollment belongs to a different machine, organization or sign-in deployment"
    )]
    BindingContext,
    #[error("the saved enrollment generation cannot be advanced")]
    GenerationExhausted,
    #[error("the requested generation is ahead of the saved enrollment")]
    FutureGeneration,
    #[error("sign-in returned an invalid refresh answer")]
    InvalidAnswer(InvalidReason),
}
