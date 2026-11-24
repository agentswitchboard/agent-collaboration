use super::*;
use switchboard_machine_enrollment::{
    CredentialStoreError, EnrollmentEnd, LeaseConfigurationFault,
};

#[test]
fn lease_failures_project_the_complete_owner_contract() {
    let unavailable = |seconds| LeaseFailure::Unavailable {
        retry_after: Duration::from_secs(seconds),
    };
    for (owner, driver) in [
        (LeaseUnavailable::NotEnrolled, unavailable(1)),
        (LeaseUnavailable::DifferentEnrollment, unavailable(1)),
        (LeaseUnavailable::StaleAccessGrant, unavailable(1)),
        (
            LeaseUnavailable::Ended {
                why: EnrollmentEnd::Revoked,
            },
            LeaseFailure::EnrollmentEnded,
        ),
        (
            LeaseUnavailable::SignInUnreachable { retry_after: None },
            unavailable(30),
        ),
        (
            LeaseUnavailable::SignInUnreachable {
                retry_after: Some(Duration::from_secs(19)),
            },
            unavailable(19),
        ),
        (LeaseUnavailable::OversizedRefreshReply, LeaseFailure::Fault),
        (
            LeaseUnavailable::Configuration(LeaseConfigurationFault::BindingContext),
            LeaseFailure::Fault,
        ),
    ] {
        assert_eq!(lease_failure(owner), driver, "{owner:?}");
    }
    for error in [
        CredentialStoreError::Read,
        CredentialStoreError::Replace,
        CredentialStoreError::Remove,
        CredentialStoreError::CleanupTimedOut,
        CredentialStoreError::OutcomeNotKnown,
    ] {
        assert_eq!(
            lease_failure(LeaseUnavailable::Store(error)),
            unavailable(5),
            "{error:?}"
        );
    }
}
