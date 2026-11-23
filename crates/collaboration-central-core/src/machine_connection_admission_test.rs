use super::{
    EnrollmentInvalidation, MachineLinkCloseCode, MachineTokenRenewalRefusal, RenewalTermination,
    project_renewal_refusal,
};
use serde_json::json;

#[test]
fn an_expired_presented_token_leaves_the_connection_open() {
    let (reason, termination) =
        project_renewal_refusal(MachineTokenRenewalRefusal::PresentedTokenExpired);

    assert_eq!(
        serde_json::to_value(reason).unwrap(),
        json!("presentedTokenExpired")
    );
    assert!(termination.is_none());
}

#[test]
fn a_connection_enrollment_ending_reports_the_reason_and_its_final_close() {
    for cause in [
        MachineLinkCloseCode::Revoked,
        MachineLinkCloseCode::Inactive,
    ] {
        let (reason, termination) =
            project_renewal_refusal(MachineTokenRenewalRefusal::CurrentAuthorityEnded(cause));

        assert_eq!(
            serde_json::to_value(reason).unwrap(),
            json!("connectionEnrollmentEnded")
        );
        assert!(matches!(
            termination,
            Some(RenewalTermination::CurrentAuthority(observed)) if observed == cause
        ));
    }
}

#[test]
fn a_committed_idle_ending_keeps_its_enrollment_invalidation() {
    let ended = EnrollmentInvalidation {
        machine_id: "019f0000-0000-7000-8000-00000000b001".parse().unwrap(),
        enrollment_id: "019f0000-0000-7000-8000-00000000b002"
            .to_owned()
            .try_into()
            .unwrap(),
        cause: MachineLinkCloseCode::Inactive,
    };
    let (reason, termination) =
        project_renewal_refusal(MachineTokenRenewalRefusal::IdleEnded(ended.clone()));

    assert_eq!(
        serde_json::to_value(reason).unwrap(),
        json!("connectionEnrollmentEnded")
    );
    assert!(matches!(
        termination,
        Some(RenewalTermination::EndEnrollment(observed)) if observed == ended
    ));
}
