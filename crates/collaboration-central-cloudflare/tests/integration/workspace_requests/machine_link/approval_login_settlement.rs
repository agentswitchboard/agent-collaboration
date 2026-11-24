//! Existing sign-in fixture entrances paired with the approval socket journey.
use super::super::private_enrollment_invalidation::{
    approve_replacement, request_token, set_last_grant_at_idle_boundary,
};
use super::{FIXTURE_DECISION_OPERATION, Fixture};
use super::{MACHINE_ID, signed};
use collaboration_central_conformance::{
    ConformanceResult as Result, approval_link_scenarios::EnrollmentLoginEnd,
};
use fleet_contracts::{MachineTokenDecision, MachineTokenRefusal};

pub(super) async fn settle_enrollment(fixture: &Fixture, event: EnrollmentLoginEnd) -> Result<()> {
    let link = fixture;
    let original = &link.enrollment;
    match event {
        EnrollmentLoginEnd::Revoke => revoke_enrollment(fixture).await,
        EnrollmentLoginEnd::Supersede => {
            let successor = approve_replacement(link).await?;
            match request_token(link, &successor).await? {
                MachineTokenDecision::Grant {
                    superseded: Some(ended),
                    ..
                } if &ended.enrollment_id == original => Ok(()),
                other => Err(format!(
                    "replacement grant did not supersede the original: {other:?}"
                )
                .into()),
            }
        }
        EnrollmentLoginEnd::Idle => {
            set_last_grant_at_idle_boundary(link).await?;
            match request_token(link, original).await? {
                MachineTokenDecision::Refuse {
                    reason: MachineTokenRefusal::IdleExpired {},
                } => Ok(()),
                other => Err(format!("grant did not idle-expire the original: {other:?}").into()),
            }
        }
    }
}

async fn revoke_enrollment(fixture: &Fixture) -> Result<()> {
    let removed: std::result::Result<
        Option<fleet_contracts::EndedMachineClient>,
        central_storage::StoreFailure,
    > = fixture
        .runtime()
        .control(
            "machine-login-revoke",
            &collaboration_central_core::MachineLoginControl::Revoke {
                person_id: signed::OWNER.to_owned().try_into()?,
                operation_id: FIXTURE_DECISION_OPERATION.to_owned().try_into()?,
                machine_id: MACHINE_ID.parse()?,
                enrollment_id: fixture.enrollment.clone(),
            },
        )
        .await?;
    if removed?.is_none() {
        return Err("enrollment was not revoked".into());
    }
    Ok(())
}
