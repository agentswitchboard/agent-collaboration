//! Apply the shared person admission checks to existing admin operations.
use super::request_verifier::VerifiedPersonRequest;
use central_storage::StoreFailure;
use fleet_signin::person_proof::{self, PersonAdmissionFacts};

pub use person_proof::{
    AdmissionRefusal, AdmittedRequest, FloorRevision, OrganizationFloor, OrganizationFloorState,
    PersonFloor, PersonFloorState, SECOND_FACTOR_WINDOW_SECONDS,
};

fn admission_facts(request: &VerifiedPersonRequest) -> PersonAdmissionFacts<'_> {
    PersonAdmissionFacts {
        authorization_subject_id: &request.authorization_subject_id,
        person_revision: request.person_revision,
        organization_revision: request.organization_revision,
        permission_policy_digest: &request.permission_policy_digest,
        permissions: &request.permissions,
        second_factor_at: request.second_factor_at,
        request_time_ms: request.request_time_ms,
        proof_time_ms: request.proof_time_ms,
        effective_expiry_ms: request.effective_expiry_ms,
    }
}

pub(crate) fn decide_admission(
    request: &VerifiedPersonRequest,
    now_ms: i64,
    organization_floor: Option<&OrganizationFloor>,
    person_floor: Option<&PersonFloor>,
    request_used: bool,
) -> Result<Result<AdmittedRequest, AdmissionRefusal>, StoreFailure> {
    let route = request.operation.route();
    person_proof::decide_admission(
        &admission_facts(request),
        now_ms,
        organization_floor,
        person_floor,
        route.permission(),
        route.changes_state(),
        request_used,
    )
}

pub(crate) fn request_time_refusal(
    request: &VerifiedPersonRequest,
    now_ms: i64,
) -> Option<AdmissionRefusal> {
    person_proof::request_time_refusal(&admission_facts(request), now_ms)
}

pub(crate) fn admission_before_person(
    request: &VerifiedPersonRequest,
    now_ms: i64,
    organization_floor: Option<&OrganizationFloor>,
) -> Result<Result<(), AdmissionRefusal>, StoreFailure> {
    person_proof::admission_before_person(&admission_facts(request), now_ms, organization_floor)
}

#[cfg(test)]
#[path = "request_admission_test.rs"]
mod tests;
