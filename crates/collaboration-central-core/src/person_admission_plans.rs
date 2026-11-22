//! Prepare fresh-proof reservation; the caller appends its effect to the same batch.
use crate::{
    authorization_floor_rows::{
        organization_guard, person_guard, read_organization_floor, read_person_floor,
    },
    person_proof_repository::{reserve_admitted_proof, used_proof_exists},
    person_request::{
        AdmissionRefusal, VerifiedPersonRequest,
        request_admission::{admission_before_person, decide_admission, request_time_refusal},
    },
};
use central_storage::{GuardedStatement, StoreFailure};
use fleet_runtime_ports::CentralStore;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_model::Timestamp;

pub(crate) struct PersonAdmissionPlan {
    /// Floor guards come first, the guarded proof reservation last. If any guard
    /// misses, re-read and re-admit; a concurrent proof reservation then refuses reuse.
    pub statements: Vec<GuardedStatement>,
}

/// `now` is the caller's sole applying sample, taken while it holds the org turn.
/// No request marker is written until the caller commits this plan with its effect.
pub(crate) async fn prepare_person_admission(
    host: &impl CentralStore,
    request: &VerifiedPersonRequest,
    now: Timestamp,
) -> Result<Result<PersonAdmissionPlan, AdmissionRefusal>, StoreFailure> {
    let now_ms = now.timestamp_millis();
    if let Some(refusal) = request_time_refusal(request, now_ms) {
        return Ok(Err(refusal));
    }
    let organization_id = request.organization_id.as_str();
    let organization = read_organization_floor(host, organization_id).await?;
    if let Err(refusal) = admission_before_person(request, now_ms, organization.as_ref())? {
        return Ok(Err(refusal));
    }
    let person = read_person_floor(host, organization_id, &request.person_id).await?;
    let admitted = match decide_admission(
        request,
        now_ms,
        organization.as_ref(),
        person.as_ref(),
        false,
    )? {
        Ok(admitted) => admitted,
        Err(refusal) => return Ok(Err(refusal)),
    };
    if used_proof_exists(host, request.request_id).await? {
        return Ok(Err(AdmissionRefusal::RequestReused));
    }
    let organization = organization.ok_or_else(|| corrupt("signin_authorization_floors.id"))?;
    let person = person.ok_or_else(|| corrupt("signin_person_authorization_floors.id"))?;
    let now_ms = admitted.now_ms;
    let statements = vec![
        organization_guard(organization_id, &organization)?,
        person_guard(organization_id, &request.person_id, &person)?,
        reserve_admitted_proof(request, now_ms)?,
    ];
    Ok(Ok(PersonAdmissionPlan { statements }))
}
