//! Read-only person admission policy, before any proof reservation or domain write.
use super::{
    compact_jwt::MAX_SAFE_INTEGER,
    verifier::{PersonProofFacts, in_request_window},
};
use central_storage::StoreFailure;
use fleet_identity_protocol::UnixSeconds;
use serde::{Deserialize, Serialize};

/// Changes need a second factor passed within this many seconds (F2a's window).
pub use fleet_contracts::PERSON_SECOND_FACTOR_WINDOW_SECONDS as SECOND_FACTOR_WINDOW_SECONDS;

/// A fence revision: a positive JavaScript-safe integer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct FloorRevision(i64);
impl FloorRevision {
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}
impl TryFrom<i64> for FloorRevision {
    type Error = StoreFailure;
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if !(1..=MAX_SAFE_INTEGER).contains(&value) {
            return Err(corrupt("minimum_revision", "revision outside 1..=2^53-1"));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrganizationFloorState {
    Active,
    Stopped,
    Deleted,
}
impl OrganizationFloorState {
    pub fn parse(value: &str) -> Result<Self, StoreFailure> {
        match value {
            "active" => Ok(Self::Active),
            "stopped" => Ok(Self::Stopped),
            "deleted" => Ok(Self::Deleted),
            _ => Err(corrupt("state", "unknown organization floor state")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersonFloorState {
    Active,
    Inactive,
}
impl PersonFloorState {
    pub fn parse(value: &str) -> Result<Self, StoreFailure> {
        match value {
            "active" => Ok(Self::Active),
            "inactive" => Ok(Self::Inactive),
            _ => Err(corrupt("state", "unknown person floor state")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrganizationFloor {
    pub revision: FloorRevision,
    pub state: OrganizationFloorState,
    pub policy_digest: String,
    pub reject_through_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersonFloor {
    pub subject_id: String,
    pub revision: FloorRevision,
    pub state: PersonFloorState,
}

/// Why admission refused; each happens before any marker, ledger or domain write.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "camelCase")]
pub enum AdmissionRefusal {
    #[error("the person's organization role does not grant this operation")]
    NotPermitted,
    #[error("this change needs a fresh second factor")]
    SecondFactorRequired,
    #[error("the token predates the floor central holds; refresh it")]
    RefreshRequired,
    #[error("central has not yet received the authorization the token names")]
    AuthorizationSyncRequired,
    #[error("the token, session or snapshot deadline has passed")]
    Expired,
    #[error("the request's time is outside its window or at or below the watermark")]
    RequestTime,
    #[error("the request ID was already used")]
    RequestReused,
}

/// The admission sample; the reservation and effect must share one batch.
#[derive(Debug)]
pub struct AdmittedRequest {
    pub now_ms: i64,
}

/// Decide before constructing the atomic fresh-proof reservation.
pub fn decide_admission(
    request: &PersonAdmissionFacts<'_>,
    now_ms: i64,
    organization_floor: Option<&OrganizationFloor>,
    person_floor: Option<&PersonFloor>,
    permission: &str,
    require_fresh_factor: bool,
    request_used: bool,
) -> Result<Result<AdmittedRequest, AdmissionRefusal>, StoreFailure> {
    check_person_admission(
        request,
        now_ms,
        organization_floor,
        person_floor,
        Some(permission),
        require_fresh_factor,
        request_used,
    )
}

pub(crate) fn decide_retained_approval_admission(
    request: &PersonAdmissionFacts<'_>,
    now_ms: i64,
    organization_floor: Option<&OrganizationFloor>,
    person_floor: Option<&PersonFloor>,
    request_used: bool,
) -> Result<Result<AdmittedRequest, AdmissionRefusal>, StoreFailure> {
    check_person_admission(
        request,
        now_ms,
        organization_floor,
        person_floor,
        None,
        false,
        request_used,
    )
}

fn check_person_admission(
    request: &PersonAdmissionFacts<'_>,
    now_ms: i64,
    organization_floor: Option<&OrganizationFloor>,
    person_floor: Option<&PersonFloor>,
    permission: Option<&str>,
    require_fresh_factor: bool,
    request_used: bool,
) -> Result<Result<AdmittedRequest, AdmissionRefusal>, StoreFailure> {
    if let Err(refusal) = admission_before_person(request, now_ms, organization_floor)? {
        return Ok(Err(refusal));
    }
    let Some(person_floor) = person_floor else {
        return Ok(Err(AdmissionRefusal::AuthorizationSyncRequired));
    };
    if person_floor.subject_id != request.authorization_subject_id {
        return Ok(Err(AdmissionRefusal::NotPermitted));
    }
    let token_person_revision = FloorRevision::try_from(request.person_revision)
        .map_err(|_| corrupt("minimum_revision", "verified token revision out of range"))?;
    if token_person_revision > person_floor.revision {
        return Ok(Err(AdmissionRefusal::AuthorizationSyncRequired));
    }
    match person_floor.state {
        PersonFloorState::Inactive => return Ok(Err(AdmissionRefusal::NotPermitted)),
        PersonFloorState::Active if token_person_revision < person_floor.revision => {
            return Ok(Err(AdmissionRefusal::RefreshRequired));
        }
        PersonFloorState::Active => {}
    }
    if permission
        .is_some_and(|permission| !request.permissions.iter().any(|grant| grant == permission))
    {
        return Ok(Err(AdmissionRefusal::NotPermitted));
    }
    if require_fresh_factor && fresh_admin_second_factor(request.second_factor_at, now_ms).is_none()
    {
        return Ok(Err(AdmissionRefusal::SecondFactorRequired));
    }
    if request_used {
        return Ok(Err(AdmissionRefusal::RequestReused));
    }
    Ok(Ok(AdmittedRequest { now_ms }))
}

pub fn request_time_refusal(
    request: &PersonAdmissionFacts<'_>,
    now_ms: i64,
) -> Option<AdmissionRefusal> {
    if now_ms >= request.effective_expiry_ms {
        return Some(AdmissionRefusal::Expired);
    }
    if !in_request_window(request.request_time_ms, now_ms)
        || !in_request_window(request.proof_time_ms, now_ms)
    {
        return Some(AdmissionRefusal::RequestTime);
    }
    None
}

pub fn admission_before_person(
    request: &PersonAdmissionFacts<'_>,
    now_ms: i64,
    organization_floor: Option<&OrganizationFloor>,
) -> Result<Result<(), AdmissionRefusal>, StoreFailure> {
    if let Some(refusal) = request_time_refusal(request, now_ms) {
        return Ok(Err(refusal));
    }
    // A missing or corrupt floor never admits; a token never creates one.
    let Some(organization_floor) = organization_floor else {
        return Ok(Err(AdmissionRefusal::AuthorizationSyncRequired));
    };
    if request.request_time_ms <= organization_floor.reject_through_ms {
        return Ok(Err(AdmissionRefusal::RequestTime));
    }
    let token_organization_revision = FloorRevision::try_from(request.organization_revision)
        .map_err(|_| corrupt("minimum_revision", "verified token revision out of range"))?;
    match organization_floor.state {
        OrganizationFloorState::Deleted => return Ok(Err(AdmissionRefusal::NotPermitted)),
        OrganizationFloorState::Active | OrganizationFloorState::Stopped
            if token_organization_revision > organization_floor.revision =>
        {
            return Ok(Err(AdmissionRefusal::AuthorizationSyncRequired));
        }
        OrganizationFloorState::Stopped => return Ok(Err(AdmissionRefusal::NotPermitted)),
        OrganizationFloorState::Active
            if token_organization_revision < organization_floor.revision =>
        {
            return Ok(Err(AdmissionRefusal::RefreshRequired));
        }
        OrganizationFloorState::Active => {}
    }
    if request.permission_policy_digest != organization_floor.policy_digest {
        return Ok(Err(AdmissionRefusal::RefreshRequired));
    }
    Ok(Ok(()))
}

// Existing admin step-up check. Person approval commands never require it.
fn fresh_admin_second_factor(second_factor_at: Option<i64>, now_ms: i64) -> Option<UnixSeconds> {
    let at = second_factor_at.and_then(|seconds| UnixSeconds::try_from(seconds).ok())?;
    let age_ms =
        at.0.checked_mul(1000)
            .and_then(|factor_ms| now_ms.checked_sub(factor_ms))?;
    if (0..=SECOND_FACTOR_WINDOW_SECONDS * 1000).contains(&age_ms) {
        Some(at)
    } else {
        None
    }
}

fn corrupt(field: &str, _message: impl std::fmt::Display) -> StoreFailure {
    fleet_runtime_ports::workspace_decode_error::corrupt(&format!("authorization_floors.{field}"))
}

/// Read-only inputs to the organization applying checks. This shape grants no authority.
pub struct PersonAdmissionFacts<'request> {
    pub authorization_subject_id: &'request str,
    pub person_revision: i64,
    pub organization_revision: i64,
    pub permission_policy_digest: &'request str,
    pub permissions: &'request [String],
    pub second_factor_at: Option<i64>,
    pub request_time_ms: i64,
    pub proof_time_ms: i64,
    pub effective_expiry_ms: i64,
}

impl<'request> From<&'request PersonProofFacts> for PersonAdmissionFacts<'request> {
    fn from(proof: &'request PersonProofFacts) -> Self {
        Self {
            authorization_subject_id: &proof.authorization_subject_id,
            person_revision: proof.person_revision,
            organization_revision: proof.organization_revision,
            permission_policy_digest: &proof.permission_policy_digest,
            permissions: &proof.permissions,
            second_factor_at: proof.second_factor_at,
            request_time_ms: proof.request_time_ms,
            proof_time_ms: proof.proof_time_ms,
            effective_expiry_ms: proof.effective_expiry_ms,
        }
    }
}

#[cfg(test)]
#[path = "admission_test.rs"]
mod tests;
