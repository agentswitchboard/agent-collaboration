//! The existing admin operations adapt the shared checked person proof.
use super::{
    admin_operations::{AdminOperation, AdminRoute},
    refusal::PersonRequestRefusal,
};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::{Clock, Ed25519Verification, Es256Verification, IssuerKeySource};
pub use fleet_signin::person_proof::PersonRequestPolicy;
use fleet_signin::{IssuerSignatureVerifier, person_proof::PersonProofVerifier};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// What arrived over HTTP, before anything is trusted.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PersonRequestInput {
    pub route: AdminRoute,
    pub method: String,
    pub authorization: String,
    pub proof: String,
    pub body: String,
}

/// The facts the applying transaction admits or refuses.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedPersonRequest {
    pub person_id: String,
    pub session_id: String,
    pub organization_id: OrganizationId,
    pub authorization_subject_id: String,
    pub person_revision: i64,
    pub organization_revision: i64,
    pub permission_policy_digest: String,
    pub permissions: Vec<String>,
    /// Seconds; absent when the session never passed a second factor.
    pub second_factor_at: Option<i64>,
    /// The fresh request ID (the proof's `jti`); only `organization_used_requests.id`.
    pub request_id: Uuid,
    pub request_time_ms: i64,
    pub proof_time_ms: i64,
    pub proof_key_thumbprint: String,
    pub intent_digest: String,
    /// min(exp, session deadline, snapshot deadline), milliseconds.
    pub effective_expiry_ms: i64,
    pub operation: AdminOperation,
}

pub struct PersonRequestVerifier<Ed, Es, Keys>(PersonProofVerifier<Ed, Es, Keys>);

impl<Ed: Ed25519Verification, Es: Es256Verification, Keys: IssuerKeySource>
    PersonRequestVerifier<Ed, Es, Keys>
{
    #[must_use]
    pub fn new(
        signatures: IssuerSignatureVerifier<Ed, Keys>,
        proofs: Es,
        clock: Arc<dyn Clock>,
        policy: PersonRequestPolicy,
    ) -> Self {
        Self(PersonProofVerifier::new(signatures, proofs, clock, policy))
    }

    pub async fn verify_person_link(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<fleet_signin::person_proof::CheckedPersonLink, PersonRequestRefusal> {
        self.0.verify_person_link(token, audience).await
    }

    pub async fn verify_approval(
        &self,
        input: fleet_signin::ApprovalPersonVerificationInput<'_>,
    ) -> Result<fleet_signin::VerifiedApprovalPersonRequest, PersonRequestRefusal> {
        input.verify_with(&self.0).await
    }

    pub async fn verify(
        &self,
        input: &PersonRequestInput,
    ) -> Result<VerifiedPersonRequest, PersonRequestRefusal> {
        let checked = self
            .0
            .verify(
                input.route.path(),
                &input.method,
                &input.authorization,
                &input.proof,
            )
            .await?;
        let person = checked.facts();
        let operation = AdminOperation::parse_for_person(
            input.route,
            input.body.as_bytes(),
            &person.person_id,
            person.second_factor_at,
        )?;
        let facts =
            checked.bind_intent(operation.organization_id(), &operation.intent_digest()?)?;
        Ok(VerifiedPersonRequest {
            person_id: facts.person_id,
            session_id: facts.session_id,
            organization_id: facts.organization_id,
            authorization_subject_id: facts.authorization_subject_id,
            person_revision: facts.person_revision,
            organization_revision: facts.organization_revision,
            permission_policy_digest: facts.permission_policy_digest,
            permissions: facts.permissions,
            second_factor_at: facts.second_factor_at,
            request_id: facts.request_id,
            request_time_ms: facts.request_time_ms,
            proof_time_ms: facts.proof_time_ms,
            proof_key_thumbprint: facts.proof_key_thumbprint,
            intent_digest: facts.intent_digest,
            effective_expiry_ms: facts.effective_expiry_ms,
            operation,
        })
    }
}
