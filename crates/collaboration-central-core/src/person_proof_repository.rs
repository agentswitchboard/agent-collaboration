//! The legacy organization admin admission plans proof consumption with its domain batch.
use crate::person_request::{VerifiedPersonRequest, request_admission::AdmittedRequest};
use central_storage::{GuardedStatement, StoreFailure};
use fleet_runtime_ports::CentralStore;
use fleet_signin::person_proof_store::{
    ProofReservation, proof_was_used, reserve_proof, used_proof_statement,
};

pub(super) enum ObservedProof<'request> {
    Unused(UnusedProof<'request>),
    Consumed,
}

/// The owning store had no marker for this verified request.
pub(super) struct UnusedProof<'request> {
    request: &'request VerifiedPersonRequest,
    proof_id: String,
}

/// The enclosing batch must commit for the proof to become consumed.
pub(super) struct PendingProofConsumption<'request> {
    request: &'request VerifiedPersonRequest,
    proof_id: String,
    admitted: AdmittedRequest,
}

pub(super) async fn observe_proof<'request>(
    host: &impl CentralStore,
    request: &'request VerifiedPersonRequest,
) -> Result<ObservedProof<'request>, StoreFailure> {
    let proof_id = request.request_id.to_string();
    let marker = host.read(used_proof_statement(&proof_id)).await?;
    Ok(if proof_was_used(&marker, &proof_id)? {
        ObservedProof::Consumed
    } else {
        ObservedProof::Unused(UnusedProof { request, proof_id })
    })
}

impl<'request> UnusedProof<'request> {
    pub(super) fn consume(self, admitted: AdmittedRequest) -> PendingProofConsumption<'request> {
        PendingProofConsumption {
            request: self.request,
            proof_id: self.proof_id,
            admitted,
        }
    }
}

impl PendingProofConsumption<'_> {
    pub(super) fn reservation_statement(self) -> Result<GuardedStatement, StoreFailure> {
        let request = self.request;
        reserve_proof(
            ProofReservation {
                proof_id: self.proof_id,
                organization: request.organization_id.as_str(),
                request_time_ms: request.request_time_ms,
                operation: request.operation.operation_id().map(|value| value.as_str()),
                person: &request.person_id,
                session: &request.session_id,
                intent_digest: &request.intent_digest,
                proof_key_thumbprint: &request.proof_key_thumbprint,
                effective_expiry_ms: request.effective_expiry_ms,
                proof_time_ms: request.proof_time_ms,
                second_factor_at: request.second_factor_at,
                require_fresh_factor: request.operation.route().changes_state(),
            },
            self.admitted.now_ms,
        )
    }
}
