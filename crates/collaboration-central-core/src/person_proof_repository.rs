//! Read and reserve a verified person proof in the owning organization store.
use crate::{
    authorization_floor_rows::{checked_proof_statement, integer},
    person_request::{VerifiedPersonRequest, request_admission::AdmittedRequest},
};
use central_storage::{GuardedStatement, ReadOutcome, SqlValue, Statement, StoreFailure};
use fleet_runtime_ports::{
    CentralStore,
    guarded_statements::{exact, text},
    stored_values::string,
    workspace_decode_error::corrupt,
};

use fleet_signin::person_proof::PersonProofFacts;

struct UsedProofRow {
    id: String,
}

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

pub(super) fn used_proof_statement(proof_id: &str) -> Statement {
    checked_proof_statement!(
        read UsedProofRow,
        "src/person_proof_queries/read_used_proof.sql",
        [text(proof_id)]
    )
}

pub(super) fn proof_was_used(marker: &ReadOutcome, proof_id: &str) -> Result<bool, StoreFailure> {
    match marker.rows.as_slice() {
        [] => Ok(false),
        [row] => {
            let stored = UsedProofRow {
                id: string(marker, row, "id")?.to_owned(),
            };
            if stored.id != proof_id {
                return Err(corrupt("admin_used_requests.id"));
            }
            Ok(true)
        }
        _ => Err(corrupt("admin_used_requests.id")),
    }
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

pub(super) fn reserve_approval_proof(
    proof: &PersonProofFacts,
    operation: Option<&workspace_ids::RequestId>,
    now_ms: i64,
) -> Result<GuardedStatement, StoreFailure> {
    reserve_proof(
        ProofReservation {
            proof_id: proof.request_id.to_string(),
            organization: proof.organization_id.as_str(),
            request_time_ms: proof.request_time_ms,
            operation: operation.map(workspace_ids::RequestId::as_str),
            person: &proof.person_id,
            session: &proof.session_id,
            intent_digest: &proof.intent_digest,
            proof_key_thumbprint: &proof.proof_key_thumbprint,
            effective_expiry_ms: proof.effective_expiry_ms,
            proof_time_ms: proof.proof_time_ms,
            second_factor_at: proof.second_factor_at,
            // C3 checks factor at the decision, including whether this answer needs it.
            require_fresh_factor: false,
        },
        now_ms,
    )
}

struct ProofReservation<'request> {
    proof_id: String,
    organization: &'request str,
    request_time_ms: i64,
    operation: Option<&'request str>,
    person: &'request str,
    session: &'request str,
    intent_digest: &'request str,
    proof_key_thumbprint: &'request str,
    effective_expiry_ms: i64,
    proof_time_ms: i64,
    second_factor_at: Option<i64>,
    require_fresh_factor: bool,
}

fn reserve_proof(
    request: ProofReservation<'_>,
    now_ms: i64,
) -> Result<GuardedStatement, StoreFailure> {
    Ok(checked_proof_statement!(
        write "src/person_proof_queries/reserve_admitted_proof.sql",
        [
            SqlValue::Text(request.proof_id),
            text(request.organization),
            integer(request.request_time_ms, "admin_used_requests.request_time_ms")?,
            request.operation.map_or(SqlValue::Null, text),
            text(request.person),
            text(request.session),
            text(request.intent_digest),
            text(request.proof_key_thumbprint),
            integer(now_ms, "admin_used_requests.created_at")?,
            integer(request.effective_expiry_ms, "person_request.effective_expiry_ms")?,
            integer(request.proof_time_ms, "person_request.proof_time_ms")?,
            request.second_factor_at
                .map(|at| integer(at, "person_request.second_factor_at"))
                .transpose()?
                .unwrap_or(SqlValue::Null),
            integer(i64::from(request.require_fresh_factor), "person_request.changes_state")?,
        ]
    ))
}
