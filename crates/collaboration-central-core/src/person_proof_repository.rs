//! Read and reserve a verified person proof in the owning organization store.
use crate::{
    authorization_floor_rows::{checked_proof_statement, integer},
    person_request::{VerifiedPersonRequest, request_admission::AdmittedRequest},
};
use central_storage::{GuardedStatement, SqlValue, Statement, StoreFailure};
use fleet_runtime_ports::{
    CentralStore,
    guarded_statements::{exact, text},
    stored_values::string,
    workspace_decode_error::corrupt,
};

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
    let marker = host
        .read(checked_proof_statement!(
            read UsedProofRow,
            "src/person_proof_queries/read_used_proof.sql",
            [text(&proof_id)]
        ))
        .await?;
    match marker.rows.as_slice() {
        [] => Ok(ObservedProof::Unused(UnusedProof { request, proof_id })),
        [row] => {
            let stored = UsedProofRow {
                id: string(&marker, row, "id")?.to_owned(),
            };
            if stored.id != proof_id {
                return Err(corrupt("admin_used_requests.id"));
            }
            Ok(ObservedProof::Consumed)
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
        let organization = request.organization_id.as_str();
        let operation = request.operation.operation_id().map(|value| value.as_str());
        let changes_state = i64::from(request.operation.route().changes_state());
        Ok(checked_proof_statement!(
            write "src/person_proof_queries/reserve_admitted_proof.sql",
            [
                SqlValue::Text(self.proof_id),
                text(organization),
                integer(
                    request.request_time_ms,
                    "admin_used_requests.request_time_ms",
                )?,
                operation.map_or(SqlValue::Null, text),
                text(&request.person_id),
                text(&request.session_id),
                text(&request.intent_digest),
                text(&request.proof_key_thumbprint),
                integer(self.admitted.now_ms, "admin_used_requests.created_at")?,
                integer(
                    request.effective_expiry_ms,
                    "person_request.effective_expiry_ms",
                )?,
                integer(request.proof_time_ms, "person_request.proof_time_ms")?,
                request
                    .second_factor_at
                    .map(|at| integer(at, "person_request.second_factor_at"))
                    .transpose()?
                    .unwrap_or(SqlValue::Null),
                integer(changes_state, "person_request.changes_state")?,
            ]
        ))
    }
}
