//! Checked reads and reservation for Sign-in person proof markers.
use crate::authorization_floor_store::{checked_proof_statement, integer};
use central_storage::{GuardedStatement, ReadOutcome, SqlValue, Statement, StoreFailure};
use fleet_runtime_ports::{
    guarded_statements::{exact, text},
    stored_values::string,
    workspace_decode_error::corrupt,
};

struct UsedProofRow {
    id: String,
}

pub fn used_proof_statement(proof_id: &str) -> Statement {
    checked_proof_statement!(
        read UsedProofRow,
        "src/person_proof_queries/read_used_proof.sql",
        [text(proof_id)]
    )
}

pub fn proof_was_used(marker: &ReadOutcome, proof_id: &str) -> Result<bool, StoreFailure> {
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

pub struct ProofReservation<'request> {
    pub proof_id: String,
    pub organization: &'request str,
    pub request_time_ms: i64,
    pub operation: Option<&'request str>,
    pub person: &'request str,
    pub session: &'request str,
    pub intent_digest: &'request str,
    pub proof_key_thumbprint: &'request str,
    pub effective_expiry_ms: i64,
    pub proof_time_ms: i64,
    pub second_factor_at: Option<i64>,
    pub require_fresh_factor: bool,
}

pub fn reserve_proof(
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
