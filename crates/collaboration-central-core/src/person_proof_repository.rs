//! Read and reserve a verified person proof in the owning organization store.
use crate::{authorization_floor_rows::integer, person_request::VerifiedPersonRequest};
use central_storage::{GuardedStatement, SqlValue, Statement, StoreFailure};
use fleet_runtime_ports::{
    CentralStore,
    guarded_statements::{exact, text},
    stored_values::string,
    workspace_decode_error::corrupt,
};
use uuid::Uuid;

struct UsedProofRow {
    id: String,
}

pub(super) async fn used_proof_exists(
    host: &impl CentralStore,
    proof_id: Uuid,
) -> Result<bool, StoreFailure> {
    let proof_id = proof_id.to_string();
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(
        UsedProofRow,
        "src/person_proof_queries/read_used_proof.sql",
        &proof_id,
    );
    let marker = host
        .read(Statement {
            sql: include_str!("person_proof_queries/read_used_proof.sql"),
            args: vec![text(&proof_id)],
        })
        .await?;
    match marker.rows.as_slice() {
        [] => Ok(false),
        [row] => {
            let stored = UsedProofRow {
                id: string(&marker, row, "id")?.to_owned(),
            };
            if stored.id != proof_id {
                return Err(corrupt("admin_used_requests.id"));
            }
            Ok(true)
        }
        _ => Err(corrupt("admin_used_requests.id")),
    }
}

pub(super) fn reserve_admitted_proof(
    request: &VerifiedPersonRequest,
    now_ms: i64,
) -> Result<GuardedStatement, StoreFailure> {
    let proof_id = request.request_id.to_string();
    let organization = request.organization_id.as_str();
    let operation = request.operation.operation_id().map(|value| value.as_str());
    let changes_state = i64::from(request.operation.route().changes_state());
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file!(
        "src/person_proof_queries/reserve_admitted_proof.sql",
        &proof_id,
        organization,
        request.request_time_ms,
        operation,
        &request.person_id,
        &request.session_id,
        &request.intent_digest,
        &request.proof_key_thumbprint,
        now_ms,
        request.effective_expiry_ms,
        request.proof_time_ms,
        request.second_factor_at,
        changes_state,
    );
    Ok(exact(
        include_str!("person_proof_queries/reserve_admitted_proof.sql"),
        vec![
            text(proof_id),
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
            integer(now_ms, "admin_used_requests.created_at")?,
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
        ],
    ))
}
