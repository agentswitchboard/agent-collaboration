//! The elapsed-time cutoff shared by approval reads and physical erasure.
use approval_request_protocol::{ApprovalRequestId, CentralTime, ErasedApprovalMarker};
use central_engine_model::{
    CENTRAL_RECORD_RETENTION_MILLIS, LAST_ALARM_GRID_UNIX_MILLIS, supported_time,
};
use central_storage::StoreFailure;
#[cfg(test)]
use fleet_engine::owner_sql::CheckedStorage;
use fleet_engine::{checked_engine_statement, checked_subject_projection};
use fleet_runtime_ports::{stored_values::json, workspace_decode_error::corrupt};

use crate::{ApprovalHolderScope, ApprovalStoreTransaction};

checked_subject_projection!(StoredErasedApproval {
    request_id: String,
    router_run_id: String,
    machine_id: String,
    enrollment_id: String,
    ending_json: String,
});

#[cfg(test)]
type CheckedErasedApproval = StoredErasedApproval<CheckedStorage>;

pub(crate) enum ErasedApprovalLookup {
    Missing,
    OtherHolder,
    Owned(ErasedApprovalMarker),
}

pub(crate) async fn read_erased_approval(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalHolderScope,
    request_id: &ApprovalRequestId,
) -> Result<ErasedApprovalLookup, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedErasedApproval],
        "src/queries/read_erased_approval.sql",
        organization = scope.organization_id.as_str().to_owned(),
        request = request_id.as_str().to_owned(),
    );
    let result = transaction.read_approval_statement(statement).await?;
    let cells = match result.rows.as_slice() {
        [] => return Ok(ErasedApprovalLookup::Missing),
        [cells] => cells,
        _ => return Err(corrupt("approval_erased_requests.request_id")),
    };
    let stored = StoredErasedApproval::read_named(&result, cells)?;
    let run = stored
        .router_run_id
        .stored_text()?
        .parse::<workspace_ids::RouterRunId>()
        .map_err(|_| corrupt("approval_erased_requests.router_run_id"))?;
    let stored_request = stored
        .request_id
        .stored_text()?
        .parse::<ApprovalRequestId>()
        .map_err(|_| corrupt("approval_erased_requests.request_id"))?;
    if stored_request != *request_id {
        return Err(corrupt("approval_erased_requests.request_id"));
    }
    if run != scope.run.router_run
        || stored.machine_id.stored_text()? != scope.run.enrollment.machine_id.as_str()
        || stored.enrollment_id.stored_text()? != scope.run.enrollment.enrollment_id.as_str()
    {
        return Ok(ErasedApprovalLookup::OtherHolder);
    }
    Ok(ErasedApprovalLookup::Owned(ErasedApprovalMarker {
        request_id: stored_request,
        router_run: run,
        ending: json(
            stored.ending_json.stored_text()?,
            "approval_erased_requests.ending_json",
        )?,
    }))
}

pub(crate) fn cutoff_at(ended_at: CentralTime) -> Result<CentralTime, StoreFailure> {
    let cutoff = ended_at
        .timestamp_millis()
        .saturating_add(CENTRAL_RECORD_RETENTION_MILLIS)
        .min(LAST_ALARM_GRID_UNIX_MILLIS);
    CentralTime::from_timestamp_millis(cutoff)
        .filter(|time| supported_time(*time))
        .ok_or_else(|| corrupt("approval_requests.erase_after_ms"))
}
