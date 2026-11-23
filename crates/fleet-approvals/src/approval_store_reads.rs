#[cfg(feature = "test-support")]
use std::num::NonZeroU16;

use central_engine_model::EngineTime;
use central_storage::{ReadOutcome, SafeInteger, SqlValue, Statement, StoreFailure};
#[cfg(test)]
use fleet_engine::owner_sql::CheckedStorage;
use fleet_engine::{checked_engine_statement, checked_subject_projection};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    ApprovalSession, ApprovalState, MAX_OUTSTANDING_PER_RUN,
    approval_store_scope::{ApprovalEnrollmentScope, ApprovalHolderScope, ApprovalRequestRowId},
    approval_store_transaction::ApprovalStoreTransaction,
};

checked_subject_projection!(StoredRunRow {
    router_run_id: String,
    organization_id: String,
    machine_id: String,
    enrollment_id: String,
    registration_order: i64,
    registered_at: String,
    retired_at: Option<String>,
    created_at: String,
    updated_at: String,
});

checked_subject_projection!(StoredApprovalRow {
    approval_row_id: String,
    organization_id: String,
    machine_id: String,
    enrollment_id: String,
    router_run_id: String,
    request_id: String,
    contents_digest: String,
    record_state: String,
    request_kind: Option<String>,
    holder_session_id: Option<String>,
    offer_json: Option<String>,
    invocation_digest: Option<String>,
    waiting_status: Option<String>,
    first_saved_at: Option<String>,
    deadline_ms: Option<i64>,
    stop_family: Option<String>,
    stop_cause: Option<String>,
    ended_at: Option<String>,
    ending_json: Option<String>,
    decided_operation_id: Option<String>,
    delivery_state: Option<String>,
    created_at: String,
    updated_at: String,
});

checked_subject_projection!(StoredDestinationRow {
    approval_destination_id: String,
    approval_row_id: String,
    organization_id: String,
    destination_kind: String,
    session_machine_id: Option<String>,
    session_enrollment_id: Option<String>,
    session_id: Option<String>,
    raised_notice_at: Option<String>,
    created_at: String,
    updated_at: String,
});

checked_subject_projection!(OutstandingApprovalCount { outstanding: i64 });

#[cfg(test)]
type CheckedRunRow = StoredRunRow<CheckedStorage>;
#[cfg(test)]
type CheckedApprovalRow = StoredApprovalRow<CheckedStorage>;
#[cfg(test)]
type CheckedDestinationRow = StoredDestinationRow<CheckedStorage>;
#[cfg(test)]
type CheckedOutstandingApprovalCount = OutstandingApprovalCount<CheckedStorage>;

#[path = "approval_store_row_decoders.rs"]
mod row_decoders;

use row_decoders::{decode_request_row, decode_request_scope, decode_run_row};

pub(crate) struct ApprovalRunRecord {
    pub scope: ApprovalHolderScope,
    pub registration_order: i64,
    pub retired_at: Option<EngineTime>,
}

pub(crate) struct ApprovalReadRecord {
    pub row_id: ApprovalRequestRowId,
    pub scope: ApprovalHolderScope,
    pub state: ApprovalState,
}

pub(crate) enum ScopedHolderRequest {
    Unseen,
    OtherHolder,
    Retained(Box<ApprovalReadRecord>),
}

pub(crate) async fn read_approval_run(
    transaction: &mut impl ApprovalStoreTransaction,
    run: &RouterRunId,
) -> Result<Option<ApprovalRunRecord>, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedRunRow],
        "src/queries/read_approval_run.sql",
        run_id = run.as_str().to_owned(),
    );
    read_run_record(transaction, statement).await
}

pub(crate) async fn read_last_approval_run(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalEnrollmentScope,
) -> Result<Option<ApprovalRunRecord>, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedRunRow],
        "src/queries/read_last_approval_run.sql",
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.enrollment.enrollment_id.as_str().to_owned(),
    );
    read_run_record(transaction, statement).await
}

async fn read_run_record(
    transaction: &mut impl ApprovalStoreTransaction,
    statement: Statement,
) -> Result<Option<ApprovalRunRecord>, StoreFailure> {
    let outcome = transaction.read_approval_statement(statement).await?;
    only_row(&outcome)?
        .map(|row| decode_run_row(StoredRunRow::read_named(&outcome, row)?))
        .transpose()
}

fn approval_request_statement(
    organization: &OrganizationId,
    request_id: &ApprovalRequestId,
) -> Statement {
    checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_approval_request.sql",
        organization_id = organization.as_str().to_owned(),
        request_id = request_id.as_str().to_owned(),
    )
}

pub(crate) async fn read_holder_request(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalHolderScope,
    request_id: &ApprovalRequestId,
) -> Result<ScopedHolderRequest, StoreFailure> {
    let outcome = transaction
        .read_approval_statement(approval_request_statement(
            &scope.organization_id,
            request_id,
        ))
        .await?;
    let Some(cells) = only_row(&outcome)? else {
        return Ok(ScopedHolderRequest::Unseen);
    };
    let stored = StoredApprovalRow::read_named(&outcome, cells)?;
    if &decode_request_scope(&stored)? != scope {
        return Ok(ScopedHolderRequest::OtherHolder);
    }
    let record = load_request_record(transaction, stored).await?;
    Ok(ScopedHolderRequest::Retained(Box::new(record)))
}

pub(crate) async fn read_people_approval_request(
    transaction: &mut impl ApprovalStoreTransaction,
    organization: &OrganizationId,
    request_id: &ApprovalRequestId,
) -> Result<Option<ApprovalReadRecord>, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_people_approval_request.sql",
        organization_id = organization.as_str().to_owned(),
        request_id = request_id.as_str().to_owned(),
    );
    read_request_record(transaction, statement).await
}

pub(crate) async fn read_parent_approval_request(
    transaction: &mut impl ApprovalStoreTransaction,
    organization: &OrganizationId,
    request_id: &ApprovalRequestId,
    parent: &ApprovalSession,
) -> Result<Option<ApprovalReadRecord>, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_parent_approval_request.sql",
        organization_id = organization.as_str().to_owned(),
        request_id = request_id.as_str().to_owned(),
        parent_machine = parent.enrollment.machine_id.as_str().to_owned(),
        parent_enrollment = parent.enrollment.enrollment_id.as_str().to_owned(),
        parent_session = parent.session_id.as_str().to_owned(),
    );
    read_request_record(transaction, statement).await
}

pub(crate) async fn read_owed_approval_endings(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalHolderScope,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_owed_approval_endings.sql",
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        router_run_id = scope.run.router_run.as_str().to_owned(),
        limit = outstanding_limit()?,
    );
    read_request_records(transaction, statement, MAX_OUTSTANDING_PER_RUN).await
}

pub(crate) async fn read_approval_due_subject(
    transaction: &mut impl ApprovalStoreTransaction,
    row_id: &ApprovalRequestRowId,
) -> Result<Option<ApprovalReadRecord>, StoreFailure> {
    let organization = transaction.organization_id();
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_approval_due_subject.sql",
        row_id = row_id.as_str().to_owned(),
        organization_id = organization.as_str().to_owned(),
    );
    read_request_record(transaction, statement).await
}

pub(crate) async fn count_outstanding_approvals(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalHolderScope,
) -> Result<usize, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedOutstandingApprovalCount],
        "src/queries/read_outstanding_approval_count.sql",
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        router_run_id = scope.run.router_run.as_str().to_owned(),
        limit = outstanding_limit()?,
    );
    let outcome = transaction.read_approval_statement(statement).await?;
    let cells = only_row(&outcome)?.ok_or_else(|| corrupt("approval_requests.outstanding"))?;
    let projected = OutstandingApprovalCount::read_named(&outcome, cells)?;
    let SqlValue::Integer(count) = projected.outstanding.raw() else {
        return Err(corrupt("approval_requests.outstanding"));
    };
    usize::try_from(count.get())
        .ok()
        .filter(|count| *count <= MAX_OUTSTANDING_PER_RUN)
        .ok_or_else(|| corrupt("approval_requests.outstanding"))
}

pub(crate) async fn read_outstanding_approval_rows(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalHolderScope,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_outstanding_approval_rows.sql",
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        router_run_id = scope.run.router_run.as_str().to_owned(),
        limit = outstanding_limit()?,
    );
    read_request_records(transaction, statement, MAX_OUTSTANDING_PER_RUN).await
}

pub(crate) async fn read_waiting_approval_session_rows(
    transaction: &mut impl ApprovalStoreTransaction,
    organization: &OrganizationId,
    session: &ApprovalSession,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_waiting_approval_session_rows.sql",
        organization_id = organization.as_str().to_owned(),
        machine_id = session.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = session.enrollment.enrollment_id.as_str().to_owned(),
        holder_session_id = session.session_id.as_str().to_owned(),
        limit = outstanding_limit()?,
    );
    read_request_records(transaction, statement, MAX_OUTSTANDING_PER_RUN).await
}

#[cfg(feature = "test-support")]
pub(crate) async fn read_person_approval_page(
    transaction: &mut impl ApprovalStoreTransaction,
    organization: &OrganizationId,
    after: Option<&ApprovalRequestRowId>,
    limit: NonZeroU16,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    let with_next = usize::from(limit.get()) + 1;
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_person_approval_page.sql",
        organization_id = organization.as_str().to_owned(),
        after = after.map_or("", ApprovalRequestRowId::as_str).to_owned(),
        limit = portable_integer(i64::from(limit.get()) + 1, "approval_requests.page_limit")?,
    );
    read_request_records(transaction, statement, with_next).await
}

async fn read_request_record(
    transaction: &mut impl ApprovalStoreTransaction,
    statement: Statement,
) -> Result<Option<ApprovalReadRecord>, StoreFailure> {
    let outcome = transaction.read_approval_statement(statement).await?;
    let Some(cells) = only_row(&outcome)? else {
        return Ok(None);
    };
    let stored = StoredApprovalRow::read_named(&outcome, cells)?;
    load_request_record(transaction, stored).await.map(Some)
}

async fn read_request_records(
    transaction: &mut impl ApprovalStoreTransaction,
    statement: Statement,
    maximum: usize,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    let outcome = transaction.read_approval_statement(statement).await?;
    if outcome.rows.len() > maximum {
        return Err(corrupt("approval_requests.selection_limit"));
    }
    let mut records = Vec::with_capacity(outcome.rows.len());
    for cells in &outcome.rows {
        let stored = StoredApprovalRow::read_named(&outcome, cells)?;
        records.push(load_request_record(transaction, stored).await?);
    }
    Ok(records)
}

async fn load_request_record(
    transaction: &mut impl ApprovalStoreTransaction,
    stored: StoredApprovalRow,
) -> Result<ApprovalReadRecord, StoreFailure> {
    let row_id = ApprovalRequestRowId::parse(stored.approval_row_id.stored_text()?)?;
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedDestinationRow],
        "src/queries/read_approval_destinations.sql",
        row_id = row_id.as_str().to_owned(),
    );
    let destinations = transaction.read_approval_statement(statement).await?;
    decode_request_row(row_id, stored, &destinations)
}

fn only_row(outcome: &ReadOutcome) -> Result<Option<&[SqlValue]>, StoreFailure> {
    match outcome.rows.as_slice() {
        [] => Ok(None),
        [row] => Ok(Some(row)),
        _ => Err(corrupt("approval_store.unique_lookup")),
    }
}

fn outstanding_limit() -> Result<SafeInteger, StoreFailure> {
    let limit = i64::try_from(MAX_OUTSTANDING_PER_RUN)
        .map_err(|_| corrupt("approval_requests.outstanding_limit"))?;
    portable_integer(limit + 1, "approval_requests.outstanding_limit")
}

fn portable_integer(value: i64, field: &str) -> Result<SafeInteger, StoreFailure> {
    SafeInteger::new(value).map_err(|_| corrupt(field))
}

#[cfg(test)]
#[path = "approval_store_reads_test.rs"]
mod tests;
