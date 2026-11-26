#[cfg(feature = "test-support")]
use std::num::NonZeroU16;

use central_engine_model::EngineTime;
use central_storage::{ReadOutcome, SafeInteger, SqlValue, Statement, StoreFailure};
#[cfg(test)]
use fleet_engine::owner_sql::CheckedStorage;
use fleet_engine::{checked_engine_statement, checked_subject_projection};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_signin::approval_person_commands::ApprovalListFilter;
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    ApprovalSession, ApprovalState, MAX_OUTSTANDING_PER_RUN,
    approval_retention::{cutoff_at, expired_ending_bound},
    approval_store_scope::{
        ApprovalDestinationRowId, ApprovalEnrollmentScope, ApprovalHolderScope,
        ApprovalRequestRowId,
    },
    approval_store_transaction::ApprovalStoreTransaction,
    approval_stored_tags::parse_stored_time,
};

checked_subject_projection!(StoredRunRow {
    router_run_id: String,
    organization_id: String,
    machine_id: String,
    enrollment_id: String,
    registration_order: i64,
    registered_at: String,
    retired_at: Option<String>,
    enrollment_cleanup_ms: Option<i64>,
    enrollment_cleanup_due_ms: Option<i64>,
    marker_cleanup_due_ms: Option<i64>,
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
    erase_after_ms: Option<i64>,
    ending_json: Option<String>,
    compact_ending_json: Option<String>,
    decided_operation_id: Option<String>,
    delivery_state: Option<String>,
    created_at: String,
    updated_at: String,
});

checked_subject_projection!(StoredDestinationRow {
    approval_destination_id: String,
    approval_row_id: String,
    organization_id: String,
    request_id: String,
    destination_kind: String,
    session_machine_id: Option<String>,
    session_enrollment_id: Option<String>,
    session_id: Option<String>,
    raised_notice_at: Option<String>,
    position: i64,
    first_read_at: Option<String>,
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

use crate::approval_lookup::{
    ApprovalLookup, ApprovalVisibility, BeforeApprovalCutoff, PastApprovalCutoff,
    observe_owed_cutoff,
};
use row_decoders::{decode_request_row, decode_run_row};

pub(crate) struct ApprovalRunRecord {
    pub scope: ApprovalHolderScope,
    pub registration_order: i64,
    pub retired_at: Option<EngineTime>,
    pub enrollment_cleanup_at: Option<EngineTime>,
}

pub(crate) struct ApprovalReadRecord {
    pub row_id: ApprovalRequestRowId,
    pub scope: ApprovalHolderScope,
    pub state: ApprovalState,
    pub session_destinations: Vec<ApprovalSessionDestination>,
}

pub(crate) struct ApprovalSessionDestination {
    pub row_id: ApprovalDestinationRowId,
    pub session: ApprovalSession,
    pub first_read_at: Option<EngineTime>,
}

pub(crate) enum ScopedHolderRequest {
    Unseen,
    OtherHolder,
    Retained(Box<ApprovalReadRecord>),
    PastCutoff(PastApprovalCutoff),
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
        query_file,
        [],
        "src/queries/read_approval_request.sql",
        organization_id = organization.as_str().to_owned(),
        request_id = request_id.as_str().to_owned(),
    )
}

pub(crate) async fn read_holder_request(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalHolderScope,
    request_id: &ApprovalRequestId,
    at: EngineTime,
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
    let Some(lookup) = ApprovalLookup::for_holder(&outcome, cells, scope)? else {
        return Ok(ScopedHolderRequest::OtherHolder);
    };
    match lookup.observe(transaction, at).await? {
        ApprovalVisibility::BeforeCutoff(lookup) => Ok(ScopedHolderRequest::Retained(Box::new(
            load_admitted_request(transaction, lookup).await?,
        ))),
        ApprovalVisibility::PastCutoff(cutoff) => Ok(ScopedHolderRequest::PastCutoff(cutoff)),
    }
}

pub(crate) async fn read_people_approval_request(
    transaction: &mut impl ApprovalStoreTransaction,
    organization: &OrganizationId,
    request_id: &ApprovalRequestId,
    at: EngineTime,
) -> Result<Option<ApprovalReadRecord>, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file,
        [],
        "src/queries/read_people_approval_request.sql",
        organization_id = organization.as_str().to_owned(),
        request_id = request_id.as_str().to_owned(),
    );
    read_visible_approval(transaction, statement, at).await
}

pub(crate) async fn read_parent_approval_request(
    transaction: &mut impl ApprovalStoreTransaction,
    organization: &OrganizationId,
    request_id: &ApprovalRequestId,
    parent: &ApprovalSession,
    at: EngineTime,
) -> Result<Option<ApprovalReadRecord>, StoreFailure> {
    let statement = checked_engine_statement!(
        query_file,
        [],
        "src/queries/read_parent_approval_request.sql",
        organization_id = organization.as_str().to_owned(),
        request_id = request_id.as_str().to_owned(),
        parent_machine = parent.enrollment.machine_id.as_str().to_owned(),
        parent_enrollment = parent.enrollment.enrollment_id.as_str().to_owned(),
        parent_session = parent.session_id.as_str().to_owned(),
    );
    read_visible_approval(transaction, statement, at).await
}

pub(crate) async fn read_session_approval_page(
    transaction: &mut impl ApprovalStoreTransaction,
    organization: &OrganizationId,
    session: &ApprovalSession,
    cursor: Option<&ApprovalRequestId>,
    at: EngineTime,
    limit: usize,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    let limit_value = i64::try_from(limit)
        .map_err(|_| corrupt("approval_request_destinations.session_page_limit"))?;
    let statement = |expired: i64, count: i64| -> Result<Statement, StoreFailure> {
        Ok(checked_engine_statement!(
            query_file,
            [],
            "src/queries/read_session_approval_page.sql",
            organization = organization.as_str().to_owned(),
            machine = session.enrollment.machine_id.as_str().to_owned(),
            enrollment = session.enrollment.enrollment_id.as_str().to_owned(),
            session = session.session_id.as_str().to_owned(),
            cursor = cursor.map(|request| request.as_str().to_owned()),
            ending_bound = expired_ending_bound(at)?,
            expired = portable_integer(expired, "approval_requests.page_mode")?,
            limit = portable_integer(count, "approval_request_destinations.session_page_limit")?,
        ))
    };
    read_visible_page(
        transaction,
        statement(1, 1)?,
        statement(0, limit_value)?,
        at,
        limit,
    )
    .await
}

pub(crate) async fn read_indexed_approval_due_subject(
    transaction: &mut impl ApprovalStoreTransaction,
    row_id: &ApprovalRequestRowId,
) -> Result<Option<ApprovalReadRecord>, StoreFailure> {
    let Some(row) = read_approval_row(transaction, row_id).await? else {
        return Ok(None);
    };
    // The scheduling projection is checked when the due lane consumes it.
    // Visibility and pagination derive the cutoff directly from the ending.
    match row.record_state.stored_text()? {
        "ended" => {
            let ended_at =
                parse_stored_time(row.ended_at.stored_text()?, "approval_requests.ended_at")?;
            let cutoff = cutoff_at(ended_at)?.timestamp_millis();
            if !matches!(row.erase_after_ms.raw(), SqlValue::Integer(indexed) if indexed.get() == cutoff)
            {
                return Err(corrupt("approval_requests.erase_after_ms"));
            }
        }
        "waiting" | "stopped_before_raise"
            if !matches!(row.erase_after_ms.raw(), SqlValue::Null) =>
        {
            return Err(corrupt("approval_requests.erase_after_ms"));
        }
        _ => {}
    }
    load_request_record(transaction, row).await.map(Some)
}

pub(crate) async fn read_approval_by_row_id(
    transaction: &mut impl ApprovalStoreTransaction,
    row_id: &ApprovalRequestRowId,
) -> Result<Option<ApprovalReadRecord>, StoreFailure> {
    let Some(stored) = read_approval_row(transaction, row_id).await? else {
        return Ok(None);
    };
    load_request_record(transaction, stored).await.map(Some)
}

async fn read_approval_row(
    transaction: &mut impl ApprovalStoreTransaction,
    row_id: &ApprovalRequestRowId,
) -> Result<Option<StoredApprovalRow>, StoreFailure> {
    let organization = transaction.organization_id();
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_approval_due_subject.sql",
        row_id = row_id.as_str().to_owned(),
        organization_id = organization.as_str().to_owned(),
    );
    let result = transaction.read_approval_statement(statement).await?;
    only_row(&result)?
        .map(|cells| StoredApprovalRow::read_named(&result, cells))
        .transpose()
}

pub(crate) async fn count_outstanding_approvals(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalHolderScope,
    at: EngineTime,
) -> Result<usize, StoreFailure> {
    observe_owed_cutoff(transaction, scope, at).await?;
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedOutstandingApprovalCount],
        "src/queries/read_outstanding_approval_count.sql",
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        router_run_id = scope.run.router_run.as_str().to_owned(),
        ending_bound = expired_ending_bound(at)?,
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
    at: EngineTime,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    observe_owed_cutoff(transaction, scope, at).await?;
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_outstanding_approval_rows.sql",
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        router_run_id = scope.run.router_run.as_str().to_owned(),
        ending_bound = expired_ending_bound(at)?,
        limit = outstanding_limit()?,
    );
    read_request_records(transaction, statement, MAX_OUTSTANDING_PER_RUN).await
}

pub(crate) async fn read_recoverable_approval_rows(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalHolderScope,
    at: EngineTime,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    observe_owed_cutoff(transaction, scope, at).await?;
    let statement = checked_engine_statement!(
        query_file_as,
        [CheckedApprovalRow],
        "src/queries/read_recoverable_approval_rows.sql",
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        router_run_id = scope.run.router_run.as_str().to_owned(),
        at = portable_integer(at.timestamp_millis(), "approval_requests.deadline_ms")?,
        ending_bound = expired_ending_bound(at)?,
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
    at: EngineTime,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    let with_next = usize::from(limit.get()) + 1;
    let statement = |expired: i64, count: i64| -> Result<Statement, StoreFailure> {
        Ok(checked_engine_statement!(
            query_file,
            [],
            "src/queries/read_person_approval_page.sql",
            organization_id = organization.as_str().to_owned(),
            after = after.map_or("", ApprovalRequestRowId::as_str).to_owned(),
            ending_bound = expired_ending_bound(at)?,
            expired = portable_integer(expired, "approval_requests.page_mode")?,
            limit = portable_integer(count, "approval_requests.page_limit")?,
        ))
    };
    read_visible_page(
        transaction,
        statement(1, 1)?,
        statement(0, i64::from(limit.get()) + 1)?,
        at,
        with_next,
    )
    .await
}

pub(crate) async fn read_approval_request_page_for_person(
    transaction: &mut impl ApprovalStoreTransaction,
    organization: &OrganizationId,
    cursor: Option<&ApprovalRequestId>,
    filter: ApprovalListFilter,
    at: EngineTime,
    limit: u16,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    let scan_limit = usize::from(limit) + 1;
    let statement = |expired: i64, count: i64| -> Result<Statement, StoreFailure> {
        Ok(checked_engine_statement!(
            query_file,
            [],
            "src/queries/read_person_approval_request_page.sql",
            organization_id = organization.as_str().to_owned(),
            cursor = cursor.map_or("", ApprovalRequestId::as_str).to_owned(),
            include_ended = portable_integer(
                i64::from(matches!(filter, ApprovalListFilter::All)),
                "approval_requests.include_ended"
            )?,
            ending_bound = expired_ending_bound(at)?,
            expired = portable_integer(expired, "approval_requests.page_mode")?,
            limit = portable_integer(count, "approval_requests.page_limit")?,
        ))
    };
    read_visible_page(
        transaction,
        statement(1, 1)?,
        statement(0, i64::from(limit) + 1)?,
        at,
        scan_limit,
    )
    .await
}

async fn read_visible_approval(
    transaction: &mut impl ApprovalStoreTransaction,
    statement: Statement,
    at: EngineTime,
) -> Result<Option<ApprovalReadRecord>, StoreFailure> {
    let result = transaction.read_approval_statement(statement).await?;
    let Some(cells) = only_row(&result)? else {
        return Ok(None);
    };
    match ApprovalLookup::from_row(&result, cells)?
        .observe(transaction, at)
        .await?
    {
        ApprovalVisibility::BeforeCutoff(lookup) => {
            load_admitted_request(transaction, lookup).await.map(Some)
        }
        ApprovalVisibility::PastCutoff(_) => Ok(None),
    }
}

async fn read_visible_page(
    transaction: &mut impl ApprovalStoreTransaction,
    observe_expired: Statement,
    select_visible: Statement,
    at: EngineTime,
    limit: usize,
) -> Result<Vec<ApprovalReadRecord>, StoreFailure> {
    let expired = transaction.read_approval_statement(observe_expired).await?;
    if let Some(row) = only_row(&expired)?
        && matches!(
            ApprovalLookup::from_row(&expired, row)?
                .observe(transaction, at)
                .await?,
            ApprovalVisibility::BeforeCutoff(_)
        )
    {
        return Err(corrupt("approval_requests.expired_page"));
    }
    let result = transaction.read_approval_statement(select_visible).await?;
    if result.rows.len() > limit {
        return Err(corrupt("approval_requests.selection_limit"));
    }
    let mut records = Vec::with_capacity(result.rows.len());
    for cells in &result.rows {
        match ApprovalLookup::from_row(&result, cells)?
            .observe(transaction, at)
            .await?
        {
            ApprovalVisibility::BeforeCutoff(lookup) => {
                records.push(load_admitted_request(transaction, lookup).await?)
            }
            ApprovalVisibility::PastCutoff(_) => {
                return Err(corrupt("approval_requests.visible_page"));
            }
        }
    }
    Ok(records)
}
async fn load_admitted_request(
    transaction: &mut impl ApprovalStoreTransaction,
    approval: BeforeApprovalCutoff,
) -> Result<ApprovalReadRecord, StoreFailure> {
    let (row_id, scope, request_id) = approval.identity();
    let record = read_approval_by_row_id(transaction, &row_id)
        .await?
        .ok_or_else(|| corrupt("approval_requests.admitted_row"))?;
    if record.scope != scope
        || !record
            .state
            .key()
            .is_some_and(|key| key.request_id == request_id)
    {
        return Err(corrupt("approval_requests.admitted_row"));
    }
    Ok(record)
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
