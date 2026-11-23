use approval_request_protocol::{CentralTime, Raise, RequestContentsDigest};
use central_engine_model::{Context, InstanceKey};
use central_storage::{GuardedStatement, RowExpectation, SafeInteger, Statement, StoreFailure};
use fleet_engine::{
    DueItemKey, DueKind, DueOwnerTiming, DueTableChange, checked_engine_statement,
    plan_due_table_change,
};
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};

use crate::{
    ApprovalRequestKey, ApprovalSession, ApprovalState, EndedApproval, RetainedApproval,
    StopMarkerCause,
    approval_store_scope::{ApprovalDestinationRowId, ApprovalHolderScope, ApprovalRequestRowId},
    approval_stored_tags::{
        delivery_state_tag, request_kind_tag, stop_marker_tags, waiting_status_tag,
    },
};

pub(crate) fn plan_register_approval_run(
    scope: &ApprovalHolderScope,
    registration_order: i64,
    at: CentralTime,
) -> Result<GuardedStatement, StoreFailure> {
    Ok(expect_one(checked_engine_statement!(
        query_file,
        [],
        "src/queries/register_approval_run.sql",
        run_id = scope.run.router_run.as_str().to_owned(),
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        order = portable_integer(
            registration_order,
            "approval_router_runs.registration_order"
        )?,
        timestamp = format_utc_millis(at),
    )))
}

pub(crate) fn plan_retire_approval_run(
    scope: &ApprovalHolderScope,
    at: CentralTime,
) -> GuardedStatement {
    expect_one(checked_engine_statement!(
        query_file,
        [],
        "src/queries/retire_approval_run.sql",
        timestamp = format_utc_millis(at),
        run_id = scope.run.router_run.as_str().to_owned(),
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
    ))
}

pub(crate) fn plan_approval_state_change(
    row_id: &ApprovalRequestRowId,
    scope: &ApprovalHolderScope,
    prior: &ApprovalState,
    next: &ApprovalState,
    context: &mut Context<'_>,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    if prior == next {
        return Ok(Vec::new());
    }
    match (prior, next) {
        (
            ApprovalState::Unraised,
            ApprovalState::StoppedBeforeRaise {
                key,
                contents_digest,
                cause,
            },
        ) => Ok(vec![plan_stop_marker(
            row_id,
            scope,
            key,
            contents_digest,
            *cause,
            context.now,
        )]),
        (ApprovalState::Unraised, ApprovalState::Waiting { saved }) => {
            let destination_id = ApprovalDestinationRowId::mint(context.ids)?;
            Ok(vec![
                plan_first_save(row_id, scope, saved)?,
                plan_destination(
                    &destination_id,
                    row_id,
                    scope,
                    saved.destinations.current_session(),
                    portable_integer(0, "approval_request_destinations.position")?,
                    saved.window.raised_at(),
                ),
            ])
        }
        (ApprovalState::Waiting { saved }, ApprovalState::Waiting { saved: next }) => {
            let destination_id = ApprovalDestinationRowId::mint(context.ids)?;
            let changed_at = context.now.max(saved.window.raised_at());
            Ok(vec![
                plan_forward_waiting(row_id, scope, saved, changed_at)?,
                plan_destination(
                    &destination_id,
                    row_id,
                    scope,
                    next.destinations.current_session(),
                    portable_integer(
                        i64::try_from(next.destinations.previous().len())
                            .map_err(|_| corrupt("approval_request_destinations.position"))?,
                        "approval_request_destinations.position",
                    )?,
                    changed_at,
                ),
            ])
        }
        (ApprovalState::Waiting { saved }, ApprovalState::Ended { closed, .. }) => {
            Ok(vec![plan_first_ending(row_id, scope, saved, closed)?])
        }
        (
            ApprovalState::Ended {
                saved,
                closed: before,
            },
            ApprovalState::Ended { closed: after, .. },
        ) => Ok(vec![plan_delivery_change(
            row_id,
            scope,
            saved,
            before,
            after,
            context.now,
        )?]),
        _ => Err(corrupt("approval_requests.transition")),
    }
}

pub(crate) fn plan_register_approval_deadline(
    row_id: &ApprovalRequestRowId,
    deadline: CentralTime,
    at: CentralTime,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    let mark = if central_engine_model::on_five_minute_grid(deadline) {
        deadline
    } else {
        central_engine_model::next_five_minute_mark(deadline)
            .map_err(|_| corrupt("approval_requests.deadline_mark"))?
    };
    let mut timing = DueOwnerTiming::at(approval_due_key(row_id), deadline)?;
    timing.base_due = fleet_engine::DueMillis::try_from(mark.timestamp_millis())
        .map_err(|_| corrupt("approval_requests.deadline_mark"))?;
    plan_due_table_change(&DueTableChange::PublishItem(timing), at)
}

pub(crate) fn plan_remove_approval_deadline(
    row_id: &ApprovalRequestRowId,
    at: CentralTime,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    plan_due_table_change(&DueTableChange::RemoveItem(approval_due_key(row_id)), at)
}

fn approval_due_key(row_id: &ApprovalRequestRowId) -> DueItemKey {
    DueItemKey {
        kind: DueKind::ApprovalRequestExpiry,
        subject: InstanceKey(row_id.as_str().to_owned()),
    }
}

fn plan_first_save(
    row_id: &ApprovalRequestRowId,
    scope: &ApprovalHolderScope,
    saved: &RetainedApproval,
) -> Result<GuardedStatement, StoreFailure> {
    let invocation = match &saved.raise {
        Raise::UnprovenCall { invocation, .. } => Some(invocation.as_str().to_owned()),
        Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => None,
    };
    Ok(expect_one(checked_engine_statement!(
        query_file,
        [],
        "src/queries/insert_retained_approval.sql",
        row_id = row_id.as_str().to_owned(),
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        run_id = scope.run.router_run.as_str().to_owned(),
        request_id = saved.key.request_id.as_str().to_owned(),
        digest = saved.contents_digest.as_str().to_owned(),
        request_kind = request_kind_tag(saved.raise.kind()).to_owned(),
        holder_session_id = saved
            .raise
            .requesting_session()
            .map(|id| id.as_str().to_owned()),
        offer = encode_stored_json(&saved.raise.offer_view(), "approval_requests.offer_json")?,
        invocation_digest = invocation,
        first_saved_at = format_utc_millis(saved.window.raised_at()),
        deadline = portable_integer(
            saved.window.deadline().timestamp_millis(),
            "approval_requests.deadline_ms"
        )?,
    )))
}

fn plan_stop_marker(
    row_id: &ApprovalRequestRowId,
    scope: &ApprovalHolderScope,
    key: &ApprovalRequestKey,
    contents_digest: &RequestContentsDigest,
    stop_marker: StopMarkerCause,
    at: CentralTime,
) -> GuardedStatement {
    let (stop_family, stop_cause) = stop_marker_tags(stop_marker);
    expect_one(checked_engine_statement!(
        query_file,
        [],
        "src/queries/insert_stopped_before_raise.sql",
        row_id = row_id.as_str().to_owned(),
        organization_id = scope.organization_id.as_str().to_owned(),
        machine_id = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment_id = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        run_id = scope.run.router_run.as_str().to_owned(),
        request_id = key.request_id.as_str().to_owned(),
        digest = contents_digest.as_str().to_owned(),
        family = stop_family.to_owned(),
        cause = stop_cause.to_owned(),
        timestamp = format_utc_millis(at),
    ))
}

fn plan_destination(
    destination_id: &ApprovalDestinationRowId,
    row_id: &ApprovalRequestRowId,
    scope: &ApprovalHolderScope,
    parent: Option<&ApprovalSession>,
    position: SafeInteger,
    at: CentralTime,
) -> GuardedStatement {
    let destination_kind = if parent.is_some() {
        "parent_session"
    } else {
        "people"
    };
    let timestamp = format_utc_millis(at);
    expect_one(checked_engine_statement!(
        query_file,
        [],
        "src/queries/insert_approval_destination.sql",
        destination_id = destination_id.as_str().to_owned(),
        row_id = row_id.as_str().to_owned(),
        organization_id = scope.organization_id.as_str().to_owned(),
        destination_kind = destination_kind.to_owned(),
        machine_id = parent.map(|session| session.enrollment.machine_id.as_str().to_owned()),
        enrollment_id = parent.map(|session| session.enrollment.enrollment_id.as_str().to_owned()),
        session_id = parent.map(|session| session.session_id.as_str().to_owned()),
        raised_notice_at = parent.map(|_| timestamp.clone()),
        timestamp = timestamp,
        position = position,
    ))
}

fn plan_first_ending(
    row_id: &ApprovalRequestRowId,
    scope: &ApprovalHolderScope,
    saved: &RetainedApproval,
    closed: &EndedApproval,
) -> Result<GuardedStatement, StoreFailure> {
    Ok(expect_one(checked_engine_statement!(
        query_file,
        [],
        "src/queries/finish_waiting_approval.sql",
        ended_at = format_utc_millis(closed.ended_at),
        ending = encode_stored_json(&closed.ending, "approval_requests.ending_json")?,
        operation = closed
            .decided_operation_id
            .as_ref()
            .map(|operation| operation.as_str().to_owned()),
        delivery = delivery_state_tag(closed.delivery).to_owned(),
        row_id = row_id.as_str().to_owned(),
        organization_id = scope.organization_id.as_str().to_owned(),
        contents_digest = saved.contents_digest.as_str().to_owned(),
        waiting_status = waiting_status_tag(saved.waiting_status()).to_owned(),
        deadline = portable_integer(
            saved.window.deadline().timestamp_millis(),
            "approval_requests.deadline_ms"
        )?,
    )))
}

fn plan_forward_waiting(
    row_id: &ApprovalRequestRowId,
    scope: &ApprovalHolderScope,
    saved: &RetainedApproval,
    at: CentralTime,
) -> Result<GuardedStatement, StoreFailure> {
    Ok(expect_one(checked_engine_statement!(
        query_file,
        [],
        "src/queries/forward_waiting_approval.sql",
        timestamp = format_utc_millis(at),
        row_id = row_id.as_str().to_owned(),
        organization_id = scope.organization_id.as_str().to_owned(),
        contents_digest = saved.contents_digest.as_str().to_owned(),
        deadline = portable_integer(
            saved.window.deadline().timestamp_millis(),
            "approval_requests.deadline_ms"
        )?,
        previous_status = waiting_status_tag(saved.waiting_status()).to_owned(),
    )))
}

fn plan_delivery_change(
    row_id: &ApprovalRequestRowId,
    scope: &ApprovalHolderScope,
    saved: &RetainedApproval,
    before: &EndedApproval,
    after: &EndedApproval,
    at: CentralTime,
) -> Result<GuardedStatement, StoreFailure> {
    Ok(expect_one(checked_engine_statement!(
        query_file,
        [],
        "src/queries/update_approval_delivery.sql",
        delivery = delivery_state_tag(after.delivery).to_owned(),
        timestamp = format_utc_millis(at),
        row_id = row_id.as_str().to_owned(),
        organization_id = scope.organization_id.as_str().to_owned(),
        contents_digest = saved.contents_digest.as_str().to_owned(),
        ending = encode_stored_json(&before.ending, "approval_requests.ending_json")?,
        previous_delivery = delivery_state_tag(before.delivery).to_owned(),
    )))
}

fn expect_one(statement: Statement) -> GuardedStatement {
    GuardedStatement {
        statement,
        expect: RowExpectation::Exactly(1),
    }
}

fn encode_stored_json(value: &impl serde::Serialize, column: &str) -> Result<String, StoreFailure> {
    serde_json::to_string(value).map_err(|_| corrupt(column))
}

fn portable_integer(value: i64, column: &str) -> Result<SafeInteger, StoreFailure> {
    SafeInteger::new(value).map_err(|_| corrupt(column))
}
