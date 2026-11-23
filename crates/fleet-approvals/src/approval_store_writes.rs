use approval_request_protocol::{CentralTime, HolderStopCause, Raise, RequestContentsDigest};
use central_engine_model::{Context, InstanceKey};
use central_storage::{GuardedStatement, RowExpectation, SafeInteger, Statement, StoreFailure};
use fleet_engine::{
    DueItemKey, DueKind, DueOwnerTiming, DueTableChange, checked_engine_statement,
    plan_due_table_change,
};
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};

use crate::{
    ApprovalRequestKey, ApprovalRouting, ApprovalSession, ApprovalState, EndedApproval,
    RetainedApproval,
    approval_store_scope::{ApprovalHolderScope, ApprovalRowId},
    approval_stored_tags::{
        delivery_state_tag, request_kind_tag, stop_cause_tag, waiting_status_tag,
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
    row_id: &ApprovalRowId,
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
            let parent = match &saved.routing {
                ApprovalRouting::Owner => None,
                ApprovalRouting::Delegate { admitted_parent } => Some(admitted_parent),
            };
            let destination_id = ApprovalRowId::mint(context.ids)?;
            Ok(vec![
                plan_first_save(row_id, scope, saved)?,
                plan_destination(
                    &destination_id,
                    row_id,
                    scope,
                    parent,
                    saved.window.raised_at(),
                ),
            ])
        }
        (ApprovalState::Waiting { saved }, ApprovalState::Waiting { .. }) => {
            let destination_id = ApprovalRowId::mint(context.ids)?;
            let changed_at = context.now.max(saved.window.raised_at());
            Ok(vec![
                plan_forward_waiting(row_id, scope, saved, changed_at)?,
                plan_destination(&destination_id, row_id, scope, None, changed_at),
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
    row_id: &ApprovalRowId,
    deadline: CentralTime,
    at: CentralTime,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    let timing = DueOwnerTiming::at(approval_due_key(row_id), deadline)?;
    plan_due_table_change(&DueTableChange::PublishItem(timing), at)
}

pub(crate) fn plan_remove_approval_deadline(
    row_id: &ApprovalRowId,
    at: CentralTime,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    plan_due_table_change(&DueTableChange::RemoveItem(approval_due_key(row_id)), at)
}

fn approval_due_key(row_id: &ApprovalRowId) -> DueItemKey {
    DueItemKey {
        kind: DueKind::ApprovalRequestExpiry,
        subject: InstanceKey(row_id.as_str().to_owned()),
    }
}

fn plan_first_save(
    row_id: &ApprovalRowId,
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
    row_id: &ApprovalRowId,
    scope: &ApprovalHolderScope,
    key: &ApprovalRequestKey,
    contents_digest: &RequestContentsDigest,
    cause: HolderStopCause,
    at: CentralTime,
) -> GuardedStatement {
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
        cause = stop_cause_tag(cause).to_owned(),
        timestamp = format_utc_millis(at),
    ))
}

fn plan_destination(
    destination_id: &ApprovalRowId,
    row_id: &ApprovalRowId,
    scope: &ApprovalHolderScope,
    parent: Option<&ApprovalSession>,
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
    ))
}

fn plan_first_ending(
    row_id: &ApprovalRowId,
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
        waiting_status = waiting_status_tag(saved.waiting_status).to_owned(),
        deadline = portable_integer(
            saved.window.deadline().timestamp_millis(),
            "approval_requests.deadline_ms"
        )?,
    )))
}

fn plan_forward_waiting(
    row_id: &ApprovalRowId,
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
    )))
}

fn plan_delivery_change(
    row_id: &ApprovalRowId,
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
