use std::str::FromStr;

use approval_request_protocol::{
    ApprovalWindow, DecidedBy, DeliverEndingRequest, Ending, InvocationDigest, OfferView,
    ParkedCallHolder, ProviderHolder, Raise, RequestContentsDigest, WaitingStatus,
};
use central_engine_model::supported_time;
use central_storage::{ReadOutcome, SqlValue, StoreFailure};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_runtime_ports::{stored_values::json, workspace_decode_error::corrupt};
use workspace_ids::RequestId;

use crate::{
    ApprovalActor, ApprovalEnrollment, ApprovalRequestKey, ApprovalRouting, ApprovalRun,
    ApprovalSession, ApprovalState, EndedApproval, RetainedApproval,
    approval_lifecycle_admission::is_destination,
    approval_store_scope::{ApprovalHolderScope, ApprovalRowId},
    approval_stored_tags::{
        parse_delivery_state, parse_stop_cause, parse_stored_time, parse_waiting_status,
        request_kind_tag,
    },
};

use super::{
    ApprovalReadRecord, ApprovalRunRecord, StoredApprovalRow, StoredDestinationRow, StoredRunRow,
};

pub(super) fn decode_run_row(stored: StoredRunRow) -> Result<ApprovalRunRecord, StoreFailure> {
    let scope = decode_holder_scope(
        stored.organization_id.stored_text()?,
        stored.machine_id.stored_text()?,
        stored.enrollment_id.stored_text()?,
        stored.router_run_id.stored_text()?,
    )?;
    let SqlValue::Integer(order) = stored.registration_order.raw() else {
        return Err(corrupt("approval_router_runs.registration_order"));
    };
    if order.get() < 1 {
        return Err(corrupt("approval_router_runs.registration_order"));
    }
    let registered_at = parse_stored_time(stored.registered_at.stored_text()?, "registered_at")?;
    let created_at = parse_stored_time(stored.created_at.stored_text()?, "created_at")?;
    let updated_at = parse_stored_time(stored.updated_at.stored_text()?, "updated_at")?;
    let retired_at = stored
        .retired_at
        .stored_optional_text()?
        .map(|value| parse_stored_time(value, "retired_at"))
        .transpose()?;
    if created_at != registered_at
        || updated_at != retired_at.unwrap_or(registered_at)
        || retired_at.is_some_and(|time| time < registered_at)
    {
        return Err(corrupt("approval_router_runs.chronology"));
    }
    Ok(ApprovalRunRecord {
        scope,
        registration_order: order.get(),
        retired_at,
    })
}

pub(super) fn decode_request_scope(
    stored: &StoredApprovalRow,
) -> Result<ApprovalHolderScope, StoreFailure> {
    decode_holder_scope(
        stored.organization_id.stored_text()?,
        stored.machine_id.stored_text()?,
        stored.enrollment_id.stored_text()?,
        stored.router_run_id.stored_text()?,
    )
}

fn decode_holder_scope(
    organization: &str,
    machine: &str,
    enrollment: &str,
    run: &str,
) -> Result<ApprovalHolderScope, StoreFailure> {
    Ok(ApprovalHolderScope {
        organization_id: OrganizationId::try_from(organization.to_owned())
            .map_err(|_| corrupt("organization_id"))?,
        run: ApprovalRun {
            router_run: parse_identifier(run, "router_run_id")?,
            enrollment: ApprovalEnrollment {
                machine_id: parse_identifier(machine, "machine_id")?,
                enrollment_id: EnrollmentId::try_from(enrollment.to_owned())
                    .map_err(|_| corrupt("enrollment_id"))?,
            },
        },
    })
}

pub(super) fn decode_request_row(
    row_id: ApprovalRowId,
    stored: StoredApprovalRow,
    destinations: &ReadOutcome,
) -> Result<ApprovalReadRecord, StoreFailure> {
    let scope = decode_request_scope(&stored)?;
    let key = ApprovalRequestKey {
        request_id: parse_identifier(
            stored.request_id.stored_text()?,
            "approval_requests.request_id",
        )?,
        run: scope.run.clone(),
    };
    let contents_digest =
        RequestContentsDigest::try_from(stored.contents_digest.stored_text()?.to_owned())
            .map_err(|_| corrupt("approval_requests.contents_digest"))?;
    let created_at = parse_stored_time(stored.created_at.stored_text()?, "created_at")?;
    let updated_at = parse_stored_time(stored.updated_at.stored_text()?, "updated_at")?;
    if updated_at < created_at {
        return Err(corrupt("approval_requests.chronology"));
    }

    let state = match stored.record_state.stored_text()? {
        "stopped_before_raise" => {
            require_null(
                &[
                    stored.request_kind.raw(),
                    stored.holder_session_id.raw(),
                    stored.offer_json.raw(),
                    stored.invocation_digest.raw(),
                    stored.waiting_status.raw(),
                    stored.first_saved_at.raw(),
                    stored.deadline_ms.raw(),
                ],
                "approval_requests.marker_fields",
            )?;
            require_unended(&stored)?;
            if !destinations.rows.is_empty() || updated_at != created_at {
                return Err(corrupt("approval_requests.marker_fields"));
            }
            let cause = parse_stop_cause(stored.stop_cause.stored_text()?)?;
            ApprovalState::StoppedBeforeRaise {
                key,
                contents_digest,
                cause,
            }
        }
        "waiting" | "ended" => {
            require_null(&[stored.stop_cause.raw()], "approval_requests.stop_cause")?;
            let saved = Box::new(decode_retained_approval(
                &stored,
                key,
                contents_digest,
                &row_id,
                &scope,
                destinations,
            )?);
            if created_at != saved.window.raised_at() {
                return Err(corrupt("approval_requests.first_saved_at"));
            }
            if stored.record_state.stored_text()? == "waiting" {
                require_unended(&stored)?;
                ApprovalState::Waiting { saved }
            } else {
                let closed = decode_ending(&stored, &saved)?;
                if updated_at < closed.ended_at {
                    return Err(corrupt("approval_requests.ending_chronology"));
                }
                ApprovalState::Ended { saved, closed }
            }
        }
        _ => return Err(corrupt("approval_requests.record_state")),
    };
    Ok(ApprovalReadRecord {
        row_id,
        scope,
        state,
    })
}

fn require_unended(stored: &StoredApprovalRow) -> Result<(), StoreFailure> {
    require_null(
        &[
            stored.ended_at.raw(),
            stored.ending_json.raw(),
            stored.decided_operation_id.raw(),
            stored.delivery_state.raw(),
        ],
        "approval_requests.unended_fields",
    )
}

fn decode_retained_approval(
    stored: &StoredApprovalRow,
    key: ApprovalRequestKey,
    contents_digest: RequestContentsDigest,
    row_id: &ApprovalRowId,
    scope: &ApprovalHolderScope,
    destinations: &ReadOutcome,
) -> Result<RetainedApproval, StoreFailure> {
    let offer: OfferView = json(
        stored.offer_json.stored_text()?,
        "approval_requests.offer_json",
    )?;
    let raise = match offer {
        OfferView::ProviderApproval { offer } => Raise::ProviderApproval {
            request_id: key.request_id.clone(),
            holder: decode_provider_holder(stored, &key.run)?,
            offer,
        },
        OfferView::ProviderQuestion { offer } => Raise::ProviderQuestion {
            request_id: key.request_id.clone(),
            holder: decode_provider_holder(stored, &key.run)?,
            offer,
        },
        OfferView::UnprovenCall { offer } => {
            require_null(
                &[stored.holder_session_id.raw()],
                "approval_requests.holder_session_id",
            )?;
            let invocation =
                InvocationDigest::try_from(stored.invocation_digest.stored_text()?.to_owned())
                    .map_err(|_| corrupt("approval_requests.invocation_digest"))?;
            Raise::UnprovenCall {
                request_id: key.request_id.clone(),
                holder: ParkedCallHolder {
                    router_run: key.run.router_run.clone(),
                },
                invocation,
                offer,
            }
        }
    };
    if stored.request_kind.stored_text()? != request_kind_tag(raise.kind()) {
        return Err(corrupt("approval_requests.request_kind"));
    }
    if raise
        .contents_digest()
        .map_err(|_| corrupt("approval_requests.contents_digest"))?
        != contents_digest
    {
        return Err(corrupt("approval_requests.contents_digest"));
    }
    let first_saved = parse_stored_time(stored.first_saved_at.stored_text()?, "first_saved_at")?;
    let window = ApprovalWindow::from_first_save(first_saved)
        .ok()
        .filter(|window| supported_time(first_saved) && supported_time(window.deadline()))
        .ok_or_else(|| corrupt("approval_requests.deadline_ms"))?;
    let SqlValue::Integer(deadline) = stored.deadline_ms.raw() else {
        return Err(corrupt("approval_requests.deadline_ms"));
    };
    if deadline.get() != window.deadline().timestamp_millis() {
        return Err(corrupt("approval_requests.deadline_ms"));
    }
    let waiting_status = parse_waiting_status(stored.waiting_status.stored_text()?)?;
    let routing = decode_route(destinations, row_id, scope, &raise, waiting_status, window)?;
    Ok(RetainedApproval {
        key,
        raise,
        contents_digest,
        window,
        routing,
        waiting_status,
    })
}

fn decode_provider_holder(
    stored: &StoredApprovalRow,
    run: &ApprovalRun,
) -> Result<ProviderHolder, StoreFailure> {
    require_null(
        &[stored.invocation_digest.raw()],
        "approval_requests.invocation_digest",
    )?;
    Ok(ProviderHolder {
        session_id: parse_identifier(
            stored.holder_session_id.stored_text()?,
            "approval_requests.holder_session_id",
        )?,
        router_run: run.router_run.clone(),
    })
}

fn decode_route(
    destinations: &ReadOutcome,
    row_id: &ApprovalRowId,
    scope: &ApprovalHolderScope,
    raise: &Raise,
    waiting_status: WaitingStatus,
    window: ApprovalWindow,
) -> Result<ApprovalRouting, StoreFailure> {
    let mut owner_present = false;
    let mut parent = None;
    for cells in &destinations.rows {
        let row = StoredDestinationRow::read_named(destinations, cells)?;
        let _: RequestId = parse_identifier(
            row.approval_destination_id.stored_text()?,
            "approval_destination_id",
        )?;
        let created_at = parse_stored_time(row.created_at.stored_text()?, "created_at")?;
        let updated_at = parse_stored_time(row.updated_at.stored_text()?, "updated_at")?;
        if row.approval_row_id.stored_text()? != row_id.as_str()
            || row.organization_id.stored_text()? != scope.organization_id.as_str()
            || created_at < window.raised_at()
            || updated_at < created_at
        {
            return Err(corrupt("approval_request_destinations.scope"));
        }
        match row.destination_kind.stored_text()? {
            "people" if !owner_present => {
                require_null(
                    &[
                        row.session_machine_id.raw(),
                        row.session_enrollment_id.raw(),
                        row.session_id.raw(),
                        row.raised_notice_at.raw(),
                    ],
                    "approval_request_destinations.people_fields",
                )?;
                owner_present = true;
            }
            "parent_session" if parent.is_none() => {
                if parse_stored_time(row.raised_notice_at.stored_text()?, "raised_notice_at")?
                    != window.raised_at()
                {
                    return Err(corrupt("approval_request_destinations.raised_notice_at"));
                }
                parent = Some(ApprovalSession {
                    enrollment: ApprovalEnrollment {
                        machine_id: parse_identifier(
                            row.session_machine_id.stored_text()?,
                            "session_machine_id",
                        )?,
                        enrollment_id: EnrollmentId::try_from(
                            row.session_enrollment_id.stored_text()?.to_owned(),
                        )
                        .map_err(|_| corrupt("session_enrollment_id"))?,
                    },
                    session_id: parse_identifier(row.session_id.stored_text()?, "session_id")?,
                });
            }
            _ => return Err(corrupt("approval_request_destinations.destination_kind")),
        }
    }
    match (owner_present, parent, waiting_status) {
        (true, None, WaitingStatus::Pending) => Ok(ApprovalRouting::Owner),
        (owner, Some(admitted_parent), status)
            if owner == (status == WaitingStatus::PassedOn)
                && raise
                    .requesting_session()
                    .is_some_and(|session| session != &admitted_parent.session_id) =>
        {
            Ok(ApprovalRouting::Delegate { admitted_parent })
        }
        _ => Err(corrupt("approval_request_destinations.membership")),
    }
}

fn decode_ending(
    stored: &StoredApprovalRow,
    saved: &RetainedApproval,
) -> Result<EndedApproval, StoreFailure> {
    let ending: DeliverEndingRequest = json(
        stored.ending_json.stored_text()?,
        "approval_requests.ending_json",
    )?;
    ending
        .check_bound_raise(&saved.raise)
        .map_err(|_| corrupt("approval_requests.ending_binding"))?;
    let ended_at = parse_stored_time(stored.ended_at.stored_text()?, "approval_requests.ended_at")?;
    let delivery = parse_delivery_state(stored.delivery_state.stored_text()?)?;
    let ended = ending.clone().into_ending();
    let decided_operation_id = stored
        .decided_operation_id
        .stored_optional_text()?
        .map(|operation| parse_identifier(operation, "approval_requests.decided_operation_id"))
        .transpose()?;
    let deciding_actor = match &ended {
        Ending::Decided { by, .. } => Some(match by {
            DecidedBy::Person { person_id } => ApprovalActor::Person(person_id.clone()),
            DecidedBy::Session { session } => match &saved.routing {
                ApprovalRouting::Delegate { admitted_parent }
                    if &admitted_parent.session_id == session =>
                {
                    ApprovalActor::Session(admitted_parent.clone())
                }
                _ => return Err(corrupt("approval_requests.decided_by")),
            },
        }),
        Ending::UnprovenCallDecided { person_id, .. } => {
            Some(ApprovalActor::Person(person_id.clone()))
        }
        Ending::Cancelled { .. } | Ending::TimedOut {} => None,
    };
    if deciding_actor.is_some() != decided_operation_id.is_some() {
        return Err(corrupt("approval_requests.decided_operation_id"));
    }
    if deciding_actor.is_some_and(|actor| !is_destination(saved, &actor)) {
        return Err(corrupt("approval_requests.decided_by"));
    }
    let follows_deadline = ended_at >= saved.window.deadline();
    if ended_at < saved.window.raised_at()
        || follows_deadline != matches!(ended, Ending::TimedOut {})
    {
        return Err(corrupt("approval_requests.ending_chronology"));
    }
    Ok(EndedApproval {
        ending,
        ended_at,
        delivery,
        decided_operation_id,
    })
}

fn parse_identifier<Identifier: FromStr>(
    value: &str,
    column: &str,
) -> Result<Identifier, StoreFailure> {
    value.parse().map_err(|_| corrupt(column))
}

fn require_null(values: &[&SqlValue], column: &str) -> Result<(), StoreFailure> {
    if values.iter().all(|value| matches!(value, SqlValue::Null)) {
        Ok(())
    } else {
        Err(corrupt(column))
    }
}
