use approval_request_protocol::{
    CentralTime, HolderStopCause, Raise, RequestContentsDigest, StopRequest, WaitingStatus,
};
use central_engine_model::{TransitionError, supported_time};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    ApprovalActor, ApprovalRefusal, ApprovalRequestKey, ApprovalRouting, ApprovalSession,
    RetainedApproval,
};

pub(super) fn is_destination(saved: &RetainedApproval, actor: &ApprovalActor) -> bool {
    match actor {
        ApprovalActor::Person(_) => {
            saved.routing == ApprovalRouting::Owner
                || saved.waiting_status == WaitingStatus::PassedOn
        }
        ApprovalActor::Session(session) => is_assigned_parent(saved, session),
    }
}

pub(super) fn is_assigned_parent(saved: &RetainedApproval, parent: &ApprovalSession) -> bool {
    matches!(&saved.routing, ApprovalRouting::Delegate { admitted_parent } if admitted_parent == parent)
        && saved
            .raise
            .requesting_session()
            .is_some_and(|requester| requester != &parent.session_id)
}

pub(super) fn applying_time(
    sample: CentralTime,
    floor: CentralTime,
) -> Result<CentralTime, TransitionError<ApprovalRefusal>> {
    if !supported_time(sample) || !supported_time(floor) {
        return Err(TransitionError::Domain(
            ApprovalRefusal::ApplyingTimeOutOfRange,
        ));
    }
    Ok(sample.max(floor))
}

pub(crate) fn stop_binding(
    stop: &StopRequest,
) -> (&ApprovalRequestId, &RouterRunId, &RequestContentsDigest) {
    match stop {
        StopRequest::Provider {
            request_id,
            router_run,
            contents_digest,
            ..
        }
        | StopRequest::UnprovenCall {
            request_id,
            router_run,
            contents_digest,
            ..
        } => (request_id, router_run, contents_digest),
    }
}

pub(super) fn stop_cause_matches_raise(cause: HolderStopCause, raise: &Raise) -> bool {
    let parked = match raise {
        Raise::UnprovenCall { .. } => true,
        Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => false,
    };
    is_parked_stop_cause(cause) == parked
}

pub(super) fn stop_marker_matches_cause(
    stored: HolderStopCause,
    incoming: HolderStopCause,
) -> bool {
    is_parked_stop_cause(stored) == is_parked_stop_cause(incoming)
}

fn is_parked_stop_cause(cause: HolderStopCause) -> bool {
    match cause {
        HolderStopCause::CallWithdrawn => true,
        HolderStopCause::TurnStopped
        | HolderStopCause::AskReplaced
        | HolderStopCause::AskWithdrawn
        | HolderStopCause::SessionClosed
        | HolderStopCause::ProviderConnectionLost => false,
    }
}

pub(super) fn matches_key(
    key: &ApprovalRequestKey,
    request_id: &ApprovalRequestId,
    run: &RouterRunId,
) -> bool {
    &key.request_id == request_id && &key.run.router_run == run
}
