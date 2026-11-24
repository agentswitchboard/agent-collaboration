use approval_request_protocol::{Raise, RequestContentsDigest, StopRequest};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    ApprovalActor, ApprovalDestinations, ApprovalRequestKey, RetainedApproval, StopMarkerCause,
};

pub(super) fn is_destination(saved: &RetainedApproval, actor: &ApprovalActor) -> bool {
    match actor {
        ApprovalActor::Person(_) => {
            matches!(saved.destinations, ApprovalDestinations::People { .. })
        }
        ApprovalActor::Session(session) => saved
            .destinations
            .sessions()
            .any(|member| member == session),
    }
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

pub(super) fn stop_cause_matches_raise(cause: StopMarkerCause, raise: &Raise) -> bool {
    let parked = match raise {
        Raise::UnprovenCall { .. } => true,
        Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => false,
    };
    is_parked_stop_cause(cause) == parked
}

pub(super) fn stop_marker_matches_cause(
    stored: StopMarkerCause,
    incoming: StopMarkerCause,
) -> bool {
    is_parked_stop_cause(stored) == is_parked_stop_cause(incoming)
}

fn is_parked_stop_cause(cause: StopMarkerCause) -> bool {
    match cause {
        StopMarkerCause::UnprovenCall(_) => true,
        StopMarkerCause::Provider(_) => false,
    }
}

pub(super) fn matches_key(
    key: &ApprovalRequestKey,
    request_id: &ApprovalRequestId,
    run: &RouterRunId,
) -> bool {
    &key.request_id == request_id && &key.run.router_run == run
}
