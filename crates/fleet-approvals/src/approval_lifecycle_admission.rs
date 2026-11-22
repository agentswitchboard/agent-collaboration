use approval_request_protocol::{RequestContentsDigest, StopRequest};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    ApprovalActor, ApprovalDestination, ApprovalRequestKey, ApprovalSession, RetainedApproval,
};

pub(super) fn is_destination(saved: &RetainedApproval, actor: &ApprovalActor) -> bool {
    match actor {
        ApprovalActor::Person(_) => saved
            .routing
            .destinations
            .contains(&ApprovalDestination::People),
        ApprovalActor::Session(session) => {
            saved.provider_session().session_id != session.session_id
                && saved
                    .routing
                    .destinations
                    .contains(&ApprovalDestination::Session(session.clone()))
        }
    }
}

pub(super) fn is_assigned_parent(saved: &RetainedApproval, parent: &ApprovalSession) -> bool {
    saved.routing.delegated_parent.as_ref() == Some(parent)
        && is_destination(saved, &ApprovalActor::Session(parent.clone()))
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
        } => (request_id, router_run, contents_digest),
    }
}

pub(super) fn matches_key(
    key: &ApprovalRequestKey,
    request_id: &ApprovalRequestId,
    run: &RouterRunId,
) -> bool {
    &key.request_id == request_id && &key.run.router_run == run
}
