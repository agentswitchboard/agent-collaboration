use approval_request_protocol::{Answer, OfferView, Raise, Refusal, StopRequest, check_answer};
use central_engine_model::TransitionError;
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    AdmittedDecider, ApprovalDestination, ApprovalRefusal, FactorStanding, PassOnAuthority,
    RetainedApproval,
};

pub(super) fn check_destination(
    saved: &RetainedApproval,
    actor: &AdmittedDecider,
) -> Result<(), TransitionError<ApprovalRefusal>> {
    if !actor.is_destination(&saved.destinations) {
        return Err(destination_refusal(Refusal::NotVisible {}));
    }
    if let AdmittedDecider::Session(session) = actor
        && saved.requester() == Some(session)
    {
        return Err(destination_refusal(Refusal::RequesterCannotAnswer {}));
    }
    Ok(())
}

pub(super) fn validate_answer(
    saved: &RetainedApproval,
    actor: &AdmittedDecider,
    answer: &Answer,
) -> Result<(), TransitionError<ApprovalRefusal>> {
    let offer = saved.raise.offer_view();
    check_answer(&offer, answer)
        .map_err(|reason| destination_refusal(Refusal::InvalidAnswer { reason }))?;
    let needs_factor = match (&offer, answer) {
        (OfferView::ProviderApproval { offer }, Answer::Approval { option_id }) => offer
            .options
            .as_slice()
            .iter()
            .any(|option| &option.id == option_id && option.needs_person_with_fresh_factor()),
        (OfferView::UnprovenCall { .. }, _) => true,
        (OfferView::ProviderApproval { .. } | OfferView::ProviderQuestion { .. }, _) => false,
    };
    if needs_factor {
        match actor {
            AdmittedDecider::Person {
                factor: FactorStanding::Fresh,
                ..
            } => {}
            AdmittedDecider::Person {
                factor: FactorStanding::AbsentOrStale,
                ..
            } => {
                return Err(destination_refusal(Refusal::SecondFactorRequired {}));
            }
            AdmittedDecider::Session(_) => {
                return Err(destination_refusal(Refusal::KeptChoiceNeedsAPerson {}));
            }
        }
    }
    Ok(())
}

pub(super) fn check_parent(
    saved: &RetainedApproval,
    authority: PassOnAuthority,
) -> Result<workspace_ids::SessionRef, TransitionError<ApprovalRefusal>> {
    let PassOnAuthority::LiveParent(parent) = authority else {
        return Err(destination_refusal(Refusal::NotVisible {}));
    };
    if saved
        .requester()
        .is_some_and(|requester| requester != &parent)
        && saved
            .destinations
            .contains(&ApprovalDestination::Session(parent.clone()))
    {
        Ok(parent)
    } else {
        Err(destination_refusal(Refusal::NotVisible {}))
    }
}

pub(super) fn destination_refusal(refusal: Refusal) -> TransitionError<ApprovalRefusal> {
    TransitionError::Domain(ApprovalRefusal::Destination { refusal })
}

pub(super) fn check_scope(
    id: &ApprovalRequestId,
    run: &RouterRunId,
    event_id: &ApprovalRequestId,
    event_run: &RouterRunId,
) -> Result<(), TransitionError<ApprovalRefusal>> {
    if id == event_id && run == event_run {
        Ok(())
    } else {
        Err(TransitionError::Stale)
    }
}

pub(super) fn check_stop(
    saved: &RetainedApproval,
    stop: &StopRequest,
) -> Result<(), TransitionError<ApprovalRefusal>> {
    let (id, run) = stop_scope(stop);
    check_scope(&saved.receipt.request_id, saved.holder_run(), id, run)?;
    match (&saved.raise, stop) {
        (
            Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. },
            StopRequest::Provider { .. },
        )
        | (Raise::UnprovenCall { .. }, StopRequest::UnprovenCall { .. }) => Ok(()),
        (
            Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. },
            StopRequest::UnprovenCall { .. },
        )
        | (Raise::UnprovenCall { .. }, StopRequest::Provider { .. }) => Err(TransitionError::Stale),
    }
}

pub(super) fn stop_scope(stop: &StopRequest) -> (&ApprovalRequestId, &RouterRunId) {
    match stop {
        StopRequest::Provider {
            request_id,
            router_run,
            ..
        }
        | StopRequest::UnprovenCall {
            request_id,
            router_run,
            ..
        } => (request_id, router_run),
    }
}

pub(super) fn raise_run(raise: &Raise) -> &RouterRunId {
    match raise {
        Raise::ProviderApproval { holder, .. } | Raise::ProviderQuestion { holder, .. } => {
            &holder.router_run
        }
        Raise::UnprovenCall { holder, .. } => &holder.router_run,
    }
}
