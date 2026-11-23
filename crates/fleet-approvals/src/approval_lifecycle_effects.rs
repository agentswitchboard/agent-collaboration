use approval_request_protocol::{
    AcceptedApprovalDecision, CentralTime, DeliverEndingRequest, Delivery, Ending,
    StopReceiptEnding,
};
use central_engine_model::TransitionError;

use crate::{
    ApprovalEffect, ApprovalReceipt, ApprovalRefusal, ApprovalRouting, ApprovalState,
    EndedApproval, RetainedApproval,
};

pub(super) type ApprovalChange =
    Result<(ApprovalState, Vec<ApprovalEffect>), TransitionError<ApprovalRefusal>>;

pub(super) fn reply(state: &ApprovalState, receipt: ApprovalReceipt) -> ApprovalChange {
    Ok((state.clone(), vec![ApprovalEffect::Reply(receipt)]))
}

pub(super) fn parent_notice(saved: &RetainedApproval) -> Option<ApprovalEffect> {
    match &saved.routing {
        ApprovalRouting::Owner => None,
        ApprovalRouting::Delegate { admitted_parent } => Some(ApprovalEffect::StoreSessionNotice {
            key: saved.key.clone(),
            session: admitted_parent.clone(),
        }),
    }
}

pub(super) fn finish(
    saved: &RetainedApproval,
    ending: Ending,
    delivery: Delivery,
    now: CentralTime,
) -> ApprovalChange {
    let ending = DeliverEndingRequest::from_raise(&saved.raise, ending)
        .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidRetainedEnding))?;
    finish_with_frame(saved, ending, delivery, now)
}

pub(super) fn finish_decided(
    saved: &RetainedApproval,
    decision: AcceptedApprovalDecision,
    now: CentralTime,
) -> ApprovalChange {
    let ending = DeliverEndingRequest::from_decision(&saved.raise, decision)
        .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidRetainedEnding))?;
    finish_with_frame(saved, ending, Delivery::Owed {}, now)
}

fn finish_with_frame(
    saved: &RetainedApproval,
    ending: DeliverEndingRequest,
    delivery: Delivery,
    now: CentralTime,
) -> ApprovalChange {
    let closed = EndedApproval {
        ending,
        // A rollback before first save cannot put the ending before its own request.
        ended_at: now.max(saved.window.raised_at()),
        delivery,
    };
    let mut effects = vec![ApprovalEffect::ClearDue {
        key: saved.key.clone(),
    }];
    if delivery == (Delivery::Owed {}) {
        effects.push(offer_ending(saved, &closed));
    }
    Ok((
        ApprovalState::Ended {
            saved: Box::new(saved.clone()),
            closed,
        },
        effects,
    ))
}

pub(super) fn offer_ending(saved: &RetainedApproval, closed: &EndedApproval) -> ApprovalEffect {
    ApprovalEffect::OfferEnding {
        holder: saved.key.run.clone(),
        request: Box::new(ending_frame(closed)),
    }
}

pub(super) fn ending_frame(closed: &EndedApproval) -> DeliverEndingRequest {
    closed.ending.clone()
}

pub(super) fn with_delivery(
    saved: &RetainedApproval,
    closed: &EndedApproval,
    delivery: Delivery,
) -> ApprovalState {
    ApprovalState::Ended {
        saved: Box::new(saved.clone()),
        closed: EndedApproval {
            delivery,
            ..closed.clone()
        },
    }
}

pub(super) fn standing(ending: &Ending) -> StopReceiptEnding {
    match ending {
        Ending::Decided { .. } | Ending::UnprovenCallDecided { .. } => {
            StopReceiptEnding::DecidedEarlier {}
        }
        Ending::Cancelled { reason } => StopReceiptEnding::Cancelled { reason: *reason },
        Ending::TimedOut {} => StopReceiptEnding::TimedOut {},
    }
}
