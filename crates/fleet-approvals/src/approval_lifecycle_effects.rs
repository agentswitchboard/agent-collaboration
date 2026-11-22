use approval_request_protocol::{
    CentralTime, DeliverEndingRequest, Delivery, Ending, PrivateEnding, StopReceiptEnding,
};
use central_engine_model::TransitionError;

use crate::{
    ApprovalEffect, ApprovalReceipt, ApprovalRefusal, ApprovalState, EndedApproval,
    RetainedApproval, SessionNoticePhase,
};

pub(super) type ApprovalChange =
    Result<(ApprovalState, Vec<ApprovalEffect>), TransitionError<ApprovalRefusal>>;

pub(super) fn reply(state: &ApprovalState, receipt: ApprovalReceipt) -> ApprovalChange {
    Ok((state.clone(), vec![ApprovalEffect::Reply(receipt)]))
}

pub(super) fn notices(saved: &RetainedApproval, phase: SessionNoticePhase) -> Vec<ApprovalEffect> {
    saved
        .routing
        .delegated_parent
        .iter()
        .map(|session| ApprovalEffect::StoreSessionNotice {
            key: saved.key.clone(),
            session: session.clone(),
            phase,
        })
        .collect()
}

pub(super) fn finish(
    saved: &RetainedApproval,
    ending: Ending,
    delivery: Delivery,
    now: CentralTime,
) -> ApprovalChange {
    let ending = PrivateEnding::from_raise(&saved.raise, ending)
        .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidRetainedEnding))?;
    let closed = EndedApproval {
        ending,
        ended_at: now,
        delivery,
    };
    let mut effects = notices(saved, SessionNoticePhase::Ended);
    effects.push(ApprovalEffect::ClearDue {
        key: saved.key.clone(),
    });
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
        request: Box::new(ending_frame(saved, closed)),
    }
}

pub(super) fn ending_frame(
    saved: &RetainedApproval,
    closed: &EndedApproval,
) -> DeliverEndingRequest {
    DeliverEndingRequest {
        request_id: saved.key.request_id.clone(),
        contents_digest: saved.contents_digest.clone(),
        ending: closed.ending.clone(),
    }
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
        Ending::Decided { .. } => StopReceiptEnding::DecidedEarlier {},
        Ending::Cancelled { reason } => StopReceiptEnding::Cancelled { reason: *reason },
        Ending::TimedOut {} => StopReceiptEnding::TimedOut {},
    }
}
