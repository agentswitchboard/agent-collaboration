use approval_request_protocol::{DeliverEndingReply, DeliverEndingRequest, Delivery};
use central_engine_model::TransitionError;

use crate::approval_lifecycle_effects::{ApprovalChange, offer_ending, with_delivery};
use crate::{ApprovalRequestKey, ApprovalState};

pub(super) fn receive(
    state: &ApprovalState,
    key: &ApprovalRequestKey,
    offered: &DeliverEndingRequest,
    reply: DeliverEndingReply,
) -> ApprovalChange {
    let ApprovalState::Ended { saved, closed } = state else {
        return Err(TransitionError::Stale);
    };
    if &saved.key != key
        || offered.request_id != key.request_id
        || offered.contents_digest != saved.contents_digest
        || offered.ending != closed.ending
    {
        return Err(TransitionError::Stale);
    }

    let delivery = reply.delivery_after_reply(closed.delivery);
    if delivery == closed.delivery {
        Ok((state.clone(), Vec::new()))
    } else {
        Ok((with_delivery(saved, closed, delivery), Vec::new()))
    }
}

pub(super) fn reoffer(state: &ApprovalState) -> ApprovalChange {
    match state {
        ApprovalState::Ended { saved, closed } if closed.delivery == (Delivery::Owed {}) => {
            Ok((state.clone(), vec![offer_ending(saved, closed)]))
        }
        ApprovalState::Unraised
        | ApprovalState::StoppedBeforeRaise { .. }
        | ApprovalState::Waiting { .. }
        | ApprovalState::Ended { .. } => Err(TransitionError::Stale),
    }
}
