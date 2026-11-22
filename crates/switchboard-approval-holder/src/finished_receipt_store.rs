//! The bounded replay record of claims that reached Closed in one run.

use std::collections::VecDeque;

use approval_request_protocol::{DeliverEndingReply, DeliverEndingRequest};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::ApprovalRequestId;

/// The last completed claims of one run, in close order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FinishedReceiptState {
    receipts: VecDeque<DeliverEndingRequest>,
}

/// Inputs the run owner sends after a wait closes or after accepting an offer's authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinishedReceiptEvent {
    /// The wait has transitioned to Closed and yielded the claim it retained.
    RetainClosedClaim(DeliverEndingRequest),
    LookupOffer(DeliverEndingRequest),
}

/// The finished-receipt answer, before the caller considers active waits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinishedReceiptLookup {
    Unretained,
    Reply(DeliverEndingReply),
}

/// Results of consulting or updating the run's receipt record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinishedReceiptEffect {
    Lookup { request_id: ApprovalRequestId, result: FinishedReceiptLookup },
    EndingConflict { request_id: ApprovalRequestId },
    Evicted { request_id: ApprovalRequestId },
}

/// A second purported closure tried to replace a different retained claim for the same request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinishedReceiptRefusal {
    ConflictingClosure,
}

/// Retains closed claims without having access to provider writer bindings.
pub struct FinishedReceiptStore;

const RECEIPT_LIMIT: usize = 256;

impl FinishedReceiptState {
    fn find(&self, request_id: &ApprovalRequestId) -> Option<&DeliverEndingRequest> {
        self.receipts.iter().find(|receipt| &receipt.request_id == request_id)
    }
}

impl StateTag for FinishedReceiptState {
    fn state_tag(&self) -> &'static str {
        if self.receipts.is_empty() { "empty" } else { "retaining" }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

impl EventTag for FinishedReceiptEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::RetainClosedClaim(_) => "retain_closed_claim",
            Self::LookupOffer(_) => "lookup_offer",
        }
    }
}

impl RefusalTag for FinishedReceiptRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::ConflictingClosure => "conflicting_closure",
        }
    }
}

impl StateMachine for FinishedReceiptStore {
    type State = FinishedReceiptState;
    type Event = FinishedReceiptEvent;
    type Effect = FinishedReceiptEffect;
    type Error = FinishedReceiptRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        let mut next = state.clone();
        let mut effects = Vec::new();
        match event {
            FinishedReceiptEvent::RetainClosedClaim(claim) => {
                if let Some(incumbent) = state.find(&claim.request_id) {
                    if incumbent != &claim {
                        return Err(TransitionError::Domain(FinishedReceiptRefusal::ConflictingClosure));
                    }
                } else {
                    next.receipts.push_back(claim);
                    if next.receipts.len() > RECEIPT_LIMIT
                        && let Some(pruned) = next.receipts.pop_front()
                    {
                        effects.push(FinishedReceiptEffect::Evicted { request_id: pruned.request_id });
                    }
                }
            }
            FinishedReceiptEvent::LookupOffer(offer) => {
                let result = match state.find(&offer.request_id) {
                    Some(claim) if claim == &offer => FinishedReceiptLookup::Reply(DeliverEndingReply::Received {}),
                    Some(_) => {
                        effects.push(FinishedReceiptEffect::EndingConflict { request_id: offer.request_id.clone() });
                        FinishedReceiptLookup::Reply(DeliverEndingReply::EndingDiffers {})
                    }
                    None => FinishedReceiptLookup::Unretained,
                };
                effects.push(FinishedReceiptEffect::Lookup { request_id: offer.request_id, result });
            }
        }
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        &RECEIPT_TRANSITIONS
    }
}

const RECEIPT_TRANSITIONS: [Transition; 4] = [
    Transition { from: "empty", event: "retain_closed_claim", to: "retaining", terminal: false },
    Transition { from: "retaining", event: "retain_closed_claim", to: "retaining", terminal: false },
    Transition { from: "empty", event: "lookup_offer", to: "empty", terminal: false },
    Transition { from: "retaining", event: "lookup_offer", to: "retaining", terminal: false },
];

#[cfg(test)]
#[path = "finished_receipt_store_test.rs"]
mod tests;
