//! A destination sees only the offer and ending for the request's own kind.
use crate::approval_wire_codec::approval_wire_union;
use serde::{Deserialize, Serialize};
use workspace_ids::ApprovalRequestId;

use crate::{
    Answer, ApprovalOffer, ApprovalWindow, CallCancellation, CallOffer, CentralTime, DecidedBy,
    Delivery, Ending, HolderEnding, InvalidApprovalEnding, OfferEnding, OfferView, OfferedOptionId,
    ProviderCancellation, QuestionAnswer, QuestionOffer, Raise, WaitingStatus, check_answer,
};

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DestinationEnding<AcceptedAnswer, Cancellation> {
    Decided {
        by: DecidedBy,
    },
    DecidedWithAnswer {
        answer: AcceptedAnswer,
        by: DecidedBy,
    },
    Cancelled {
        reason: Cancellation,
    },
    TimedOut {},
}
}

impl<AcceptedAnswer, Cancellation> DestinationEnding<AcceptedAnswer, Cancellation> {
    fn project(
        ending: HolderEnding<AcceptedAnswer, Cancellation>,
        destination: &DecidedBy,
    ) -> Self {
        match ending {
            HolderEnding::Decided { answer, by } if &by == destination => {
                Self::DecidedWithAnswer { answer, by }
            }
            HolderEnding::Decided { by, .. } => Self::Decided { by },
            HolderEnding::Cancelled { reason } => Self::Cancelled { reason },
            HolderEnding::TimedOut {} => Self::TimedOut {},
        }
    }
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum EndingView {
    ProviderApproval {
        ending: DestinationEnding<OfferedOptionId, ProviderCancellation>,
    },
    ProviderQuestion {
        ending: DestinationEnding<QuestionAnswer, ProviderCancellation>,
    },
    UnprovenCall {
        ending: DestinationEnding<OfferedOptionId, CallCancellation>,
    },
}
}

impl EndingView {
    /// Central has already established that this reader is a destination.
    pub fn for_raise(
        raise: &Raise,
        ending: Ending,
        reader: &DecidedBy,
    ) -> Result<Self, InvalidApprovalEnding> {
        Ok(match OfferEnding::from_raise(raise, ending)? {
            OfferEnding::ProviderApproval { ending, .. } => Self::ProviderApproval {
                ending: DestinationEnding::project(ending, reader),
            },
            OfferEnding::ProviderQuestion { ending, .. } => Self::ProviderQuestion {
                ending: DestinationEnding::project(ending, reader),
            },
            OfferEnding::UnprovenCall { ending, .. } => Self::UnprovenCall {
                ending: DestinationEnding::project(ending, reader),
            },
        })
    }
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RequestStateView<AcceptedAnswer, Cancellation> {
    Waiting {
        status: WaitingStatus,
    },
    Ended {
        ending: DestinationEnding<AcceptedAnswer, Cancellation>,
        ended_at: CentralTime,
        delivery: Delivery,
    },
}
}

impl<AcceptedAnswer, Cancellation> RequestStateView<AcceptedAnswer, Cancellation> {
    fn check_answer_membership(
        &self,
        offer: &OfferView,
        answer_for_kind: impl FnOnce(&AcceptedAnswer) -> Answer,
    ) -> Result<(), InvalidApprovalEnding> {
        if let Self::Ended {
            ending: DestinationEnding::DecidedWithAnswer { answer, .. },
            ..
        } = self
        {
            check_answer(offer, &answer_for_kind(answer)).map_err(InvalidApprovalEnding::Answer)?;
        }
        Ok(())
    }
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
enum RequestViewFields {
    ProviderApproval {
        request_id: ApprovalRequestId,
        offer: ApprovalOffer,
        state: RequestStateView<OfferedOptionId, ProviderCancellation>,
        window: ApprovalWindow,
    },
    ProviderQuestion {
        request_id: ApprovalRequestId,
        offer: QuestionOffer,
        state: RequestStateView<QuestionAnswer, ProviderCancellation>,
        window: ApprovalWindow,
    },
    UnprovenCall {
        request_id: ApprovalRequestId,
        offer: CallOffer,
        state: RequestStateView<OfferedOptionId, CallCancellation>,
        window: ApprovalWindow,
    },
}
}

/// Named request-kind arms carry only the matching offer and ending.
/// Decoding also checks any disclosed answer against the dynamic offer contents.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RequestViewFields", into = "RequestViewFields")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct RequestView(RequestViewFields);

impl RequestView {
    #[must_use]
    pub fn waiting(raise: &Raise, status: WaitingStatus, window: ApprovalWindow) -> Self {
        let request_id = raise.request_id().clone();
        Self(match raise {
            Raise::ProviderApproval { offer, .. } => RequestViewFields::ProviderApproval {
                request_id,
                offer: offer.clone(),
                state: RequestStateView::Waiting { status },
                window,
            },
            Raise::ProviderQuestion { offer, .. } => RequestViewFields::ProviderQuestion {
                request_id,
                offer: offer.clone(),
                state: RequestStateView::Waiting { status },
                window,
            },
            Raise::UnprovenCall { offer, .. } => RequestViewFields::UnprovenCall {
                request_id,
                offer: offer.clone(),
                state: RequestStateView::Waiting { status },
                window,
            },
        })
    }

    /// Central has already checked this reader's destination standing.
    pub fn ended(
        raise: &Raise,
        window: ApprovalWindow,
        ending: Ending,
        reader: &DecidedBy,
        ended_at: CentralTime,
        delivery: Delivery,
    ) -> Result<Self, InvalidApprovalEnding> {
        let request_id = raise.request_id().clone();
        Ok(Self(
            match (raise, EndingView::for_raise(raise, ending, reader)?) {
                (
                    Raise::ProviderApproval { offer, .. },
                    EndingView::ProviderApproval { ending },
                ) => RequestViewFields::ProviderApproval {
                    request_id,
                    offer: offer.clone(),
                    state: RequestStateView::Ended {
                        ending,
                        ended_at,
                        delivery,
                    },
                    window,
                },
                (
                    Raise::ProviderQuestion { offer, .. },
                    EndingView::ProviderQuestion { ending },
                ) => RequestViewFields::ProviderQuestion {
                    request_id,
                    offer: offer.clone(),
                    state: RequestStateView::Ended {
                        ending,
                        ended_at,
                        delivery,
                    },
                    window,
                },
                (Raise::UnprovenCall { offer, .. }, EndingView::UnprovenCall { ending }) => {
                    RequestViewFields::UnprovenCall {
                        request_id,
                        offer: offer.clone(),
                        state: RequestStateView::Ended {
                            ending,
                            ended_at,
                            delivery,
                        },
                        window,
                    }
                }
                _ => return Err(InvalidApprovalEnding::BindingMismatch),
            },
        ))
    }

    #[must_use]
    pub const fn request_id(&self) -> &ApprovalRequestId {
        match &self.0 {
            RequestViewFields::ProviderApproval { request_id, .. }
            | RequestViewFields::ProviderQuestion { request_id, .. }
            | RequestViewFields::UnprovenCall { request_id, .. } => request_id,
        }
    }

    #[must_use]
    pub const fn window(&self) -> ApprovalWindow {
        match &self.0 {
            RequestViewFields::ProviderApproval { window, .. }
            | RequestViewFields::ProviderQuestion { window, .. }
            | RequestViewFields::UnprovenCall { window, .. } => *window,
        }
    }
}

impl TryFrom<RequestViewFields> for RequestView {
    type Error = InvalidApprovalEnding;

    fn try_from(view: RequestViewFields) -> Result<Self, Self::Error> {
        match &view {
            RequestViewFields::ProviderApproval { offer, state, .. } => state
                .check_answer_membership(
                    &OfferView::ProviderApproval {
                        offer: offer.clone(),
                    },
                    |option_id| Answer::Approval {
                        option_id: option_id.clone(),
                    },
                )?,
            RequestViewFields::ProviderQuestion { offer, state, .. } => state
                .check_answer_membership(
                    &OfferView::ProviderQuestion {
                        offer: offer.clone(),
                    },
                    |answer| Answer::Question {
                        answer: answer.clone(),
                    },
                )?,
            RequestViewFields::UnprovenCall { offer, state, .. } => state.check_answer_membership(
                &OfferView::UnprovenCall {
                    offer: offer.clone(),
                },
                |option_id| Answer::Call {
                    option_id: option_id.clone(),
                },
            )?,
        }
        Ok(Self(view))
    }
}

impl From<RequestView> for RequestViewFields {
    fn from(view: RequestView) -> Self {
        view.0
    }
}
