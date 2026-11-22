//! A destination sees only the offer and ending for the request's own kind.
use crate::approval_wire_codec::{approval_wire_record, approval_wire_union};
use serde::{Deserialize, Serialize};
use workspace_ids::ApprovalRequestId;

use crate::{
    AdmittedDestination, Answer, ApprovalOffer, ApprovalWindow, CentralTime, DecidedBy,
    DeliverEndingRequest, Delivery, DestinationProjection, HolderEnding, InvalidApprovalEnding,
    OfferView, OfferedOptionId, PrivateEnding, ProviderCancellation, QuestionAnswer, QuestionOffer,
    Raise, WaitingStatus, check_answer,
};

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum StandingEnding<Cancellation> {
    Decided { by: DecidedBy },
    Cancelled { reason: Cancellation },
    TimedOut {},
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct GiverDecision<AcceptedAnswer> {
    pub by: DecidedBy,
    pub answer: AcceptedAnswer,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum DestinationEnding<AcceptedAnswer, Cancellation> {
    Standing { ending: StandingEnding<Cancellation> },
    Giver { decision: GiverDecision<AcceptedAnswer> },
}
}

impl<AcceptedAnswer, Cancellation> DestinationEnding<AcceptedAnswer, Cancellation> {
    fn project(
        ending: HolderEnding<AcceptedAnswer, Cancellation>,
        destination: &DecidedBy,
    ) -> Self {
        let standing = match ending {
            HolderEnding::Decided { answer, by } if &by == destination => {
                return Self::Giver {
                    decision: GiverDecision { answer, by },
                };
            }
            HolderEnding::Decided { by, .. } => StandingEnding::Decided { by },
            HolderEnding::Cancelled { reason } => StandingEnding::Cancelled { reason },
            HolderEnding::TimedOut {} => StandingEnding::TimedOut {},
        };
        Self::Standing { ending: standing }
    }
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum EndingViewWire {
    ProviderApproval {
        ending: DestinationEnding<OfferedOptionId, ProviderCancellation>,
    },
    ProviderQuestion {
        ending: DestinationEnding<QuestionAnswer, ProviderCancellation>,
    },
}
}

/// Recipient-scoped view of a stored, bound ending.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct EndingView(EndingViewWire);

impl EndingView {
    /// Project an ending to its admitted person or session. Only the giver receives the value.
    pub fn for_destination<'request>(
        admitted: &AdmittedDestination<'request>,
        ending: &DeliverEndingRequest,
    ) -> Result<DestinationProjection<'request, Self>, InvalidApprovalEnding> {
        ending.check_bound_raise(admitted.raise())?;
        let actor = admitted.actor();
        let projected = match ending.ending.clone() {
            PrivateEnding::ProviderApproval { ending } => EndingViewWire::ProviderApproval {
                ending: DestinationEnding::project(ending, actor),
            },
            PrivateEnding::ProviderQuestion { ending } => EndingViewWire::ProviderQuestion {
                ending: DestinationEnding::project(ending, actor),
            },
        };
        Ok(DestinationProjection {
            actor,
            view: Self(projected),
        })
    }

    /// Inspect this shape after decoding a reply on the receiving side.
    #[must_use]
    pub const fn wire_shape(&self) -> &EndingViewWire {
        &self.0
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
    fn check_ended_view(
        &self,
        window: ApprovalWindow,
        offer: &OfferView,
        answer_for_kind: impl FnOnce(&AcceptedAnswer) -> Answer,
    ) -> Result<(), InvalidApprovalEnding> {
        let Self::Ended {
            ending, ended_at, ..
        } = self
        else {
            return Ok(());
        };
        let timed_out = matches!(
            ending,
            DestinationEnding::Standing {
                ending: StandingEnding::TimedOut {}
            }
        );
        if *ended_at < window.raised_at() || (*ended_at >= window.deadline()) != timed_out {
            return Err(InvalidApprovalEnding::Chronology);
        }

        if let DestinationEnding::Giver { decision } = ending {
            check_answer(offer, &answer_for_kind(&decision.answer))
                .map_err(InvalidApprovalEnding::Answer)?;
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
    pub fn waiting<'request>(
        admitted: &AdmittedDestination<'request>,
        status: WaitingStatus,
        window: ApprovalWindow,
    ) -> DestinationProjection<'request, Self> {
        let raise = admitted.raise();
        let request_id = raise.request_id().clone();
        let fields = match raise {
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
        };
        DestinationProjection {
            actor: admitted.actor(),
            view: Self(fields),
        }
    }

    pub fn ended<'request>(
        admitted: &AdmittedDestination<'request>,
        window: ApprovalWindow,
        ending: &DeliverEndingRequest,
        ended_at: CentralTime,
        delivery: Delivery,
    ) -> Result<DestinationProjection<'request, Self>, InvalidApprovalEnding> {
        let raise = admitted.raise();
        let request_id = raise.request_id().clone();
        let ending = EndingView::for_destination(admitted, ending)?.view.0;
        let fields = match (raise, ending) {
            (
                Raise::ProviderApproval { offer, .. },
                EndingViewWire::ProviderApproval { ending },
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
                EndingViewWire::ProviderQuestion { ending },
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
            _ => return Err(InvalidApprovalEnding::BindingMismatch),
        };
        Ok(DestinationProjection {
            actor: admitted.actor(),
            view: Self::try_from(fields)?,
        })
    }

    #[must_use]
    pub const fn request_id(&self) -> &ApprovalRequestId {
        match &self.0 {
            RequestViewFields::ProviderApproval { request_id, .. }
            | RequestViewFields::ProviderQuestion { request_id, .. } => request_id,
        }
    }

    #[must_use]
    pub const fn window(&self) -> ApprovalWindow {
        match &self.0 {
            RequestViewFields::ProviderApproval { window, .. }
            | RequestViewFields::ProviderQuestion { window, .. } => *window,
        }
    }
}

impl TryFrom<RequestViewFields> for RequestView {
    type Error = InvalidApprovalEnding;

    fn try_from(view: RequestViewFields) -> Result<Self, Self::Error> {
        match &view {
            RequestViewFields::ProviderApproval {
                offer,
                state,
                window,
                ..
            } => state.check_ended_view(
                *window,
                &OfferView::ProviderApproval {
                    offer: offer.clone(),
                },
                |option_id| Answer::Approval {
                    option_id: option_id.clone(),
                },
            )?,
            RequestViewFields::ProviderQuestion {
                offer,
                state,
                window,
                ..
            } => state.check_ended_view(
                *window,
                &OfferView::ProviderQuestion {
                    offer: offer.clone(),
                },
                |answer| Answer::Question {
                    answer: answer.clone(),
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
