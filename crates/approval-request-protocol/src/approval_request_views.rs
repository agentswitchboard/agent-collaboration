//! A destination sees only the offer and ending for the request's own kind.
use crate::approval_wire_codec::{approval_wire_record, approval_wire_union};
use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, PersonId};

use crate::{
    AdmittedDestination, Answer, ApprovalOffer, ApprovalWindow, CallCancellation, CallOffer,
    CentralTime, DecidedBy, DeliverEndingRequest, Delivery, DestinationProjection, HolderEnding,
    InvalidApprovalEnding, OfferView, OfferedOptionId, ParkedCallEnding, PrivateEnding,
    ProviderCancellation, QuestionAnswer, QuestionOffer, Raise, SessionCallDecision, WaitingStatus,
    check_answer,
};

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum StandingEnding<Cancellation> {
    Decided { by: DecidedBy },
    Cancelled { reason: Cancellation },
    TimedOut {},
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct GiverDecision<AcceptedAnswer> {
    pub by: DecidedBy,
    pub answer: AcceptedAnswer,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
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
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ParkedCallEndingView {
    Decided {
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
    },
    Giver {
        decision: SessionCallDecision,
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
    },
    Cancelled { reason: CallCancellation },
    TimedOut {},
}
}

impl ParkedCallEndingView {
    pub(crate) fn for_reader(ending: ParkedCallEnding, actor: &DecidedBy) -> Self {
        match ending {
            ParkedCallEnding::Decided {
                decision,
                person_id,
            } if actor
                == &(DecidedBy::Person {
                    person_id: person_id.clone(),
                }) =>
            {
                Self::Giver {
                    decision,
                    person_id,
                }
            }
            ParkedCallEnding::Decided { person_id, .. } => Self::Decided { person_id },
            ParkedCallEnding::Cancelled { reason } => Self::Cancelled { reason },
            ParkedCallEnding::TimedOut {} => Self::TimedOut {},
        }
    }
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ParkedCallStateView {
    Waiting {},
    Ended {
        ending: ParkedCallEndingView,
        ended_at: CentralTime,
        delivery: Delivery,
    },
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
    UnprovenCall { ending: ParkedCallEndingView },
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ProviderEndingView {
    ProviderApproval {
        ending: DestinationEnding<OfferedOptionId, ProviderCancellation>,
    },
    ProviderQuestion {
        ending: DestinationEnding<QuestionAnswer, ProviderCancellation>,
    },
}
}

impl TryFrom<EndingView> for ProviderEndingView {
    type Error = InvalidApprovalEnding;

    fn try_from(view: EndingView) -> Result<Self, Self::Error> {
        match view.0 {
            EndingViewWire::ProviderApproval { ending } => Ok(Self::ProviderApproval { ending }),
            EndingViewWire::ProviderQuestion { ending } => Ok(Self::ProviderQuestion { ending }),
            EndingViewWire::UnprovenCall { .. } => Err(InvalidApprovalEnding::BindingMismatch),
        }
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
        let projected = match ending.ending().clone() {
            PrivateEnding::ProviderApproval { ending } => EndingViewWire::ProviderApproval {
                ending: DestinationEnding::project(ending, actor),
            },
            PrivateEnding::ProviderQuestion { ending } => EndingViewWire::ProviderQuestion {
                ending: DestinationEnding::project(ending, actor),
            },
            PrivateEnding::UnprovenCall { ending } => EndingViewWire::UnprovenCall {
                ending: ParkedCallEndingView::for_reader(ending, actor),
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
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
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
        check_ending_time(window, *ended_at, timed_out)?;

        if let DestinationEnding::Giver { decision } = ending {
            check_answer(offer, &answer_for_kind(&decision.answer))
                .map_err(InvalidApprovalEnding::Answer)?;
        }
        Ok(())
    }
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub(crate) enum RequestViewFields {
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
        state: ParkedCallStateView,
        window: ApprovalWindow,
    },
}
}

/// Named request-kind arms carry only the matching offer and ending.
/// Decoding also checks any disclosed answer against the dynamic offer contents.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(try_from = "RequestViewFields", into = "RequestViewFields")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct RequestView(RequestViewFields);

impl RequestView {
    pub(crate) const fn fields(&self) -> &RequestViewFields {
        &self.0
    }

    pub fn waiting<'request>(
        admitted: &AdmittedDestination<'request>,
        status: WaitingStatus,
        window: ApprovalWindow,
    ) -> Result<DestinationProjection<'request, Self>, InvalidApprovalEnding> {
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
            Raise::UnprovenCall { offer, .. } => match status {
                WaitingStatus::Pending => RequestViewFields::UnprovenCall {
                    request_id,
                    offer: offer.clone(),
                    state: ParkedCallStateView::Waiting {},
                    window,
                },
                WaitingStatus::PassedOn => return Err(InvalidApprovalEnding::BindingMismatch),
            },
        };
        Ok(DestinationProjection {
            actor: admitted.actor(),
            view: Self(fields),
        })
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
            (Raise::UnprovenCall { offer, .. }, EndingViewWire::UnprovenCall { ending }) => {
                RequestViewFields::UnprovenCall {
                    request_id,
                    offer: offer.clone(),
                    state: ParkedCallStateView::Ended {
                        ending,
                        ended_at,
                        delivery,
                    },
                    window,
                }
            }
            (
                Raise::ProviderApproval { .. },
                EndingViewWire::ProviderQuestion { .. } | EndingViewWire::UnprovenCall { .. },
            )
            | (
                Raise::ProviderQuestion { .. },
                EndingViewWire::ProviderApproval { .. } | EndingViewWire::UnprovenCall { .. },
            )
            | (
                Raise::UnprovenCall { .. },
                EndingViewWire::ProviderApproval { .. } | EndingViewWire::ProviderQuestion { .. },
            ) => {
                return Err(InvalidApprovalEnding::BindingMismatch);
            }
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
            RequestViewFields::UnprovenCall {
                state: ParkedCallStateView::Waiting {},
                ..
            } => {}
            RequestViewFields::UnprovenCall {
                state:
                    ParkedCallStateView::Ended {
                        ending, ended_at, ..
                    },
                window,
                ..
            } => {
                check_ending_time(
                    *window,
                    *ended_at,
                    matches!(ending, ParkedCallEndingView::TimedOut {}),
                )?;
            }
        }
        Ok(Self(view))
    }
}

fn check_ending_time(
    window: ApprovalWindow,
    ended_at: CentralTime,
    timed_out: bool,
) -> Result<(), InvalidApprovalEnding> {
    if ended_at < window.raised_at() || (ended_at >= window.deadline()) != timed_out {
        return Err(InvalidApprovalEnding::Chronology);
    }
    Ok(())
}

impl From<RequestView> for RequestViewFields {
    fn from(view: RequestView) -> Self {
        view.0
    }
}
