//! Deliverable endings carry only the answer and cancellation of their request kind.
use crate::approval_wire_codec::approval_wire_union;
use workspace_ids::ApprovalRequestId;

use crate::{
    Answer, CallStopCause, CancelReason, DecidedBy, Ending, HolderStopCause, InvalidAnswer,
    InvalidApprovalEnding, OfferedOptionId, ParkedCallHolder, ProviderHolder, ProviderStopCause,
    QuestionAnswer, Raise, check_answer,
};

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum HolderEnding<AcceptedAnswer, Cancellation> {
    Decided {
        answer: AcceptedAnswer,
        by: DecidedBy,
    },
    Cancelled {
        reason: Cancellation,
    },
    TimedOut {},
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ProviderCancellation {
    Stopped { cause: ProviderStopCause },
    RequesterGone {},
    SessionEnded {},
    EnrollmentEnded {},
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum CallCancellation {
    Stopped { cause: CallStopCause },
    RequesterGone {},
    EnrollmentEnded {},
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum OfferEnding {
    ProviderApproval {
        request_id: ApprovalRequestId,
        holder: ProviderHolder,
        ending: HolderEnding<OfferedOptionId, ProviderCancellation>,
    },
    ProviderQuestion {
        request_id: ApprovalRequestId,
        holder: ProviderHolder,
        ending: HolderEnding<QuestionAnswer, ProviderCancellation>,
    },
    UnprovenCall {
        request_id: ApprovalRequestId,
        holder: ParkedCallHolder,
        ending: HolderEnding<OfferedOptionId, CallCancellation>,
    },
}
}

impl OfferEnding {
    /// Project the lifecycle's ending only for the exact recorded raise.
    pub fn from_raise(raise: &Raise, ending: Ending) -> Result<Self, InvalidApprovalEnding> {
        match &ending {
            Ending::Decided { answer, .. } => {
                check_answer(&raise.offer_view(), answer).map_err(InvalidApprovalEnding::Answer)?
            }
            Ending::Cancelled { .. } | Ending::TimedOut {} => {}
        }
        match raise {
            Raise::ProviderApproval {
                request_id, holder, ..
            } => Ok(Self::ProviderApproval {
                request_id: request_id.clone(),
                holder: holder.clone(),
                ending: project_ending(ending, take_approval, ProviderCancellation::try_from)?,
            }),
            Raise::ProviderQuestion {
                request_id, holder, ..
            } => Ok(Self::ProviderQuestion {
                request_id: request_id.clone(),
                holder: holder.clone(),
                ending: project_ending(ending, take_question, ProviderCancellation::try_from)?,
            }),
            Raise::UnprovenCall {
                request_id, holder, ..
            } => Ok(Self::UnprovenCall {
                request_id: request_id.clone(),
                holder: holder.clone(),
                ending: project_ending(ending, take_call, CallCancellation::try_from)?,
            }),
        }
    }

    /// The Mac calls this before handing an offer to its exact bound responder.
    pub fn check_bound_raise(&self, raise: &Raise) -> Result<(), InvalidApprovalEnding> {
        let expected = Self::from_raise(raise, self.clone().into_ending())?;
        if &expected == self {
            Ok(())
        } else {
            Err(InvalidApprovalEnding::BindingMismatch)
        }
    }

    #[must_use]
    pub fn into_ending(self) -> Ending {
        match self {
            Self::ProviderApproval { ending, .. } => {
                ending.into_ending(|option_id| Answer::Approval { option_id })
            }
            Self::ProviderQuestion { ending, .. } => {
                ending.into_ending(|answer| Answer::Question { answer })
            }
            Self::UnprovenCall { ending, .. } => {
                ending.into_ending(|option_id| Answer::Call { option_id })
            }
        }
    }
}

impl<AcceptedAnswer, Cancellation: Into<CancelReason>> HolderEnding<AcceptedAnswer, Cancellation> {
    fn into_ending(self, answer_for_kind: impl FnOnce(AcceptedAnswer) -> Answer) -> Ending {
        match self {
            Self::Decided { answer, by } => Ending::Decided {
                answer: answer_for_kind(answer),
                by,
            },
            Self::Cancelled { reason } => Ending::Cancelled {
                reason: reason.into(),
            },
            Self::TimedOut {} => Ending::TimedOut {},
        }
    }
}

fn project_ending<AcceptedAnswer, Cancellation>(
    ending: Ending,
    answer_for_kind: impl FnOnce(Answer) -> Option<AcceptedAnswer>,
    cancellation_for_kind: impl FnOnce(CancelReason) -> Result<Cancellation, InvalidApprovalEnding>,
) -> Result<HolderEnding<AcceptedAnswer, Cancellation>, InvalidApprovalEnding> {
    match ending {
        Ending::Decided { answer, by } => {
            let answer = answer_for_kind(answer)
                .ok_or(InvalidApprovalEnding::Answer(InvalidAnswer::WrongKind {}))?;
            Ok(HolderEnding::Decided { answer, by })
        }
        Ending::Cancelled { reason } => Ok(HolderEnding::Cancelled {
            reason: cancellation_for_kind(reason)?,
        }),
        Ending::TimedOut {} => Ok(HolderEnding::TimedOut {}),
    }
}

fn take_approval(answer: Answer) -> Option<OfferedOptionId> {
    match answer {
        Answer::Approval { option_id } => Some(option_id),
        Answer::Call { .. } | Answer::Question { .. } => None,
    }
}

fn take_question(answer: Answer) -> Option<QuestionAnswer> {
    match answer {
        Answer::Question { answer } => Some(answer),
        Answer::Approval { .. } | Answer::Call { .. } => None,
    }
}

fn take_call(answer: Answer) -> Option<OfferedOptionId> {
    match answer {
        Answer::Call { option_id } => Some(option_id),
        Answer::Approval { .. } | Answer::Question { .. } => None,
    }
}

impl TryFrom<CancelReason> for ProviderCancellation {
    type Error = InvalidApprovalEnding;

    fn try_from(reason: CancelReason) -> Result<Self, Self::Error> {
        Ok(match reason {
            CancelReason::Stopped { cause } => Self::Stopped {
                cause: ProviderStopCause::try_from(cause)?,
            },
            CancelReason::RequesterGone {} => Self::RequesterGone {},
            CancelReason::SessionEnded {} => Self::SessionEnded {},
            CancelReason::EnrollmentEnded {} => Self::EnrollmentEnded {},
        })
    }
}

impl From<ProviderCancellation> for CancelReason {
    fn from(reason: ProviderCancellation) -> Self {
        match reason {
            ProviderCancellation::Stopped { cause } => Self::Stopped {
                cause: cause.holder_cause(),
            },
            ProviderCancellation::RequesterGone {} => Self::RequesterGone {},
            ProviderCancellation::SessionEnded {} => Self::SessionEnded {},
            ProviderCancellation::EnrollmentEnded {} => Self::EnrollmentEnded {},
        }
    }
}

impl TryFrom<CancelReason> for CallCancellation {
    type Error = InvalidApprovalEnding;

    fn try_from(reason: CancelReason) -> Result<Self, Self::Error> {
        match reason {
            CancelReason::Stopped {
                cause: HolderStopCause::CallWithdrawn,
            } => Ok(Self::Stopped {
                cause: CallStopCause::CallWithdrawn,
            }),
            CancelReason::RequesterGone {} => Ok(Self::RequesterGone {}),
            CancelReason::EnrollmentEnded {} => Ok(Self::EnrollmentEnded {}),
            CancelReason::Stopped { .. } | CancelReason::SessionEnded {} => {
                Err(InvalidApprovalEnding::Cancellation)
            }
        }
    }
}

impl From<CallCancellation> for CancelReason {
    fn from(reason: CallCancellation) -> Self {
        match reason {
            CallCancellation::Stopped {
                cause: CallStopCause::CallWithdrawn,
            } => Self::Stopped {
                cause: HolderStopCause::CallWithdrawn,
            },
            CallCancellation::RequesterGone {} => Self::RequesterGone {},
            CallCancellation::EnrollmentEnded {} => Self::EnrollmentEnded {},
        }
    }
}
