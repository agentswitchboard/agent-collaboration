//! Deliverable endings carry only the answer and cancellation of their request kind.
use crate::approval_wire_codec::{approval_wire_record, approval_wire_union};
use workspace_ids::ApprovalRequestId;

use crate::{
    Answer, CancelReason, DecidedBy, Ending, InvalidAnswer, InvalidApprovalEnding, OfferedOptionId,
    ProviderStopCause, QuestionAnswer, Raise, RequestContentsDigest, check_answer,
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
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum PrivateEnding {
    ProviderApproval { ending: HolderEnding<OfferedOptionId, ProviderCancellation> },
    ProviderQuestion { ending: HolderEnding<QuestionAnswer, ProviderCancellation> },
}
}

impl PrivateEnding {
    pub fn from_raise(raise: &Raise, ending: Ending) -> Result<Self, InvalidApprovalEnding> {
        if let Ending::Decided { answer, by } = &ending {
            check_decision_actor(raise, answer, by)?;
        }
        match raise {
            Raise::ProviderApproval { .. } => Ok(Self::ProviderApproval {
                ending: project_ending(ending, take_approval, ProviderCancellation::from)?,
            }),
            Raise::ProviderQuestion { .. } => Ok(Self::ProviderQuestion {
                ending: project_ending(ending, take_question, ProviderCancellation::from)?,
            }),
        }
    }

    pub fn check_bound_raise(&self, raise: &Raise) -> Result<(), InvalidApprovalEnding> {
        let expected = Self::from_raise(raise, self.clone().into_ending())?;
        if *self == expected {
            Ok(())
        } else {
            Err(InvalidApprovalEnding::BindingMismatch)
        }
    }

    #[must_use]
    pub fn into_ending(self) -> Ending {
        match self {
            Self::ProviderApproval { ending } => ending.into_ending(
                |option_id| Answer::Approval { option_id },
                ProviderCancellation::into_reason,
            ),
            Self::ProviderQuestion { ending } => ending.into_ending(
                |answer| Answer::Question { answer },
                ProviderCancellation::into_reason,
            ),
        }
    }
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct DeliverEndingRequest {
    pub request_id: ApprovalRequestId,
    pub contents_digest: RequestContentsDigest,
    pub ending: PrivateEnding,
}
}

impl DeliverEndingRequest {
    pub fn from_raise(raise: &Raise, ending: Ending) -> Result<Self, InvalidApprovalEnding> {
        Ok(Self {
            request_id: raise.request_id().clone(),
            contents_digest: raise.contents_digest()?,
            ending: PrivateEnding::from_raise(raise, ending)?,
        })
    }

    /// Check the holder's exact frozen Raise and its selected answer before handover.
    pub fn check_bound_raise(&self, raise: &Raise) -> Result<(), InvalidApprovalEnding> {
        if &self.request_id != raise.request_id()
            || self.contents_digest != raise.contents_digest()?
        {
            return Err(InvalidApprovalEnding::BindingMismatch);
        }
        self.ending.check_bound_raise(raise)
    }
}

impl<AcceptedAnswer, Cancellation> HolderEnding<AcceptedAnswer, Cancellation> {
    fn into_ending(
        self,
        answer_for_kind: impl FnOnce(AcceptedAnswer) -> Answer,
        cancellation_for_kind: impl FnOnce(Cancellation) -> CancelReason,
    ) -> Ending {
        match self {
            Self::Decided { answer, by } => Ending::Decided {
                answer: answer_for_kind(answer),
                by,
            },
            Self::Cancelled { reason } => Ending::Cancelled {
                reason: cancellation_for_kind(reason),
            },
            Self::TimedOut {} => Ending::TimedOut {},
        }
    }
}

fn project_ending<AcceptedAnswer, Cancellation>(
    ending: Ending,
    answer_for_kind: impl FnOnce(Answer) -> Option<AcceptedAnswer>,
    cancellation_for_kind: impl FnOnce(CancelReason) -> Cancellation,
) -> Result<HolderEnding<AcceptedAnswer, Cancellation>, InvalidApprovalEnding> {
    match ending {
        Ending::Decided { answer, by } => {
            let answer = answer_for_kind(answer)
                .ok_or(InvalidApprovalEnding::Answer(InvalidAnswer::WrongKind {}))?;
            Ok(HolderEnding::Decided { answer, by })
        }
        Ending::Cancelled { reason } => Ok(HolderEnding::Cancelled {
            reason: cancellation_for_kind(reason),
        }),
        Ending::TimedOut {} => Ok(HolderEnding::TimedOut {}),
    }
}

pub(crate) fn requires_fresh_factor(raise: &Raise, answer: &Answer) -> bool {
    match (raise, answer) {
        (Raise::ProviderApproval { offer, .. }, Answer::Approval { option_id }) => offer
            .options
            .as_slice()
            .iter()
            .any(|option| &option.id == option_id && option.needs_person_with_fresh_factor()),
        _ => false,
    }
}

pub(crate) fn check_decision_actor(
    raise: &Raise,
    answer: &Answer,
    actor: &DecidedBy,
) -> Result<(), InvalidApprovalEnding> {
    let offer = raise.offer_view();
    check_answer(&offer, answer).map_err(InvalidApprovalEnding::Answer)?;
    if let DecidedBy::Session { session } = actor
        && (raise.requesting_session() == session || requires_fresh_factor(raise, answer))
    {
        return Err(InvalidApprovalEnding::IneligibleDecider);
    }
    Ok(())
}

fn take_approval(answer: Answer) -> Option<OfferedOptionId> {
    match answer {
        Answer::Approval { option_id } => Some(option_id),
        Answer::Question { .. } => None,
    }
}

fn take_question(answer: Answer) -> Option<QuestionAnswer> {
    match answer {
        Answer::Question { answer } => Some(answer),
        Answer::Approval { .. } => None,
    }
}

impl From<CancelReason> for ProviderCancellation {
    fn from(reason: CancelReason) -> Self {
        match reason {
            CancelReason::Stopped { cause } => Self::Stopped {
                cause: cause.into(),
            },
            CancelReason::RequesterGone {} => Self::RequesterGone {},
            CancelReason::SessionEnded {} => Self::SessionEnded {},
            CancelReason::EnrollmentEnded {} => Self::EnrollmentEnded {},
        }
    }
}

impl ProviderCancellation {
    fn into_reason(self) -> CancelReason {
        match self {
            ProviderCancellation::Stopped { cause } => CancelReason::Stopped {
                cause: cause.holder_cause(),
            },
            ProviderCancellation::RequesterGone {} => CancelReason::RequesterGone {},
            ProviderCancellation::SessionEnded {} => CancelReason::SessionEnded {},
            ProviderCancellation::EnrollmentEnded {} => CancelReason::EnrollmentEnded {},
        }
    }
}
