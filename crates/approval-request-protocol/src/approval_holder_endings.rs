//! Deliverable endings carry only the answer and cancellation of their request kind.
use crate::approval_wire_codec::{approval_wire_record, approval_wire_union};
use workspace_ids::{ApprovalRequestId, PersonId};

use crate::{
    Answer, CallStopCause, CancelReason, DecidedBy, Ending, HolderStopCause, InvalidAnswer,
    InvalidApprovalEnding, OfferedOptionId, ProviderAnswer, ProviderStopCause, QuestionAnswer,
    Raise, RequestContentsDigest, SessionCallDecision, check_answer,
};

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, schemars::JsonSchema)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum CallCancellation {
    Stopped { cause: CallStopCause },
    RequesterGone {},
    EnrollmentEnded {},
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ParkedCallEnding {
    Decided {
        decision: SessionCallDecision,
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
    },
    Cancelled { reason: CallCancellation },
    TimedOut {},
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum PrivateEnding {
    ProviderApproval { ending: HolderEnding<OfferedOptionId, ProviderCancellation> },
    ProviderQuestion { ending: HolderEnding<QuestionAnswer, ProviderCancellation> },
    UnprovenCall { ending: ParkedCallEnding },
}
}

impl PrivateEnding {
    pub fn from_raise(raise: &Raise, ending: Ending) -> Result<Self, InvalidApprovalEnding> {
        if let Ending::Decided { answer, by } = &ending {
            check_decision_actor(raise, &answer.clone().into(), by)?;
        }
        match raise {
            Raise::ProviderApproval { .. } => Ok(Self::ProviderApproval {
                ending: project_ending(ending, take_approval, ProviderCancellation::try_from)?,
            }),
            Raise::ProviderQuestion { .. } => Ok(Self::ProviderQuestion {
                ending: project_ending(ending, take_question, ProviderCancellation::try_from)?,
            }),
            Raise::UnprovenCall { .. } => Ok(Self::UnprovenCall {
                ending: ParkedCallEnding::from_ending(ending)?,
            }),
        }
    }

    pub fn check_bound_raise(&self, raise: &Raise) -> Result<(), InvalidApprovalEnding> {
        if let Self::UnprovenCall { .. } = self {
            return match raise {
                Raise::UnprovenCall { .. } => Ok(()),
                Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => {
                    Err(InvalidApprovalEnding::BindingMismatch)
                }
            };
        }
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
                |option_id| ProviderAnswer::Approval { option_id },
                ProviderCancellation::into_reason,
            ),
            Self::ProviderQuestion { ending } => ending.into_ending(
                |answer| ProviderAnswer::Question { answer },
                ProviderCancellation::into_reason,
            ),
            Self::UnprovenCall { ending } => ending.into_ending(),
        }
    }
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct DeliverEndingRequest {
    request_id: ApprovalRequestId,
    contents_digest: RequestContentsDigest,
    ending: PrivateEnding,
}
}

/// A decision produced by checking an admitted destination and the current factor.
#[derive(Debug)]
pub struct AcceptedApprovalDecision(DeliverEndingRequest);

impl AcceptedApprovalDecision {
    pub(crate) fn from_admitted(
        raise: &Raise,
        answer: Answer,
        actor: DecidedBy,
    ) -> Result<Self, InvalidApprovalEnding> {
        let holder_frame = match answer {
            Answer::Approval { option_id } => DeliverEndingRequest::from_raise(
                raise,
                Ending::Decided {
                    answer: ProviderAnswer::Approval { option_id },
                    by: actor,
                },
            )?,
            Answer::Question { answer } => DeliverEndingRequest::from_raise(
                raise,
                Ending::Decided {
                    answer: ProviderAnswer::Question { answer },
                    by: actor,
                },
            )?,
            Answer::UnprovenCall { decision } => {
                let DecidedBy::Person { person_id } = actor else {
                    return Err(InvalidApprovalEnding::IneligibleDecider);
                };
                DeliverEndingRequest {
                    request_id: raise.request_id().clone(),
                    contents_digest: raise.contents_digest()?,
                    ending: PrivateEnding::UnprovenCall {
                        ending: ParkedCallEnding::Decided {
                            decision,
                            person_id,
                        },
                    },
                }
            }
        };
        Ok(Self(holder_frame))
    }
}

impl DeliverEndingRequest {
    /// Carry a checked decision to the holder for the same immutable Raise.
    pub fn from_decision(
        raise: &Raise,
        decision: AcceptedApprovalDecision,
    ) -> Result<Self, InvalidApprovalEnding> {
        let frame = decision.0;
        frame.check_bound_raise(raise)?;
        Ok(frame)
    }

    #[must_use]
    pub fn request_id(&self) -> &ApprovalRequestId {
        &self.request_id
    }

    #[must_use]
    pub fn contents_digest(&self) -> &RequestContentsDigest {
        &self.contents_digest
    }

    #[must_use]
    pub fn ending(&self) -> &PrivateEnding {
        &self.ending
    }

    #[must_use]
    pub fn into_ending(self) -> Ending {
        self.ending.into_ending()
    }

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
        answer_for_kind: impl FnOnce(AcceptedAnswer) -> ProviderAnswer,
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
    answer_for_kind: impl FnOnce(ProviderAnswer) -> Option<AcceptedAnswer>,
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
        Ending::UnprovenCallDecided { .. } => Err(InvalidApprovalEnding::IneligibleDecider),
    }
}

pub(crate) fn requires_fresh_factor(raise: &Raise, answer: &Answer) -> bool {
    match (raise, answer) {
        (Raise::ProviderApproval { offer, .. }, Answer::Approval { option_id }) => offer
            .options
            .as_slice()
            .iter()
            .any(|option| &option.id == option_id && option.needs_person_with_fresh_factor()),
        (Raise::UnprovenCall { .. }, Answer::UnprovenCall { .. }) => true,
        (Raise::ProviderApproval { .. }, Answer::Question { .. } | Answer::UnprovenCall { .. })
        | (
            Raise::ProviderQuestion { .. },
            Answer::Approval { .. } | Answer::Question { .. } | Answer::UnprovenCall { .. },
        )
        | (Raise::UnprovenCall { .. }, Answer::Approval { .. } | Answer::Question { .. }) => false,
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
        && (raise.requesting_session() == Some(session) || requires_fresh_factor(raise, answer))
    {
        return Err(InvalidApprovalEnding::IneligibleDecider);
    }
    Ok(())
}

fn take_approval(answer: ProviderAnswer) -> Option<OfferedOptionId> {
    match answer {
        ProviderAnswer::Approval { option_id } => Some(option_id),
        ProviderAnswer::Question { .. } => None,
    }
}

fn take_question(answer: ProviderAnswer) -> Option<QuestionAnswer> {
    match answer {
        ProviderAnswer::Question { answer } => Some(answer),
        ProviderAnswer::Approval { .. } => None,
    }
}

impl TryFrom<CancelReason> for ProviderCancellation {
    type Error = InvalidApprovalEnding;
    fn try_from(reason: CancelReason) -> Result<Self, Self::Error> {
        Ok(match reason {
            CancelReason::Stopped { cause } => Self::Stopped {
                cause: cause.try_into()?,
            },
            CancelReason::RequesterGone {} => Self::RequesterGone {},
            CancelReason::SessionEnded {} => Self::SessionEnded {},
            CancelReason::EnrollmentEnded {} => Self::EnrollmentEnded {},
        })
    }
}

impl ParkedCallEnding {
    fn from_ending(ending: Ending) -> Result<Self, InvalidApprovalEnding> {
        match ending {
            Ending::Decided { .. } | Ending::UnprovenCallDecided { .. } => {
                Err(InvalidApprovalEnding::IneligibleDecider)
            }
            Ending::Cancelled { reason } => {
                let reason = match reason {
                    CancelReason::Stopped {
                        cause: HolderStopCause::CallWithdrawn,
                    } => CallCancellation::Stopped {
                        cause: CallStopCause::CallWithdrawn,
                    },
                    CancelReason::RequesterGone {} => CallCancellation::RequesterGone {},
                    CancelReason::EnrollmentEnded {} => CallCancellation::EnrollmentEnded {},
                    CancelReason::Stopped {
                        cause:
                            HolderStopCause::TurnStopped
                            | HolderStopCause::AskReplaced
                            | HolderStopCause::AskWithdrawn
                            | HolderStopCause::SessionClosed
                            | HolderStopCause::ProviderConnectionLost,
                    }
                    | CancelReason::SessionEnded {} => {
                        return Err(InvalidApprovalEnding::BindingMismatch);
                    }
                };
                Ok(Self::Cancelled { reason })
            }
            Ending::TimedOut {} => Ok(Self::TimedOut {}),
        }
    }

    fn into_ending(self) -> Ending {
        match self {
            ParkedCallEnding::Decided {
                decision,
                person_id,
            } => Ending::UnprovenCallDecided {
                decision,
                person_id,
            },
            ParkedCallEnding::Cancelled { reason } => Ending::Cancelled {
                reason: match reason {
                    CallCancellation::Stopped {
                        cause: CallStopCause::CallWithdrawn,
                    } => CancelReason::Stopped {
                        cause: HolderStopCause::CallWithdrawn,
                    },
                    CallCancellation::RequesterGone {} => CancelReason::RequesterGone {},
                    CallCancellation::EnrollmentEnded {} => CancelReason::EnrollmentEnded {},
                },
            },
            ParkedCallEnding::TimedOut {} => Ending::TimedOut {},
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
