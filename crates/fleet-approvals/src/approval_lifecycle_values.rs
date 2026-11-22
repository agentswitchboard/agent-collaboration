use approval_request_protocol::{
    Answer, CentralTime, DecidedBy, Delivery, Ending, EndingView, HolderStopCause, OfferEnding,
    Raise, RaiseReceipt, RaiseReply, Refusal, ReportDeliveryReply, ReportDeliveryRequest,
    StopReceiptEnding, StopReply, StopRequest, WaitingStatus,
};
use central_engine_model::{EventTag, RefusalTag, StateTag};
use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, PersonId, RouterRunId, SessionRef};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ApprovalDestination {
    People,
    Session(SessionRef),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedApproval {
    pub raise: Raise,
    pub receipt: RaiseReceipt,
    pub destinations: Vec<ApprovalDestination>,
}

impl RetainedApproval {
    pub(crate) fn holder_run(&self) -> &RouterRunId {
        match &self.raise {
            Raise::ProviderApproval { holder, .. } | Raise::ProviderQuestion { holder, .. } => {
                &holder.router_run
            }
            Raise::UnprovenCall { holder, .. } => &holder.router_run,
        }
    }

    pub(crate) fn requester(&self) -> Option<&SessionRef> {
        match &self.raise {
            Raise::ProviderApproval { holder, .. } | Raise::ProviderQuestion { holder, .. } => {
                Some(&holder.session)
            }
            Raise::UnprovenCall { .. } => None,
        }
    }

    pub(crate) fn offer_ending(&self, ending: Ending) -> OfferEnding {
        let request_id = self.receipt.request_id.clone();
        match &self.raise {
            Raise::ProviderApproval { holder, .. } | Raise::ProviderQuestion { holder, .. } => {
                OfferEnding::Provider {
                    request_id,
                    holder: holder.clone(),
                    ending,
                }
            }
            Raise::UnprovenCall { holder, .. } => OfferEnding::UnprovenCall {
                request_id,
                holder: holder.clone(),
                ending,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndedApproval {
    pub ending: Ending,
    pub ended_at: CentralTime,
    pub delivery: Delivery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalState {
    Unraised,
    StoppedBeforeRaise {
        request_id: ApprovalRequestId,
        router_run: RouterRunId,
        cause: HolderStopCause,
        stopped_at: CentralTime,
    },
    Waiting {
        saved: Box<RetainedApproval>,
        status: WaitingStatus,
    },
    Ended {
        saved: Box<RetainedApproval>,
        closed: EndedApproval,
    },
    /// Duplicate-suppression marker retained while the run is registered.
    Erased {
        request_id: ApprovalRequestId,
        router_run: RouterRunId,
        ending: StopReceiptEnding,
    },
}

impl StateTag for ApprovalState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Unraised => "unraised",
            Self::StoppedBeforeRaise { .. } => "stoppedBeforeRaise",
            Self::Waiting { status, .. } => match status {
                WaitingStatus::Pending => "pending",
                WaitingStatus::PassedOn => "passedOn",
            },
            Self::Erased { .. } => "erased",
            Self::Ended { closed, .. } => match closed.delivery {
                Delivery::NoHandoverOwed {} => "noHandoverOwed",
                Delivery::AwaitingReport {} => "awaitingReport",
                Delivery::Reported { .. } => "reported",
                Delivery::ReceiptUnknown {} => "receiptUnknown",
            },
        }
    }

    fn is_terminal(&self) -> bool {
        match self {
            Self::Unraised | Self::Waiting { .. } => false,
            Self::StoppedBeforeRaise { .. } | Self::Erased { .. } => true,
            Self::Ended { closed, .. } => match closed.delivery {
                Delivery::AwaitingReport {} => false,
                Delivery::NoHandoverOwed {}
                | Delivery::Reported { .. }
                | Delivery::ReceiptUnknown {} => true,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FactorStanding {
    Fresh,
    AbsentOrStale,
}

/// The applying caller has already authenticated this actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmittedDecider {
    Person {
        person_id: PersonId,
        factor: FactorStanding,
    },
    Session(SessionRef),
}

impl AdmittedDecider {
    pub(crate) fn is_destination(&self, destinations: &[ApprovalDestination]) -> bool {
        match self {
            Self::Person { .. } => destinations.contains(&ApprovalDestination::People),
            Self::Session(session) => destinations.iter().any(
                |destination| matches!(destination, ApprovalDestination::Session(target) if target == session)
            ),
        }
    }

    pub(crate) fn identity(&self) -> DecidedBy {
        match self {
            Self::Person { person_id, .. } => DecidedBy::Person {
                person_id: person_id.clone(),
            },
            Self::Session(session) => DecidedBy::Session {
                session: session.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PassOnAuthority {
    LiveParent(SessionRef),
    NotLiveParent,
}

/// Events for one request whose organization, enrollment and authority are already scoped.
/// The caller fences retired runs and supplies the destination and live-parent facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalEvent {
    Raise {
        raise: Box<Raise>,
        destinations: Vec<ApprovalDestination>,
    },
    Answer {
        actor: AdmittedDecider,
        answer: Answer,
    },
    PassOn {
        authority: PassOnAuthority,
    },
    Stop(StopRequest),
    ClockReached,
    RunRetired,
    SessionEnded,
    EnrollmentEnded,
    OfferAgain {
        router_run: RouterRunId,
    },
    Report(ReportDeliveryRequest),
    Erase,
}

impl EventTag for ApprovalEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Raise { .. } => "raise",
            Self::Answer { .. } => "answer",
            Self::PassOn { .. } => "passOn",
            Self::Stop(_) => "stop",
            Self::ClockReached => "clockReached",
            Self::RunRetired => "runRetired",
            Self::SessionEnded => "sessionEnded",
            Self::EnrollmentEnded => "enrollmentEnded",
            Self::OfferAgain { .. } => "offerAgain",
            Self::Report(_) => "report",
            Self::Erase => "erase",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionNoticePhase {
    Raised,
    Ended,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalReceipt {
    Raise(RaiseReply),
    Answer(EndingView),
    AnswerRefused(Refusal),
    PassedOn,
    PassOnRefused(Refusal),
    Stop(StopReply),
    Report(ReportDeliveryReply),
}

/// Data for the caller to commit or return with the next state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalEffect {
    Reply(ApprovalReceipt),
    StoreSessionNotice {
        request_id: ApprovalRequestId,
        session: SessionRef,
        phase: SessionNoticePhase,
    },
    RegisterDue {
        request_id: ApprovalRequestId,
        deadline: CentralTime,
    },
    ClearDue {
        request_id: ApprovalRequestId,
    },
    OfferEnding(Box<OfferEnding>),
    /// Settle an unpaid report in the same write that discards the retained request.
    SettleErasedDeliveryAsUnknown {
        request_id: ApprovalRequestId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ApprovalRefusal {
    Destination { refusal: Refusal },
    ContentsDiffer,
    ReportDiffers,
    NoHandoverOwed,
    DeadlineOutOfRange,
}

impl RefusalTag for ApprovalRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::Destination { .. } => "destination",
            Self::ContentsDiffer => "contentsDiffer",
            Self::ReportDiffers => "reportDiffers",
            Self::NoHandoverOwed => "noHandoverOwed",
            Self::DeadlineOutOfRange => "deadlineOutOfRange",
        }
    }
}
