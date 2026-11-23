use std::fmt;

use approval_request_protocol::{
    Answer, ApprovalWindow, CallStopCause, CentralTime, DecideRequest, DecidedBy,
    DeliverEndingReply, DeliverEndingRequest, Delivery, EndingView, HolderStopCause, PassOnReply,
    ProviderStopCause, Raise, RaiseReply, RequestContentsDigest, RequestView, StopReply,
    StopRequest, WaitingStatus,
};
use central_engine_model::{EventTag, RefusalTag, StateTag};

use fleet_identity_protocol::EnrollmentId;
use serde::{Deserialize, Serialize};
use workspace_ids::{
    ApprovalRequestId, CentralSessionId, MachineId, PersonId, RequestId, RouterRunId,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApprovalEnrollment {
    pub machine_id: MachineId,
    pub enrollment_id: EnrollmentId,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApprovalRun {
    pub enrollment: ApprovalEnrollment,
    pub router_run: RouterRunId,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApprovalRequestKey {
    pub run: ApprovalRun,
    pub request_id: ApprovalRequestId,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApprovalSession {
    pub enrollment: ApprovalEnrollment,
    pub session_id: CentralSessionId,
}

/// The initial audience resolved from the requesting session's Central record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalRouting {
    Owner,
    Delegate { admitted_parent: ApprovalSession },
}

/// A forward can add another parent or reach the people destination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalRouteDestination {
    Session(ApprovalSession),
    People,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ApprovalDestinations {
    Session {
        previous: Vec<ApprovalSession>,
        current: ApprovalSession,
    },
    People {
        previous: Vec<ApprovalSession>,
    },
}

impl ApprovalDestinations {
    pub(crate) fn initial(route: ApprovalRouting) -> Self {
        match route {
            ApprovalRouting::Owner => Self::People {
                previous: Vec::new(),
            },
            ApprovalRouting::Delegate { admitted_parent } => Self::Session {
                previous: Vec::new(),
                current: admitted_parent,
            },
        }
    }

    pub(crate) fn current_session(&self) -> Option<&ApprovalSession> {
        match self {
            Self::Session { current, .. } => Some(current),
            Self::People { .. } => None,
        }
    }

    pub(crate) fn previous(&self) -> &[ApprovalSession] {
        match self {
            Self::Session { previous, .. } | Self::People { previous } => previous,
        }
    }

    pub(crate) fn sessions(&self) -> impl Iterator<Item = &ApprovalSession> {
        self.previous().iter().chain(self.current_session())
    }

    pub(crate) fn waiting_status(&self) -> WaitingStatus {
        if self.previous().is_empty() {
            WaitingStatus::Pending
        } else {
            WaitingStatus::PassedOn
        }
    }

    pub(crate) fn add(
        &self,
        from: &ApprovalSession,
        to: &ApprovalRouteDestination,
    ) -> Option<Self> {
        let Self::Session { previous, current } = self else {
            return None;
        };
        if current != from {
            return None;
        }
        let mut previous = previous.clone();
        previous.push(current.clone());
        match to {
            ApprovalRouteDestination::Session(session)
                if !previous
                    .iter()
                    .any(|scope| scope.session_id == session.session_id) =>
            {
                Some(Self::Session {
                    previous,
                    current: session.clone(),
                })
            }
            ApprovalRouteDestination::Session(_) => None,
            ApprovalRouteDestination::People => Some(Self::People { previous }),
        }
    }
}

/// The applying caller has already authenticated this actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmittedDecider {
    Person { person_id: PersonId },
    Session(ApprovalSession),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalActor {
    Person(PersonId),
    Session(ApprovalSession),
}

impl AdmittedDecider {
    pub(crate) fn actor(&self) -> ApprovalActor {
        match self {
            Self::Person { person_id, .. } => ApprovalActor::Person(person_id.clone()),
            Self::Session(session) => ApprovalActor::Session(session.clone()),
        }
    }
}

impl ApprovalActor {
    pub(crate) fn identity(&self) -> DecidedBy {
        match self {
            Self::Person(person_id) => DecidedBy::Person {
                person_id: person_id.clone(),
            },
            Self::Session(session) => DecidedBy::Session {
                session: session.session_id.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedApproval {
    pub key: ApprovalRequestKey,
    pub raise: Raise,
    pub contents_digest: RequestContentsDigest,
    pub window: ApprovalWindow,
    pub(crate) destinations: ApprovalDestinations,
}

impl RetainedApproval {
    pub fn waiting_status(&self) -> WaitingStatus {
        self.destinations.waiting_status()
    }

    pub fn initial_routing(&self) -> ApprovalRouting {
        self.destinations
            .sessions()
            .next()
            .map_or(ApprovalRouting::Owner, |parent| ApprovalRouting::Delegate {
                admitted_parent: parent.clone(),
            })
    }

    pub fn provider_session(&self) -> Option<ApprovalSession> {
        self.raise
            .requesting_session()
            .map(|session_id| ApprovalSession {
                enrollment: self.key.run.enrollment.clone(),
                session_id: session_id.clone(),
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndedApproval {
    pub ending: DeliverEndingRequest,
    pub ended_at: CentralTime,
    pub delivery: Delivery,
    pub decided_operation_id: Option<RequestId>,
}

/// The Stop family and cause retained before Central has a Raise to match.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopMarkerCause {
    Provider(ProviderStopCause),
    UnprovenCall(CallStopCause),
}

impl StopMarkerCause {
    #[must_use]
    pub const fn holder_cause(self) -> HolderStopCause {
        match self {
            Self::Provider(cause) => cause.holder_cause(),
            Self::UnprovenCall(cause) => HolderStopCause::from_call(cause),
        }
    }
}

impl From<&StopRequest> for StopMarkerCause {
    fn from(stop: &StopRequest) -> Self {
        match stop {
            StopRequest::Provider { cause, .. } => Self::Provider(*cause),
            StopRequest::UnprovenCall { cause, .. } => Self::UnprovenCall(*cause),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalState {
    Unraised,
    StoppedBeforeRaise {
        key: ApprovalRequestKey,
        contents_digest: RequestContentsDigest,
        cause: StopMarkerCause,
    },
    Waiting {
        saved: Box<RetainedApproval>,
    },
    Ended {
        saved: Box<RetainedApproval>,
        closed: EndedApproval,
    },
}

impl ApprovalState {
    pub fn key(&self) -> Option<&ApprovalRequestKey> {
        match self {
            Self::Unraised => None,
            Self::StoppedBeforeRaise { key, .. } => Some(key),
            Self::Waiting { saved } | Self::Ended { saved, .. } => Some(&saved.key),
        }
    }

    pub fn retained(&self) -> Option<&RetainedApproval> {
        match self {
            Self::Waiting { saved } | Self::Ended { saved, .. } => Some(saved),
            Self::Unraised | Self::StoppedBeforeRaise { .. } => None,
        }
    }

    pub fn occupies_capacity(&self) -> bool {
        match self {
            Self::Waiting { .. } => true,
            Self::Ended { closed, .. } => closed.delivery == (Delivery::Owed {}),
            Self::Unraised | Self::StoppedBeforeRaise { .. } => false,
        }
    }
}

impl StateTag for ApprovalState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Unraised => "unraised",
            Self::StoppedBeforeRaise { .. } => "stoppedBeforeRaise",
            Self::Waiting { saved } => match saved.waiting_status() {
                WaitingStatus::Pending => "pending",
                WaitingStatus::PassedOn => "passedOn",
            },
            Self::Ended { closed, .. } => match closed.delivery {
                Delivery::Owed {} => "owed",
                Delivery::ReceivedByHolder {} => "receivedByHolder",
                Delivery::ClosedWithoutReceipt {} => "closedWithoutReceipt",
            },
        }
    }

    fn is_terminal(&self) -> bool {
        match self {
            Self::Unraised | Self::Waiting { .. } => false,
            Self::Ended { closed, .. } => closed.delivery != (Delivery::Owed {}),
            Self::StoppedBeforeRaise { .. } => true,
        }
    }
}

/// A destination payload can be released only for the request and recipient it was built for.
#[derive(Clone, Eq, PartialEq)]
pub struct DestinationReply<Payload> {
    request: ApprovalRequestKey,
    recipient: DecidedBy,
    payload: Payload,
}

impl<Payload> DestinationReply<Payload> {
    pub(crate) fn new(
        request: &ApprovalRequestKey,
        recipient: DecidedBy,
        payload: Payload,
    ) -> Self {
        Self {
            request: request.clone(),
            recipient,
            payload,
        }
    }

    pub fn request(&self) -> &ApprovalRequestKey {
        &self.request
    }

    pub fn recipient(&self) -> &DecidedBy {
        &self.recipient
    }

    pub fn for_recipient(
        &self,
        request: &ApprovalRequestKey,
        recipient: &DecidedBy,
    ) -> Result<&Payload, DestinationReplyMismatch> {
        if &self.request != request {
            return Err(DestinationReplyMismatch::Request);
        }
        if &self.recipient != recipient {
            return Err(DestinationReplyMismatch::Recipient);
        }
        Ok(&self.payload)
    }
}

impl<Payload> fmt::Debug for DestinationReply<Payload> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DestinationReply")
            .field("request", &self.request)
            .field("recipient", &self.recipient)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DestinationReplyMismatch {
    Request,
    Recipient,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderSessionAvailability {
    Live,
    NotAvailable,
    SessionFree,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RaiseAdmission {
    pub provider_session: ProviderSessionAvailability,
    pub outstanding: usize,
}

/// Shared applying input. The parent-session wire can supply only provider answers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalDecideInput {
    pub request_id: ApprovalRequestId,
    pub operation_id: RequestId,
    pub value: Answer,
}

impl From<DecideRequest> for ApprovalDecideInput {
    fn from(request: DecideRequest) -> Self {
        Self {
            request_id: request.request_id,
            operation_id: request.operation_id,
            value: request.value.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalEvent {
    Raise {
        key: ApprovalRequestKey,
        raise: Box<Raise>,
        routing: ApprovalRouting,
        admission: RaiseAdmission,
    },
    Decide {
        key: ApprovalRequestKey,
        actor: AdmittedDecider,
        request: ApprovalDecideInput,
    },
    Read {
        key: ApprovalRequestKey,
        actor: ApprovalActor,
    },
    PassOn {
        key: ApprovalRequestKey,
        live_parent: ApprovalSession,
        to: ApprovalRouteDestination,
    },
    Stop {
        key: ApprovalRequestKey,
        request: StopRequest,
    },
    Alarm {
        deadline: CentralTime,
    },
    RunRetired,
    SessionEnded,
    EnrollmentEnded,
    Reoffer,
    DeliveryReply {
        key: ApprovalRequestKey,
        request: Box<DeliverEndingRequest>,
        reply: DeliverEndingReply,
    },
}

impl EventTag for ApprovalEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            ApprovalEvent::Raise { .. } => "raise",
            ApprovalEvent::Decide { .. } => "decide",
            ApprovalEvent::Read { .. } => "read",
            ApprovalEvent::PassOn { .. } => "passOn",
            ApprovalEvent::Stop { .. } => "stop",
            ApprovalEvent::Alarm { .. } => "alarm",
            ApprovalEvent::RunRetired => "runRetired",
            ApprovalEvent::SessionEnded => "sessionEnded",
            ApprovalEvent::EnrollmentEnded => "enrollmentEnded",
            ApprovalEvent::Reoffer => "reoffer",
            ApprovalEvent::DeliveryReply { .. } => "deliveryReply",
        }
    }
}

/// An applying result for a person or session. The owning route uses its checked wire builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalDecisionReply {
    SavedDecision { answer: Answer, by: DecidedBy },
    Standing { ending: EndingView },
    NotVisible {},
    Invalid {},
    TooLate {},
    OperationConflict {},
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalReceipt {
    Raise(RaiseReply),
    Decide(Box<DestinationReply<ApprovalDecisionReply>>),
    Read(Box<DestinationReply<Option<RequestView>>>),
    PassOn(Box<DestinationReply<PassOnReply>>),
    Stop(StopReply),
}

impl ApprovalReceipt {
    pub(crate) fn decide_for(
        key: &ApprovalRequestKey,
        recipient: DecidedBy,
        payload: ApprovalDecisionReply,
    ) -> Self {
        Self::Decide(Box::new(DestinationReply::new(key, recipient, payload)))
    }

    pub(crate) fn read_for(
        key: &ApprovalRequestKey,
        recipient: DecidedBy,
        payload: Option<RequestView>,
    ) -> Self {
        Self::Read(Box::new(DestinationReply::new(key, recipient, payload)))
    }

    pub(crate) fn pass_on_for(
        key: &ApprovalRequestKey,
        recipient: DecidedBy,
        payload: PassOnReply,
    ) -> Self {
        Self::PassOn(Box::new(DestinationReply::new(key, recipient, payload)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalEffect {
    Reply(ApprovalReceipt),
    StoreSessionNotice {
        key: ApprovalRequestKey,
        session: ApprovalSession,
    },
    RegisterDue {
        key: ApprovalRequestKey,
        deadline: CentralTime,
    },
    ClearDue {
        key: ApprovalRequestKey,
    },
    OfferEnding {
        holder: ApprovalRun,
        request: Box<DeliverEndingRequest>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ApprovalRefusal {
    InvalidRetainedEnding,
    InvalidDigest,
    DeadlineOutOfRange,
    ApplyingTimeOutOfRange,
    InvalidInitialRoute,
}

impl RefusalTag for ApprovalRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::InvalidRetainedEnding => "invalid_retained_ending",
            Self::InvalidDigest => "invalid_digest",
            Self::DeadlineOutOfRange => "deadline_out_of_range",
            Self::ApplyingTimeOutOfRange => "applying_time_out_of_range",
            Self::InvalidInitialRoute => "invalid_initial_route",
        }
    }
}
