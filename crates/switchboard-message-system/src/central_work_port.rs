//! Typed session and approval calls through the message system's current generation.

use central_link_protocol::{LinkCallRefusal, MacWorkRpc};
use router_clock::ClockInstant;
use switchboard_central_link::{
    central_call_lane::{
        CallAdmissionRefusal, CallSettlement, CentralCallHandle, CentralCallSubmission,
    },
    link_policy::LinkStatus,
};
use switchboard_machine_enrollment::EnrollmentEnd;
use tokio::sync::watch;

use crate::{MessageSystemObservation, MessageSystemReason, MessageSystemStatus, State};

pub use switchboard_central_link::central_call_lane::{CallEnding, CallNotReached};

/// The five ways a Central work call can end.
pub enum CentralWorkOutcome<Reply> {
    Answered(Reply),
    RefusedByCentral(LinkCallRefusal),
    RefusedByLink(CentralWorkRefusal),
    NotReached(CallNotReached),
    AnswerLost(CallEnding),
}

/// A link refusal captures the state and reason from the submission turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CentralWorkRefusal {
    pub status: CentralWorkRefusalStatus,
    pub reason: MessageSystemReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CentralWorkRefusalStatus {
    Starting,
    Connecting,
    Registering,
    Restarting,
    RegistrationFailed,
    Fault,
    EnrollmentUnreadable,
    NotEnrolled,
    EnrollmentEnded,
    EnrollmentMismatch,
    RunRetired,
    Stopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CentralWorkNextMove {
    Wait,
    EnrollThisMac,
    EnrollThisMacAgain,
    NoneForThisRun,
}

impl CentralWorkRefusal {
    #[must_use]
    pub const fn valid_move(self) -> CentralWorkNextMove {
        use CentralWorkRefusalStatus as Status;

        match self.status {
            Status::Starting
            | Status::Connecting
            | Status::Registering
            | Status::Restarting
            | Status::RegistrationFailed
            | Status::Fault
            | Status::EnrollmentUnreadable => CentralWorkNextMove::Wait,
            Status::NotEnrolled => CentralWorkNextMove::EnrollThisMac,
            Status::EnrollmentEnded | Status::EnrollmentMismatch => {
                CentralWorkNextMove::EnrollThisMacAgain
            }
            Status::RunRetired | Status::Stopping | Status::Stopped => {
                CentralWorkNextMove::NoneForThisRun
            }
        }
    }

    fn project(context: &WorkCallContext, link: &LinkStatus, gate: CallAdmissionRefusal) -> Self {
        use CentralWorkRefusalStatus as Refusal;
        use MessageSystemStatus as Status;

        if gate == CallAdmissionRefusal::EnrollmentEnded {
            return Self {
                status: Refusal::EnrollmentEnded,
                reason: context.stored_ending.map_or(
                    MessageSystemReason::EnrollmentEndingPending,
                    MessageSystemReason::stored_ending,
                ),
            };
        }
        let observation =
            MessageSystemObservation::project(&context.supervisor, link, context.stored_ending);
        let status = match observation.status {
            Status::Starting => Refusal::Starting,
            Status::Connecting => Refusal::Connecting,
            Status::Registering => Refusal::Registering,
            Status::Restarting => Refusal::Restarting,
            Status::RegistrationFailed => Refusal::RegistrationFailed,
            Status::Fault => Refusal::Fault,
            Status::EnrollmentUnreadable => Refusal::EnrollmentUnreadable,
            Status::NotEnrolled => Refusal::NotEnrolled,
            Status::EnrollmentEnded => Refusal::EnrollmentEnded,
            Status::EnrollmentMismatch => Refusal::EnrollmentMismatch,
            Status::RunRetired => Refusal::RunRetired,
            Status::Stopping => Refusal::Stopping,
            Status::Stopped => Refusal::Stopped,
            Status::Connected => {
                return Self {
                    status: Refusal::Connecting,
                    reason: MessageSystemReason::NoReason,
                };
            }
        };
        Self {
            status,
            reason: observation.reason,
        }
    }
}

pub(crate) struct WorkCallContext {
    pub(crate) supervisor: State,
    pub(crate) lane: Option<CentralCallHandle>,
    pub(crate) stored_ending: Option<EnrollmentEnd>,
}

/// Submits one of the nine Mac work RPCs through the current registered link.
#[derive(Clone)]
pub struct CentralWorkPort {
    context: watch::Receiver<WorkCallContext>,
    link: watch::Receiver<LinkStatus>,
}

impl CentralWorkPort {
    pub(crate) fn new(
        context: watch::Receiver<WorkCallContext>,
        link: watch::Receiver<LinkStatus>,
    ) -> Self {
        Self { context, link }
    }

    pub async fn call<Rpc: MacWorkRpc>(
        &self,
        target: Rpc::Target,
        request: Rpc::Request,
        deadline: ClockInstant,
    ) -> CentralWorkOutcome<Rpc::Reply> {
        let call = Rpc::work_request(target, request);
        let receipt = {
            // These borrows pin the supervisor generation and its live link
            // observation across the synchronous admission decision.
            let context = self.context.borrow();
            let link = self.link.borrow();
            let submission = match &context.lane {
                Some(lane) => lane.submit(call, deadline),
                None => CentralCallSubmission::RefusedByLink(CallAdmissionRefusal::Unavailable),
            };
            match submission {
                CentralCallSubmission::Accepted(receipt) => receipt,
                CentralCallSubmission::NotReached(reason) => {
                    return CentralWorkOutcome::NotReached(reason);
                }
                CentralCallSubmission::RefusedByLink(gate) => {
                    return CentralWorkOutcome::RefusedByLink(CentralWorkRefusal::project(
                        &context, &link, gate,
                    ));
                }
            }
        };
        match receipt.resolve().await {
            CallSettlement::Answered(reply) => match Rpc::take_work_reply(*reply) {
                Some(reply) => CentralWorkOutcome::Answered(reply),
                None => {
                    tracing::error!("verified Central work reply did not match its submitted RPC");
                    CentralWorkOutcome::AnswerLost(CallEnding::InvalidFrame)
                }
            },
            CallSettlement::RefusedByCentral(reason) => {
                CentralWorkOutcome::RefusedByCentral(reason)
            }
            CallSettlement::NotReached(reason) => CentralWorkOutcome::NotReached(reason),
            CallSettlement::AnswerLost(cause) => CentralWorkOutcome::AnswerLost(cause),
        }
    }
}

#[cfg(test)]
#[path = "central_work_port_test.rs"]
mod tests;
