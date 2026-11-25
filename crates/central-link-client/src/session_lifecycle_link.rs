//! The Mac's session lifecycle calls on its admitted machine connection.

use std::{future::Future, pin::Pin};

use central_link_protocol::{LinkCallRefusal, MachineLinkCloseCode};
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CheckedCreateReply, CreateFleetSession,
    EndFleetSession, EndSessionOutcome, EnrollmentId, OrganizationId,
};
use router_clock::ClockInstant;
use workspace_ids::MachineId;

use crate::{AdmittedConnection, ConnectionAttempt};

/// The exact enrollment under which a start or end has custody of Central work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionLifecycleScope {
    pub organization: OrganizationId,
    pub machine: MachineId,
    pub enrollment: EnrollmentId,
}

/// A momentary view of whether this enrollment can accept a lifecycle call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleLinkReadiness {
    Ready(AdmittedConnection),
    NotReady,
    EnrollmentEnded,
}

/// Evidence that no bytes for this call could have reached Central.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleCallNotSent {
    LinkNotReady,
    EnrollmentEnded,
    Busy,
    Deadline,
    InvalidFrame,
}

/// The link disposition is separate from the domain reply inside an answered call.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LifecycleCallError {
    #[error("the lifecycle call was not sent: {0:?}")]
    NotSent(LifecycleCallNotSent),
    /// A matched Central refusal. Authority lapse here is advisory until the link confirms it.
    #[error("Central refused the lifecycle call before apply: {0:?}")]
    RefusedBeforeApply(LinkCallRefusal),
    /// The call may have committed. Retry only by the domain operation's replay rule.
    #[error("the lifecycle call answer was lost")]
    AnswerLost,
    /// A matched response contradicted the expected call, target or bound address.
    #[error("Central's lifecycle reply contradicted the call")]
    ContradictedReply,
    /// The link confirmed enrollment retirement after the call could have been sent.
    #[error("the lifecycle call's enrollment ended: {0:?}")]
    EnrollmentEnded(MachineLinkCloseCode),
}

/// Completes with the checked reply or the transport evidence for this exact call.
pub type LifecycleCallFuture<'call, Reply> =
    Pin<Box<dyn Future<Output = Result<Reply, LifecycleCallError>> + Send + 'call>>;

/// Runs typed lifecycle calls beside the mail lane on the existing admitted socket.
///
/// Calls must use the supplied scope and physical connection, and may start
/// writing only before `send_before` on the injected clock. An implementation
/// checks the reply's connection, correlation, type and target before answering.
pub trait AdmittedLifecycleLink: Send + Sync {
    /// Observes admission for the given scope. Callers recheck it at dispatch.
    fn lifecycle_readiness(&self, scope: &SessionLifecycleScope) -> LifecycleLinkReadiness;

    /// Reserves or returns the same Central session according to the operation and attempt.
    fn create_fleet_session<'call>(
        &'call self,
        scope: &'call SessionLifecycleScope,
        connection: &'call ConnectionAttempt,
        request: CreateFleetSession,
        send_before: ClockInstant,
    ) -> LifecycleCallFuture<'call, CheckedCreateReply>;

    /// Binds the exact harness address. An Active reply must match that address.
    fn bind_fleet_session<'call>(
        &'call self,
        scope: &'call SessionLifecycleScope,
        connection: &'call ConnectionAttempt,
        request: BindFleetSession,
        send_before: ClockInstant,
    ) -> LifecycleCallFuture<'call, BindFleetSessionReply>;

    /// Settles an exact Central ID or abandons a reservation by its operation and digest.
    fn end_fleet_session<'call>(
        &'call self,
        scope: &'call SessionLifecycleScope,
        connection: &'call ConnectionAttempt,
        request: EndFleetSession,
        send_before: ClockInstant,
    ) -> LifecycleCallFuture<'call, EndSessionOutcome>;
}
