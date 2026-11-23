//! Route one session lifecycle call to its owning organization.

use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CheckedCreateReply, CreateFleetSession,
    CreateFleetSessionReply, CreateReplyContext, EndFleetSession, EndFleetSessionReply,
    EndReplyContext, EndSessionOutcome, OrganizationId, SessionReplyMismatch,
};
use fleet_rpc::{CorrelatedRpc, IndependentReplyRpc, Rpc};

/// Use the correlated exchange and dispatch to check caller-dependent replies.
///
/// ```compile_fail,E0277
/// use fleet_contracts::CreateFleetSessionRpc;
/// use fleet_rpc::IndependentReplyRpc;
/// fn accepts_independent_reply<Call: IndependentReplyRpc>() {}
/// accepts_independent_reply::<CreateFleetSessionRpc>();
/// ```
pub struct CreateFleetSessionRpc;

impl Rpc for CreateFleetSessionRpc {
    type Target = OrganizationId;
    type Request = CreateFleetSession;
    type Reply = CreateFleetSessionReply;
}

impl CorrelatedRpc for CreateFleetSessionRpc {
    type ReplyContext = CreateReplyContext;
    type CheckedReply = CheckedCreateReply;
    type InvalidReply = SessionReplyMismatch;

    fn reply_context(request: &Self::Request) -> Self::ReplyContext {
        CreateReplyContext::from_request(request)
    }

    fn check_reply(
        context: Self::ReplyContext,
        reply: Self::Reply,
    ) -> Result<Self::CheckedReply, Self::InvalidReply> {
        context.check(reply)
    }

    fn into_wire(checked: Self::CheckedReply) -> Self::Reply {
        checked.into_wire()
    }
}

pub struct BindFleetSessionRpc;

impl Rpc for BindFleetSessionRpc {
    type Target = OrganizationId;
    type Request = BindFleetSession;
    type Reply = BindFleetSessionReply;
}
impl IndependentReplyRpc for BindFleetSessionRpc {}

/// Use the correlated exchange and dispatch to check target-dependent replies.
///
/// ```compile_fail,E0277
/// use fleet_contracts::EndFleetSessionRpc;
/// use fleet_rpc::IndependentReplyRpc;
/// fn accepts_independent_reply<Call: IndependentReplyRpc>() {}
/// accepts_independent_reply::<EndFleetSessionRpc>();
/// ```
pub struct EndFleetSessionRpc;

impl Rpc for EndFleetSessionRpc {
    type Target = OrganizationId;
    type Request = EndFleetSession;
    type Reply = EndFleetSessionReply;
}

impl CorrelatedRpc for EndFleetSessionRpc {
    type ReplyContext = EndReplyContext;
    type CheckedReply = EndSessionOutcome;
    type InvalidReply = SessionReplyMismatch;

    fn reply_context(request: &Self::Request) -> Self::ReplyContext {
        EndReplyContext::from_request(request)
    }

    fn check_reply(
        context: Self::ReplyContext,
        reply: Self::Reply,
    ) -> Result<Self::CheckedReply, Self::InvalidReply> {
        context.check(reply)
    }

    fn into_wire(checked: Self::CheckedReply) -> Self::Reply {
        checked.into_wire()
    }
}

#[cfg(test)]
#[path = "session_lifecycle_rpc_test.rs"]
mod tests;

#[cfg(test)]
#[path = "session_reply_pairs_test.rs"]
mod reply_pair_tests;
