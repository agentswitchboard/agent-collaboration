//! Rust-only projection of the nine Central calls a Mac can start as work.
#![deny(clippy::wildcard_enum_match_arm)]

use std::fmt;

use fleet_rpc::{CorrelatedRpc, Envelope, IndependentReplyRpc};

use crate::{
    CentralCall, CentralCallReply, CentralLinkRpc,
    approval_rpc::{
        DecideApprovalAsSessionRpc, ListApprovalsForSessionRpc, PassOnApprovalForSessionRpc,
        RaiseApprovalRpc, ReadApprovalForSessionRpc, StopApprovalRpc,
    },
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc},
};

mod sealed {
    pub trait Work {}
}

/// A registered RPC that the Mac's work lane can start.
///
/// ```compile_fail,E0277
/// use central_link_protocol::{MacWorkRpc, approval_rpc::RegisterRunRpc};
/// fn mac_work<Rpc: MacWorkRpc>() {}
/// mac_work::<RegisterRunRpc>();
/// ```
///
/// ```compile_fail,E0277
/// use central_link_protocol::{MacWorkRpc, person_link_rpc::ListPersonApprovalsRpc};
/// fn mac_work<Rpc: MacWorkRpc>() {}
/// mac_work::<ListPersonApprovalsRpc>();
/// ```
pub trait MacWorkRpc: CentralLinkRpc + sealed::Work {
    fn work_request(target: Self::Target, body: Self::Request) -> MacWorkCall;

    fn take_work_reply(reply: MacWorkReply) -> Option<Self::Reply> {
        Self::take_reply(reply.0).map(|envelope| envelope.body)
    }
}

/// The owner of a Central call outside the Mac work lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NonMacWorkCall {
    #[error("run registration belongs to the link driver")]
    Registration,
    #[error("this call belongs to the person link")]
    Person,
}

/// A bounded diagnostic for a reply that must leave its call waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MacReplyMismatch {
    #[error("reply kind does not match the call")]
    WrongKind,
    #[error("reply target does not match the call")]
    WrongTarget,
    #[error("reply is invalid for the originating request")]
    InvalidForRequest,
}

/// A reply whose kind, target, and request-dependent constraints have been checked.
pub struct MacWorkReply(CentralCallReply);

impl fmt::Debug for MacWorkReply {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MacWorkReply")
    }
}

macro_rules! mac_work_calls {
    ($( $variant:ident($marker:ty, $check:ident) ),+ $(,)?) => {
        /// A Rust-only request for the Mac work lane.
        pub struct MacWorkCall(CentralCall);

        /// The reply constraints captured from a request before it enters the writer.
        pub struct MacReplyExpectation(WorkReplyCheck);

        enum WorkReplyCheck {
            $( $variant($check<$marker>), )+
        }

        impl MacWorkCall {
            /// Build the wire request and the expectation at the same boundary.
            pub fn prepare(self) -> Result<(CentralCall, MacReplyExpectation), NonMacWorkCall> {
                let expectation = MacReplyExpectation::from_request(&self.0)?;
                Ok((self.0, expectation))
            }
        }

        impl MacReplyExpectation {
            pub fn check(&self, reply: CentralCallReply) -> Result<MacWorkReply, MacReplyMismatch> {
                match &self.0 {
                    $( WorkReplyCheck::$variant(check) => check.check(reply), )+
                }.map(MacWorkReply)
            }
        }

        $(
            impl sealed::Work for $marker {}

            impl MacWorkRpc for $marker {
                fn work_request(target: Self::Target, body: Self::Request) -> MacWorkCall {
                    MacWorkCall(CentralCall::$variant(Envelope { target, body }))
                }
            }
        )+
    };
}

mac_work_calls! {
    Raise(RaiseApprovalRpc, IndependentCheck),
    Stop(StopApprovalRpc, IndependentCheck),
    ListApprovalsForSession(ListApprovalsForSessionRpc, IndependentCheck),
    ReadApprovalForSession(ReadApprovalForSessionRpc, IndependentCheck),
    DecideApprovalAsSession(DecideApprovalAsSessionRpc, IndependentCheck),
    PassOnApproval(PassOnApprovalForSessionRpc, IndependentCheck),
    CreateFleetSession(CreateFleetSessionRpc, CorrelatedCheck),
    BindFleetSession(BindFleetSessionRpc, IndependentCheck),
    EndFleetSession(EndFleetSessionRpc, CorrelatedCheck),
}

impl MacReplyExpectation {
    fn from_request(call: &CentralCall) -> Result<Self, NonMacWorkCall> {
        use WorkReplyCheck as Check;

        let check = match call {
            CentralCall::Raise(request) => Check::Raise(IndependentCheck::from_request(request)),
            CentralCall::Stop(request) => Check::Stop(IndependentCheck::from_request(request)),
            CentralCall::ListApprovalsForSession(request) => {
                Check::ListApprovalsForSession(IndependentCheck::from_request(request))
            }
            CentralCall::ReadApprovalForSession(request) => {
                Check::ReadApprovalForSession(IndependentCheck::from_request(request))
            }
            CentralCall::DecideApprovalAsSession(request) => {
                Check::DecideApprovalAsSession(IndependentCheck::from_request(request))
            }
            CentralCall::PassOnApproval(request) => {
                Check::PassOnApproval(IndependentCheck::from_request(request))
            }
            CentralCall::CreateFleetSession(request) => {
                Check::CreateFleetSession(CorrelatedCheck::from_request(request))
            }
            CentralCall::BindFleetSession(request) => {
                Check::BindFleetSession(IndependentCheck::from_request(request))
            }
            CentralCall::EndFleetSession(request) => {
                Check::EndFleetSession(CorrelatedCheck::from_request(request))
            }
            CentralCall::RegisterRun(_) => return Err(NonMacWorkCall::Registration),
            CentralCall::ListOrganizationWorkspaces(_)
            | CentralCall::CreateOrganizationWorkspace(_)
            | CentralCall::LookupOrganizationWorkspace(_)
            | CentralCall::ReadOrganizationChangeRecords(_)
            | CentralCall::ListPersonApprovals(_)
            | CentralCall::ReadPersonApproval(_)
            | CentralCall::DecidePersonApproval(_) => return Err(NonMacWorkCall::Person),
        };
        Ok(Self(check))
    }
}

struct IndependentCheck<Marker: IndependentReplyRpc> {
    target: Marker::Target,
}

impl<Marker: CentralLinkRpc + IndependentReplyRpc> IndependentCheck<Marker> {
    fn from_request(request: &Envelope<Marker::Target, Marker::Request>) -> Self {
        Self {
            target: request.target.clone(),
        }
    }

    fn check(&self, reply: CentralCallReply) -> Result<CentralCallReply, MacReplyMismatch> {
        let envelope = matching_envelope::<Marker>(&self.target, reply)?;
        Ok(Marker::reply(envelope))
    }
}

struct CorrelatedCheck<Marker: CorrelatedRpc> {
    target: Marker::Target,
    context: Marker::ReplyContext,
}

impl<Marker: CentralLinkRpc + CorrelatedRpc> CorrelatedCheck<Marker>
where
    Marker::ReplyContext: Clone,
{
    fn from_request(request: &Envelope<Marker::Target, Marker::Request>) -> Self {
        Self {
            target: request.target.clone(),
            context: Marker::reply_context(&request.body),
        }
    }

    fn check(&self, reply: CentralCallReply) -> Result<CentralCallReply, MacReplyMismatch> {
        let envelope = matching_envelope::<Marker>(&self.target, reply)?;
        let checked = Marker::check_reply(self.context.clone(), envelope.body)
            .map_err(|_| MacReplyMismatch::InvalidForRequest)?;
        Ok(Marker::reply(Envelope {
            target: envelope.target,
            body: Marker::into_wire(checked),
        }))
    }
}

fn matching_envelope<Marker: CentralLinkRpc>(
    expected: &Marker::Target,
    reply: CentralCallReply,
) -> Result<Envelope<Marker::Target, Marker::Reply>, MacReplyMismatch> {
    let envelope = Marker::take_reply(reply).ok_or(MacReplyMismatch::WrongKind)?;
    if &envelope.target != expected {
        return Err(MacReplyMismatch::WrongTarget);
    }
    Ok(envelope)
}

#[cfg(test)]
#[path = "mac_work_call_test.rs"]
mod tests;
