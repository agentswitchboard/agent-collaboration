//! Route one session lifecycle call to its owning organization.

use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    EndFleetSession, EndFleetSessionReply, OrganizationId,
};
use fleet_rpc::Rpc;

pub struct CreateFleetSessionRpc;

impl Rpc for CreateFleetSessionRpc {
    type Target = OrganizationId;
    type Request = CreateFleetSession;
    type Reply = CreateFleetSessionReply;
}

pub struct BindFleetSessionRpc;

impl Rpc for BindFleetSessionRpc {
    type Target = OrganizationId;
    type Request = BindFleetSession;
    type Reply = BindFleetSessionReply;
}

pub struct EndFleetSessionRpc;

impl Rpc for EndFleetSessionRpc {
    type Target = OrganizationId;
    type Request = EndFleetSession;
    type Reply = EndFleetSessionReply;
}

#[cfg(test)]
#[path = "session_lifecycle_rpc_test.rs"]
mod tests;
