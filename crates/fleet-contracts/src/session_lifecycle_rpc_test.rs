use super::*;

fn require_organization_rpc<Call, Request, Reply>()
where
    Call: Rpc<Target = OrganizationId, Request = Request, Reply = Reply>,
{
}

#[test]
fn every_session_lifecycle_call_has_its_own_organization_rpc() {
    require_organization_rpc::<CreateFleetSessionRpc, CreateFleetSession, CreateFleetSessionReply>(
    );
    require_organization_rpc::<BindFleetSessionRpc, BindFleetSession, BindFleetSessionReply>();
    require_organization_rpc::<EndFleetSessionRpc, EndFleetSession, EndFleetSessionReply>();
}
