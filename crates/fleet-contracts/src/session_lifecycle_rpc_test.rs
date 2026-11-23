use crate::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc};
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    EndFleetSession, EndFleetSessionReply, OrganizationId,
};
use fleet_rpc::{Envelope, Rpc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

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

#[test]
fn every_session_request_and_reply_uses_a_named_organization_envelope() {
    let session = "019a3dce-2000-7000-8000-000000000001";
    let operation = "019a3dce-2000-7000-8000-000000000002";
    let secret = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    check_rpc::<CreateFleetSessionRpc>(
        json!({
            "operationId": operation,
            "attempt": 1,
            "creator": {"kind":"routerOperator"},
            "agentProgram": "codex",
            "sessionKind": {"kind":"ordinary"},
            "project":{"kind":"named","projectRef":{"workspace":"019a3dce-2000-7000-8000-000000000004","project":"019a3dce-2000-7000-8000-000000000005"}},
        }),
        json!({"result": "superseded"}),
    );
    check_rpc::<BindFleetSessionRpc>(
        json!({"sessionId":session,"harnessSessionId":"harness/thread","secret":secret}),
        json!({"result": "wrongSecret"}),
    );
    check_rpc::<EndFleetSessionRpc>(
        json!({"target":{"kind":"centralId","sessionId":session},"cause":{"kind":"givenUp"}}),
        json!({"result": "noSuchStart"}),
    );
}

fn check_rpc<Call: Rpc<Target = OrganizationId>>(request: Value, reply: Value) {
    check_envelope::<Call::Request>(request);
    check_envelope::<Call::Reply>(reply);
}

fn check_envelope<Body: DeserializeOwned + Serialize>(body: Value) {
    let target = "019a3dce-2000-7000-8000-000000000003";
    let wire = json!({"target":target,"body":body});
    let decoded: Envelope<OrganizationId, Body> = serde_json::from_str(&wire.to_string()).unwrap();

    assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
    for invalid in [
        json!([target, body]),
        json!([body, target]),
        json!({"target":target,"body":body,"machineId":target}),
        json!({"target":target,"body":body,"kind":"unknown"}),
        json!({"target":target,"body":body,"result":"unknown"}),
        json!({"target":target,"body":body,"state":"unknown"}),
        json!({"target":target}),
        json!({"body":body}),
        json!({"target":null,"body":body}),
        json!({"target":target,"body":null}),
        Value::Null,
    ] {
        assert!(
            serde_json::from_str::<Envelope<OrganizationId, Body>>(&invalid.to_string()).is_err(),
            "{invalid}"
        );
    }
}
