use fleet_identity_protocol::{
    BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply, EndFleetSessionReply,
    OrganizationId,
};
use fleet_rpc::Envelope;
use serde_json::{Value, json};

use super::{MacReplyExpectation, MacReplyMismatch, MacWorkCall, MacWorkRpc, NonMacWorkCall};
use crate::{
    CentralCall, CentralLinkRpc,
    approval_rpc::{
        DecideApprovalAsSessionRpc, ListApprovalsForSessionRpc, PassOnApprovalForSessionRpc,
        RaiseApprovalRpc, ReadApprovalForSessionRpc, StopApprovalRpc,
    },
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc},
};

const ORGANIZATION: &str = "019f0000-0000-7000-8000-000000000001";
const SESSION: &str = "019f0000-0000-7000-8000-000000000002";
const OTHER_ORGANIZATION: &str = "019f0000-0000-7000-8000-000000000003";

fn call(name: &str, target: Value, body: Value) -> CentralCall {
    serde_json::from_value(json!({"call": name, "envelope": {"target": target, "body": body}}))
        .unwrap()
}

#[test]
fn the_work_entry_exposes_the_nine_mac_markers() {
    fn admitted<Marker: MacWorkRpc>() {}

    admitted::<RaiseApprovalRpc>();
    admitted::<StopApprovalRpc>();
    admitted::<ListApprovalsForSessionRpc>();
    admitted::<ReadApprovalForSessionRpc>();
    admitted::<DecideApprovalAsSessionRpc>();
    admitted::<PassOnApprovalForSessionRpc>();
    admitted::<CreateFleetSessionRpc>();
    admitted::<BindFleetSessionRpc>();
    admitted::<EndFleetSessionRpc>();
}

#[test]
fn the_wire_classifier_identifies_driver_and_person_calls() {
    let registration = call(
        "registerRun",
        json!({"organizationId": ORGANIZATION}),
        json!({"routerRun": SESSION}),
    );
    let person = call(
        "listOrganizationWorkspaces",
        json!(ORGANIZATION),
        json!({"operationId": SESSION, "after": null, "limit": 7}),
    );

    assert!(matches!(
        MacReplyExpectation::from_request(&registration),
        Err(NonMacWorkCall::Registration)
    ));
    assert!(matches!(
        MacReplyExpectation::from_request(&person),
        Err(NonMacWorkCall::Person)
    ));
}

#[test]
fn create_checks_the_whole_reply_and_retains_the_expectation_after_each_mismatch() {
    let target: OrganizationId = ORGANIZATION.to_owned().try_into().unwrap();
    let request: CreateFleetSession = serde_json::from_value(json!({
        "operationId": SESSION, "attempt": 1, "creator": {"kind": "routerOperator"},
        "agentProgram": "codex", "sessionKind": {"kind": "ordinary"}
    }))
    .unwrap();
    let expected = CreateFleetSessionRpc::request(Envelope {
        target: target.clone(),
        body: request.clone(),
    });
    let (wire, check) = CreateFleetSessionRpc::work_request(target.clone(), request)
        .prepare()
        .unwrap();
    assert_eq!(wire, expected);

    let wrong_kind = BindFleetSessionRpc::reply(Envelope {
        target: target.clone(),
        body: BindFleetSessionReply::NoSuchStart {},
    });
    let wrong_target = CreateFleetSessionRpc::reply(Envelope {
        target: OTHER_ORGANIZATION.to_owned().try_into().unwrap(),
        body: CreateFleetSessionReply::Superseded {},
    });
    let invalid_for_operator = CreateFleetSessionRpc::reply(Envelope {
        target: target.clone(),
        body: CreateFleetSessionReply::NoSecret {},
    });

    assert_eq!(
        check.check(wrong_kind).unwrap_err(),
        MacReplyMismatch::WrongKind
    );
    assert_eq!(
        check.check(wrong_target).unwrap_err(),
        MacReplyMismatch::WrongTarget
    );
    assert_eq!(
        check.check(invalid_for_operator).unwrap_err(),
        MacReplyMismatch::InvalidForRequest
    );
    let fitting = CreateFleetSessionReply::Superseded {};
    let checked = check
        .check(CreateFleetSessionRpc::reply(Envelope {
            target,
            body: fitting.clone(),
        }))
        .unwrap();
    assert_eq!(
        CreateFleetSessionRpc::take_work_reply(checked),
        Some(fitting)
    );
}

#[test]
fn end_rejects_a_matching_envelope_whose_body_cannot_answer_the_selected_target() {
    let request = call(
        "endFleetSession",
        json!(ORGANIZATION),
        json!({"target":{"kind":"centralId","sessionId":SESSION},"cause":{"kind":"givenUp"}}),
    );
    let (wire, check) = MacWorkCall(request.clone()).prepare().unwrap();
    assert_eq!(wire, request);
    let invalid_for_central_id = EndFleetSessionRpc::reply(Envelope {
        target: ORGANIZATION.to_owned().try_into().unwrap(),
        body: EndFleetSessionReply::NotYoursToEnd {},
    });
    assert_eq!(
        check.check(invalid_for_central_id).unwrap_err(),
        MacReplyMismatch::InvalidForRequest
    );

    let fitting = EndFleetSessionReply::NoSuchStart {};
    let checked = check
        .check(EndFleetSessionRpc::reply(Envelope {
            target: ORGANIZATION.to_owned().try_into().unwrap(),
            body: fitting.clone(),
        }))
        .unwrap();
    assert_eq!(EndFleetSessionRpc::take_work_reply(checked), Some(fitting));
}
