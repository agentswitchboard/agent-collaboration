//! C3 fixtures shared by the Rust wire reader and the generated TypeScript.

use super::{Deserialize, Serialize, WireSpecimen};
use fleet_signin::{
    FleetPermission,
    approval_person_wire::{ApprovalPersonOperation, ApprovalPersonRoute},
};
use serde_json::{Value, json};

pub fn approval_wire_requests(name: &str) -> Vec<WireSpecimen> {
    let values = match name {
        "FleetPermission" => [
            FleetPermission::CreateWorkspace,
            FleetPermission::AllowMachine,
            FleetPermission::WithdrawMachine,
            FleetPermission::RemoveSession,
            FleetPermission::EnrollMachine,
            FleetPermission::RevokeMachine,
            FleetPermission::SetMachineTokenLifetime,
            FleetPermission::ViewOrganization,
            FleetPermission::ReadOrganizationAudit,
            FleetPermission::DecideApproval,
        ]
        .into_iter()
        .map(|permission| json!(permission.as_str()))
        .collect(),
        "ApprovalListRequest" => {
            let mut first = intent_vectors()[0]["body"].clone();
            first.as_object_mut().unwrap().remove("cursor");
            vec![first, intent_vectors()[0]["body"].clone()]
        }
        "ApprovalReadRequest" => vec![intent_vectors()[1]["body"].clone()],
        "ApprovalDecideRequest" => vec![intent_vectors()[2]["body"].clone()],
        _ => panic!("missing request fixtures for {name}"),
    };
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| WireSpecimen {
            phase: Deserialize,
            label: format!("{name} {}", index + 1),
            value,
        })
        .collect()
}

pub fn approval_wire_replies(name: &str) -> Vec<WireSpecimen> {
    let values: Vec<Value> = if name == "ApprovalIntent" {
        intent_vectors()
            .iter()
            .map(|vector| serde_json::from_str(vector["canonical"].as_str().unwrap()).unwrap())
            .collect()
    } else {
        reply_vectors()[name]
            .as_array()
            .unwrap()
            .iter()
            .map(|canonical| serde_json::from_str(canonical.as_str().unwrap()).unwrap())
            .collect()
    };
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| WireSpecimen {
            phase: Serialize,
            label: format!("{name} {}", index + 1),
            value,
        })
        .collect()
}

#[test]
fn signed_approval_intents_and_closed_replies_match_their_fixed_bytes() {
    use fleet_signin::approval_person_wire::{
        ApprovalDecideReply, ApprovalDecideRequest, ApprovalListReply, ApprovalListRequest,
        ApprovalReadReply, ApprovalReadRequest,
    };

    let vectors = intent_vectors();
    let operations = [
        ApprovalPersonOperation::List(
            serde_json::from_value::<ApprovalListRequest>(vectors[0]["body"].clone()).unwrap(),
        ),
        ApprovalPersonOperation::Read(
            serde_json::from_value::<ApprovalReadRequest>(vectors[1]["body"].clone()).unwrap(),
        ),
        ApprovalPersonOperation::Decide(
            serde_json::from_value::<ApprovalDecideRequest>(vectors[2]["body"].clone()).unwrap(),
        ),
    ];
    for (operation, vector) in operations.into_iter().zip(vectors) {
        let intent = operation.intent();
        assert_eq!(
            intent.canonical().unwrap(),
            vector["canonical"].as_str().unwrap()
        );
        assert_eq!(intent.digest().unwrap(), vector["digest"].as_str().unwrap());
    }

    assert_eq!(
        ApprovalPersonRoute::List.path(),
        "/organization-admin/v1/approval-list"
    );
    assert_eq!(
        ApprovalPersonRoute::Read.path(),
        "/organization-admin/v1/approval-read"
    );
    assert_eq!(
        ApprovalPersonRoute::Decide.path(),
        "/organization-admin/v1/approval-decide"
    );

    let replies = reply_vectors();
    check_replies::<ApprovalListReply>(&replies["ApprovalListReply"]);
    check_replies::<ApprovalReadReply>(&replies["ApprovalReadReply"]);
    check_replies::<ApprovalDecideReply>(&replies["ApprovalDecideReply"]);
    let read = serde_json::from_str::<ApprovalReadReply>(
        replies["ApprovalReadReply"][0].as_str().unwrap(),
    )
    .unwrap();
    let ApprovalReadReply::Request { view } = read else {
        panic!("expected request view")
    };
    let list: Value =
        serde_json::from_str(replies["ApprovalListReply"][0].as_str().unwrap()).unwrap();
    assert_eq!(
        serde_json::to_value(approval_request_protocol::RequestSummary::from(&view)).unwrap(),
        list["items"][0]
    );
}

fn check_replies<Reply: serde::de::DeserializeOwned + serde::Serialize>(vectors: &Value) {
    for canonical in vectors.as_array().unwrap() {
        let bytes = canonical.as_str().unwrap();
        let decoded: Reply = serde_json::from_str(bytes).unwrap();
        let encoded = serde_json::to_value(decoded).unwrap();
        assert_eq!(
            approval_request_protocol::canonical_json(&encoded).unwrap(),
            bytes
        );
    }
}

#[test]
fn approval_people_list_requires_its_nullable_continuation() {
    use fleet_signin::approval_person_wire::{
        ApprovalDecideReply, ApprovalListReply, ApprovalListRequest,
    };
    let last: ApprovalListReply =
        serde_json::from_str(r#"{"kind":"page","items":[],"next":null}"#).unwrap();
    assert_eq!(
        serde_json::to_value(last).unwrap(),
        json!({"kind":"page","items":[],"next":null})
    );
    for malformed in [
        r#"{"kind":"page","items":[]}"#,
        r#"["page",[],null]"#,
        r#"{"kind":"page","items":[],"next":null,"extra":false}"#,
    ] {
        assert!(
            serde_json::from_str::<ApprovalListReply>(malformed).is_err(),
            "{malformed}"
        );
    }
    assert!(
        serde_json::from_str::<ApprovalDecideReply>(
            r#"{"kind":"factorRequired","withinSeconds":901}"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<ApprovalDecideReply>(
            r#"{"kind":"factorRequired","withinSeconds":"900"}"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<ApprovalListRequest>(
            r#"["019a3dce-2000-7000-8000-000000000001","waiting",null]"#,
        )
        .is_err()
    );
}

fn intent_vectors() -> Vec<Value> {
    serde_json::from_str(include_str!("../contracts/approval-intent-vector.json")).unwrap()
}

fn reply_vectors() -> Value {
    serde_json::from_str(include_str!(
        "../contracts/approval-person-reply-vectors.json"
    ))
    .unwrap()
}
