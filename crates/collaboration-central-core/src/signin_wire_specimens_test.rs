use super::{
    AdminOperation, AdminRoute, AuthorizationRevision, AuthorizationRevisionAcknowledgement,
    CreateWorkspaceRequest, DecideDeviceLogin, Deserialize, MachineAccessTokenClaims,
    MachineAccessTokenHeader, MachineTokenClaims, MachineTokenDecision, OrganizationAdminIntent,
    OrganizationAdminResult, OrganizationChangeRecord, OrganizationStampClaims,
    OrganizationStampHeader, Serialize, UnixMillis, UnixSeconds, WirePhase, WireSpecimen,
    change_records_replies, contract_directory, core_reply, core_request, read_contract_json,
};
use crate::person_request::signed_claims_specimens;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::fs;

fn wire_specimen(label: impl Into<String>, phase: WirePhase, value: Value) -> WireSpecimen {
    WireSpecimen {
        label: label.into(),
        phase,
        value,
    }
}

fn serde_roundtrip<Contract: DeserializeOwned + serde::Serialize>(value: Value) -> Value {
    let decoded: Contract = serde_json::from_value(value).unwrap();
    serde_json::to_value(decoded).unwrap()
}

fn checked_binding<Contract: DeserializeOwned + serde::Serialize>(
    name: &'static str,
    accepted: Value,
    producer: Value,
    invalid: Value,
    invalid_phase: WirePhase,
) -> (&'static str, Vec<WireSpecimen>, WireSpecimen) {
    assert_eq!(
        serde_roundtrip::<Contract>(accepted.clone()),
        producer,
        "{name}"
    );
    assert_eq!(
        serde_roundtrip::<Contract>(producer.clone()),
        producer,
        "{name}"
    );
    assert!(
        serde_json::from_value::<Contract>(invalid.clone()).is_err(),
        "reader accepted malformed {name}"
    );
    (
        name,
        vec![
            wire_specimen(format!("{name} produced payload"), Serialize, producer),
            wire_specimen(format!("{name} accepted payload"), Deserialize, accepted),
        ],
        wire_specimen(format!("malformed {name}"), invalid_phase, invalid),
    )
}

fn fleet_signed_bindings() -> [(&'static str, Vec<WireSpecimen>, WireSpecimen); 4] {
    let stamp_header = json!({"typ":"fleet-organization-stamp+jwt","alg":"EdDSA","kid":"stamp-a"});
    let mut invalid_stamp_header = stamp_header.clone();
    invalid_stamp_header["kid"] = json!(10);
    let stamp_first = json!({
        "purpose":"fleet-organization-stamp", "version":1, "iss":"https://issuer.example",
        "aud":"central", "sub":"019a3dce-2000-7000-8000-000000000001",
        "organization_id":"019a3dce-2000-7000-8000-000000000001", "iat":1793448000,
        "exp":1793448600
    });
    assert_eq!(
        serde_roundtrip::<OrganizationStampClaims>(stamp_first.clone()),
        stamp_first
    );
    let mut stamp_renewal = stamp_first.clone();
    stamp_renewal["replaces"] = json!("a".repeat(64));
    let mut invalid_stamp_renewal = stamp_renewal.clone();
    invalid_stamp_renewal["replaces"] = Value::Null;
    let mut stamp_binding = checked_binding::<OrganizationStampClaims>(
        "OrganizationStampClaims",
        stamp_renewal.clone(),
        stamp_renewal,
        invalid_stamp_renewal,
        Deserialize,
    );
    stamp_binding.1.extend([
        wire_specimen(
            "initial stamp producer omits replaces",
            Serialize,
            stamp_first.clone(),
        ),
        wire_specimen(
            "initial stamp reader accepts omitted replaces",
            Deserialize,
            stamp_first,
        ),
    ]);

    let machine_header = json!({"typ":"at+jwt","alg":"EdDSA","kid":"issuer-a"});
    let mut invalid_machine_header = machine_header.clone();
    invalid_machine_header
        .as_object_mut()
        .unwrap()
        .remove("typ");
    let machine_producer = json!({
        "iss":"https://issuer.example", "aud":"central", "iat":1793448000, "exp":1793448600,
        "client_id":"019a3dce-2000-7000-8000-000000000002", "azp":"019a3dce-2000-7000-8000-000000000002",
        "principal":{"kind":"machine","machine_id":"019a3dce-2000-7000-8000-000000000003","enrollment_id":"019a3dce-2000-7000-8000-000000000002"},
        "organization_id":"019a3dce-2000-7000-8000-000000000001", "grant_expires_at":1793448600
    });
    let mut machine_reader = machine_producer.clone();
    machine_reader["sid"] = Value::Null;
    let mut invalid_machine = machine_reader.clone();
    invalid_machine["workspace_id"] = Value::Null;

    [
        checked_binding::<OrganizationStampHeader>(
            "OrganizationStampHeader",
            stamp_header.clone(),
            stamp_header,
            invalid_stamp_header,
            Deserialize,
        ),
        stamp_binding,
        checked_binding::<MachineAccessTokenHeader>(
            "MachineAccessTokenHeader",
            machine_header.clone(),
            machine_header,
            invalid_machine_header,
            Deserialize,
        ),
        checked_binding::<MachineAccessTokenClaims>(
            "MachineAccessTokenClaims",
            machine_reader,
            machine_producer,
            invalid_machine,
            Deserialize,
        ),
    ]
}

pub(super) fn checked_signed_specimens(name: &str) -> Vec<WireSpecimen> {
    if let Some((_, produced, accepted, _)) = signed_claims_specimens()
        .into_iter()
        .find(|binding| binding.0 == name)
    {
        return vec![
            wire_specimen(format!("{name} produced payload"), Serialize, produced),
            wire_specimen(format!("{name} accepted payload"), Deserialize, accepted),
        ];
    }
    fleet_signed_bindings()
        .into_iter()
        .find(|binding| binding.0 == name)
        .expect("signed wire binding")
        .1
}

pub(super) fn invalid_specimens() -> Vec<(&'static str, WireSpecimen)> {
    let mut grant = core_reply("MachineTokenDecision").remove(0).value;
    grant["decidedAt"] = json!("1793448000");
    let numeric = wire_specimen("grant time is a string", Serialize, grant);
    let mut grant = core_reply("MachineTokenDecision").remove(0).value;
    grant["superseded"] = Value::Null;
    let omit_only = wire_specimen(
        "serialized grant includes null superseded",
        Serialize,
        grant,
    );
    let mut device_login = core_request("DecideDeviceLogin").remove(0).value;
    device_login
        .as_object_mut()
        .unwrap()
        .remove("approverSecondFactorAt");
    let required_nullable = wire_specimen(
        "device approval omits its required nullable factor",
        Deserialize,
        device_login,
    );
    let mut intent = core_reply("OrganizationAdminIntent").remove(0).value;
    intent["operation"] = json!("createWorkspace");
    let wrong_tag = wire_specimen(
        "intent uses a camelCase operation discriminant",
        Serialize,
        intent,
    );
    let mut intent = core_reply("OrganizationAdminIntent").remove(0).value;
    intent["v"] = json!(2);
    let wrong_version = wire_specimen("intent uses version two", Serialize, intent);
    let mut read_intent = core_reply("OrganizationAdminIntent").remove(1).value;
    read_intent.as_object_mut().unwrap().remove("operationId");
    let required_null = wire_specimen(
        "read intent omits its null operation ID",
        Serialize,
        read_intent,
    );
    let mut workspace_page = core_reply("ListWorkspacesResult")
        .into_iter()
        .find(|specimen| {
            specimen.value["next"].is_null() && specimen.value["result"] == "workspaces"
        })
        .expect("last workspace page fixture")
        .value;
    workspace_page.as_object_mut().unwrap().remove("next");
    let missing_serialized_cursor = wire_specimen(
        "workspace page reply omits its required nullable cursor",
        Serialize,
        workspace_page.clone(),
    );
    let mut retired_page = json!({"result":"engineRetired","items":[],"next":null});
    retired_page.as_object_mut().unwrap().remove("next");
    let missing_retired_cursor = wire_specimen(
        "retired page reader sees no required nullable cursor",
        Deserialize,
        retired_page,
    );
    let missing_accepted_cursor = wire_specimen(
        "workspace page reader sees no required nullable cursor",
        Deserialize,
        workspace_page,
    );
    vec![
        ("MachineTokenDecision", numeric),
        ("MachineTokenDecision", omit_only),
        ("DecideDeviceLogin", required_nullable),
        ("OrganizationAdminIntent", wrong_tag),
        ("OrganizationAdminIntent", wrong_version),
        ("OrganizationAdminIntent", required_null),
        ("ListWorkspacesResult", missing_serialized_cursor),
        ("ListWorkspacesResult", missing_accepted_cursor),
        ("OrganizationAdminResult", missing_retired_cursor),
    ]
    .into_iter()
    .chain(
        signed_claims_specimens()
            .into_iter()
            .map(|(name, _, _, invalid)| {
                (
                    name,
                    wire_specimen(format!("malformed {name}"), Deserialize, invalid),
                )
            }),
    )
    .chain(
        fleet_signed_bindings()
            .into_iter()
            .map(|(name, _, invalid)| (name, invalid)),
    )
    .chain(allowance_record_negatives())
    .collect()
}

fn allowance_record_negatives() -> impl Iterator<Item = (&'static str, WireSpecimen)> {
    let negatives: Vec<Value> =
        read_contract_json(&contract_directory().join("OrganizationAdminResult.negatives.json"));
    negatives
        .into_iter()
        .filter(|reply| reply["records"][0]["kind"] == "machineAllowanceChanged")
        .enumerate()
        .flat_map(|(index, reply)| {
            [Serialize, Deserialize].map(move |phase| {
                (
                    "OrganizationChangeRecord",
                    wire_specimen(
                        format!("invalid machine allowance record {}", index + 1),
                        phase,
                        reply["records"][0].clone(),
                    ),
                )
            })
        })
}

fn check_integer<Contract: DeserializeOwned + serde::Serialize>(base: &Value, field: &str) {
    const MAXIMUM: i64 = 9_007_199_254_740_991;
    let mut input = base.clone();
    *input.pointer_mut(field).unwrap() = json!(MAXIMUM);
    assert_eq!(
        serde_roundtrip::<Contract>(input.clone()),
        input,
        "safe value at {field}"
    );
    for invalid in [MAXIMUM + 1, i64::MAX, -1] {
        *input.pointer_mut(field).unwrap() = json!(invalid);
        assert!(
            serde_json::from_value::<Contract>(input.clone()).is_err(),
            "accepted {invalid} at {field}"
        );
    }
}

pub(super) fn numeric_boundary_specimens() {
    check_integer::<UnixSeconds>(&Value::Null, "");
    check_integer::<UnixMillis>(&Value::Null, "");
    let approval = core_request("DecideDeviceLogin").remove(0).value;
    for field in ["/approverSecondFactorAt", "/codeExpiresAt"] {
        check_integer::<DecideDeviceLogin>(&approval, field);
    }
    let grant = core_reply("MachineTokenDecision").remove(0).value;
    for field in ["/decidedAt", "/grantExpiresAt"] {
        check_integer::<MachineTokenDecision>(&grant, field);
    }
    let claims = core_request("MachineTokenClaims").remove(0).value;
    check_integer::<MachineTokenClaims>(&claims, "/grant_expires_at");
    let record = change_records_replies("OrganizationChangeRecord")
        .remove(0)
        .value;
    check_integer::<OrganizationChangeRecord>(&record, "/createdAtMillis");
    for revision in core_request("AuthorizationRevision") {
        check_integer::<AuthorizationRevision>(&revision.value, "/revision");
    }
    let acknowledgement = core_reply("AuthorizationRevisionAcknowledgement")
        .remove(0)
        .value;
    check_integer::<AuthorizationRevisionAcknowledgement>(
        &acknowledgement,
        "/acknowledgedRevision",
    );
}

fn check_description<Contract: DeserializeOwned + serde::Serialize>(mut input: Value, field: &str) {
    let half_limit = fleet_contracts::MAXIMUM_DESCRIPTION_BYTES / 2;
    *input.pointer_mut(field).unwrap() = json!("é".repeat(half_limit));
    assert_eq!(serde_roundtrip::<Contract>(input.clone()), input);
    *input.pointer_mut(field).unwrap() = json!("é".repeat(half_limit + 1));
    assert!(serde_json::from_value::<Contract>(input).is_err());
}

pub(super) fn description_codec_specimens() {
    check_description::<CreateWorkspaceRequest>(
        core_request("CreateWorkspaceRequest").remove(0).value,
        "/description",
    );
    check_description::<OrganizationAdminResult>(
        core_reply("OrganizationAdminResult").remove(0).value,
        "/workspace/description",
    );
    check_description::<OrganizationAdminIntent>(
        core_reply("OrganizationAdminIntent").remove(0).value,
        "/params/description",
    );
}

pub(super) fn signed_intent_vector() {
    let path = contract_directory().join("organization-admin-intent-vector.json");
    let request = core_request("CreateWorkspaceRequest").remove(0).value;
    let operation = AdminOperation::parse(
        AdminRoute::CreateWorkspace,
        &serde_json::to_vec(&request).unwrap(),
    )
    .unwrap();
    let canonical = operation.canonical_intent().unwrap();
    let actual = json!({
        "route":"createWorkspace",
        "requestBody":request,
        "canonicalIntent":canonical,
        "intentSha256Base64Url":operation.intent_digest().unwrap()
    });
    if std::env::var("UPDATE_CENTRAL_SIGNIN_TYPES").as_deref() == Ok("1") {
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&actual).unwrap()),
        )
        .unwrap();
    }
    assert_eq!(read_contract_json::<Value>(&path), actual);
}
