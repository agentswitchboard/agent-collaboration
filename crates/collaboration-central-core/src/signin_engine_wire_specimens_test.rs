//! Fixed engine-admin byte vectors, also checked against the exported wire types.

use super::{
    AdminOperation, AdminRoute, Deserialize, OrganizationAdminResult, Serialize, WireSpecimen,
    contract_directory, read_contract_json,
};
use fleet_signin::FleetPermission;
use serde_json::Value;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IntentVector {
    path: String,
    request_body: Value,
    canonical_intent: String,
    intent_sha256_base64_url: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyVector {
    name: String,
    bytes: String,
}

fn intent_vectors() -> Vec<IntentVector> {
    read_contract_json(&contract_directory().join("engine-admin-intent-vectors.json"))
}

fn reply_vectors() -> Vec<ReplyVector> {
    read_contract_json(&contract_directory().join("engine-admin-reply-vectors.json"))
}

pub(super) fn engine_intent_specimens() -> impl Iterator<Item = WireSpecimen> {
    intent_vectors().into_iter().map(|vector| WireSpecimen {
        label: vector.path,
        phase: Serialize,
        value: serde_json::from_str(&vector.canonical_intent).unwrap(),
    })
}

pub(super) fn engine_reply_specimens() -> impl Iterator<Item = WireSpecimen> {
    reply_vectors().into_iter().flat_map(|vector| {
        let value: Value = serde_json::from_str(&vector.bytes).unwrap();
        [Serialize, Deserialize].map(|phase| WireSpecimen {
            label: format!("engine admin {} ({})", vector.name, phase.suffix()),
            phase,
            value: value.clone(),
        })
    })
}

pub(super) fn fleet_permission_specimens(_: &str) -> Vec<WireSpecimen> {
    let permission = FleetPermission::RepairWork;
    let value = serde_json::json!("organization:repairWork");
    assert_eq!(serde_json::to_value(permission).unwrap(), value);
    assert_eq!(
        serde_json::from_value::<FleetPermission>(value.clone()).unwrap(),
        permission
    );
    vec![WireSpecimen {
        label: "repairWork Fleet permission".into(),
        phase: Deserialize,
        value,
    }]
}

#[test]
fn engine_admin_intents_match_canonical_signing_bytes() {
    let routes = [
        AdminRoute::EngineHealth,
        AdminRoute::EngineListRetired,
        AdminRoute::EngineRetry,
        AdminRoute::EngineDiscard,
    ];
    let vectors = intent_vectors();
    assert_eq!(vectors.len(), routes.len());
    for (vector, route) in vectors.into_iter().zip(routes) {
        assert_eq!(route.path(), vector.path);
        let operation =
            AdminOperation::parse(route, &serde_json::to_vec(&vector.request_body).unwrap())
                .unwrap();
        assert_eq!(
            operation.canonical_intent().unwrap(),
            vector.canonical_intent
        );
        assert_eq!(
            operation.intent_digest().unwrap(),
            vector.intent_sha256_base64_url
        );
    }
}

#[test]
fn engine_admin_replies_match_fixed_wire_bytes() {
    let vectors = reply_vectors();
    assert_eq!(vectors.len(), 25);
    for vector in vectors {
        let result: OrganizationAdminResult = serde_json::from_str(&vector.bytes).unwrap();
        assert_eq!(
            serde_json::to_string(&result).unwrap(),
            vector.bytes,
            "{}",
            vector.name
        );
    }
}
