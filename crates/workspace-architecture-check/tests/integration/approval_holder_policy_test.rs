use super::{super::CargoMetadata, CRATES_IO, check_declared, check_resolved};
use serde_json::json;

fn metadata(
    declared: serde_json::Value,
    holder_edges: serde_json::Value,
    external_edges: serde_json::Value,
) -> CargoMetadata {
    serde_json::from_value(json!({
        "packages": [
            {"id":"holder","name":"switchboard-approval-holder","manifest_path":"/workspace/holder/Cargo.toml","dependencies":declared},
            {"id":"harness","name":"acp-client-runtime","manifest_path":"/workspace/harness/Cargo.toml"},
            {"id":"hegel","name":"hegeltest","manifest_path":"/registry/hegel/Cargo.toml","source":CRATES_IO}
        ],
        "workspace_members":["holder","harness"],
        "resolve":{"nodes":[
            {"id":"holder","deps":holder_edges},
            {"id":"hegel","deps":external_edges},
            {"id":"harness","deps":[]}
        ]}
    })).unwrap()
}

#[test]
fn holder_dependency_rejects_inactive_declared_workspace_dev_path() {
    let graph = metadata(
        json!([
            {"name":"acp-client-runtime","kind":"dev","path":"/workspace/harness","features":[],"uses_default_features":true}
        ]),
        json!([]),
        json!([]),
    );
    assert_eq!(
        check_declared(&graph)
            .err()
            .map(|error| error.to_string())
            .as_deref(),
        Some(
            "BD1: forbidden declared Dev dependency switchboard-approval-holder -> acp-client-runtime"
        )
    );
}

#[test]
fn holder_dependency_rejects_resolved_workspace_dev_edge() {
    let graph = metadata(
        json!([]),
        json!([
            {"pkg":"harness","dep_kinds":[{"kind":"dev"}]}
        ]),
        json!([]),
    );
    assert_eq!(
        check_resolved(&graph)
            .err()
            .map(|error| error.to_string())
            .as_deref(),
        Some(
            "BD1: forbidden resolved Dev dependency switchboard-approval-holder -> acp-client-runtime"
        )
    );
}

#[test]
fn holder_dependency_walks_test_closure_without_dependency_dev_edges() {
    let direct = json!([{"pkg":"hegel","dep_kinds":[{"kind":"dev"}]}]);
    let normal = metadata(
        json!([]),
        direct.clone(),
        json!([
            {"pkg":"harness","dep_kinds":[{"kind":null}]}
        ]),
    );
    assert_eq!(
        check_resolved(&normal)
            .err()
            .map(|error| error.to_string())
            .as_deref(),
        Some(
            "BD1: forbidden reachable Normal dependency switchboard-approval-holder -> acp-client-runtime"
        )
    );
    let dev = metadata(
        json!([]),
        direct,
        json!([
            {"pkg":"harness","dep_kinds":[{"kind":"dev"}]}
        ]),
    );
    assert!(check_resolved(&dev).is_ok());
}
