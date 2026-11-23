use super::{
    APPROVAL_PORTS, CRATES_IO, CargoMetadata, check_declared, check_resolved, check_rooted_graph,
};
use serde_json::{Value, json};

fn edge(package_id: &str, kind: Option<&str>) -> Value {
    json!({"pkg": package_id, "dep_kinds": [{"kind": kind}]})
}

fn registry_dependency(name: &str, kind: Option<&str>) -> Value {
    json!({"name": name, "kind": kind, "source": CRATES_IO, "features": [],
           "uses_default_features": true, "optional": true})
}

fn fixture(
    declaration: Value,
    root_edges: Vec<Value>,
    protocol_edges: Vec<Value>,
) -> CargoMetadata {
    serde_json::from_value(json!({
        "workspace_members": [APPROVAL_PORTS, "workspace-ids", "approval-request-protocol"],
        "packages": [
            {"id": APPROVAL_PORTS, "name": APPROVAL_PORTS, "manifest_path": "/root/ports/Cargo.toml",
             "dependencies": [declaration]},
            {"id": "workspace-ids", "name": "workspace-ids", "manifest_path": "/root/ids/Cargo.toml"},
            {"id": "approval-request-protocol", "name": "approval-request-protocol",
             "manifest_path": "/root/protocol/Cargo.toml"},
            {"id": "tokio", "name": "tokio", "manifest_path": "/registry/tokio/Cargo.toml", "source": CRATES_IO},
            {"id": "thiserror", "name": "thiserror", "manifest_path": "/registry/thiserror/Cargo.toml", "source": CRATES_IO}
        ],
        "resolve": {"nodes": [
            {"id": APPROVAL_PORTS, "deps": root_edges},
            {"id": "approval-request-protocol", "deps": protocol_edges},
            {"id": "workspace-ids", "deps": []},
            {"id": "thiserror", "deps": []},
            {"id": "tokio", "deps": []}
        ]}
    })).expect("Cargo metadata fixture")
}

const EDGE_KINDS: [(Option<&str>, &str); 3] = [
    (None, "Normal"),
    (Some("build"), "Build"),
    (Some("dev"), "Dev"),
];

#[test]
fn approval_ports_reject_forwarding_with_an_unchanged_package_set() {
    let mut metadata = fixture(
        registry_dependency("thiserror", None),
        vec![edge("thiserror", None)],
        vec![],
    );
    let root = metadata
        .packages
        .iter_mut()
        .find(|package| package.name == APPROVAL_PORTS)
        .expect("fixture root");
    root.package_features.insert(
        "wire-export".to_owned(),
        vec!["approval-request-protocol/wire-export".to_owned()],
    );
    assert_eq!(check_resolved(&metadata), Ok(()));
    assert_eq!(check_declared(&metadata), Err(
        "approval ports forbid declared package features: {\"wire-export\": [\"approval-request-protocol/wire-export\"]}".to_owned()
    ));
}

#[test]
fn approval_ports_rooted_graph_rejects_runtime_and_unapproved_features() {
    let root = format!("{APPROVAL_PORTS} v1\t\t\napproval-request-protocol v1\t\t");
    assert_eq!(check_rooted_graph(&root), Ok(()));
    assert_eq!(
        check_rooted_graph(&format!("{root}\ntokio v1\trt\t")),
        Err("approval ports forbid rooted dependency tokio".to_owned())
    );
    assert_eq!(
        check_rooted_graph(&format!("{root}\nworkspace-ids v1\twire-export\t")),
        Err("approval ports forbid rooted features for workspace-ids: wire-export".to_owned())
    );
}

#[test]
fn approval_ports_reject_inactive_declared_edges_of_each_kind() {
    for (kind, label) in EDGE_KINDS {
        let metadata = fixture(registry_dependency("tokio", kind), vec![], vec![]);
        assert_eq!(check_resolved(&metadata), Ok(()));
        assert_eq!(
            check_declared(&metadata),
            Err(format!(
                "approval ports forbid declared {label} dependency tokio"
            ))
        );
    }
}

#[test]
fn approval_ports_reject_resolved_edges_of_each_kind() {
    for (kind, label) in EDGE_KINDS {
        let metadata = fixture(
            registry_dependency("thiserror", None),
            vec![edge("tokio", kind)],
            vec![],
        );
        assert_eq!(check_declared(&metadata), Ok(()));
        assert_eq!(
            check_resolved(&metadata),
            Err(format!(
                "approval ports forbid resolved {label} dependency tokio"
            ))
        );
    }
}

#[test]
fn approval_ports_reject_declared_feature_overrides() {
    for (features, default_features) in [(json!(["wire-export"]), true), (json!([]), false)] {
        let metadata = fixture(
            json!({
                "name": "workspace-ids", "path": "/root/ids",
                "features": features, "uses_default_features": default_features
            }),
            vec![edge("workspace-ids", None)],
            vec![],
        );
        let features = metadata
            .packages
            .iter()
            .find(|package| package.id == APPROVAL_PORTS)
            .expect("fixture root")
            .dependencies
            .first()
            .expect("fixture edge");
        assert_eq!(
            check_declared(&metadata),
            Err(format!(
                "approval ports forbid declared feature overrides on workspace-ids: {:?}, default features {}",
                features.features, features.uses_default_features
            ))
        );
    }
}

#[test]
fn approval_ports_reject_runtime_reached_only_when_features_enable_it() {
    let root_edges = || vec![edge("approval-request-protocol", None)];
    let protocol = || {
        json!({
            "name": "approval-request-protocol", "path": "/root/protocol",
            "features": [], "uses_default_features": true
        })
    };
    let default = fixture(protocol(), root_edges(), vec![edge("tokio", Some("dev"))]);
    let enabled = fixture(
        protocol(),
        root_edges(),
        vec![edge("tokio", Some("dev")), edge("tokio", None)],
    );
    assert_eq!(check_declared(&enabled), Ok(()));
    assert_eq!(check_resolved(&default), Ok(()));
    assert_eq!(
        check_resolved(&enabled),
        Err("approval ports forbid reachable Normal dependency tokio".to_owned())
    );
}
