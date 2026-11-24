use serde_json::{Value, json};

use super::{CRATES_IO, CargoMetadata, START, check_declared, check_resolved};

fn with_edge(package: &str, kind: Option<&str>, registry: bool) -> CargoMetadata {
    let source = registry.then_some(CRATES_IO);
    let path = (!registry).then(|| format!("/workspace/{package}"));
    let members = if registry {
        vec![START]
    } else {
        vec![START, package]
    };
    serde_json::from_value(json!({
        "packages": [
            {"id": START, "name": START, "manifest_path": "/workspace/start/Cargo.toml",
             "dependencies": [{"name": package, "kind": kind, "source": source, "path": path,
                               "uses_default_features": true, "features": []}]},
            {"id": package, "name": package, "source": source,
             "manifest_path": format!("/workspace/{package}/Cargo.toml")}
        ],
        "workspace_members": members,
        "resolve": {"nodes": [
            {"id": START, "deps": [{"pkg": package, "dep_kinds": [{"kind": kind}]}]},
            {"id": package, "deps": []}
        ]}
    }))
    .unwrap()
}

#[test]
fn session_start_refuses_normal_tokio() {
    assert_forbidden_edge("tokio", None, true, "Normal");
}

#[test]
fn session_start_rejects_link_store_and_harness_in_every_direct_kind() {
    for package in [
        "switchboard-central-link",
        "switchboard-session-store",
        "switchboard-harness-contracts",
    ] {
        for (kind, label) in [
            (None, "Normal"),
            (Some("dev"), "Dev"),
            (Some("build"), "Build"),
        ] {
            assert_forbidden_edge(package, kind, false, label);
        }
    }
}

fn assert_forbidden_edge(package: &str, kind: Option<&str>, registry: bool, label: &str) {
    let metadata = with_edge(package, kind, registry);
    for (where_checked, checked) in [
        ("declared", check_declared(&metadata)),
        ("resolved", check_resolved(&metadata)),
    ] {
        assert_eq!(
            checked.unwrap_err().to_string(),
            format!("BD1: forbidden {where_checked} {label} dependency {START} -> {package}")
        );
    }
}

#[test]
fn session_start_allows_only_the_named_workspace_dependency_closure() {
    for dependency in ["message-board", "switchboard-session-store"] {
        let packages = [
            json!({"id": START, "name": START, "manifest_path": "/workspace/start/Cargo.toml"}),
            json!({"id": "protocol", "name": "fleet-identity-protocol",
                   "manifest_path": "/workspace/protocol/Cargo.toml"}),
            json!({"id": dependency, "name": dependency, "manifest_path": "/workspace/dependency/Cargo.toml"}),
        ];
        let graph = |kind: Value| {
            serde_json::from_value(json!({
            "packages": packages,
            "workspace_members": [START, "protocol", dependency],
            "resolve": {"nodes": [
                {"id": START, "deps": [{"pkg": "protocol", "dep_kinds": [{"kind": null}]}]},
                {"id": "protocol", "deps": [{"pkg": dependency, "dep_kinds": [{"kind": kind}]}]},
                {"id": dependency, "deps": []}
            ]}
        })).unwrap()
        };
        assert!(check_resolved(&graph(json!("dev"))).is_ok());
        assert_eq!(
            check_resolved(&graph(Value::Null)).is_ok(),
            dependency == "message-board"
        );
    }
}
