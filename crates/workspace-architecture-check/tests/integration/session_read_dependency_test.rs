use super::{
    CargoMetadata, DependencyKind, SESSION_READ_CRATES, check_boundaries,
    check_declared_session_read_edges, check_package_tree_output, graph,
};
use serde_json::{Value, json};

#[test]
fn private_read_resolved_graph_rejects_a_logic_edge_from_either_crate() {
    for (owner, _) in SESSION_READ_CRATES {
        for kind in [DependencyKind::Normal, DependencyKind::Build] {
            let graph = graph(&[(owner, "acp-client-runtime", kind)]);
            let rejection = check_boundaries(&graph)
                .err()
                .map(|error| error.to_string());
            assert_eq!(
                rejection,
                Some(format!(
                    "BD1: forbidden {kind:?} dependency {owner} -> acp-client-runtime"
                )),
                "the resolved graph must name the forbidden logic edge",
            );
        }
    }
}

#[test]
fn private_read_declarations_reject_an_inactive_logic_edge_by_kind() {
    for (owner, _) in SESSION_READ_CRATES {
        for (serde_kind, kind) in [
            (None, DependencyKind::Normal),
            (Some("build"), DependencyKind::Build),
            (Some("dev"), DependencyKind::Dev),
        ] {
            let metadata = metadata_with_workspace_dependency(
                owner,
                "acp-client-runtime",
                json!({
                    "name": "acp-client-runtime",
                    "kind": serde_kind,
                    "path": "/workspace/acp-client-runtime",
                    "rename": "optional-client",
                    "optional": kind != DependencyKind::Dev,
                    "features": [],
                    "uses_default_features": true
                }),
            );
            let rejection = check_declared_session_read_edges(&metadata)
                .err()
                .map(|error| error.to_string());
            assert_eq!(
                rejection,
                Some(format!(
                    "BD1: forbidden {kind:?} dependency {owner} -> acp-client-runtime"
                )),
                "the declaration scanner must name the forbidden logic edge",
            );
        }
    }
}

#[test]
fn private_read_declarations_reserve_normal_serde_json_for_payloads() {
    for (owner, _) in SESSION_READ_CRATES {
        let metadata = metadata_with_workspace_dependency(
            owner,
            "serde_json",
            json!({
                "name": "serde_json",
                "source": "registry+https://github.com/rust-lang/crates.io-index",
                "features": [],
                "uses_default_features": true,
            }),
        );
        let rejection = check_declared_session_read_edges(&metadata)
            .err()
            .map(|error| error.to_string());
        let expected = (owner == "switchboard-session-calls")
            .then(|| format!("BD1: forbidden Normal dependency {owner} -> serde_json"));
        assert_eq!(rejection, expected, "{owner}");
    }
}

#[test]
fn private_read_declarations_allow_jsonschema_only_for_payload_tests_without_defaults() {
    for (owner, _) in SESSION_READ_CRATES {
        for (serde_kind, kind, default_features) in [
            (None, DependencyKind::Normal, false),
            (Some("build"), DependencyKind::Build, false),
            (Some("dev"), DependencyKind::Dev, false),
            (Some("dev"), DependencyKind::Dev, true),
        ] {
            let metadata = metadata_with_workspace_dependency(
                owner,
                "jsonschema",
                json!({
                    "name": "jsonschema",
                    "source": "registry+https://github.com/rust-lang/crates.io-index",
                    "kind": serde_kind,
                    "features": [],
                    "uses_default_features": default_features,
                }),
            );
            let result = check_declared_session_read_edges(&metadata);
            if owner == "switchboard-session-payloads" && kind == DependencyKind::Dev {
                if default_features {
                    let error = result.expect_err("jsonschema defaults must stay off");
                    assert!(
                        error.to_string().contains(
                            "forbidden Dev feature settings switchboard-session-payloads -> jsonschema"
                        ),
                        "{error}"
                    );
                } else {
                    assert!(result.is_ok(), "{result:?}");
                }
            } else {
                assert_eq!(
                    result.err().map(|error| error.to_string()),
                    Some(format!(
                        "BD1: forbidden {kind:?} dependency {owner} -> jsonschema"
                    )),
                );
            }
        }
    }
}

#[test]
fn private_read_declarations_check_workspace_ids_feature_settings() {
    for (owner, _) in SESSION_READ_CRATES {
        for (features, uses_default_features, expected) in [
            (json!([]), true, None),
            (json!(["wire-export"]), true, Some("wire-export")),
            (json!([]), false, Some("default features: false")),
        ] {
            let metadata = metadata_with_workspace_dependency(
                owner,
                "workspace-ids",
                json!({
                    "name": "workspace-ids",
                    "path": "/workspace/workspace-ids",
                    "features": features,
                    "uses_default_features": uses_default_features,
                }),
            );
            let result = check_declared_session_read_edges(&metadata);
            if let Some(expected) = expected {
                let error = result
                    .expect_err("the declaration gate must reject a forbidden feature setting");
                assert!(error.to_string().contains(expected), "{error}");
            } else {
                assert!(result.is_ok(), "{result:?}");
            }
        }
    }
}

#[test]
fn private_read_package_trees_reject_resolved_wire_export() {
    for (owner, _) in SESSION_READ_CRATES {
        let allowed = format!("{owner} v1\t\nworkspace-ids v1\t");
        assert!(check_package_tree_output(&allowed, &[], owner).is_ok());

        let forbidden = format!("{allowed}wire-export");
        let error = check_package_tree_output(&forbidden, &[], owner)
            .expect_err("the isolated feature gate must reject resolved workspace-ids/wire-export");
        assert!(error.to_string().contains("wire-export"), "{error}");
    }

    let central = "workspace-ids v1\twire-export\nspecta v2\tderive";
    assert!(check_package_tree_output(central, &[], "workspace-ids").is_ok());
}

#[test]
fn private_read_resolved_dev_edges_reject_logic_dependencies() {
    for (owner, _) in SESSION_READ_CRATES {
        let graph = graph(&[(owner, "acp-client-runtime", DependencyKind::Dev)]);
        let error = check_boundaries(&graph);
        assert_eq!(
            error.err().map(|rejection| rejection.to_string()),
            Some(format!(
                "BD1: forbidden Dev dependency {owner} -> acp-client-runtime"
            )),
            "the resolved dev gate must reject a workspace logic edge",
        );
    }
}

fn metadata_with_workspace_dependency(
    owner: &str,
    dependency_name: &str,
    declaration: Value,
) -> CargoMetadata {
    let mut packages: Vec<_> = SESSION_READ_CRATES
        .iter()
        .map(|(name, _)| {
            json!({
                "id": name,
                "name": name,
                "manifest_path": format!("/workspace/{name}/Cargo.toml"),
                "dependencies": if *name == owner { json!([declaration]) } else { json!([]) },
            })
        })
        .collect();
    packages.push(json!({
        "id": dependency_name,
        "name": dependency_name,
        "manifest_path": format!("/workspace/{dependency_name}/Cargo.toml"),
    }));
    serde_json::from_value(json!({
        "packages": packages,
        "workspace_members": ["switchboard-session-payloads","switchboard-session-calls",dependency_name],
        "resolve": {"nodes":[]},
    }))
    .unwrap()
}
