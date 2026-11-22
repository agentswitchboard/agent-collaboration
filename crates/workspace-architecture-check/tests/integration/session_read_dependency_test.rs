use super::{
    CargoMetadata, DependencyKind, SESSION_READ_CRATES, check_boundaries,
    check_declared_session_read_edges, graph,
};
use serde_json::json;

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
            let dependencies = json!([{
                "name": "acp-client-runtime",
                "kind": serde_kind,
                "path": "/workspace/client",
                "rename": "optional-client",
                "optional": kind != DependencyKind::Dev
            }]);
            let metadata: CargoMetadata = serde_json::from_value(json!({
                "packages": [
                    {
                        "id":"payloads",
                        "name":"switchboard-session-payloads",
                        "manifest_path":"/workspace/payloads/Cargo.toml",
                        "dependencies": if owner == "switchboard-session-payloads" { dependencies.clone() } else { json!([]) }
                    },
                    {
                        "id":"calls",
                        "name":"switchboard-session-calls",
                        "manifest_path":"/workspace/calls/Cargo.toml",
                        "dependencies": if owner == "switchboard-session-calls" { dependencies } else { json!([]) }
                    },
                    {"id":"client","name":"acp-client-runtime","manifest_path":"/workspace/client/Cargo.toml"}
                ],
                "workspace_members": ["payloads","calls","client"],
                "resolve": {"nodes":[]}
            })).unwrap();
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
