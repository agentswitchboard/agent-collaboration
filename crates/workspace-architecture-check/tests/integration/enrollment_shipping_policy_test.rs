use serde_json::{Value, json};

use super::{CargoMetadata, ENROLLMENT, check_shipping_graph};

fn fixture() -> Value {
    json!({
        "packages": [
            {"id":"enrollment","name":ENROLLMENT,"manifest_path":"/workspace/enrollment/Cargo.toml"},
            {"id":"clock","name":"router-clock","manifest_path":"/workspace/clock/Cargo.toml"},
            {"id":"ids","name":"workspace-ids","manifest_path":"/workspace/ids/Cargo.toml"},
            {"id":"host","name":"codex-router-host","manifest_path":"/workspace/host/Cargo.toml"}
        ],
        "workspace_members":["enrollment","clock","ids","host"],
        "resolve":{"nodes":[
            {"id":"enrollment","deps":[{"pkg":"clock","dep_kinds":[{"kind":null}]},{"pkg":"ids","dep_kinds":[{"kind":null}]}]},
            {"id":"clock","deps":[]},{"id":"ids","deps":[]},{"id":"host","deps":[]}
        ]}
    })
}

fn check(metadata: Value) -> Option<String> {
    let metadata: CargoMetadata = serde_json::from_value(metadata).expect("metadata");
    check_shipping_graph(&metadata, &metadata.workspace_members, "enrollment")
        .err()
        .map(|error| error.to_string())
}

#[test]
fn shipping_resolution_cannot_enable_workspace_fixture_or_wire_features() {
    assert_eq!(check(fixture()), None);
    for (index, package, feature) in [
        (0, ENROLLMENT, "test-support"),
        (1, "router-clock", "test-support"),
        (2, "workspace-ids", "wire-export"),
    ] {
        let mut metadata = fixture();
        metadata["resolve"]["nodes"][index]["features"] = json!([feature]);
        assert_eq!(
            check(metadata),
            Some(format!(
                "BD1: forbidden shipping feature {package}:{feature}"
            ))
        );
    }
}

#[test]
fn shipping_closure_rejects_an_upward_edge_under_an_allowed_workspace_crate() {
    let mut graph = fixture();
    graph["resolve"]["nodes"][1]["deps"] = json!([{"pkg":"host","dep_kinds":[{"kind":"dev"}]}]);
    assert_eq!(check(graph.clone()), None);
    graph["resolve"]["nodes"][1]["deps"][0]["dep_kinds"][0]["kind"] = Value::Null;
    assert_eq!(
        check(graph),
        Some(format!(
            "BD1: {ENROLLMENT} shipping closure reaches codex-router-host"
        ))
    );
}
