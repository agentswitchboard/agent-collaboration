use serde_json::{Value, json};

use super::{CRATES_IO, CargoMetadata, ENROLLMENT, check_declared, check_resolved};

fn fixture() -> Value {
    json!({
        "packages":[
            {
                "id":"enrollment-id","name":ENROLLMENT,
                "manifest_path":"/workspace/enrollment/Cargo.toml",
                "dependencies":[
                    {"name":"router-clock","kind":null,"path":"/workspace/clock","source":null,"features":[],"uses_default_features":true},
                    {"name":"reqwest","kind":null,"path":null,"source":CRATES_IO,"features":[],"uses_default_features":true},
                    {"name":"axum","kind":"dev","path":null,"source":CRATES_IO,"features":[],"uses_default_features":true}
                ]
            },
            {"id":"clock-id","name":"router-clock","manifest_path":"/workspace/clock/Cargo.toml"},
            {"id":"reqwest-id","name":"reqwest","manifest_path":"/registry/reqwest/Cargo.toml","source":CRATES_IO},
            {"id":"axum-id","name":"axum","manifest_path":"/registry/axum/Cargo.toml","source":CRATES_IO},
            {"id":"store-id","name":"codex-router-secret-store","manifest_path":"/workspace/secrets/Cargo.toml"}
        ],
        "workspace_members":["enrollment-id","clock-id","store-id"],
        "resolve":{"nodes":[{"id":"enrollment-id","deps":[
            {"pkg":"clock-id","dep_kinds":[{"kind":null}]},
            {"pkg":"reqwest-id","dep_kinds":[{"kind":null}]},
            {"pkg":"axum-id","dep_kinds":[{"kind":"dev"}]}
        ]}]}
    })
}

fn decode(input: Value) -> CargoMetadata {
    serde_json::from_value(input).expect("metadata fixture")
}

fn violation(result: super::PolicyResult) -> Option<String> {
    result.err().map(|error| error.to_string())
}

#[test]
fn enrollment_rejects_an_inactive_secret_store_declaration() {
    let mut input = fixture();
    input["packages"][0]["dependencies"]
        .as_array_mut()
        .expect("dependencies")
        .push(json!({
            "name":"codex-router-secret-store","kind":null,"optional":true,
            "path":"/workspace/secrets","source":null,"features":[],"uses_default_features":true
        }));
    let metadata = decode(input);
    check_resolved(&metadata).expect("the optional edge is inactive");
    assert_eq!(
        violation(check_declared(&metadata)),
        Some(format!(
            "BD1: forbidden declared Normal dependency {ENROLLMENT} -> codex-router-secret-store (Workspace)"
        ))
    );
}

#[test]
fn enrollment_checks_resolved_package_source_and_edge_kind() {
    let mut redirected = fixture();
    redirected["packages"][2]["source"] = json!("git+https://example.invalid/reqwest#1234");
    let mut shipping_server = fixture();
    shipping_server["resolve"]["nodes"][0]["deps"][2]["dep_kinds"][0]["kind"] = Value::Null;
    let mut build_clock = fixture();
    build_clock["resolve"]["nodes"][0]["deps"][0]["dep_kinds"][0]["kind"] = json!("build");

    for (case, kind, dependency, source) in [
        (redirected, "Normal", "reqwest", "Other"),
        (shipping_server, "Normal", "axum", "CratesIo"),
        (build_clock, "Build", "router-clock", "Workspace"),
    ] {
        let metadata = decode(case);
        check_declared(&metadata).expect("allowed manifest declaration");
        assert_eq!(
            violation(check_resolved(&metadata)),
            Some(format!(
                "BD1: forbidden resolved {kind} dependency {ENROLLMENT} -> {dependency} ({source})"
            ))
        );
    }
}

#[test]
fn enrollment_checks_declared_dependency_origin_and_kind() {
    let mut undeclared_origin = fixture();
    undeclared_origin["packages"][0]["dependencies"][1]["source"] =
        json!("git+https://example.invalid/reqwest");
    let mut shipping_fixture = fixture();
    shipping_fixture["packages"][0]["dependencies"][2]["kind"] = Value::Null;
    for (case, kind, dependency, source) in [
        (undeclared_origin, "Normal", "reqwest", "Other"),
        (shipping_fixture, "Normal", "axum", "CratesIo"),
    ] {
        let metadata = decode(case);
        check_resolved(&metadata).expect("the locked resolved fixture is valid");
        assert_eq!(
            violation(check_declared(&metadata)),
            Some(format!(
                "BD1: forbidden declared {kind} dependency {ENROLLMENT} -> {dependency} ({source})"
            ))
        );
    }
}
