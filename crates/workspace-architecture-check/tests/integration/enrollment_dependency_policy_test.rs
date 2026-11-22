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
                    {"name":"reqwest","kind":null,"path":null,"source":CRATES_IO,"features":["blocking","rustls-tls"],"uses_default_features":false},
                    {"name":"axum","kind":"dev","path":null,"source":CRATES_IO,"features":["http1","tokio"],"uses_default_features":false}
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

fn append_dependency(fixture: &mut Value, owner_index: usize, dependency: Value) {
    fixture["packages"][owner_index]["dependencies"]
        .as_array_mut()
        .expect("dependency list")
        .push(dependency);
}

#[test]
fn enrollment_rejects_feature_changes_on_an_approved_dependency() {
    let mut feature = fixture();
    feature["packages"][0]["dependencies"][1]["features"] =
        json!(["blocking", "rustls-tls", "json"]);
    let mut defaults = fixture();
    defaults["packages"][0]["dependencies"][1]["uses_default_features"] = json!(true);
    for case in [feature, defaults] {
        let error = violation(check_declared(&decode(case)));
        assert!(
            error.is_some_and(|error| error.contains("forbidden declared Normal features")
                && error.contains("reqwest"))
        );
    }
}

#[test]
fn clock_test_support_belongs_only_on_the_dev_edge() {
    let mut allowed = fixture();
    append_dependency(
        &mut allowed,
        0,
        json!({
            "name":"router-clock","kind":"dev","path":"/workspace/clock","source":null,
            "features":["test-support"],"uses_default_features":true
        }),
    );
    check_declared(&decode(allowed.clone())).expect("test clock is available on a dev edge");
    allowed["packages"][0]["dependencies"][0]["features"] = json!(["test-support"]);
    assert!(
        violation(check_declared(&decode(allowed)))
            .is_some_and(|error| error.contains("forbidden declared Normal features")
                && error.contains("router-clock"))
    );
}

#[test]
fn normal_consumers_cannot_enable_the_plaintext_fixture() {
    for forwarded in [false, true] {
        let mut metadata = fixture();
        metadata["packages"][4]["dependencies"] = json!([{
            "name": ENROLLMENT, "rename":"enrollment", "kind":null, "path":"/workspace/enrollment",
            "source":null, "features": if forwarded {vec![]} else {vec!["test-support"]},
            "uses_default_features":true
        }]);
        if forwarded {
            metadata["packages"][4]["features"] = json!({"debug":["enrollment?/test-support"]});
        }
        assert_eq!(violation(check_declared(&decode(metadata))), Some("BD1: codex-router-secret-store enables enrollment test-support on a shipping dependency".to_owned()));
    }
}
