use super::{CargoMetadata, DependencyKind, check, check_shipped_closure};
use serde_json::{Value, json};

fn fixture(
    owner: &str,
    declared: Value,
    target: &str,
    resolved_kind: Option<DependencyKind>,
) -> CargoMetadata {
    let kind = resolved_kind.map(|kind| match kind {
        DependencyKind::Normal => "normal",
        DependencyKind::Build => "build",
        DependencyKind::Dev => "dev",
    });
    let to = |name: &str| format!("path+file:///model/{name}#0.1.158");
    let target_id = to(target);
    let owner_id = to(owner);
    let mut packages = vec![json!({
        "id":owner_id,"name":owner,"manifest_path":format!("/model/{owner}/Cargo.toml"),
        "source":null,"dependencies":declared
    })];
    let mut workspace_members = vec![owner_id.clone()];
    let mut nodes = vec![json!({
        "id":owner_id,
        "deps":kind.map_or_else(Vec::new, |kind| vec![json!({"pkg":target_id,"dep_kinds":[{"kind":kind}]})]),
    })];
    if owner != target {
        packages.push(json!({
            "id":target_id,"name":target,"manifest_path":format!("/model/{target}/Cargo.toml"),
            "source":null,"dependencies":[]
        }));
        workspace_members.push(target_id.clone());
        nodes.push(json!({"id":target_id,"deps":[]}));
    }
    serde_json::from_value(json!({"packages":packages,"workspace_members":workspace_members,"resolve":{"nodes":nodes}})).unwrap()
}

fn declared(name: &str, kind: Option<&str>, features: &[&str]) -> Value {
    json!([{
        "name":name,"kind":kind,"path":format!("/model/{name}"),"source":null,
        "features":features,"uses_default_features":true,
    }])
}

#[test]
fn link_declared_features_do_not_enable_the_protocol_test_vocabulary() {
    let metadata = fixture(
        "switchboard-central-link",
        declared("central-link-protocol", None, &["test-support"]),
        "central-link-protocol",
        None,
    );
    let error = check(&metadata).unwrap_err().to_string();
    assert!(
        error.contains("forbidden link declared features"),
        "{error}"
    );
}

#[test]
fn link_declared_and_resolved_edges_reject_a_harness_dependency() {
    let metadata = fixture(
        "central-link-client",
        declared("acp-client-runtime", Some("build"), &[]),
        "acp-client-runtime",
        None,
    );
    let error = check(&metadata).unwrap_err().to_string();
    assert!(
        error.contains("forbidden link declared Build dependency"),
        "{error}"
    );
    let metadata = fixture(
        "switchboard-central-link",
        json!([]),
        "acp-client-runtime",
        Some(DependencyKind::Normal),
    );
    let error = check(&metadata).unwrap_err().to_string();
    assert!(
        error.contains("forbidden link resolved Normal dependency"),
        "{error}"
    );
}

#[test]
fn host_cannot_enable_the_loopback_test_constructor() {
    let metadata = fixture(
        "codex-router-host",
        declared("switchboard-central-link", None, &["test-support"]),
        "switchboard-central-link",
        None,
    );
    let error = check(&metadata).unwrap_err().to_string();
    assert!(
        error.contains("codex-router-host enables the link's loopback test constructor"),
        "{error}"
    );
}

#[test]
fn central_server_is_rejected_on_a_shipped_path_and_allowed_in_the_fixture_dev_graph() {
    let metadata = fixture(
        "switchboard-central-link",
        json!([]),
        "collaboration-central-tokio",
        Some(DependencyKind::Normal),
    );
    let root = metadata.packages.first().unwrap();
    let error = check_shipped_closure(&metadata, root)
        .unwrap_err()
        .to_string();
    assert!(error.contains("reaches Central server package"), "{error}");
    let metadata = fixture(
        "switchboard-central-link-test",
        declared(
            "collaboration-central-tokio",
            Some("dev"),
            &["test-support"],
        ),
        "collaboration-central-tokio",
        Some(DependencyKind::Dev),
    );
    assert!(check(&metadata).is_ok());
}
