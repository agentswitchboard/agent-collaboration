use super::{CUSTODY, CargoMetadata, CustodyBuildView, check_declared, check_resolved, check_tree};
use serde_json::{Value, json};

fn workspace_edge(name: &str, kind: Option<&str>, features: &[&str]) -> Value {
    json!({
        "name": name,
        "path": format!("/work/{name}"),
        "kind": kind,
        "features": features,
        "uses_default_features": true,
        "optional": true,
        "rename": "alias-is-not-a-separate-permission",
    })
}

fn custody_graph(edge: Value, name: &str, resolved_kind: Option<Option<&str>>) -> CargoMetadata {
    let resolved = resolved_kind.map_or_else(Vec::new, |kind| {
        vec![json!({"pkg": name, "dep_kinds": [{"kind": kind}]})]
    });
    serde_json::from_value(json!({
        "packages": [
            {"id": CUSTODY, "name": CUSTODY, "manifest_path": format!("/work/{CUSTODY}/Cargo.toml"), "dependencies": [edge]},
            {"id": name, "name": name, "manifest_path": format!("/work/{name}/Cargo.toml")},
        ],
        "workspace_members": [CUSTODY, name],
        "resolve": {"nodes": [{"id": CUSTODY, "deps": resolved}]},
    })).unwrap()
}

#[test]
fn custody_direct_edges_reject_inactive_upper_layers_and_lookalike_sources() {
    let allowed = custody_graph(
        workspace_edge("central-link-client", None, &[]),
        "central-link-client",
        Some(None),
    );
    assert!(check_declared(&allowed).is_ok());
    for name in [
        "switchboard-central-link",
        "codex-router-host",
        "collaboration-runtime",
        "switchboard-harness-contracts",
        "collaboration-central-tokio",
        "switchboard-machine-enrollment",
    ] {
        let forbidden = custody_graph(workspace_edge(name, Some("dev"), &[]), name, None);
        assert!(
            check_declared(&forbidden)
                .unwrap_err()
                .to_string()
                .contains("forbidden declared Dev custody edge")
        );
    }
    let mut lookalike = workspace_edge("central-link-client", None, &[]);
    lookalike["path"] = json!("/other/central-link-client");
    let missing_workspace = custody_graph(lookalike, "central-link-client", None);
    assert!(
        check_declared(&missing_workspace)
            .unwrap_err()
            .to_string()
            .contains("(Other)")
    );
}

#[test]
fn custody_declared_features_allow_the_test_clock_on_dev_edges_only() {
    let name = "router-clock";
    let runtime = custody_graph(workspace_edge(name, None, &["test-support"]), name, None);
    let tests = custody_graph(
        workspace_edge(name, Some("dev"), &["test-support"]),
        name,
        Some(Some("dev")),
    );
    assert!(check_declared(&tests).is_ok());
    assert!(
        check_declared(&runtime)
            .unwrap_err()
            .to_string()
            .contains("forbidden declared Normal custody features")
    );
}

#[test]
fn custody_resolved_edges_check_kind_and_real_package() {
    let good = "central-link-client";
    let declared_good_but_resolved_build =
        custody_graph(workspace_edge(good, None, &[]), good, Some(Some("build")));
    assert!(check_declared(&declared_good_but_resolved_build).is_ok());
    assert!(
        check_resolved(&declared_good_but_resolved_build)
            .unwrap_err()
            .to_string()
            .contains("forbidden resolved Build")
    );
    let bad = "switchboard-central-link";
    let runtime_driver = custody_graph(workspace_edge(bad, None, &[]), bad, Some(None));
    assert!(
        check_resolved(&runtime_driver)
            .unwrap_err()
            .to_string()
            .contains(bad)
    );
}

#[test]
fn custody_effective_features_and_reachable_upper_layers_are_guarded() {
    let root = format!("{CUSTODY} v0.1.0\t\t\n");
    let clock = format!("{root}router-clock v0.1.0\ttest-support\t\n");
    assert!(check_tree(&clock, CustodyBuildView::Tests).is_ok());
    assert!(
        check_tree(&clock, CustodyBuildView::Runtime)
            .unwrap_err()
            .to_string()
            .contains("router-clock/test-support")
    );
    let driver = format!("{root}switchboard-central-link v0.1.0\t\t\n");
    assert!(
        check_tree(&driver, CustodyBuildView::Tests)
            .unwrap_err()
            .to_string()
            .contains("forbidden reachable")
    );
    assert!(
        check_tree("router-clock v0.1.0", CustodyBuildView::Runtime)
            .unwrap_err()
            .to_string()
            .contains("feature column")
    );
}
