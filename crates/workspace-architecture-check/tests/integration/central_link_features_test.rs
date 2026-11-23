use serde_json::json;

use super::{
    ActiveFeatures, DRIVER, FeatureMetadata, FeaturePolicyError, LOOPBACK,
    check_declared_activation, check_fixture_activation, check_no_loopback,
};

fn feature_fixture(
    default_features: &[&str],
    forwards: &[&str],
    active: &[&str],
) -> FeatureMetadata {
    serde_json::from_value(json!({
        "packages": [
            {"id": "driver", "name": DRIVER, "features": {"default": default_features, "loopback-alias":[LOOPBACK], "test-support": []}, "dependencies": []},
            {"id": "host", "name": "codex-router-host", "features": {"default": ["nested"], "nested": forwards}, "dependencies": [
                {"name": DRIVER, "rename": "driver-alias", "kind": null, "features": [], "uses_default_features": true}
            ]}
        ],
        "workspace_members": ["driver", "host"],
        "resolve": {"nodes": [{"id": "driver", "features": active}, {"id": "host", "features": ["default", "nested"]}]}
    })).unwrap()
}

#[test]
fn loopback_cannot_be_activated_by_a_driver_default_alias() {
    let metadata = feature_fixture(
        &["loopback-alias"],
        &[],
        &["default", "loopback-alias", LOOPBACK],
    );
    assert!(
        matches!(check_declared_activation(&metadata), Err(FeaturePolicyError::DeclaredLoopback(package)) if package == DRIVER)
    );
}

#[test]
fn loopback_cannot_be_forwarded_through_a_renamed_dependency() {
    let metadata = feature_fixture(&[], &["driver-alias?/loopback-alias"], &[LOOPBACK]);
    assert!(
        matches!(check_declared_activation(&metadata), Err(FeaturePolicyError::DeclaredLoopback(package)) if package == "codex-router-host")
    );
}

#[test]
fn loopback_seen_in_a_selected_production_graph_is_refused() {
    let graph = ActiveFeatures::from([(DRIVER.to_owned(), [LOOPBACK.to_owned()].into())]);
    assert!(matches!(
        check_no_loopback(&graph, "codex-router-host"),
        Err(FeaturePolicyError::ShippedLoopback("codex-router-host"))
    ));
}

#[test]
fn real_central_fixture_is_allowed_and_must_have_loopback_enabled() {
    let metadata = feature_fixture(&[], &[], &[LOOPBACK]);
    let enabled = ActiveFeatures::from([(DRIVER.to_owned(), [LOOPBACK.to_owned()].into())]);
    assert!(check_declared_activation(&metadata).is_ok());
    assert!(check_fixture_activation(&metadata, &enabled).is_ok());
    assert!(matches!(
        check_fixture_activation(&metadata, &ActiveFeatures::new()),
        Err(FeaturePolicyError::FixtureWithoutLoopback)
    ));
    let inactive_metadata = feature_fixture(&[], &[], &[]);
    assert!(matches!(
        check_fixture_activation(&inactive_metadata, &enabled),
        Err(FeaturePolicyError::FixtureWithoutLoopback)
    ));
}
