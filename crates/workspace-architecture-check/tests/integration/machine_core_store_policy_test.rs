use super::{
    BuildView, CORE_STORE, CRATES_IO, CargoMetadata, check_declared, check_resolved, check_tree,
};
use serde_json::{Value, json};

fn workspace_edge(name: &str, kind: Option<&str>, features: &[&str]) -> Value {
    json!({
        "name": name,
        "path": format!("/workspace/{name}"),
        "kind": kind,
        "features": features,
        "uses_default_features": true,
        "optional": true,
        "rename": "can-hide-behind-an-alias",
    })
}

fn registry_edge(
    name: &str,
    kind: Option<&str>,
    default_features: bool,
    features: &[&str],
) -> Value {
    json!({
        "name": name,
        "source": CRATES_IO,
        "kind": kind,
        "features": features,
        "uses_default_features": default_features,
    })
}

fn metadata(
    dependency: Value,
    target_is_workspace: bool,
    resolved_kind: Option<Option<&str>>,
) -> CargoMetadata {
    let name = dependency["name"].as_str().unwrap().to_owned();
    let workspace_members = if target_is_workspace {
        vec![CORE_STORE, &name]
    } else {
        vec![CORE_STORE]
    };
    let node_dependencies = resolved_kind.map_or_else(Vec::new, |kind| {
        vec![json!({"pkg": name, "dep_kinds": [{"kind": kind}]})]
    });
    serde_json::from_value(json!({
        "packages": [
            {
                "id": CORE_STORE,
                "name": CORE_STORE,
                "manifest_path": format!("/workspace/{CORE_STORE}/Cargo.toml"),
                "dependencies": [dependency],
            },
            {
                "id": name,
                "name": name,
                "manifest_path": format!("/workspace/{name}/Cargo.toml"),
                "source": if target_is_workspace { None } else { Some(CRATES_IO) },
            },
        ],
        "workspace_members": workspace_members,
        "resolve": {"nodes": [{"id": CORE_STORE, "deps": node_dependencies}]},
    }))
    .unwrap()
}

#[test]
fn declared_inactive_or_aliased_upper_layer_is_refused_by_kind() {
    for name in [
        "codex-router-host",
        "collaboration-runtime",
        "switchboard-harness-contracts",
        "central-link-client",
        "collaboration-central-core",
    ] {
        for (kind, label) in [
            (None, "Normal"),
            (Some("build"), "Build"),
            (Some("dev"), "Dev"),
        ] {
            let fixture = metadata(workspace_edge(name, kind, &[]), true, None);
            let rejection = check_declared(&fixture).unwrap_err().to_string();
            assert!(
                rejection.contains(&format!(
                    "declared {label} dependency {CORE_STORE} -> {name}"
                )),
                "{rejection}"
            );
        }
    }
}

#[test]
fn approved_names_require_the_repo_source_and_the_correct_kind() {
    let actual = metadata(workspace_edge("workspace-ids", None, &[]), true, Some(None));
    assert!(check_declared(&actual).is_ok());
    assert!(check_resolved(&actual).is_ok());

    let mut lookalike = workspace_edge("workspace-ids", None, &[]);
    lookalike["path"] = json!("/other/workspace-ids");
    let unrelated = metadata(lookalike, true, None);
    assert!(
        check_declared(&unrelated)
            .unwrap_err()
            .to_string()
            .contains("(Other)")
    );

    let build = metadata(
        workspace_edge("workspace-ids", Some("build"), &[]),
        true,
        Some(Some("build")),
    );
    assert!(
        check_declared(&build)
            .unwrap_err()
            .to_string()
            .contains("declared Build")
    );
    assert!(
        check_resolved(&build)
            .unwrap_err()
            .to_string()
            .contains("resolved Build")
    );
}

#[test]
fn declared_features_reject_runtime_test_support_and_non_sqlite_sqlx() {
    let approved_clock = metadata(
        workspace_edge("router-clock", Some("dev"), &["test-support"]),
        true,
        None,
    );
    assert!(check_declared(&approved_clock).is_ok());
    let production_clock = metadata(
        workspace_edge("router-clock", None, &["test-support"]),
        true,
        None,
    );
    assert!(
        check_declared(&production_clock)
            .unwrap_err()
            .to_string()
            .contains("declared Normal features")
    );
    let ids = metadata(
        workspace_edge("workspace-ids", None, &["wire-export"]),
        true,
        None,
    );
    assert!(
        check_declared(&ids)
            .unwrap_err()
            .to_string()
            .contains("wire-export")
    );

    let sqlite = metadata(
        registry_edge("sqlx", None, false, &["sqlite", "migrate"]),
        false,
        None,
    );
    assert!(check_declared(&sqlite).is_ok());
    for (defaults, features) in [(true, &["sqlite"][..]), (false, &["postgres"][..])] {
        let invalid = metadata(registry_edge("sqlx", None, defaults, features), false, None);
        assert!(
            check_declared(&invalid)
                .unwrap_err()
                .to_string()
                .contains("declared Normal features")
        );
    }
}

#[test]
fn resolved_edges_reject_forbidden_packages_even_without_a_matching_declaration() {
    for (kind, label) in [
        (None, "Normal"),
        (Some("build"), "Build"),
        (Some("dev"), "Dev"),
    ] {
        let mut fixture = metadata(
            workspace_edge("collaboration-runtime", kind, &[]),
            true,
            Some(kind),
        );
        fixture.packages[0].dependencies.clear();
        assert!(check_declared(&fixture).is_ok());
        let rejection = check_resolved(&fixture).unwrap_err().to_string();
        assert!(
            rejection.contains(&format!("resolved {label} dependency")),
            "{rejection}"
        );
    }
}

#[test]
fn isolated_trees_reject_indirect_upper_layers_and_production_test_packages() {
    let allowable = format!("{CORE_STORE} v1\t\nsqlx v0.9\tsqlite,sqlite-bundled\t");
    assert!(check_tree(&allowable, BuildView::Production).is_ok());
    for name in [
        "codex-router-host",
        "collaboration-runtime",
        "acp-client-runtime",
        "switchboard-central-link",
        "collaboration-central-tokio",
    ] {
        let tree = format!("{allowable}\n{name} v1\t\t");
        for view in [BuildView::Production, BuildView::Tests] {
            let rejection = check_tree(&tree, view).unwrap_err().to_string();
            assert!(
                rejection.contains(&format!(
                    "forbidden reachable {view:?} dependency {CORE_STORE} -> {name}"
                )),
                "{rejection}"
            );
        }
    }
    let test_tree = format!("{allowable}\nhegeltest v1\tstatic-engine\t");
    assert!(check_tree(&test_tree, BuildView::Tests).is_ok());
    assert!(
        check_tree(&test_tree, BuildView::Production)
            .unwrap_err()
            .to_string()
            .contains("hegeltest")
    );
}

#[test]
fn isolated_trees_reject_transitively_enabled_features_by_build_view() {
    let tree = format!("{CORE_STORE} v1\t\t\nrouter-clock v1\ttest-support\t (*)");
    assert!(check_tree(&tree, BuildView::Tests).is_ok());
    let error = check_tree(&tree, BuildView::Production)
        .unwrap_err()
        .to_string();
    assert!(error.contains("router-clock/test-support"), "{error}");
    for (package, feature) in [
        ("workspace-ids", "wire-export"),
        ("sqlite-storage-maintenance", "test-support"),
        ("sqlx", "postgres"),
    ] {
        let tree = format!("{CORE_STORE} v1\t\t\n{package} v1\t{feature}\t");
        for view in [BuildView::Production, BuildView::Tests] {
            let error = check_tree(&tree, view).unwrap_err().to_string();
            assert!(error.contains(&format!("{package}/{feature}")), "{error}");
        }
    }
}

#[test]
fn isolated_tree_requires_a_matching_root_and_feature_column() {
    assert!(check_tree("workspace-ids v1\t\t", BuildView::Production).is_err());
    assert!(check_tree(CORE_STORE, BuildView::Tests).is_err());
}
