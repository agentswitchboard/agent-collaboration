//! Keep normal/build adjacency downward; dev dependencies are reported separately.
//! Uses the NonDevDependencyGraph pattern from the Host's ACP graph tests.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;

#[path = "test_support_policy.rs"]
mod test_support_policy;

#[path = "workspace_boundaries.rs"]
mod workspace_boundaries;

#[path = "crate_layer_policy.rs"]
mod crate_layer_policy;
use crate_layer_policy::layer_policy;

#[derive(Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
    workspace_members: BTreeSet<String>,
    resolve: CargoResolve,
}

#[derive(Deserialize)]
struct CargoPackage {
    id: String,
    name: String,
    manifest_path: PathBuf,
    #[serde(default)]
    dependencies: Vec<ManifestDependency>,
}

#[derive(Deserialize)]
struct ManifestDependency {
    name: String,
    kind: Option<DependencyKind>,
    path: Option<PathBuf>,
}

#[derive(Deserialize)]
struct CargoResolve {
    nodes: Vec<CargoNode>,
}

#[derive(Deserialize)]
struct CargoNode {
    id: String,
    deps: Vec<CargoDependency>,
}

#[derive(Deserialize)]
struct CargoDependency {
    pkg: String,
    dep_kinds: Vec<CargoDependencyKind>,
}

#[derive(Deserialize)]
struct CargoDependencyKind {
    kind: Option<DependencyKind>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "lowercase")]
enum DependencyKind {
    Normal,
    Build,
    Dev,
}

struct NonDevDependencyGraph {
    names: BTreeMap<String, String>,
    edges: BTreeMap<String, BTreeSet<(String, DependencyKind)>>,
    dev_edges: BTreeMap<String, BTreeSet<String>>,
    workspace_members: BTreeSet<String>,
}

impl NonDevDependencyGraph {
    fn from_metadata(metadata: CargoMetadata) -> Self {
        let names = metadata
            .packages
            .into_iter()
            .map(|package| (package.id, package.name))
            .collect();
        let mut graph = Self {
            names,
            edges: BTreeMap::new(),
            dev_edges: BTreeMap::new(),
            workspace_members: metadata.workspace_members,
        };
        for node in metadata.resolve.nodes {
            for dependency in node.deps {
                for kind in dependency.dep_kinds {
                    match kind.kind.unwrap_or(DependencyKind::Normal) {
                        DependencyKind::Dev => {
                            graph
                                .dev_edges
                                .entry(node.id.clone())
                                .or_default()
                                .insert(dependency.pkg.clone());
                        }
                        kind @ (DependencyKind::Normal | DependencyKind::Build) => {
                            graph
                                .edges
                                .entry(node.id.clone())
                                .or_default()
                                .insert((dependency.pkg.clone(), kind));
                        }
                    }
                }
            }
        }
        graph
    }

    fn workspace_id(&self, package_name: &str) -> Option<&str> {
        self.workspace_members
            .iter()
            .find(|id| self.names.get(*id).is_some_and(|name| name == package_name))
            .map(String::as_str)
    }

    fn check_policy(&self) -> Result<(), LayerPolicyViolation> {
        for (source_name, allowed) in layer_policy() {
            let Some(source) = self.workspace_id(source_name) else {
                continue;
            };
            for (destination, kind) in self.edges.get(source).into_iter().flatten() {
                for (destination_name, _) in layer_policy() {
                    if self.workspace_id(destination_name) == Some(destination.as_str())
                        && !allowed.contains(&destination_name)
                    {
                        return Err(LayerPolicyViolation {
                            source_name,
                            destination_name,
                            kind: *kind,
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("forbidden {kind:?} dependency: {source_name} -> {destination_name}")]
struct LayerPolicyViolation {
    source_name: &'static str,
    destination_name: &'static str,
    kind: DependencyKind,
}

#[test]
fn collaboration_crate_layers_point_downward() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new("cargo")
        .args(["metadata", "--locked", "--offline", "--format-version", "1"])
        .current_dir(&root)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let metadata: CargoMetadata = serde_json::from_slice(&output.stdout)?;
    test_support_policy::check_workspace(&metadata.packages, &metadata.workspace_members)?;
    workspace_boundaries::check_declared_session_owner_edges(&metadata)?;
    let graph = NonDevDependencyGraph::from_metadata(metadata);
    for (source, destinations) in &graph.dev_edges {
        if !graph.workspace_members.contains(source) {
            continue;
        }
        for destination in destinations {
            if let (Some(from), Some(to)) = (graph.names.get(source), graph.names.get(destination))
            {
                eprintln!("allowed dev dependency: {from} -> {to}");
            }
        }
    }
    for (package, _) in layer_policy() {
        eprintln!(
            "layer policy: {package}: {}",
            if graph.workspace_id(package).is_some() {
                "checked"
            } else {
                "absent; skipped"
            }
        );
    }
    graph
        .check_policy()
        .inspect_err(|error| eprintln!("{error}"))?;
    workspace_boundaries::check_boundaries(&graph)?;
    workspace_boundaries::check_workspace_package_trees(&root)?;
    Ok(())
}

fn fixture_graph(
    source: &str,
    destination: &str,
    kinds: Vec<Option<DependencyKind>>,
) -> NonDevDependencyGraph {
    let packages: Vec<_> = [
        ("contracts", "collaboration-contracts"),
        ("runtime", "collaboration-runtime"),
        ("service", "collaboration-service"),
        ("host", "codex-router-host"),
        ("mcp", "collaboration-mcp"),
        ("session-store", "switchboard-session-store"),
        ("session-events", "switchboard-session-events"),
        ("router-clock", "router-clock"),
        ("fleet-ports", "fleet-runtime-ports"),
        ("fleet-contracts", "fleet-contracts"),
        ("fleet-runtime", "fleet-runtime"),
        ("fleet-signin", "fleet-signin"),
        ("central-core", "collaboration-central-core"),
        ("fleet-access", "fleet-workspace-access"),
        ("fleet-storage-schema", "fleet-storage-schema"),
        ("fleet-store-sqlx", "fleet-store-sqlx"),
        ("central-native", "collaboration-central-tokio"),
        ("central-worker", "collaboration-central-cloudflare"),
    ]
    .into_iter()
    .map(|(id, name)| CargoPackage {
        id: id.to_owned(),
        name: name.to_owned(),
        manifest_path: PathBuf::from(format!("/workspace/{id}/Cargo.toml")),
        dependencies: Vec::new(),
    })
    .collect();
    let workspace_members = packages.iter().map(|package| package.id.clone()).collect();
    NonDevDependencyGraph::from_metadata(CargoMetadata {
        packages,
        workspace_members,
        resolve: CargoResolve {
            nodes: vec![CargoNode {
                id: source.to_owned(),
                deps: vec![CargoDependency {
                    pkg: destination.to_owned(),
                    dep_kinds: kinds
                        .into_iter()
                        .map(|kind| CargoDependencyKind { kind })
                        .collect(),
                }],
            }],
        },
    })
}

#[test]
fn session_owner_packages_reject_unlisted_workspace_edges() {
    let cases = [
        (
            "session-store",
            "runtime",
            DependencyKind::Normal,
            "BD1: forbidden Normal dependency switchboard-session-store -> collaboration-runtime",
        ),
        (
            "session-store",
            "mcp",
            DependencyKind::Build,
            "BD1: forbidden Build dependency switchboard-session-store -> collaboration-mcp",
        ),
        (
            "session-events",
            "host",
            DependencyKind::Build,
            "BD1: forbidden Build dependency switchboard-session-events -> codex-router-host",
        ),
        (
            "session-events",
            "mcp",
            DependencyKind::Normal,
            "BD1: forbidden Normal dependency switchboard-session-events -> collaboration-mcp",
        ),
    ];
    let mut observed = Vec::new();
    for (source, destination, kind, _) in cases {
        let graph = fixture_graph(source, destination, vec![Some(kind)]);
        observed.push(
            workspace_boundaries::check_boundaries(&graph)
                .err()
                .map(|error| error.to_string()),
        );
    }
    assert_eq!(
        observed,
        cases.map(|(_, _, _, message)| Some(message.to_owned()))
    );
}

#[test]
fn session_owner_policy_covers_inactive_optional_declarations() {
    for (kind, expected) in [
        (
            None,
            Some("BD1: forbidden Normal dependency switchboard-session-store -> router-clock"),
        ),
        (
            Some("build"),
            Some("BD1: forbidden Build dependency switchboard-session-store -> router-clock"),
        ),
        (Some("dev"), None),
    ] {
        let optional = kind != Some("dev");
        let features = if optional {
            serde_json::json!({"default": [], "probe": ["dep:clock-alias"]})
        } else {
            serde_json::json!({"default": []})
        };
        let resolved = if optional {
            serde_json::json!([])
        } else {
            serde_json::json!([{"pkg": "clock", "dep_kinds": [{"kind": "dev"}]}])
        };
        let metadata: CargoMetadata = serde_json::from_value(serde_json::json!({
            "packages": [
                {
                    "id": "store",
                    "name": "switchboard-session-store",
                    "manifest_path": "/workspace/store/Cargo.toml",
                    "features": features,
                    "dependencies": [{
                        "name": "router-clock",
                        "rename": "clock-alias",
                        "kind": kind,
                        "optional": optional,
                        "path": "/workspace/clock"
                    }]
                },
                {
                    "id": "events",
                    "name": "switchboard-session-events",
                    "manifest_path": "/workspace/events/Cargo.toml"
                },
                {
                    "id": "clock",
                    "name": "router-clock",
                    "manifest_path": "/workspace/clock/Cargo.toml"
                }
            ],
            "workspace_members": ["store", "events", "clock"],
            "resolve": {"nodes": [{"id": "store", "deps": resolved}]}
        }))
        .expect("default-feature Cargo metadata with the optional dependency inactive");

        let observed = workspace_boundaries::check_declared_session_owner_edges(&metadata)
            .err()
            .map(|error| error.to_string());
        assert_eq!(observed.as_deref(), expected, "declared kind {kind:?}");
    }
}

#[test]
fn synthetic_normal_and_build_upward_edges_fail() {
    for kind in [None, Some(DependencyKind::Build)] {
        let graph = fixture_graph("contracts", "runtime", vec![kind]);
        let violation = graph
            .check_policy()
            .expect_err("upward normal/build dependency must fail");
        assert_eq!(violation.source_name, "collaboration-contracts");
        assert_eq!(violation.destination_name, "collaboration-runtime");
        assert_eq!(violation.kind, kind.unwrap_or(DependencyKind::Normal));
    }
}

#[test]
fn synthetic_downward_normal_and_build_edges_pass() {
    let graph = fixture_graph(
        "runtime",
        "contracts",
        vec![None, Some(DependencyKind::Build)],
    );
    assert!(graph.check_policy().is_ok());
}

#[test]
fn fleet_layer_registration_rejects_incoming_router_clock_edges() {
    for (destination, expected) in [
        ("fleet-ports", "fleet-runtime-ports"),
        ("fleet-contracts", "fleet-contracts"),
        ("fleet-access", "fleet-workspace-access"),
    ] {
        for kind in [None, Some(DependencyKind::Build)] {
            let graph = fixture_graph("router-clock", destination, vec![kind]);

            let violation = graph
                .check_policy()
                .expect_err("router-clock must not depend on Fleet interfaces");

            assert_eq!(violation.source_name, "router-clock");
            assert_eq!(violation.destination_name, expected);
            assert_eq!(violation.kind, kind.unwrap_or(DependencyKind::Normal));
        }
    }
}

#[test]
fn dev_edges_are_separate_and_do_not_hide_build_edges() {
    let graph = fixture_graph("contracts", "runtime", vec![Some(DependencyKind::Dev)]);
    assert!(graph.check_policy().is_ok());
    assert_eq!(
        graph.dev_edges,
        BTreeMap::from([(
            "contracts".to_owned(),
            BTreeSet::from(["runtime".to_owned()])
        )])
    );
    let mixed = fixture_graph(
        "contracts",
        "runtime",
        vec![Some(DependencyKind::Dev), Some(DependencyKind::Build)],
    );
    assert!(mixed.check_policy().is_err());
}

#[test]
fn missing_policy_packages_are_skipped() {
    let graph = NonDevDependencyGraph::from_metadata(CargoMetadata {
        packages: Vec::new(),
        workspace_members: BTreeSet::new(),
        resolve: CargoResolve { nodes: Vec::new() },
    });
    assert!(graph.check_policy().is_ok());
}

#[test]
fn only_the_native_central_host_can_reach_the_executor_directly() {
    for kind in [None, Some(DependencyKind::Build)] {
        let native = fixture_graph("central-native", "fleet-store-sqlx", vec![kind]);
        assert!(native.check_policy().is_ok());

        for (source, package) in [
            ("central-worker", "collaboration-central-cloudflare"),
            ("fleet-runtime", "fleet-runtime"),
            ("fleet-access", "fleet-workspace-access"),
            ("fleet-storage-schema", "fleet-storage-schema"),
        ] {
            let graph = fixture_graph(source, "fleet-store-sqlx", vec![kind]);
            let violation = graph
                .check_policy()
                .expect_err("the portable crate cannot use the native executor");
            assert_eq!(violation.source_name, package);
            assert_eq!(violation.destination_name, "fleet-store-sqlx");
            assert_eq!(violation.kind, kind.unwrap_or(DependencyKind::Normal));
        }
    }
}

#[test]
fn fleet_signin_can_use_the_runtime_but_cannot_reach_core_or_host_adapters() {
    for kind in [None, Some(DependencyKind::Build)] {
        assert!(
            fixture_graph("fleet-signin", "fleet-runtime", vec![kind])
                .check_policy()
                .is_ok()
        );
        for (target, name) in [
            ("central-core", "collaboration-central-core"),
            ("central-native", "collaboration-central-tokio"),
            ("central-worker", "collaboration-central-cloudflare"),
        ] {
            let violation = fixture_graph("fleet-signin", target, vec![kind])
                .check_policy()
                .expect_err("sign-in must not depend upward on a composer or host");
            assert_eq!(violation.source_name, "fleet-signin");
            assert_eq!(violation.destination_name, name);
            assert_eq!(violation.kind, kind.unwrap_or(DependencyKind::Normal));
        }
    }
}
