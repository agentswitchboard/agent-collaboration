//! BD1's closed workspace dependencies and BD4's transitive proxy boundary.
use super::{CargoMetadata, DependencyKind, NonDevDependencyGraph};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::path::Path;

#[path = "workspace_package_tree_policy.rs"]
mod workspace_package_tree_policy;
pub(super) use workspace_package_tree_policy::check_workspace_package_trees;
#[cfg(test)]
use workspace_package_tree_policy::{
    SERVICE_SUPPORT_PACKAGES, check_package_targets, check_package_tree, check_package_tree_output,
};

#[cfg(test)]
#[path = "job_layer_tests.rs"]
mod job_layer_tests;

pub(super) const FLEET_PURE_CRATES: [(&str, &[&str]); 3] = [
    (FLEET_RPC, &[]),
    (
        "fleet-runtime-ports",
        &["central-engine-model", "central-storage", "workspace-ids"],
    ),
    (
        "fleet-contracts",
        &[
            "approval-request-protocol",
            "central-engine-model",
            "central-storage",
            "fleet-identity-protocol",
            FLEET_RPC,
            "fleet-runtime-ports",
            "message-board",
            "workspace-ids",
            "workspace-model",
        ],
    ),
];
pub(super) const LENT_TRANSACTION: &str = "fleet-lent-transaction";
const FLEET_RPC: &str = "fleet-rpc";
const WORKSPACE_SERVICE: &str = "fleet-workspace-service";
const CENTRAL_CORE: &str = "collaboration-central-core";
const CENTRAL_WORKER: &str = "collaboration-central-cloudflare";
const FLEET_ACCESS: &str = "fleet-workspace-access";
const FLEET_DIRECTORY: &str = "fleet-workspace-directory";
const FLEET_ENGINE: &str = "fleet-engine";
pub(super) const REQUEST_LOG: &str = "fleet-request-log";
const FLEET_MAIL: &str = "fleet-mail";
const FLEET_NOTICES: &str = "fleet-notices";
const FLEET_WAKES: &str = "fleet-wakes";
const FLEET_SCHEDULES: &str = "fleet-schedules";
const FLEET_TIMER_POLICY: &str = "fleet-timer-policy";
pub(super) const HARNESS_CONTRACTS: (&str, &[&str]) = ("switchboard-harness-contracts", &[]);
pub(super) const HARNESS_SHARED_MECHANICS: (&str, &[&str]) = (
    "switchboard-harness-shared-mechanics",
    &[HARNESS_CONTRACTS.0],
);
const HARNESS_LAYERS: [(&str, &[&str]); 2] = [HARNESS_CONTRACTS, HARNESS_SHARED_MECHANICS];
pub(super) const SESSION_STORE: (&str, &[&str]) = (
    "switchboard-session-store",
    &["collaboration-protocol", "sqlite-storage-maintenance"],
);
pub(super) const SESSION_EVENTS: (&str, &[&str]) = (
    "switchboard-session-events",
    &[
        "collaboration-contracts",
        "collaboration-protocol",
        "message-board",
        "router-clock",
        "session-event-model",
        "switchboard-session-store",
    ],
);
pub(super) const TIMER_CRATES: [(&str, &[&str]); 3] = [
    (
        FLEET_WAKES,
        &[
            "central-engine-model",
            "central-storage",
            "fleet-contracts",
            "fleet-engine",
            "fleet-runtime-ports",
            "fleet-timer-policy",
            "workspace-ids",
            "workspace-model",
        ],
    ),
    (
        FLEET_SCHEDULES,
        &[
            "agent-automation",
            "central-engine-model",
            "central-storage",
            "fleet-contracts",
            "fleet-engine",
            "fleet-runtime-ports",
            "fleet-timer-policy",
            "workspace-ids",
            "workspace-model",
        ],
    ),
    (FLEET_TIMER_POLICY, &["central-engine-model"]),
];
pub(super) const MAIL_LAYER: (&str, &[&str]) = (
    FLEET_MAIL,
    &[
        "central-engine-model",
        "central-storage",
        "fleet-contracts",
        LENT_TRANSACTION,
        "fleet-engine",
        "fleet-request-log",
        "fleet-runtime-ports",
        "fleet-workspace-access",
        "workspace-ids",
        "workspace-model",
    ],
);
pub(super) const NOTICES_LAYER: (&str, &[&str]) = (
    FLEET_NOTICES,
    &[
        "central-engine-model",
        "central-storage",
        "fleet-contracts",
        "fleet-engine",
        "fleet-runtime",
        "fleet-runtime-ports",
        "fleet-workspace-access",
        "workspace-ids",
        "workspace-model",
    ],
);
pub(super) const DIRECTORY_LAYER: (&str, &[&str]) = (
    FLEET_DIRECTORY,
    &[
        "central-storage",
        "fleet-contracts",
        "fleet-runtime-ports",
        "message-board",
        "workspace-model",
    ],
);
pub(super) const ENGINE_EDGES: &[&str] = &[
    "central-engine-model",
    "central-storage",
    "fleet-contracts",
    LENT_TRANSACTION,
    "fleet-runtime",
    "fleet-runtime-ports",
    "workspace-model",
];

pub(super) const SESSION_READ_CRATES: [(&str, &[&str]); 2] = [
    ("switchboard-session-payloads", &["workspace-ids"]),
    ("switchboard-session-calls", &["workspace-ids"]),
];

const APPROVAL_HOLDER: (&str, &[&str]) = (
    "switchboard-approval-holder",
    &[
        "approval-request-protocol",
        "state-machine-core",
        "workspace-ids",
    ],
);

const PURE_CRATES: [(&str, &[&str]); 9] = [
    APPROVAL_HOLDER,
    ("workspace-ids", &[]),
    (
        "workspace-model",
        &[
            "workspace-ids",
            "agent-automation",
            // The delivery frame reuses the pure session and machine label contracts.
            "collaboration-mail-protocol",
            "message-board",
        ],
    ),
    (
        "central-link-protocol",
        &["workspace-ids", "workspace-model"],
    ),
    ("central-storage", &[]),
    ("state-machine-core", &[]),
    (
        "central-engine-model",
        &["central-storage", "state-machine-core"],
    ),
    ("workspace-rules", &["workspace-model", "central-storage"]),
    ("vendor-protocol", &[]),
];
const PROXY_CRATES: &[&str] = &[
    "codex-router-proxy",
    "codex-router-state",
    "codex-router-quota",
    "codex-router-selection",
    "codex-router-auth",
    "codex-router-quota-service",
];
fn is_collaboration(name: &str) -> bool {
    name.starts_with("collaboration-")
        || name.starts_with("message-board")
        || name.starts_with("board-")
        || name.starts_with("fleet-")
        || name.starts_with("workspace-")
        || name.starts_with("central-")
        || matches!(name, "agent-collaboration" | "agent-automation")
}
fn is_effectful(name: &str) -> bool {
    name.starts_with("tokio")
        || name.starts_with("futures")
        || name.starts_with("async-")
        || name.starts_with("sqlx")
        || name.starts_with("turso")
        || name.starts_with("rusqlite")
        || name == "libsqlite3-sys"
        || name.starts_with("worker")
        || name.starts_with("reqwest")
        || name.starts_with("hyper")
        || matches!(
            name,
            "smol" | "mio" | "durable-steps" | "runtime-primitives" | "router-clock"
        )
}
fn reachable<'a>(graph: &'a NonDevDependencyGraph, source: &'a str) -> BTreeSet<&'a str> {
    let mut seen = BTreeSet::new();
    let mut pending = vec![source];
    while let Some(current) = pending.pop() {
        for (destination, _) in graph.edges.get(current).into_iter().flatten() {
            if seen.insert(destination.as_str()) {
                pending.push(destination);
            }
        }
    }
    seen
}
pub(super) fn check_boundaries(
    graph: &NonDevDependencyGraph,
) -> Result<(), Box<dyn std::error::Error>> {
    for name in [
        "collaboration-central-core",
        "collaboration-central-cloudflare",
        "collaboration-central-tokio",
    ] {
        let Some(source) = graph.workspace_id(name) else {
            continue;
        };
        for (destination, _) in graph.edges.get(source).into_iter().flatten() {
            if graph
                .names
                .get(destination)
                .is_some_and(|target| target == "collaboration-protocol")
            {
                return Err(format!(
                    "C1: {name} retains a legacy collaboration-protocol dependency"
                )
                .into());
            }
        }
    }
    for (name, allowed) in PURE_CRATES
        .into_iter()
        .chain(FLEET_PURE_CRATES)
        .chain(SESSION_READ_CRATES)
    {
        let Some(source) = graph.workspace_id(name) else {
            continue;
        };
        for (destination, kind) in graph.edges.get(source).into_iter().flatten() {
            let target = graph
                .names
                .get(destination)
                .ok_or("dependency name absent")?;
            if is_effectful(target) {
                return Err(format!("BD2: {name} depends on effectful package {target}").into());
            }
            if graph.workspace_members.contains(destination) && !allowed.contains(&target.as_str())
            {
                return Err(
                    format!("BD1: forbidden {kind:?} dependency {name} -> {target}").into(),
                );
            }
        }
        check_workspace_driver_edges(graph, source)?;
    }
    check_resolved_session_read_dev_edges(graph)?;
    check_closed_workspace_edges(
        graph,
        WORKSPACE_SERVICE,
        &["central-storage", "fleet-contracts", "workspace-model"],
    )?;
    check_job_reachability(graph, WORKSPACE_SERVICE, &["fleet-runtime"])?;
    check_job_reachability(graph, CENTRAL_CORE, &[WORKSPACE_SERVICE])?;
    check_closed_workspace_edges(graph, FLEET_ENGINE, ENGINE_EDGES)?;
    for (name, allowed) in TIMER_CRATES {
        check_closed_workspace_edges(graph, name, allowed)?;
    }
    check_closed_workspace_edges(graph, MAIL_LAYER.0, MAIL_LAYER.1)?;
    check_closed_workspace_edges(graph, NOTICES_LAYER.0, NOTICES_LAYER.1)?;
    check_closed_workspace_edges(graph, DIRECTORY_LAYER.0, DIRECTORY_LAYER.1)?;
    check_closed_workspace_edges(graph, SESSION_STORE.0, SESSION_STORE.1)?;
    check_closed_workspace_edges(graph, SESSION_EVENTS.0, SESSION_EVENTS.1)?;
    check_harness_layers(graph)?;
    check_job_reachability(
        graph,
        FLEET_WAKES,
        &[FLEET_SCHEDULES, FLEET_MAIL, FLEET_NOTICES, FLEET_DIRECTORY],
    )?;
    check_job_reachability(
        graph,
        FLEET_SCHEDULES,
        &[FLEET_WAKES, FLEET_MAIL, FLEET_NOTICES, FLEET_DIRECTORY],
    )?;
    check_job_reachability(
        graph,
        FLEET_MAIL,
        &[
            FLEET_WAKES,
            FLEET_SCHEDULES,
            FLEET_TIMER_POLICY,
            FLEET_NOTICES,
            FLEET_DIRECTORY,
        ],
    )?;
    check_job_reachability(
        graph,
        FLEET_NOTICES,
        &[
            FLEET_MAIL,
            FLEET_WAKES,
            FLEET_SCHEDULES,
            FLEET_TIMER_POLICY,
            FLEET_DIRECTORY,
        ],
    )?;
    check_job_reachability(graph, FLEET_ENGINE, &[FLEET_DIRECTORY])?;
    check_job_reachability(
        graph,
        FLEET_DIRECTORY,
        &[
            LENT_TRANSACTION,
            FLEET_ACCESS,
            FLEET_ENGINE,
            FLEET_MAIL,
            FLEET_NOTICES,
            FLEET_WAKES,
            FLEET_SCHEDULES,
            FLEET_TIMER_POLICY,
            "fleet-workspace-requests",
            "fleet-runtime",
            "fleet-signin",
            "fleet-machine-login",
            "fleet-admin",
        ],
    )?;
    for name in PROXY_CRATES {
        let Some(source) = graph.workspace_id(name) else {
            continue;
        };
        for destination in reachable(graph, source) {
            let target = graph
                .names
                .get(destination)
                .ok_or("dependency name absent")?;
            if is_collaboration(target) {
                return Err(
                    format!("BD4: {name} reaches collaboration dependency {target}").into(),
                );
            }
        }
    }
    Ok(())
}

const JOB_FORBIDDEN_SERVICES: &[&str] = &[
    "fleet-application",
    "collaboration-central-core",
    "collaboration-central-tokio",
    "collaboration-central-cloudflare",
    "codex-router-host",
    "codex-router-debug-hosts",
    "codex-router-tailnet-verification-host",
];

fn check_job_reachability(
    graph: &NonDevDependencyGraph,
    job: &str,
    siblings: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(source) = graph.workspace_id(job) else {
        return Ok(());
    };
    for destination in reachable(graph, source) {
        if !graph.workspace_members.contains(destination) {
            continue;
        }
        let target = graph
            .names
            .get(destination)
            .ok_or("dependency name absent")?;
        if siblings.contains(&target.as_str()) || JOB_FORBIDDEN_SERVICES.contains(&target.as_str())
        {
            return Err(format!("BD1: forbidden reachable dependency {job} -> {target}").into());
        }
    }
    Ok(())
}

// Default feature resolution omits inactive optional dependencies. Match the
// manifest declaration's package name and path against workspace members.
pub(super) fn check_declared_session_owner_edges(
    metadata: &CargoMetadata,
) -> Result<(), Box<dyn std::error::Error>> {
    let workspace_packages: Vec<_> = metadata
        .packages
        .iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .collect();
    for (owner, allowed) in [SESSION_STORE, SESSION_EVENTS]
        .into_iter()
        .chain(HARNESS_LAYERS)
    {
        let package = workspace_packages
            .iter()
            .find(|package| package.name == owner)
            .ok_or_else(|| format!("closed dependency policy package missing: {owner}"))?;
        for dependency in &package.dependencies {
            let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
            if kind == DependencyKind::Dev && !is_harness_layer(owner) {
                continue;
            }
            if let Some(path) = dependency.path.as_deref()
                && workspace_packages.iter().any(|package| {
                    package.name == dependency.name && package.manifest_path.parent() == Some(path)
                })
            {
                check_closed_workspace_edge(owner, &dependency.name, kind, allowed)?;
            }
        }
    }
    Ok(())
}

pub(super) fn check_declared_approval_holder_edges(
    metadata: &CargoMetadata,
) -> Result<(), Box<dyn std::error::Error>> {
    let (owner, normal_edges) = APPROVAL_HOLDER;
    let workspace_packages = || {
        metadata
            .packages
            .iter()
            .filter(|package| metadata.workspace_members.contains(&package.id))
    };
    let holder = workspace_packages()
        .find(|package| package.name == owner)
        .ok_or_else(|| format!("closed dependency policy package missing: {owner}"))?;
    for dependency in &holder.dependencies {
        let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
        if kind == DependencyKind::Dev {
            continue;
        }
        let workspace_edge = dependency.path.as_deref().is_some_and(|path| {
            workspace_packages().any(|package| {
                package.name == dependency.name && package.manifest_path.parent() == Some(path)
            })
        });
        let allowed = if kind == DependencyKind::Normal && workspace_edge {
            normal_edges
        } else {
            &[]
        };
        check_closed_workspace_edge(owner, &dependency.name, kind, allowed)?;
    }
    Ok(())
}

// Only the declaration inventory sees every inactive optional dependency.
pub(super) fn check_declared_session_read_edges(
    metadata: &CargoMetadata,
) -> Result<(), Box<dyn std::error::Error>> {
    let workspace_packages: Vec<_> = metadata
        .packages
        .iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .collect();
    for (owner, allowed_workspace) in SESSION_READ_CRATES {
        let package = workspace_packages
            .iter()
            .find(|package| package.name == owner)
            .ok_or_else(|| format!("closed dependency policy package missing: {owner}"))?;
        for dependency in &package.dependencies {
            let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
            let workspace_edge = dependency.path.as_deref().is_some_and(|path| {
                workspace_packages.iter().any(|package| {
                    package.name == dependency.name && package.manifest_path.parent() == Some(path)
                })
            });
            let registry_edge = dependency
                .source
                .as_deref()
                .is_some_and(|source| source.starts_with("registry+"))
                && dependency.path.is_none();
            let allowed: &[&str] = match (kind, workspace_edge, registry_edge) {
                (DependencyKind::Normal, true, false) => allowed_workspace,
                (DependencyKind::Normal, false, true) if owner == "switchboard-session-calls" => {
                    &["serde", "schemars", "thiserror"]
                }
                (DependencyKind::Normal, false, true) => &["serde", "schemars"],
                (DependencyKind::Dev, false, true) => allowed_session_read_dev_dependencies(owner),
                _ => &[],
            };
            check_closed_workspace_edge(owner, &dependency.name, kind, allowed)?;
            let allowed_features: &[&str] = match dependency.name.as_str() {
                "serde" => &["derive"],
                "hegeltest" => &["static-engine"],
                _ => &[],
            };
            if dependency.features != allowed_features || !dependency.uses_default_features {
                return Err(format!(
                    "BD1: forbidden {kind:?} feature settings {owner} -> {}: features {:?}, default features: {}",
                    dependency.name, dependency.features, dependency.uses_default_features
                ).into());
            }
        }
    }
    Ok(())
}

fn allowed_session_read_dev_dependencies(owner: &str) -> &'static [&'static str] {
    if owner == "switchboard-session-calls" {
        &["serde_json", "serde_path_to_error", "hegeltest"]
    } else {
        &["serde_json", "serde_path_to_error"]
    }
}

fn check_resolved_session_read_dev_edges(
    graph: &NonDevDependencyGraph,
) -> Result<(), Box<dyn std::error::Error>> {
    for (owner, _) in SESSION_READ_CRATES {
        let Some(source) = graph.workspace_id(owner) else {
            continue;
        };
        for destination in graph.dev_edges.get(source).into_iter().flatten() {
            let target = graph
                .names
                .get(destination)
                .ok_or("dependency name absent")?;
            let allowed = if graph.workspace_members.contains(destination) {
                &[]
            } else {
                allowed_session_read_dev_dependencies(owner)
            };
            check_closed_workspace_edge(owner, target, DependencyKind::Dev, allowed)?;
        }
    }
    Ok(())
}

fn check_closed_workspace_edge(
    source: &str,
    destination: &str,
    kind: DependencyKind,
    allowed: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    if !allowed.contains(&destination) {
        return Err(format!("BD1: forbidden {kind:?} dependency {source} -> {destination}").into());
    }
    Ok(())
}

fn check_closed_workspace_edges(
    graph: &NonDevDependencyGraph,
    package: &str,
    allowed: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(source) = graph.workspace_id(package) else {
        return Ok(());
    };
    let normal_and_build = graph
        .edges
        .get(source)
        .into_iter()
        .flatten()
        .map(|(destination, kind)| (destination, *kind));
    let harness_dev = graph
        .dev_edges
        .get(source)
        .filter(|_| is_harness_layer(package))
        .into_iter()
        .flatten()
        .map(|destination| (destination, DependencyKind::Dev));
    for (destination, kind) in normal_and_build.chain(harness_dev) {
        if graph.workspace_members.contains(destination) {
            let target = graph
                .names
                .get(destination)
                .ok_or("dependency name absent")?;
            check_closed_workspace_edge(package, target, kind, allowed)?;
        }
    }
    Ok(())
}

fn is_harness_layer(package: &str) -> bool {
    HARNESS_LAYERS.iter().any(|(name, _)| *name == package)
}

pub(super) fn check_harness_layers(
    graph: &NonDevDependencyGraph,
) -> Result<(), Box<dyn std::error::Error>> {
    for (package, allowed) in HARNESS_LAYERS {
        check_closed_workspace_edges(graph, package, allowed)?;
        check_harness_reachability(graph, package, allowed)?;
    }
    Ok(())
}

fn check_harness_reachability(
    graph: &NonDevDependencyGraph,
    package: &str,
    allowed: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(source) = graph.workspace_id(package) else {
        return Ok(());
    };
    // A dependency's own dev edges are not part of the root package's tests.
    let test_dependencies = graph.dev_edges.get(source).into_iter().flatten();
    for root in std::iter::once(source).chain(test_dependencies.map(String::as_str)) {
        for destination in reachable(graph, root) {
            let target = graph
                .names
                .get(destination)
                .ok_or("dependency name absent")?;
            if package == HARNESS_CONTRACTS.0 && matches!(target.as_str(), "tokio" | "tokio-util") {
                return Err(
                    format!("BD1: forbidden reachable runtime {package} -> {target}").into(),
                );
            }
            if destination != source
                && graph.workspace_members.contains(destination)
                && !allowed.contains(&target.as_str())
            {
                return Err(
                    format!("BD1: forbidden reachable dependency {package} -> {target}").into(),
                );
            }
        }
    }
    Ok(())
}

// Workspace-wide metadata merges unrelated features into external packages.
// Follow workspace edges here; the package-scoped trees below check full closures.
fn check_workspace_driver_edges(
    graph: &NonDevDependencyGraph,
    source: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut seen = BTreeSet::new();
    let mut pending = vec![source];
    while let Some(current) = pending.pop() {
        if !seen.insert(current) {
            continue;
        }
        for (destination, _) in graph.edges.get(current).into_iter().flatten() {
            let target = graph
                .names
                .get(destination)
                .ok_or("dependency name absent")?;
            if is_effectful(target) {
                return Err(format!("BD2: pure workspace dependency reaches {target}").into());
            }
            if graph.workspace_members.contains(destination) {
                pending.push(destination);
            }
        }
    }
    Ok(())
}

#[test]
fn pure_package_tree_rejects_effectful_transitive_dependencies() {
    assert!(
        check_package_tree_output(
            "workspace-rules v1\nserde v1\ntokio v1",
            &[],
            "workspace-rules"
        )
        .is_err()
    );
    assert!(
        check_package_tree_output("workspace-rules v1\nsqlx-sqlite v1", &[], "workspace-rules")
            .is_err()
    );
    assert!(
        check_package_tree_output(
            "workspace-rules v1\nworkspace-model v1\nuuid v1\nserde v1",
            &[],
            "workspace-rules",
        )
        .is_ok()
    );
}

#[test]
fn access_dependency_tree_allows_tokio_sync_but_rejects_the_executor() {
    let valid = "fleet-workspace-access v1\t\ntokio v1\tsync";
    assert!(check_package_tree_output(valid, &[], FLEET_ACCESS).is_ok());

    let invalid = "fleet-workspace-access v1\t\ntokio v1\trt,sync";
    let violation = check_package_tree_output(invalid, &[], FLEET_ACCESS)
        .expect_err("workspace access cannot enable the Tokio executor");
    assert_eq!(
        violation.to_string(),
        "BD2: fleet-workspace-access reaches Tokio features rt,sync; only sync is allowed"
    );
}

#[test]
fn fleet_package_tree_rejects_an_indirect_wasm_worker_dependency() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/wasm-worker-dependency");
    for package in ["fleet-contracts", "fleet-runtime-ports", FLEET_ACCESS] {
        check_package_tree(&root, package, None).expect("the native dependency tree is pure");

        let violation = check_package_targets(&root, package)
            .expect_err("the Fleet package must reject the indirect wasm-only Worker dependency");

        assert_eq!(
            violation.to_string(),
            format!(
                "{package} dependency tree for wasm32-unknown-unknown: BD2: pure rules transitively reach worker"
            )
        );
    }
}

#[test]
fn mail_contract_dependency_keeps_model_and_link_transitively_driver_free() {
    for kind in [DependencyKind::Normal, DependencyKind::Build] {
        assert!(check_boundaries(&graph(&[("workspace-model", "agent-automation", kind)])).is_ok());
        assert!(
            check_boundaries(&graph(&[
                ("workspace-model", "collaboration-mail-protocol", kind),
                ("workspace-model", "message-board", kind),
            ]))
            .is_ok()
        );
        assert!(
            check_boundaries(&graph(&[
                ("workspace-model", "agent-automation", kind),
                ("agent-automation", "tokio", kind),
            ]))
            .is_err()
        );
        assert!(
            check_boundaries(&graph(&[
                ("central-link-protocol", "workspace-model", kind),
                ("workspace-model", "agent-automation", kind),
                ("agent-automation", "sqlx-sqlite", kind),
            ]))
            .is_err()
        );
        assert!(check_boundaries(&graph(&[("workspace-model", "unlisted-host", kind)])).is_err());
    }
}

#[test]
fn central_owners_reject_direct_legacy_protocol_edges() {
    for owner in [
        "collaboration-central-core",
        "collaboration-central-cloudflare",
        "collaboration-central-tokio",
    ] {
        for kind in [DependencyKind::Normal, DependencyKind::Build] {
            assert!(check_boundaries(&graph(&[(owner, "collaboration-protocol", kind)])).is_err());
        }
        assert!(
            check_boundaries(&graph(&[(
                owner,
                "collaboration-protocol",
                DependencyKind::Dev
            )]))
            .is_ok()
        );
    }
}

#[test]
fn fleet_interfaces_reject_upward_normal_and_build_edges() {
    for kind in [DependencyKind::Normal, DependencyKind::Build] {
        assert!(
            check_boundaries(&graph(&[("fleet-contracts", "fleet-runtime-ports", kind)])).is_ok()
        );
    }
    for interface in ["fleet-runtime-ports", "fleet-contracts"] {
        for owner in [
            "collaboration-central-core",
            "collaboration-central-tokio",
            "collaboration-central-cloudflare",
        ] {
            for kind in [DependencyKind::Normal, DependencyKind::Build] {
                let violation = check_boundaries(&graph(&[(interface, owner, kind)]))
                    .expect_err("the Fleet interface must reject an upward edge");
                assert_eq!(
                    violation.to_string(),
                    format!("BD1: forbidden {kind:?} dependency {interface} -> {owner}")
                );
            }
        }
    }
}

#[test]
fn workspace_rpc_rejects_upward_normal_and_build_edges() {
    for kind in [DependencyKind::Normal, DependencyKind::Build] {
        for (from, to) in [
            (FLEET_RPC, "fleet-runtime"),
            (WORKSPACE_SERVICE, CENTRAL_WORKER),
            (WORKSPACE_SERVICE, CENTRAL_CORE),
            (WORKSPACE_SERVICE, "fleet-runtime"),
            (CENTRAL_CORE, WORKSPACE_SERVICE),
        ] {
            let violation = check_boundaries(&graph(&[(from, to, kind)]))
                .expect_err("the workspace RPC layer must reject an upward edge");
            let diagnostic = violation.to_string();
            assert!(
                diagnostic.contains(from) && diagnostic.contains(to),
                "{diagnostic}"
            );
        }
    }
}

#[test]
fn workspace_rpc_rejects_effects_and_concrete_sqlite_in_the_package_tree() {
    for (package, allowed, dependency) in [
        (FLEET_RPC, &[][..], "sqlx"),
        (FLEET_RPC, &[][..], "worker"),
        (WORKSPACE_SERVICE, SERVICE_SUPPORT_PACKAGES, "sqlx-sqlite"),
        (WORKSPACE_SERVICE, SERVICE_SUPPORT_PACKAGES, "tokio"),
        (CENTRAL_WORKER, &[][..], "libsqlite3-sys"),
    ] {
        let violation =
            check_package_tree_output(&format!("{package} v1\n{dependency} v1"), allowed, package)
                .expect_err("the workspace RPC layer must reject this dependency");
        assert!(violation.to_string().contains(dependency));
    }
}

fn graph(edges: &[(&str, &str, DependencyKind)]) -> NonDevDependencyGraph {
    let names: BTreeMap<_, _> = edges
        .iter()
        .flat_map(|(from, to, _)| [*from, *to])
        .map(|name| (name.to_owned(), name.to_owned()))
        .collect();
    let mut graph = NonDevDependencyGraph {
        workspace_members: names
            .keys()
            .filter(|name| !matches!(name.as_str(), "tokio" | "sqlx-sqlite"))
            .cloned()
            .collect(),
        names,
        edges: BTreeMap::new(),
        dev_edges: BTreeMap::new(),
    };
    for (from, to, kind) in edges {
        match kind {
            DependencyKind::Dev => {
                graph
                    .dev_edges
                    .entry((*from).to_owned())
                    .or_default()
                    .insert((*to).to_owned());
            }
            DependencyKind::Normal | DependencyKind::Build => {
                graph
                    .edges
                    .entry((*from).to_owned())
                    .or_default()
                    .insert(((*to).to_owned(), *kind));
            }
        }
    }
    graph
}
#[test]
fn lent_transaction_allows_sqlx_without_a_backend() {
    let neutral = "fleet-lent-transaction v1\nsqlx v0.9\nsqlx-core v0.9";
    assert!(check_package_tree_output(neutral, &[], LENT_TRANSACTION).is_ok());
    for backend in [
        "sqlx-sqlite",
        "sqlx-postgres",
        "sqlx-mysql",
        "sqlx-durable-object",
    ] {
        let with_backend = format!("{neutral}\n{backend} v0.9");
        assert!(check_package_tree_output(&with_backend, &[], LENT_TRANSACTION).is_err());
    }
}

#[test]
fn pure_rules_reject_unlisted_workspace_and_transitive_external_effects() {
    for kind in [DependencyKind::Normal, DependencyKind::Build] {
        assert!(check_boundaries(&graph(&[("workspace-rules", "unlisted-host", kind)])).is_err());
        assert!(
            check_boundaries(&graph(&[
                ("workspace-rules", "workspace-model", kind),
                ("workspace-model", "tokio", kind)
            ]))
            .is_err()
        );
        assert!(check_boundaries(&graph(&[("workspace-rules", "sqlx-sqlite", kind)])).is_err());
        assert!(
            check_boundaries(&graph(&[("fleet-runtime-ports", LENT_TRANSACTION, kind)])).is_err()
        );
    }
    assert!(
        check_boundaries(&graph(&[
            ("workspace-rules", "workspace-model", DependencyKind::Normal),
            ("workspace-model", "workspace-ids", DependencyKind::Build)
        ]))
        .is_ok()
    );
}
#[test]
fn proxy_boundary_rejects_direct_and_transitive_normal_and_build_edges() {
    for kind in [DependencyKind::Normal, DependencyKind::Build] {
        assert!(
            check_boundaries(&graph(&[(
                "codex-router-proxy",
                "collaboration-runtime",
                kind
            )]))
            .is_err()
        );
        assert!(
            check_boundaries(&graph(&[
                ("codex-router-auth", "unlisted-middle", kind),
                ("unlisted-middle", "message-board", kind)
            ]))
            .is_err()
        );
    }
    assert!(
        check_boundaries(&graph(&[(
            "codex-router-host",
            "collaboration-runtime",
            DependencyKind::Normal
        )]))
        .is_ok()
    );
    assert!(
        check_boundaries(&graph(&[(
            "codex-router-proxy",
            "collaboration-runtime",
            DependencyKind::Dev
        )]))
        .is_ok()
    );
}

#[cfg(test)]
#[path = "approval_holder_dependency_test.rs"]
mod approval_holder_dependency_tests;

#[cfg(test)]
#[path = "session_read_dependency_test.rs"]
mod session_read_dependency_tests;
