//! Message custody owns neither the socket, the Host, enrollment nor Central's server.

use super::{
    CargoMetadata, CargoPackage, DependencyKind, harness_stage_policy::CRATES_IO, read_metadata,
};
use std::{path::Path, process::Command};

const CUSTODY: &str = "switchboard-message-custody";
const CLOCK_SOURCE: &str = "git+https://github.com/agent-collaboration/agent-fleet-sync.git?rev=b75d072c116afa46cfa82facd34db6317f380687";
const CUSTODY_CONTRACTS: &[&str] = &[
    "central-link-client",
    "central-link-protocol",
    "router-clock",
    "state-machine-core",
    "switchboard-machine-core-store",
    "workspace-ids",
    "workspace-model",
];

type PolicyResult<T> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn custody_dependency_graph_is_isolated() -> PolicyResult<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let default = read_metadata(&root, &[])?;
    check_declared(&default)?;
    check_resolved(&default)?;
    let optional = read_metadata(&root, &["--all-features"])?;
    check_resolved(&optional)?;
    check_package_trees(&root)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CustodyDependencySource {
    Workspace,
    Registry,
    PinnedClock,
    Other,
}

fn custody_package(metadata: &CargoMetadata) -> PolicyResult<&CargoPackage> {
    metadata
        .packages
        .iter()
        .find(|package| package.name == CUSTODY && workspace_package(metadata, package))
        .ok_or_else(|| format!("missing workspace package {CUSTODY}").into())
}

fn workspace_package(metadata: &CargoMetadata, package: &CargoPackage) -> bool {
    metadata.workspace_members.contains(&package.id) && package.source.is_none()
}

fn external_source(source: Option<&str>) -> CustodyDependencySource {
    match source {
        Some(CRATES_IO) => CustodyDependencySource::Registry,
        Some(source) if source.split('#').next() == Some(CLOCK_SOURCE) => {
            CustodyDependencySource::PinnedClock
        }
        Some(_) | None => CustodyDependencySource::Other,
    }
}

fn permitted_edge(kind: DependencyKind, name: &str, source: CustodyDependencySource) -> bool {
    use CustodyDependencySource::{PinnedClock, Registry, Workspace};
    match (kind, source) {
        (DependencyKind::Normal, Workspace) => CUSTODY_CONTRACTS.contains(&name),
        (DependencyKind::Normal, PinnedClock) => name == "runtime-primitives",
        (DependencyKind::Normal, Registry) => matches!(name, "thiserror" | "tokio"),
        (DependencyKind::Dev, Workspace) => name == "router-clock",
        (DependencyKind::Dev, Registry) => {
            matches!(name, "chrono" | "hegeltest" | "tempfile" | "tokio")
        }
        _ => false,
    }
}

pub(super) fn check_declared(metadata: &CargoMetadata) -> PolicyResult<()> {
    for dependency in &custody_package(metadata)?.dependencies {
        let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
        let source = match dependency.path.as_deref() {
            Some(path)
                if dependency.source.is_none()
                    && metadata.packages.iter().any(|package| {
                        workspace_package(metadata, package)
                            && package.name == dependency.name
                            && package.manifest_path.parent() == Some(path)
                    }) =>
            {
                CustodyDependencySource::Workspace
            }
            None => external_source(dependency.source.as_deref()),
            Some(_) => CustodyDependencySource::Other,
        };
        let name = dependency.name.as_str();
        if !permitted_edge(kind, name, source) {
            return Err(
                format!("forbidden declared {kind:?} custody edge to {name} ({source:?})").into(),
            );
        }
        let allowed = |feature: &str| match (kind, name) {
            (DependencyKind::Dev, "router-clock") => feature == "test-support",
            (DependencyKind::Dev, "hegeltest") => feature == "static-engine",
            (_, "tokio") => matches!(
                feature,
                "rt" | "rt-multi-thread"
                    | "macros"
                    | "time"
                    | "sync"
                    | "net"
                    | "signal"
                    | "process"
                    | "io-util"
            ),
            _ => false,
        };
        if !dependency.features.iter().all(|feature| allowed(feature)) {
            return Err(format!(
                "forbidden declared {kind:?} custody features for {name}: {:?}",
                dependency.features
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn check_resolved(metadata: &CargoMetadata) -> PolicyResult<()> {
    let custody = custody_package(metadata)?;
    let node = metadata
        .resolve
        .nodes
        .iter()
        .find(|node| node.id == custody.id)
        .ok_or("custody is missing from Cargo's resolve graph")?;
    for dependency in &node.deps {
        let target = metadata
            .packages
            .iter()
            .find(|package| package.id == dependency.pkg)
            .ok_or("custody resolution points to a missing package")?;
        let source = if workspace_package(metadata, target) {
            CustodyDependencySource::Workspace
        } else {
            external_source(target.source.as_deref())
        };
        for edge in &dependency.dep_kinds {
            let kind = edge.kind.unwrap_or(DependencyKind::Normal);
            if !permitted_edge(kind, &target.name, source) {
                return Err(format!(
                    "forbidden resolved {kind:?} custody edge to {} ({source:?})",
                    target.name
                )
                .into());
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum CustodyBuildView {
    Runtime,
    Tests,
}

pub(super) fn check_package_trees(root: &Path) -> PolicyResult<()> {
    for (view, edges) in [
        (CustodyBuildView::Runtime, "normal,build"),
        (CustodyBuildView::Tests, "normal,build,dev"),
    ] {
        let output = Command::new("cargo")
            .args([
                "tree",
                "--locked",
                "--offline",
                "--all-features",
                "--color",
                "never",
                "-p",
                CUSTODY,
                "--edges",
                edges,
                "--prefix",
                "none",
                "--format",
                "{p}\t{f}\t",
            ])
            .current_dir(root)
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "custody {view:?} dependency tree exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        check_tree(&String::from_utf8(output.stdout)?, view)?;
    }
    Ok(())
}

fn check_tree(tree: &str, view: CustodyBuildView) -> PolicyResult<()> {
    let mut contains_root = false;
    for line in tree.lines() {
        let (identity, tail) = line
            .split_once('\t')
            .ok_or("custody tree is missing the feature column")?;
        let name = identity
            .split_whitespace()
            .next()
            .ok_or("custody tree is missing a package name")?;
        let features = tail
            .split_once('\t')
            .ok_or("custody tree lost the feature delimiter")?
            .0;
        contains_root |= name == CUSTODY;
        let forbidden = name.starts_with("collaboration-central-")
            || name.contains("harness")
            || matches!(
                name,
                "switchboard-central-link"
                    | "switchboard-machine-enrollment"
                    | "codex-router-host"
                    | "codex-router-debug-hosts"
                    | "collaboration-runtime"
                    | "collaboration-service"
                    | "fleet-engine"
                    | "fleet-runtime"
                    | "fleet-store-sqlx"
            );
        if forbidden || (matches!(view, CustodyBuildView::Runtime) && name == "hegeltest") {
            return Err(format!("forbidden reachable {view:?} custody dependency {name}").into());
        }
        let leaked_feature = features.split(',').find(|feature| {
            feature.contains("test-support")
                && !(matches!(view, CustodyBuildView::Tests)
                    && name == "router-clock"
                    && *feature == "test-support")
        });
        if let Some(feature) = leaked_feature {
            return Err(
                format!("forbidden reachable {view:?} custody feature {name}/{feature}").into(),
            );
        }
    }
    if !contains_root {
        return Err(format!("custody {view:?} tree is missing the root {CUSTODY}").into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "message_custody_policy_test.rs"]
mod tests;
