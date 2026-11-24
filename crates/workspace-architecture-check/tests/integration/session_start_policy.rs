//! Direct and reachable dependencies for the pure session-start machine.

use std::collections::{BTreeMap, BTreeSet};

use super::{CargoMetadata, CargoPackage, DependencyKind, harness_stage_policy::CRATES_IO};

const START: &str = "switchboard-session-start";
const NORMAL_WORKSPACE: &[&str] = &[
    "fleet-identity-protocol",
    "state-machine-core",
    "workspace-ids",
];
const NORMAL_REGISTRY: &[&str] = &["thiserror"];
const DEV_REGISTRY: &[&str] = &["hegeltest", "serde_json"];
// fleet-identity-protocol uses message-board's validated enrollment record ID.
const TRANSITIVE_WORKSPACE: &[&str] = &["message-board", "workspace-ids"];

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn start_package(metadata: &CargoMetadata) -> Result<&CargoPackage> {
    metadata
        .packages
        .iter()
        .find(|package| is_workspace(package, metadata) && package.name == START)
        .ok_or_else(|| format!("session start dependency policy package missing: {START}").into())
}

fn is_workspace(package: &CargoPackage, metadata: &CargoMetadata) -> bool {
    package.source.is_none() && metadata.workspace_members.contains(&package.id)
}

fn allowed_direct(name: &str, kind: DependencyKind, workspace: bool, registry: bool) -> bool {
    match kind {
        DependencyKind::Normal => {
            (workspace && NORMAL_WORKSPACE.contains(&name))
                || (registry && NORMAL_REGISTRY.contains(&name))
        }
        DependencyKind::Dev => registry && DEV_REGISTRY.contains(&name),
        DependencyKind::Build => false,
    }
}

pub(super) fn check_declared(metadata: &CargoMetadata) -> Result<()> {
    for dependency in &start_package(metadata)?.dependencies {
        let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
        let workspace = dependency.source.is_none()
            && metadata.packages.iter().any(|package| {
                is_workspace(package, metadata)
                    && package.name == dependency.name
                    && package.manifest_path.parent() == dependency.path.as_deref()
            });
        let registry = dependency.source.as_deref() == Some(CRATES_IO) && dependency.path.is_none();
        if !allowed_direct(&dependency.name, kind, workspace, registry) {
            return Err(format!(
                "BD1: forbidden declared {kind:?} dependency {START} -> {}",
                dependency.name
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn check_resolved(metadata: &CargoMetadata) -> Result<()> {
    let root = start_package(metadata)?;
    let packages: BTreeMap<_, _> = metadata
        .packages
        .iter()
        .map(|pkg| (pkg.id.as_str(), pkg))
        .collect();
    let nodes: BTreeMap<_, _> = metadata
        .resolve
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    let mut seen = BTreeSet::new();
    let mut pending = vec![root.id.as_str()];
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        let node = nodes
            .get(id)
            .ok_or_else(|| format!("session start closure node missing: {id}"))?;
        let direct = id == root.id;
        for dependency in &node.deps {
            let target = packages.get(dependency.pkg.as_str()).ok_or_else(|| {
                format!("session start closure package missing: {}", dependency.pkg)
            })?;
            let workspace = is_workspace(target, metadata);
            let registry = target.source.as_deref() == Some(CRATES_IO);
            for entry in &dependency.dep_kinds {
                let kind = entry.kind.unwrap_or(DependencyKind::Normal);
                if !direct && kind == DependencyKind::Dev {
                    continue;
                }
                let allowed = if direct {
                    allowed_direct(&target.name, kind, workspace, registry)
                } else {
                    registry || (workspace && TRANSITIVE_WORKSPACE.contains(&target.name.as_str()))
                };
                if !allowed {
                    let edge = if direct { "resolved" } else { "reachable" };
                    return Err(format!(
                        "BD1: forbidden {edge} {kind:?} dependency {START} -> {}",
                        target.name
                    )
                    .into());
                }
                pending.push(&target.id);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "session_start_policy_test.rs"]
mod tests;
