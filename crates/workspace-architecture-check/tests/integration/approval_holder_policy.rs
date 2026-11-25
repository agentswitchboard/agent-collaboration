//! Declared dependencies and the shipping and test closure of the pure holder.

use std::collections::{BTreeMap, BTreeSet};

use super::{CargoMetadata, CargoPackage, DependencyKind, harness_stage_policy::CRATES_IO};

const HOLDER: &str = "switchboard-approval-holder";
const NORMAL: &[&str] = &[
    "approval-request-protocol",
    "state-machine-core",
    "switchboard-run-gate",
    "workspace-ids",
];
const DEV: &[&str] = &["hegeltest", "serde_json", "tokio", "turmoil"];

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn holder(metadata: &CargoMetadata) -> Result<&CargoPackage> {
    metadata
        .packages
        .iter()
        .find(|package| {
            package.name == HOLDER
                && package.source.is_none()
                && metadata.workspace_members.contains(&package.id)
        })
        .ok_or_else(|| format!("closed dependency policy package missing: {HOLDER}").into())
}

pub(super) fn check_declared(metadata: &CargoMetadata) -> Result<()> {
    for dependency in &holder(metadata)?.dependencies {
        let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
        let allowed = match kind {
            DependencyKind::Normal => {
                dependency.source.is_none()
                    && NORMAL.contains(&dependency.name.as_str())
                    && metadata.packages.iter().any(|package| {
                        package.name == dependency.name
                            && metadata.workspace_members.contains(&package.id)
                            && package.manifest_path.parent() == dependency.path.as_deref()
                    })
            }
            DependencyKind::Dev => {
                dependency.path.is_none()
                    && dependency.source.as_deref() == Some(CRATES_IO)
                    && DEV.contains(&dependency.name.as_str())
            }
            DependencyKind::Build => false,
        };
        if !allowed {
            return Err(format!(
                "BD1: forbidden declared {kind:?} dependency {HOLDER} -> {}",
                dependency.name
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn check_resolved(metadata: &CargoMetadata) -> Result<()> {
    let root = holder(metadata)?;
    let packages: BTreeMap<_, _> = metadata
        .packages
        .iter()
        .map(|package| (package.id.as_str(), package))
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
            .ok_or_else(|| format!("holder closure node missing: {id}"))?;
        for dependency in &node.deps {
            for resolved in &dependency.dep_kinds {
                let kind = resolved.kind.unwrap_or(DependencyKind::Normal);
                let direct = id == root.id;
                if !direct && kind == DependencyKind::Dev {
                    continue;
                }
                let target = packages.get(dependency.pkg.as_str()).ok_or_else(|| {
                    format!(
                        "holder closure dependency package missing: {}",
                        dependency.pkg
                    )
                })?;
                let workspace =
                    target.source.is_none() && metadata.workspace_members.contains(&target.id);
                let registry = target.source.as_deref() == Some(CRATES_IO) && !workspace;
                if direct {
                    let allowed = match kind {
                        DependencyKind::Normal => {
                            workspace && NORMAL.contains(&target.name.as_str())
                        }
                        DependencyKind::Dev => registry && DEV.contains(&target.name.as_str()),
                        DependencyKind::Build => false,
                    };
                    if !allowed {
                        return Err(format!(
                            "BD1: forbidden resolved {kind:?} dependency {HOLDER} -> {}",
                            target.name
                        )
                        .into());
                    }
                } else if !(registry || workspace && NORMAL.contains(&target.name.as_str())) {
                    return Err(format!(
                        "BD1: forbidden reachable {kind:?} dependency {HOLDER} -> {}",
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
#[path = "approval_holder_policy_test.rs"]
mod tests;
