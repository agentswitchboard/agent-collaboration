//! The approval ports may depend only on the two workspace data crates and thiserror.

use std::collections::BTreeSet;

use super::{CargoMetadata, CargoPackage, DependencyKind, harness_stage_policy::CRATES_IO};

const APPROVAL_PORTS: &str = "switchboard-approval-ports";
const DATA_CRATES: [&str; 2] = ["approval-request-protocol", "workspace-ids"];

fn workspace_package<'a>(metadata: &'a CargoMetadata, name: &str) -> Option<&'a CargoPackage> {
    metadata.packages.iter().find(|package| {
        package.name == name
            && package.source.is_none()
            && metadata.workspace_members.contains(&package.id)
    })
}

pub(super) fn check_declared(metadata: &CargoMetadata) -> Result<(), String> {
    let root = workspace_package(metadata, APPROVAL_PORTS)
        .ok_or_else(|| format!("approval ports workspace package missing: {APPROVAL_PORTS}"))?;
    for dependency in &root.dependencies {
        let name = dependency.name.as_str();
        let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
        let approved_source = if DATA_CRATES.contains(&name) {
            dependency.source.is_none()
                && workspace_package(metadata, name).is_some_and(|package| {
                    dependency.path.as_deref() == package.manifest_path.parent()
                })
        } else {
            name == "thiserror"
                && dependency.source.as_deref() == Some(CRATES_IO)
                && dependency.path.is_none()
        };
        if kind != DependencyKind::Normal || !approved_source {
            return Err(format!(
                "approval ports forbid declared {kind:?} dependency {name}"
            ));
        }
        if !dependency.features.is_empty() || !dependency.uses_default_features {
            return Err(format!(
                "approval ports forbid declared feature overrides on {name}: {:?}, default features {}",
                dependency.features, dependency.uses_default_features
            ));
        }
    }
    Ok(())
}

pub(super) fn check_resolved(metadata: &CargoMetadata) -> Result<(), String> {
    let root = workspace_package(metadata, APPROVAL_PORTS)
        .ok_or_else(|| format!("approval ports workspace package missing: {APPROVAL_PORTS}"))?;
    let mut remaining = vec![root.id.as_str()];
    let mut inspected = BTreeSet::new();
    while let Some(package_id) = remaining.pop() {
        if !inspected.insert(package_id) {
            continue;
        }
        let node = metadata
            .resolve
            .nodes
            .iter()
            .find(|node| node.id == package_id)
            .ok_or_else(|| format!("approval ports resolved package missing: {package_id}"))?;
        for dependency in &node.deps {
            let target = metadata
                .packages
                .iter()
                .find(|package| package.id == dependency.pkg)
                .ok_or_else(|| {
                    format!(
                        "approval ports resolved dependency missing: {}",
                        dependency.pkg
                    )
                })?;
            let name = target.name.as_str();
            let workspace_data = DATA_CRATES.contains(&name)
                && workspace_package(metadata, name).is_some_and(|package| package.id == target.id);
            for edge in &dependency.dep_kinds {
                let kind = edge.kind.unwrap_or(DependencyKind::Normal);
                if node.id == root.id {
                    let registry_error = name == "thiserror"
                        && target.source.as_deref() == Some(CRATES_IO)
                        && !metadata.workspace_members.contains(&target.id);
                    if kind != DependencyKind::Normal || !(workspace_data || registry_error) {
                        return Err(format!(
                            "approval ports forbid resolved {kind:?} dependency {name}"
                        ));
                    }
                } else if kind == DependencyKind::Dev {
                    // A dependency's own tests are outside this crate's build.
                    continue;
                } else if (!workspace_data && metadata.workspace_members.contains(&target.id))
                    || is_runtime_or_storage(name)
                {
                    return Err(format!(
                        "approval ports forbid reachable {kind:?} dependency {name}"
                    ));
                }
                remaining.push(target.id.as_str());
            }
        }
    }
    Ok(())
}

fn is_runtime_or_storage(name: &str) -> bool {
    // The workspace-wide metadata union includes chrono's Wasm js-sys closure
    // and the helper features other workspace roots enable on futures-util.
    // These crates supply futures primitives, without an executor.
    !matches!(
        name,
        "futures-channel"
            | "futures-core"
            | "futures-io"
            | "futures-macro"
            | "futures-sink"
            | "futures-task"
            | "futures-util"
    ) && super::workspace_boundaries::is_effectful(name)
}

#[cfg(test)]
#[path = "approval_ports_policy_test.rs"]
mod tests;
