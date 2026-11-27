//! Direct dependency sources and kinds for the harness contract and carrier.

use super::{
    CargoMetadata, CargoPackage, DependencyKind,
    workspace_boundaries::{HARNESS_CONTRACTS, HARNESS_SHARED_MECHANICS},
};

type PolicyError = Box<dyn std::error::Error>;
pub(super) const CRATES_IO: &str = "registry+https://github.com/rust-lang/crates.io-index";
const STATE_MACHINE_CORE: &str = "state-machine-core";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DependencySource {
    WorkspaceContract,
    WorkspaceStateMachineCore,
    CratesIo,
    Other,
}

fn permits(owner: &str, kind: DependencyKind, name: &str, source: DependencySource) -> bool {
    use DependencyKind::{Dev, Normal};
    use DependencySource::{CratesIo, WorkspaceContract, WorkspaceStateMachineCore};

    if owner == HARNESS_CONTRACTS.0 {
        return matches!(
            (kind, name, source),
            (Normal, "uuid" | "serde" | "schemars" | "base64", CratesIo)
        );
    }

    (kind, name, source) == (Normal, HARNESS_CONTRACTS.0, WorkspaceContract)
        || (owner == HARNESS_SHARED_MECHANICS.0
            && (kind, name, source) == (Normal, STATE_MACHINE_CORE, WorkspaceStateMachineCore))
        || matches!(
            (kind, name, source),
            (Normal, "tokio" | "tokio-util", CratesIo) | (Dev, "hegeltest", CratesIo)
        )
}

fn workspace_harness_package<'a>(
    metadata: &'a CargoMetadata,
    name: &str,
) -> Result<&'a CargoPackage, PolicyError> {
    find_workspace_package(metadata, name)
        .ok_or_else(|| format!("harness stage workspace package missing: {name}").into())
}

fn find_workspace_package<'a>(metadata: &'a CargoMetadata, name: &str) -> Option<&'a CargoPackage> {
    metadata.packages.iter().find(|package| {
        package.name == name
            && package.source.is_none()
            && metadata.workspace_members.contains(&package.id)
    })
}

pub(super) fn check_declared(metadata: &CargoMetadata) -> Result<(), PolicyError> {
    let contract = workspace_harness_package(metadata, HARNESS_CONTRACTS.0)?;
    let machine_core = find_workspace_package(metadata, STATE_MACHINE_CORE);
    for package in [
        contract,
        workspace_harness_package(metadata, HARNESS_SHARED_MECHANICS.0)?,
    ] {
        for dependency in &package.dependencies {
            let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
            let source = if dependency.name == contract.name
                && dependency.source.is_none()
                && dependency.path.as_deref() == contract.manifest_path.parent()
            {
                DependencySource::WorkspaceContract
            } else if machine_core.is_some_and(|core| {
                dependency.name == core.name
                    && dependency.source.is_none()
                    && dependency.path.as_deref() == core.manifest_path.parent()
            }) {
                DependencySource::WorkspaceStateMachineCore
            } else if dependency.path.is_none() && dependency.source.as_deref() == Some(CRATES_IO) {
                DependencySource::CratesIo
            } else {
                DependencySource::Other
            };
            if !permits(&package.name, kind, &dependency.name, source) {
                return Err(format!(
                    "BD1: forbidden declared {kind:?} dependency {} -> {} ({source:?})",
                    package.name, dependency.name
                )
                .into());
            }
        }
    }
    Ok(())
}

pub(super) fn check_resolved(metadata: &CargoMetadata) -> Result<(), PolicyError> {
    let contract = workspace_harness_package(metadata, HARNESS_CONTRACTS.0)?;
    let machine_core = find_workspace_package(metadata, STATE_MACHINE_CORE);
    for package in [
        contract,
        workspace_harness_package(metadata, HARNESS_SHARED_MECHANICS.0)?,
    ] {
        let node = metadata
            .resolve
            .nodes
            .iter()
            .find(|node| node.id == package.id)
            .ok_or_else(|| format!("harness stage resolved package missing: {}", package.name))?;
        for dependency in &node.deps {
            let target = metadata
                .packages
                .iter()
                .find(|target| target.id == dependency.pkg)
                .ok_or_else(|| {
                    format!(
                        "harness stage dependency package missing for {}",
                        package.name
                    )
                })?;
            let source = if target.id == contract.id {
                DependencySource::WorkspaceContract
            } else if machine_core.is_some_and(|core| target.id == core.id) {
                DependencySource::WorkspaceStateMachineCore
            } else if !metadata.workspace_members.contains(&target.id)
                && target.source.as_deref() == Some(CRATES_IO)
            {
                DependencySource::CratesIo
            } else {
                DependencySource::Other
            };
            for resolved in &dependency.dep_kinds {
                let kind = resolved.kind.unwrap_or(DependencyKind::Normal);
                if !permits(&package.name, kind, &target.name, source) {
                    return Err(format!(
                        "BD1: forbidden resolved {kind:?} dependency {} -> {} ({source:?})",
                        package.name, target.name
                    )
                    .into());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "harness_stage_policy_test.rs"]
mod tests;
