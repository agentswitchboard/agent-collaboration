//! The enrollment client has only its clock, identifier and approved external dependencies.

use super::{CargoMetadata, CargoPackage, DependencyKind, harness_stage_policy::CRATES_IO};

const ENROLLMENT: &str = "switchboard-machine-enrollment";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DependencySource {
    Workspace,
    CratesIo,
    Other,
}

type PolicyResult = Result<(), Box<dyn std::error::Error>>;

fn allows(kind: DependencyKind, name: &str, source: DependencySource) -> bool {
    use DependencyKind::{Dev, Normal};
    use DependencySource::{CratesIo, Workspace};

    match (kind, source) {
        (Normal, Workspace) => matches!(name, "router-clock" | "workspace-ids"),
        (Dev, Workspace) => name == "router-clock",
        (Normal, CratesIo) => matches!(
            name,
            "futures-util"
                | "httpdate"
                | "reqwest"
                | "serde"
                | "serde_json"
                | "thiserror"
                | "tokio"
                | "url"
                | "uuid"
                | "zeroize"
        ),
        (Dev, CratesIo) => matches!(
            name,
            "axum" | "chrono" | "hegeltest" | "tracing" | "tracing-subscriber"
        ),
        _ => false,
    }
}

fn workspace_package(metadata: &CargoMetadata, package: &CargoPackage) -> bool {
    package.source.is_none() && metadata.workspace_members.contains(&package.id)
}

fn enrollment(metadata: &CargoMetadata) -> Result<&CargoPackage, Box<dyn std::error::Error>> {
    metadata
        .packages
        .iter()
        .find(|package| package.name == ENROLLMENT && workspace_package(metadata, package))
        .ok_or_else(|| format!("BD1: missing workspace package {ENROLLMENT}").into())
}

pub(super) fn check_declared(metadata: &CargoMetadata) -> PolicyResult {
    let package = enrollment(metadata)?;
    for dependency in &package.dependencies {
        let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
        let source = match dependency.path.as_deref() {
            Some(path)
                if dependency.source.is_none()
                    && metadata.packages.iter().any(|candidate| {
                        candidate.name == dependency.name
                            && candidate.manifest_path.parent() == Some(path)
                            && workspace_package(metadata, candidate)
                    }) =>
            {
                DependencySource::Workspace
            }
            None if dependency.source.as_deref() == Some(CRATES_IO) => DependencySource::CratesIo,
            _ => DependencySource::Other,
        };
        if !allows(kind, &dependency.name, source) {
            return Err(format!(
                "BD1: forbidden declared {kind:?} dependency {ENROLLMENT} -> {} ({source:?})",
                dependency.name,
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn check_resolved(metadata: &CargoMetadata) -> PolicyResult {
    let package = enrollment(metadata)?;
    let node = metadata
        .resolve
        .nodes
        .iter()
        .find(|node| node.id == package.id)
        .ok_or_else(|| format!("BD1: missing resolved node for {ENROLLMENT}"))?;
    for dependency in &node.deps {
        let target = metadata
            .packages
            .iter()
            .find(|candidate| candidate.id == dependency.pkg)
            .ok_or_else(|| format!("BD1: dependency package {} is missing", dependency.pkg))?;
        let source = if workspace_package(metadata, target) {
            DependencySource::Workspace
        } else if !metadata.workspace_members.contains(&target.id)
            && target.source.as_deref() == Some(CRATES_IO)
        {
            DependencySource::CratesIo
        } else {
            DependencySource::Other
        };
        for edge in &dependency.dep_kinds {
            let kind = edge.kind.unwrap_or(DependencyKind::Normal);
            if !allows(kind, &target.name, source) {
                return Err(format!(
                    "BD1: forbidden resolved {kind:?} dependency {ENROLLMENT} -> {} ({source:?})",
                    target.name,
                )
                .into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "enrollment_dependency_policy_test.rs"]
mod tests;
