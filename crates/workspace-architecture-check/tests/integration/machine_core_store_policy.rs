//! Direct dependencies and isolated feature closures for the Mac core store.
use super::{CargoMetadata, CargoPackage, DependencyKind, harness_stage_policy::CRATES_IO};
use std::{path::Path, process::Command};

const CORE_STORE: &str = "switchboard-machine-core-store";
const STORE_CONTRACTS: &[&str] = &[
    "central-link-protocol",
    "collaboration-mail-protocol",
    "fleet-mail",
    "router-clock",
    "sqlite-storage-maintenance",
    "workspace-ids",
    "workspace-model",
];
const STORE_LIBRARIES: &[&str] = &[
    "chrono",
    "schemars",
    "serde",
    "serde_json",
    "sqlx",
    "thiserror",
    "tokio",
    "uuid",
];
const DEV_LIBRARIES: &[&str] = &["chrono", "hegeltest", "serde_json", "tempfile", "tokio"];
const SQLITE_FEATURES: &[&str] = &["macros", "migrate", "runtime-tokio", "sqlite"];
const SQLITE_RESOLVED_FEATURES: &[&str] = &[
    "_rt-tokio",
    "_sqlite",
    "derive",
    "macros",
    "migrate",
    "runtime-tokio",
    "sqlite",
    "sqlite-bundled",
    "sqlite-deserialize",
    "sqlite-load-extension",
    "sqlite-unlock-notify",
    "sqlx-macros",
    "sqlx-sqlite",
];

type PolicyResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DependencySource {
    Workspace,
    CratesIo,
    Other,
}

fn core_store(metadata: &CargoMetadata) -> PolicyResult<&CargoPackage> {
    metadata
        .packages
        .iter()
        .find(|package| {
            package.name == CORE_STORE
                && package.source.is_none()
                && metadata.workspace_members.contains(&package.id)
        })
        .ok_or_else(|| format!("machine core workspace package missing: {CORE_STORE}").into())
}

fn permits_direct(kind: DependencyKind, name: &str, source: DependencySource) -> bool {
    match (kind, source) {
        (DependencyKind::Normal, DependencySource::Workspace) => STORE_CONTRACTS.contains(&name),
        (DependencyKind::Normal, DependencySource::CratesIo) => STORE_LIBRARIES.contains(&name),
        (DependencyKind::Dev, DependencySource::Workspace) => name == "router-clock",
        (DependencyKind::Dev, DependencySource::CratesIo) => DEV_LIBRARIES.contains(&name),
        _ => false,
    }
}

pub(super) fn check_declared(metadata: &CargoMetadata) -> PolicyResult<()> {
    for dependency in &core_store(metadata)?.dependencies {
        let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
        let source = match (dependency.path.as_deref(), dependency.source.as_deref()) {
            (Some(path), None)
                if metadata.packages.iter().any(|package| {
                    package.name == dependency.name
                        && package.source.is_none()
                        && metadata.workspace_members.contains(&package.id)
                        && package.manifest_path.parent() == Some(path)
                }) =>
            {
                DependencySource::Workspace
            }
            (None, Some(CRATES_IO)) => DependencySource::CratesIo,
            _ => DependencySource::Other,
        };
        let name = dependency.name.as_str();
        if !permits_direct(kind, name, source) {
            return Err(format!(
                "forbidden declared {kind:?} dependency {CORE_STORE} -> {name} ({source:?})"
            )
            .into());
        }
        let features_allowed = match (kind, name) {
            (_, "sqlx") => {
                !dependency.uses_default_features
                    && dependency
                        .features
                        .iter()
                        .all(|feature| SQLITE_FEATURES.contains(&feature.as_str()))
            }
            (DependencyKind::Dev, "router-clock") => dependency
                .features
                .iter()
                .all(|feature| feature == "test-support"),
            (_, name) if STORE_CONTRACTS.contains(&name) => dependency.features.is_empty(),
            _ => true,
        };
        if !features_allowed {
            return Err(format!(
                "forbidden declared {kind:?} features {CORE_STORE} -> {name} {:?} (defaults: {})",
                dependency.features, dependency.uses_default_features
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn check_resolved(metadata: &CargoMetadata) -> PolicyResult<()> {
    let store = core_store(metadata)?;
    let node = metadata
        .resolve
        .nodes
        .iter()
        .find(|node| node.id == store.id)
        .ok_or_else(|| format!("machine core resolved package missing: {CORE_STORE}"))?;
    for dependency in &node.deps {
        let target = metadata
            .packages
            .iter()
            .find(|target| target.id == dependency.pkg)
            .ok_or_else(|| format!("machine core dependency missing: {}", dependency.pkg))?;
        let source = match (
            metadata.workspace_members.contains(&target.id),
            target.source.as_deref(),
        ) {
            (true, None) => DependencySource::Workspace,
            (false, Some(CRATES_IO)) => DependencySource::CratesIo,
            _ => DependencySource::Other,
        };
        for resolution in &dependency.dep_kinds {
            let kind = resolution.kind.unwrap_or(DependencyKind::Normal);
            if !permits_direct(kind, &target.name, source) {
                return Err(format!(
                    "forbidden resolved {kind:?} dependency {CORE_STORE} -> {} ({source:?})",
                    target.name
                )
                .into());
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum BuildView {
    Production,
    Tests,
}

impl BuildView {
    const fn edge_kinds(self) -> &'static str {
        match self {
            Self::Production => "normal,build",
            Self::Tests => "normal,build,dev",
        }
    }
}

pub(super) fn check_package_trees(root: &Path) -> PolicyResult<()> {
    for view in [BuildView::Production, BuildView::Tests] {
        let output = Command::new("cargo")
            .args([
                "tree",
                "--locked",
                "--offline",
                "--color",
                "never",
                "--all-features",
                "--package",
                CORE_STORE,
                "--edges",
                view.edge_kinds(),
                "--prefix",
                "none",
                "--format",
                "{p}\t{f}\t",
            ])
            .current_dir(root)
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "machine core {view:?} cargo tree exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        check_tree(&String::from_utf8(output.stdout)?, view)?;
    }
    Ok(())
}

fn check_tree(tree: &str, view: BuildView) -> PolicyResult<()> {
    let mut found_store = false;
    for line in tree.lines() {
        let mut columns = line.split('\t');
        let name = columns
            .next()
            .and_then(|identity| identity.split_whitespace().next())
            .ok_or("machine core tree is missing a package name")?;
        let features = columns
            .next()
            .ok_or("machine core tree is missing a feature column")?;
        found_store |= name == CORE_STORE;
        if upper_layer(name) || matches!(view, BuildView::Production) && name == "hegeltest" {
            return Err(
                format!("forbidden reachable {view:?} dependency {CORE_STORE} -> {name}").into(),
            );
        }
        for feature in features.split(',').filter(|feature| !feature.is_empty()) {
            let forbidden = match name {
                CORE_STORE
                | "central-link-protocol"
                | "workspace-model"
                | "workspace-ids"
                | "sqlite-storage-maintenance" => true,
                "router-clock" => match view {
                    BuildView::Production => true,
                    BuildView::Tests => feature != "test-support",
                },
                "sqlx" => !SQLITE_RESOLVED_FEATURES.contains(&feature),
                _ => false,
            };
            if forbidden {
                return Err(format!(
                    "forbidden resolved {view:?} feature {CORE_STORE} -> {name}/{feature}"
                )
                .into());
            }
        }
    }
    if !found_store {
        return Err(format!("machine core {view:?} tree is missing {CORE_STORE}").into());
    }
    Ok(())
}

fn upper_layer(name: &str) -> bool {
    name.starts_with("collaboration-central-")
        || name.contains("harness")
        || matches!(
            name,
            "acp-client-runtime"
                | "acp-provider-adapters"
                | "codex-acp-adapter"
                | "codex-native-integration"
                | "codex-router-host"
                | "codex-router-debug-hosts"
                | "collaboration-runtime"
                | "collaboration-service"
                | "central-link-client"
                | "switchboard-central-link"
                | "fleet-engine"
                | "fleet-runtime"
                | "fleet-rpc"
                | "fleet-store-sqlx"
        )
}

#[cfg(test)]
#[path = "machine_core_store_policy_test.rs"]
mod tests;
