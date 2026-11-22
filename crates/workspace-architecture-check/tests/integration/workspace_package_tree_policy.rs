//! Package-scoped native and Wasm checks for backend and runtime dependencies.
use super::{
    CENTRAL_WORKER, FLEET_ACCESS, FLEET_DIRECTORY, FLEET_ENGINE, FLEET_MAIL, FLEET_NOTICES,
    FLEET_PURE_CRATES, FLEET_SCHEDULES, FLEET_TIMER_POLICY, FLEET_WAKES, LENT_TRANSACTION,
    PURE_CRATES, REQUEST_LOG, TIMER_CRATES, WORKSPACE_SERVICE, is_effectful,
};
use std::{path::Path, process::Command};

pub(super) const SERVICE_SUPPORT_PACKAGES: &[&str] = &[
    "sqlx",
    "sqlx-core",
    "sqlx-macros",
    "sqlx-macros-core",
    "futures-core",
    "futures-intrusive",
    "futures-io",
    "futures-sink",
    "futures-task",
    "futures-util",
];
const FLEET_WASM_TARGET: &str = "wasm32-unknown-unknown";
// Chrono's wasm support reaches these runtime-independent helpers via js-sys.
const FLEET_WASM_SUPPORT_PACKAGES: &[&str] = &["futures-core", "futures-task", "futures-util"];

/// Workspace metadata merges features from unrelated roots (notably uuid's wasm
/// support). A package-scoped tree checks the actual pure-rule dependency closure.
pub(in crate::crate_layers) fn check_workspace_package_trees(
    workspace_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for package_name in PURE_CRATES
        .into_iter()
        .chain(FLEET_PURE_CRATES)
        .map(|(package_name, _)| package_name)
        .chain([
            LENT_TRANSACTION,
            FLEET_ACCESS,
            FLEET_ENGINE,
            FLEET_MAIL,
            REQUEST_LOG,
            FLEET_NOTICES,
            FLEET_DIRECTORY,
            WORKSPACE_SERVICE,
        ])
        .chain(
            TIMER_CRATES
                .into_iter()
                .map(|(package_name, _)| package_name),
        )
    {
        check_package_targets(workspace_root, package_name)?;
    }
    check_package_tree(workspace_root, CENTRAL_WORKER, Some(FLEET_WASM_TARGET))
}

pub(super) fn check_package_targets(
    workspace_root: &Path,
    package_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    check_package_tree(workspace_root, package_name, None)?;
    if matches!(
        package_name,
        LENT_TRANSACTION
            | WORKSPACE_SERVICE
            | FLEET_ACCESS
            | FLEET_DIRECTORY
            | FLEET_ENGINE
            | FLEET_MAIL
            | REQUEST_LOG
            | FLEET_NOTICES
            | FLEET_WAKES
            | FLEET_SCHEDULES
            | FLEET_TIMER_POLICY
    ) || FLEET_PURE_CRATES
        .iter()
        .any(|(dependency_name, _)| *dependency_name == package_name)
    {
        check_package_tree(workspace_root, package_name, Some(FLEET_WASM_TARGET)).map_err(
            |error| format!("{package_name} dependency tree for {FLEET_WASM_TARGET}: {error}"),
        )?;
    }
    Ok(())
}

pub(super) fn check_package_tree(
    workspace_root: &Path,
    package_name: &str,
    target_triple: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cargo_tree_command = Command::new("cargo");
    cargo_tree_command.args([
        "tree",
        "--locked",
        "--offline",
        "-p",
        package_name,
        "-e",
        "normal,build",
        "--prefix",
        "none",
        "--format",
        "{p}\t{f}",
    ]);
    if let Some(target_triple) = target_triple {
        cargo_tree_command.args(["--target", target_triple]);
    }
    let cargo_tree_output = cargo_tree_command.current_dir(workspace_root).output()?;
    if !cargo_tree_output.status.success() {
        return Err(format!(
            "pure dependency tree failed: {}",
            String::from_utf8_lossy(&cargo_tree_output.stderr)
        )
        .into());
    }
    let allowed_support_packages = if matches!(
        package_name,
        WORKSPACE_SERVICE
            | FLEET_ENGINE
            | FLEET_MAIL
            | REQUEST_LOG
            | FLEET_NOTICES
            | FLEET_WAKES
            | FLEET_SCHEDULES
    ) {
        SERVICE_SUPPORT_PACKAGES
    } else if target_triple == Some(FLEET_WASM_TARGET) {
        FLEET_WASM_SUPPORT_PACKAGES
    } else {
        &[]
    };
    check_package_tree_output(
        &String::from_utf8(cargo_tree_output.stdout)?,
        allowed_support_packages,
        package_name,
    )
}
pub(super) fn check_package_tree_output(
    dependency_tree: &str,
    allowed_support_packages: &[&str],
    package_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for package_line in dependency_tree.lines() {
        let (package_identity, enabled_features) =
            package_line.split_once('\t').unwrap_or((package_line, ""));
        let Some(dependency_name) = package_identity.split_whitespace().next() else {
            continue;
        };
        if package_name == LENT_TRANSACTION {
            if dependency_name.starts_with("sqlx-") && dependency_name != "sqlx-core" {
                return Err(format!(
                    "{LENT_TRANSACTION} reaches SQLx backend or tooling {dependency_name}"
                )
                .into());
            }
            continue;
        }
        if package_name == CENTRAL_WORKER {
            if matches!(dependency_name, "sqlx-sqlite" | "libsqlite3-sys") {
                return Err(format!(
                    "R1: Cloudflare reaches stock SQLite through {dependency_name}"
                )
                .into());
            }
            continue;
        }
        if !is_effectful(dependency_name) || allowed_support_packages.contains(&dependency_name) {
            continue;
        }
        if matches!(
            package_name,
            FLEET_ACCESS
                | FLEET_ENGINE
                | FLEET_MAIL
                | REQUEST_LOG
                | FLEET_NOTICES
                | FLEET_WAKES
                | FLEET_SCHEDULES
        ) && dependency_name == "tokio"
        {
            let runtime_features = enabled_features
                .strip_suffix(" (*)")
                .unwrap_or(enabled_features)
                .trim();
            if matches!(runtime_features, "" | "sync") {
                continue;
            }
            return Err(format!(
                "BD2: {package_name} reaches Tokio features {runtime_features}; only sync is allowed"
            )
            .into());
        }
        return Err(format!("BD2: pure rules transitively reach {dependency_name}").into());
    }
    Ok(())
}
