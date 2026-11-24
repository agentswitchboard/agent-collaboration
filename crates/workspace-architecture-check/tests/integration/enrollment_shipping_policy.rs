//! A dependency-only Cargo root exposes the enrollment crate's shipping features.

use std::{collections::BTreeSet, fs, path::Path};

use super::{CargoMetadata, DependencyKind, read_cargo_metadata};

use super::enrollment_dependency_policy::{ENROLLMENT, PolicyResult};

pub(super) fn check_shipping(root: &Path, workspace: &CargoMetadata) -> PolicyResult {
    let enrollment = super::enrollment_dependency_policy::enrollment(workspace)?;
    let crate_path = enrollment
        .manifest_path
        .parent()
        .ok_or("missing enrollment package path")?;
    let directory = tempfile::Builder::new()
        .prefix("enrollment-shipping-")
        .tempdir()?;
    let manifest_path = directory.path().join("Cargo.toml");
    let quoted_path = serde_json::to_string(crate_path)?;
    fs::write(
        &manifest_path,
        format!(
            "[workspace]\nresolver = \"3\"\n\n[package]\nname = \"enrollment-shipping-probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[lib]\npath = \"probe.rs\"\n\n[dependencies]\n{ENROLLMENT} = {{ path = {quoted_path} }}\n"
        ),
    )?;
    fs::write(directory.path().join("probe.rs"), "")?;
    // Seed resolution from the checked-in lock. Cargo can remove unused
    // workspace roots from the temporary lock only. Verify every selected
    // dependency still has its exact package ID from the real locked graph.
    fs::copy(root.join("Cargo.lock"), directory.path().join("Cargo.lock"))?;
    let manifest_arg = manifest_path
        .to_str()
        .ok_or("invalid shipping probe path")?;
    let shipping = read_cargo_metadata(root, ["--manifest-path", manifest_arg])?;
    let workspace_package_ids: BTreeSet<_> = workspace
        .packages
        .iter()
        .map(|package| package.id.as_str())
        .collect();
    for package in &shipping.packages {
        if !shipping.workspace_members.contains(&package.id)
            && !workspace_package_ids.contains(package.id.as_str())
        {
            return Err(format!(
                "BD1: shipping probe selected an unlocked package {}",
                package.name
            )
            .into());
        }
    }
    check_shipping_graph(&shipping, &workspace.workspace_members, &enrollment.id)
}

fn resolved_feature_set(name: &str) -> Option<&'static [&'static str]> {
    Some(match name {
        ENROLLMENT | "router-clock" | "state-machine-core" | "workspace-ids" | "httpdate" => &[],
        "futures-util" => &[
            "alloc",
            "futures-io",
            "futures-sink",
            "io",
            "memchr",
            "sink",
            "slab",
            "std",
        ],
        "reqwest" => &[
            "__rustls",
            "__rustls-ring",
            "__tls",
            "blocking",
            "rustls-tls",
            "rustls-tls-webpki-roots",
            "rustls-tls-webpki-roots-no-provider",
        ],
        "serde" => &["alloc", "default", "derive", "serde_derive", "std"],
        "serde_json" => &["alloc", "default", "std"],
        "thiserror" => &["default", "std"],
        "tokio" => &[
            "bytes",
            "default",
            "io-util",
            "libc",
            "macros",
            "mio",
            "net",
            "process",
            "rt",
            "rt-multi-thread",
            "signal",
            "signal-hook-registry",
            "socket2",
            "sync",
            "time",
            "tokio-macros",
            "windows-sys",
        ],
        "url" => &["default", "serde", "std"],
        "uuid" => &["default", "rng", "serde", "sha1", "std", "v5", "v7"],
        "zeroize" => &["alloc", "default"],
        _ => return None,
    })
}

pub(super) fn check_shipping_graph(
    metadata: &CargoMetadata,
    workspace_members: &BTreeSet<String>,
    enrollment_id: &str,
) -> PolicyResult {
    let root = metadata
        .resolve
        .nodes
        .iter()
        .find(|node| node.id == enrollment_id)
        .ok_or("BD1: missing enrollment shipping root")?;
    let direct: BTreeSet<_> = root.deps.iter().map(|edge| edge.pkg.as_str()).collect();
    let mut visited = BTreeSet::new();
    let mut package_ids = vec![enrollment_id];
    while let Some(id) = package_ids.pop() {
        if !visited.insert(id) {
            continue;
        }
        let package = metadata
            .packages
            .iter()
            .find(|package| package.id == id)
            .ok_or_else(|| format!("BD1: missing shipping package {id}"))?;
        let node = metadata
            .resolve
            .nodes
            .iter()
            .find(|node| node.id == id)
            .ok_or_else(|| format!("BD1: missing shipping node {id}"))?;
        if workspace_members.contains(id)
            && !matches!(
                package.name.as_str(),
                ENROLLMENT | "router-clock" | "state-machine-core" | "workspace-ids"
            )
        {
            return Err(format!(
                "BD1: {ENROLLMENT} shipping closure reaches {}",
                package.name
            )
            .into());
        }
        if (id == enrollment_id || direct.contains(id))
            && let Some(allowed) = resolved_feature_set(&package.name)
            && let Some(feature) = node
                .features
                .iter()
                .find(|feature| !allowed.contains(&feature.as_str()))
        {
            return Err(
                format!("BD1: forbidden shipping feature {}:{feature}", package.name).into(),
            );
        }
        for edge in &node.deps {
            if edge
                .dep_kinds
                .iter()
                .any(|kind| kind.kind != Some(DependencyKind::Dev))
            {
                package_ids.push(&edge.pkg);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "enrollment_shipping_policy_test.rs"]
mod tests;
