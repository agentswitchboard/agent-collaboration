//! Keep the fixture's feature activation out of both default build graphs.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Command,
};

use serde::Deserialize;

use super::{CLIENT, DRIVER, REAL_TEST};

const LOOPBACK: &str = "test-support";

#[derive(Debug, thiserror::Error)]
enum FeaturePolicyError {
    #[error("BD1: Cargo {command} failed: {detail}")]
    CargoFailed {
        command: &'static str,
        detail: String,
    },
    #[error("BD1: the {0} feature package or node is missing")]
    MissingPackage(&'static str),
    #[error("BD1: {0} can activate the loopback test constructor")]
    DeclaredLoopback(String),
    #[error("BD1: fixture graph does not activate the link's loopback constructor")]
    FixtureWithoutLoopback,
    #[error("BD1: {0} activates the loopback constructor on a production path")]
    ShippedLoopback(&'static str),
    #[error("BD1: shipped {package} features differ: found {actual:?}, expected {expected:?}")]
    ShippedFeatures {
        package: &'static str,
        actual: BTreeSet<String>,
        expected: BTreeSet<String>,
    },
    #[error("BD1: malformed Cargo dependency view")]
    InvalidTree,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Metadata(#[from] serde_json::Error),
    #[error(transparent)]
    Text(#[from] std::string::FromUtf8Error),
}

#[derive(Deserialize)]
struct FeatureMetadata {
    packages: Vec<FeaturePackage>,
    workspace_members: BTreeSet<String>,
    resolve: FeatureResolve,
}

#[derive(Deserialize)]
struct FeaturePackage {
    id: String,
    name: String,
    features: BTreeMap<String, Vec<String>>,
    dependencies: Vec<FeatureDependency>,
}

#[derive(Deserialize)]
struct FeatureDependency {
    name: String,
    rename: Option<String>,
    kind: Option<super::DependencyKind>,
    features: Vec<String>,
}

#[derive(Deserialize)]
struct FeatureResolve {
    nodes: Vec<FeatureNode>,
}

#[derive(Deserialize)]
struct FeatureNode {
    id: String,
    features: BTreeSet<String>,
}

type ActiveFeatures = BTreeMap<String, BTreeSet<String>>;

impl FeatureMetadata {
    fn driver(&self) -> Result<&FeaturePackage, FeaturePolicyError> {
        self.packages
            .iter()
            .find(|package| package.name == DRIVER && self.workspace_members.contains(&package.id))
            .ok_or(FeaturePolicyError::MissingPackage(DRIVER))
    }
}

impl FeaturePackage {
    fn enables_loopback(&self, requested: &str) -> bool {
        let mut pending = vec![requested];
        let mut visited = BTreeSet::new();
        while let Some(feature) = pending.pop() {
            if feature == LOOPBACK {
                return true;
            }
            if visited.insert(feature)
                && let Some(aliases) = self.features.get(feature)
            {
                pending.extend(aliases.iter().map(String::as_str));
            }
        }
        false
    }
}

fn check_declared_activation(metadata: &FeatureMetadata) -> Result<(), FeaturePolicyError> {
    let driver = metadata.driver()?;
    if driver.enables_loopback("default") {
        return Err(FeaturePolicyError::DeclaredLoopback(DRIVER.to_owned()));
    }
    // Cargo metadata lists inactive target and optional declarations here as well.
    for owner in metadata
        .packages
        .iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
    {
        for dependency in owner
            .dependencies
            .iter()
            .filter(|dependency| dependency.name == DRIVER)
        {
            if owner.name == REAL_TEST && dependency.kind == Some(super::DependencyKind::Dev) {
                continue;
            }
            let alias = dependency.rename.as_deref().unwrap_or(DRIVER);
            let forwarded = owner.features.values().flatten().filter_map(|feature| {
                let (destination, requested) = feature.split_once('/')?;
                (destination.trim_end_matches('?') == alias).then_some(requested)
            });
            if dependency
                .features
                .iter()
                .map(String::as_str)
                .chain(forwarded)
                .any(|feature| driver.enables_loopback(feature))
            {
                return Err(FeaturePolicyError::DeclaredLoopback(owner.name.clone()));
            }
        }
    }
    Ok(())
}

fn check_fixture_activation(
    metadata: &FeatureMetadata,
    fixture_graph: &ActiveFeatures,
) -> Result<(), FeaturePolicyError> {
    let driver = metadata.driver()?;
    let resolved = metadata
        .resolve
        .nodes
        .iter()
        .find(|node| node.id == driver.id)
        .ok_or(FeaturePolicyError::MissingPackage(DRIVER))?;
    if !resolved.features.contains(LOOPBACK)
        || !fixture_graph
            .get(DRIVER)
            .is_some_and(|features| features.contains(LOOPBACK))
    {
        return Err(FeaturePolicyError::FixtureWithoutLoopback);
    }
    Ok(())
}

fn check_no_loopback(graph: &ActiveFeatures, root: &'static str) -> Result<(), FeaturePolicyError> {
    if graph
        .get(DRIVER)
        .is_some_and(|features| features.contains(LOOPBACK))
    {
        return Err(FeaturePolicyError::ShippedLoopback(root));
    }
    Ok(())
}

fn check_driver_features(graph: &ActiveFeatures) -> Result<(), FeaturePolicyError> {
    check_no_loopback(graph, DRIVER)?;
    for (package, allowed) in [
        (DRIVER, [].as_slice()),
        (CLIENT, &[]),
        ("central-link-protocol", &[]),
        ("log", &["max_level_debug"]),
        (
            "hyper-rustls",
            &["http1", "http2", "ring", "tls12", "webpki-roots"],
        ),
        (
            "tokio-tungstenite",
            &[
                "__rustls-tls",
                "connect",
                "handshake",
                "rustls",
                "rustls-pki-types",
                "rustls-tls-webpki-roots",
                "stream",
                "tokio-rustls",
                "webpki-roots",
            ],
        ),
        (
            "tungstenite",
            &[
                "__rustls-tls",
                "data-encoding",
                "handshake",
                "http",
                "httparse",
                "rustls",
                "rustls-pki-types",
                "sha1",
            ],
        ),
    ] {
        let actual = graph
            .get(package)
            .ok_or(FeaturePolicyError::MissingPackage(package))?;
        let expected = allowed
            .iter()
            .map(|feature| (*feature).to_owned())
            .collect::<BTreeSet<_>>();
        if *actual != expected {
            return Err(FeaturePolicyError::ShippedFeatures {
                package,
                actual: actual.clone(),
                expected,
            });
        }
    }
    Ok(())
}

fn cargo_output(
    root: &Path,
    command: &'static str,
    arguments: &[&str],
) -> Result<Vec<u8>, FeaturePolicyError> {
    let output = Command::new("cargo")
        .arg(command)
        .args(["--locked", "--offline"])
        .args(arguments)
        .current_dir(root)
        .output()?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(FeaturePolicyError::CargoFailed {
            command,
            detail: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

fn resolved_feature_graph(
    root: &Path,
    package: &str,
    kinds: &str,
) -> Result<ActiveFeatures, FeaturePolicyError> {
    let output = cargo_output(
        root,
        "tree",
        &[
            "-p", package, "-e", kinds, "--prefix", "none", "--format", "{p}|{f}",
        ],
    )?;
    let mut graph = ActiveFeatures::new();
    for line in String::from_utf8(output)?.lines() {
        let (package, features) = line
            .split_once('|')
            .ok_or(FeaturePolicyError::InvalidTree)?;
        let name = package
            .split_whitespace()
            .next()
            .ok_or(FeaturePolicyError::InvalidTree)?;
        let features = features.strip_suffix(" (*)").unwrap_or(features);
        graph.entry(name.to_owned()).or_default().extend(
            features
                .split(',')
                .filter(|feature| !feature.is_empty())
                .map(str::to_owned),
        );
    }
    Ok(graph)
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Cargo read failures propagate while the feature contract fails at an assertion."
)]
fn central_link_default_and_fixture_feature_graphs_are_isolated() -> Result<(), FeaturePolicyError>
{
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let metadata: FeatureMetadata = serde_json::from_slice(&cargo_output(
        &root,
        "metadata",
        &["--format-version", "1"],
    )?)?;
    let declaration = check_declared_activation(&metadata);
    assert!(declaration.is_ok(), "{declaration:?}");
    // The workspace metadata resolve unifies dev features onto the driver. Ask Cargo to
    // render each selected root with its own active edges before checking shipping features.
    let shipped = check_driver_features(&resolved_feature_graph(&root, DRIVER, "normal,build")?);
    assert!(shipped.is_ok(), "{shipped:?}");
    for package in ["codex-router-host", "codex-router-cli"] {
        let production = check_no_loopback(
            &resolved_feature_graph(&root, package, "normal,build")?,
            package,
        );
        assert!(production.is_ok(), "{production:?}");
    }
    let fixture = check_fixture_activation(
        &metadata,
        &resolved_feature_graph(&root, REAL_TEST, "normal,build,dev")?,
    );
    assert!(fixture.is_ok(), "{fixture:?}");
    Ok(())
}

#[cfg(test)]
#[path = "central_link_features_test.rs"]
mod tests;
