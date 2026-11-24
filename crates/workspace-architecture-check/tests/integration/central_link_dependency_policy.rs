//! Cargo metadata rules for the link client, socket owner and one real-Central test crate.

use std::collections::BTreeSet;

use super::{
    CargoMetadata, CargoPackage, DependencyKind, ManifestDependency,
    harness_stage_policy::CRATES_IO,
};

const CLIENT: &str = "central-link-client";
const DRIVER: &str = "switchboard-central-link";
const MAC_STORE: &str = "switchboard-machine-core-store";
const LEASE: &str = "switchboard-link-lease";
const ENROLLMENT: &str = "switchboard-machine-enrollment";
const ENROLLMENT_STORE: &str = "switchboard-machine-enrollment-store";
const REAL_TEST: &str = "switchboard-central-link-test";
const TOKIO_CENTRAL: &str = "collaboration-central-tokio";
const CLOCK_SOURCE: &str = "git+https://github.com/agent-collaboration/agent-fleet-sync.git?rev=b75d072c116afa46cfa82facd34db6317f380687";

#[path = "central_link_features.rs"]
mod central_link_features;

type PolicyError = Box<dyn std::error::Error>;

#[derive(Clone, Copy)]
struct Edge {
    name: &'static str,
    external_source: Option<&'static str>,
    features: &'static [&'static str],
    defaults: bool,
}

const fn workspace(name: &'static str, features: &'static [&'static str]) -> Edge {
    Edge {
        name,
        external_source: None,
        features,
        defaults: true,
    }
}

const fn registry(name: &'static str, defaults: bool, features: &'static [&'static str]) -> Edge {
    Edge {
        name,
        external_source: Some(CRATES_IO),
        features,
        defaults,
    }
}

const CLIENT_NORMAL: &[Edge] = &[
    workspace("central-link-protocol", &[]),
    workspace("workspace-ids", &[]),
    workspace("workspace-model", &[]),
    workspace("router-clock", &[]),
    registry("serde_json", true, &[]),
    registry("thiserror", true, &[]),
    registry("tokio", false, &["sync"]),
    registry("uuid", true, &["v5", "v7"]),
];
const CLIENT_DEV: &[Edge] = &[
    workspace("central-link-protocol", &["test-support"]),
    workspace("router-clock", &["test-support"]),
];
const DRIVER_NORMAL: &[Edge] = &[
    workspace(CLIENT, &[]),
    workspace("central-link-protocol", &[]),
    workspace("router-clock", &[]),
    workspace("state-machine-core", &[]),
    workspace("workspace-ids", &[]),
    workspace("workspace-model", &[]),
    registry("serde_json", true, &[]),
    registry("futures-util", false, &["sink", "std"]),
    registry("http-body-util", true, &[]),
    registry("hyper", true, &["http1", "http2", "client", "server"]),
    registry(
        "hyper-rustls",
        false,
        &["http1", "http2", "tls12", "webpki-roots", "ring"],
    ),
    registry(
        "hyper-util",
        true,
        &[
            "client",
            "client-legacy",
            "server",
            "http1",
            "http2",
            "tokio",
        ],
    ),
    registry("log", true, &[]),
    registry("tower-service", true, &[]),
    registry(
        "tokio",
        true,
        &[
            "rt-multi-thread",
            "macros",
            "net",
            "time",
            "sync",
            "signal",
            "process",
            "io-util",
        ],
    ),
    registry(
        "tokio-tungstenite",
        false,
        &["connect", "rustls-tls-webpki-roots"],
    ),
];
const DRIVER_DEV: &[Edge] = &[
    workspace("router-clock", &["test-support"]),
    registry("hegeltest", true, &["static-engine"]),
    registry("tracing", true, &[]),
    registry("tracing-subscriber", true, &["env-filter", "fmt", "json"]),
];
const LEASE_NORMAL: &[Edge] = &[
    workspace(CLIENT, &[]),
    workspace(DRIVER, &[]),
    workspace(ENROLLMENT, &[]),
    workspace("router-clock", &[]),
    registry(
        "tokio",
        true,
        &[
            "rt-multi-thread",
            "macros",
            "net",
            "time",
            "sync",
            "signal",
            "process",
            "io-util",
        ],
    ),
];
const LEASE_DEV: &[Edge] = &[
    workspace("router-clock", &["test-support"]),
    registry("hegeltest", true, &["static-engine"]),
];
const REAL_TEST_DEV: &[Edge] = &[
    registry("axum", false, &["http1", "tokio"]),
    registry("tungstenite", false, &[]),
    workspace(DRIVER, &["test-support"]),
    workspace(CLIENT, &[]),
    workspace("central-link-protocol", &[]),
    workspace("central-storage", &[]),
    workspace("router-clock", &["test-support"]),
    workspace(TOKIO_CENTRAL, &["test-support"]),
    Edge {
        name: "runtime-primitives",
        external_source: Some(CLOCK_SOURCE),
        features: &[],
        defaults: false,
    },
    workspace("fleet-runtime", &["test-support"]),
    workspace(MAC_STORE, &["test-support"]),
    workspace(LEASE, &[]),
    workspace(ENROLLMENT, &[]),
    workspace(ENROLLMENT_STORE, &["test-support"]),
    workspace("switchboard-message-custody", &[]),
    workspace("switchboard-message-unit", &["test-support"]),
    workspace("workspace-ids", &[]),
    workspace("workspace-model", &[]),
    registry(
        "tokio",
        true,
        &[
            "rt-multi-thread",
            "macros",
            "net",
            "time",
            "sync",
            "signal",
            "process",
            "io-util",
        ],
    ),
    registry("reqwest", false, &["blocking", "rustls-tls"]),
    registry("serde_json", true, &[]),
    registry("turmoil", true, &[]),
    workspace(
        "collaboration-central-conformance",
        &["signin-test-support"],
    ),
];

fn edges(owner: &str, kind: DependencyKind) -> &[Edge] {
    match (owner, kind) {
        (CLIENT, DependencyKind::Normal) => CLIENT_NORMAL,
        (CLIENT, DependencyKind::Dev) => CLIENT_DEV,
        (DRIVER, DependencyKind::Normal) => DRIVER_NORMAL,
        (DRIVER, DependencyKind::Dev) => DRIVER_DEV,
        (LEASE, DependencyKind::Normal) => LEASE_NORMAL,
        (LEASE, DependencyKind::Dev) => LEASE_DEV,
        (REAL_TEST, DependencyKind::Dev) => REAL_TEST_DEV,
        _ => &[],
    }
}

fn workspace_package(metadata: &CargoMetadata, package: &CargoPackage) -> bool {
    package.source.is_none() && metadata.workspace_members.contains(&package.id)
}

fn declared_source_is_exact(
    metadata: &CargoMetadata,
    dependency: &ManifestDependency,
    edge: Edge,
) -> bool {
    if let Some(source) = edge.external_source {
        dependency.path.is_none()
            && dependency
                .source
                .as_deref()
                .and_then(|value| value.split('#').next())
                == Some(source)
    } else {
        dependency.source.is_none()
            && dependency.path.as_deref().is_some_and(|path| {
                metadata.packages.iter().any(|package| {
                    workspace_package(metadata, package)
                        && package.name == edge.name
                        && package.manifest_path.parent() == Some(path)
                })
            })
    }
}

fn check_declaration(
    metadata: &CargoMetadata,
    package: &CargoPackage,
    dependency: &ManifestDependency,
) -> Result<(), PolicyError> {
    let kind = dependency.kind.unwrap_or(DependencyKind::Normal);
    let allowed = edges(&package.name, kind)
        .iter()
        .find(|edge| edge.name == dependency.name);
    let Some(edge) = allowed.filter(|edge| declared_source_is_exact(metadata, dependency, **edge))
    else {
        return Err(format!(
            "BD1: forbidden link declared {kind:?} dependency {} -> {}",
            package.name, dependency.name
        )
        .into());
    };
    let actual: BTreeSet<_> = dependency.features.iter().map(String::as_str).collect();
    let expected: BTreeSet<_> = edge.features.iter().copied().collect();
    if actual != expected || dependency.uses_default_features != edge.defaults {
        return Err(format!(
            "BD1: forbidden link declared features {} -> {}",
            package.name, dependency.name
        )
        .into());
    }
    Ok(())
}

fn test_constructor_scope(package: &CargoPackage) -> Result<(), PolicyError> {
    for dependency in &package.dependencies {
        let test_api = match dependency.name.as_str() {
            DRIVER => "the link's loopback test constructor",
            MAC_STORE => "the inbox commit pause",
            ENROLLMENT_STORE => "the enrollment store test Keychain",
            _ => continue,
        };
        if dependency
            .features
            .iter()
            .any(|feature| feature == "test-support")
            && (package.name != REAL_TEST || dependency.kind != Some(DependencyKind::Dev))
        {
            return Err(format!("BD1: {} enables {test_api}", package.name,).into());
        }
    }
    Ok(())
}

fn check_resolved(metadata: &CargoMetadata, package: &CargoPackage) -> Result<(), PolicyError> {
    let node = metadata
        .resolve
        .nodes
        .iter()
        .find(|node| node.id == package.id)
        .ok_or("link policy resolved package missing")?;
    for dependency in &node.deps {
        let target = metadata
            .packages
            .iter()
            .find(|target| target.id == dependency.pkg)
            .ok_or("link policy dependency package missing")?;
        for resolved in &dependency.dep_kinds {
            let kind = resolved.kind.unwrap_or(DependencyKind::Normal);
            if !edges(&package.name, kind).iter().any(|edge| {
                target.name == edge.name
                    && if let Some(source) = edge.external_source {
                        target
                            .source
                            .as_deref()
                            .and_then(|value| value.split('#').next())
                            == Some(source)
                    } else {
                        workspace_package(metadata, target)
                    }
            }) {
                return Err(format!(
                    "BD1: forbidden link resolved {kind:?} dependency {} -> {}",
                    package.name, target.name
                )
                .into());
            }
        }
    }
    Ok(())
}

fn check_shipped_closure(metadata: &CargoMetadata, root: &CargoPackage) -> Result<(), PolicyError> {
    let mut pending = vec![root.id.as_str()];
    let mut visited = BTreeSet::new();
    let mut tungstenite = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let package = metadata
            .packages
            .iter()
            .find(|package| package.id == id)
            .ok_or("link policy closure package missing")?;
        match package.name.as_str() {
            "collaboration-central-tokio"
            | "collaboration-central-cloudflare"
            | "collaboration-central-core"
            | "collaboration-central-conformance" => {
                return Err(format!(
                    "BD1: shipped {} reaches Central server package {}",
                    root.name, package.name
                )
                .into());
            }
            "reqwest-websocket" | "native-tls" | "openssl" | "openssl-sys" => {
                return Err(format!(
                    "BD1: shipped {} reaches forbidden transport {}",
                    root.name, package.name
                )
                .into());
            }
            "tungstenite" => {
                tungstenite.insert(id);
            }
            _ => {}
        }
        let node = metadata
            .resolve
            .nodes
            .iter()
            .find(|node| node.id == id)
            .ok_or("link policy closure resolve node missing")?;
        pending.extend(
            node.deps
                .iter()
                .filter(|dependency| {
                    dependency
                        .dep_kinds
                        .iter()
                        .any(|kind| kind.kind != Some(DependencyKind::Dev))
                })
                .map(|dependency| dependency.pkg.as_str()),
        );
    }
    if tungstenite.len() > 1 {
        return Err(format!(
            "BD1: shipped {} resolves multiple tungstenite versions",
            root.name
        )
        .into());
    }
    Ok(())
}

pub(super) fn check(metadata: &CargoMetadata) -> Result<(), PolicyError> {
    for package in metadata
        .packages
        .iter()
        .filter(|package| workspace_package(metadata, package))
    {
        test_constructor_scope(package)?;
        if matches!(package.name.as_str(), CLIENT | DRIVER | LEASE | REAL_TEST) {
            for dependency in &package.dependencies {
                check_declaration(metadata, package, dependency)?;
            }
            check_resolved(metadata, package)?;
            check_shipped_closure(metadata, package)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "central_link_dependency_policy_test.rs"]
mod tests;
