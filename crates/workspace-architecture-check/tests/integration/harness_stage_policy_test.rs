use super::*;
use crate::crate_layers::{NonDevDependencyGraph, workspace_boundaries};
use serde_json::{Value, json};

const CONTRACT: &str = HARNESS_CONTRACTS.0;
const CARRIER: &str = HARNESS_SHARED_MECHANICS.0;

fn registry_manifest(name: &str, kind: Option<&str>) -> Value {
    json!({
        "name": name,
        "kind": kind,
        "source": CRATES_IO,
        "path": null,
        "optional": false,
        "features": [],
        "uses_default_features": true,
    })
}

fn resolved_dependency(package_id: &str, kind: Option<&str>) -> Value {
    json!({
        "pkg": package_id,
        "dep_kinds": [{"kind": kind}],
    })
}

fn stage_metadata() -> Value {
    let registry_package = |id: &str| {
        json!({
            "id": id,
            "name": id,
            "manifest_path": format!("/registry/{id}/Cargo.toml"),
            "source": CRATES_IO,
        })
    };
    json!({
        "packages": [
            {
                "id": "contract-id",
                "name": CONTRACT,
                "manifest_path": "/workspace/contracts/Cargo.toml",
                "source": null,
                "dependencies": [registry_manifest("uuid", None)],
            },
            {
                "id": "carrier-id",
                "name": CARRIER,
                "manifest_path": "/workspace/carrier/Cargo.toml",
                "source": null,
                "dependencies": [
                    {
                        "name": CONTRACT,
                        "kind": null,
                        "source": null,
                        "path": "/workspace/contracts",
                        "features": [],
                        "uses_default_features": true,
                    },
                    registry_manifest("tokio", None),
                    registry_manifest("tokio-util", None),
                    registry_manifest("hegeltest", Some("dev")),
                ],
            },
            registry_package("uuid"),
            registry_package("tokio"),
            registry_package("tokio-util"),
            registry_package("hegeltest"),
            registry_package("serde"),
            registry_package("schemars"),
            registry_package("base64"),
        ],
        "workspace_members": ["contract-id", "carrier-id"],
        "resolve": {"nodes": [
            {"id": "contract-id", "deps": [resolved_dependency("uuid", None)]},
            {"id": "carrier-id", "deps": [
                resolved_dependency("contract-id", None),
                resolved_dependency("tokio", None),
                resolved_dependency("tokio-util", None),
                resolved_dependency("hegeltest", Some("dev")),
            ]},
            {"id": "uuid", "features": [], "deps": []},
            {"id": "tokio", "deps": []},
            {"id": "tokio-util", "deps": []},
            {"id": "hegeltest", "deps": []},
        ]},
    })
}

fn decode(metadata: Value) -> CargoMetadata {
    serde_json::from_value(metadata).expect("fixture follows Cargo metadata format")
}

fn append(array: &mut Value, item: Value) {
    array.as_array_mut().expect("fixture array").push(item);
}

fn violation(result: Result<(), PolicyError>) -> Option<String> {
    result.err().map(|error| error.to_string())
}

#[test]
fn harness_stage_accepts_contract_registry_data_dependencies() {
    let mut metadata = stage_metadata();
    for dependency in ["serde", "schemars", "base64"] {
        append(
            &mut metadata["packages"][0]["dependencies"],
            registry_manifest(dependency, None),
        );
        append(
            &mut metadata["resolve"]["nodes"][0]["deps"],
            resolved_dependency(dependency, None),
        );
    }

    let metadata = decode(metadata);
    let declared = violation(check_declared(&metadata));
    let resolved = violation(check_resolved(&metadata));
    assert_eq!((declared, resolved), (None, None));
}

#[test]
fn harness_stage_rejects_contract_data_dependencies_from_other_sources_or_kinds() {
    for dependency in ["serde", "schemars", "base64"] {
        for (kind, expected_kind, source, expected_source) in [
            (Some("dev"), "Dev", Some(CRATES_IO), "CratesIo"),
            (Some("build"), "Build", Some(CRATES_IO), "CratesIo"),
            (
                None,
                "Normal",
                Some("git+https://example.invalid/repo"),
                "Other",
            ),
            (None, "Normal", None, "Other"),
        ] {
            let mut metadata = stage_metadata();
            let mut manifest = registry_manifest(dependency, kind);
            manifest["source"] = json!(source);
            let target = metadata["packages"]
                .as_array_mut()
                .expect("fixture packages")
                .iter_mut()
                .find(|package| package["id"] == dependency)
                .expect("fixture dependency package");
            target["source"] = json!(source);
            if source.is_none() {
                manifest["path"] = json!(format!("/workspace/{dependency}"));
                target["manifest_path"] = json!(format!("/workspace/{dependency}/Cargo.toml"));
                append(&mut metadata["workspace_members"], json!(dependency));
            }
            append(&mut metadata["packages"][0]["dependencies"], manifest);
            append(
                &mut metadata["resolve"]["nodes"][0]["deps"],
                resolved_dependency(dependency, kind),
            );

            let metadata = decode(metadata);
            let edge = format!(
                "{expected_kind} dependency {CONTRACT} -> {dependency} ({expected_source})"
            );
            assert_eq!(
                (
                    violation(check_declared(&metadata)),
                    violation(check_resolved(&metadata))
                ),
                (
                    Some(format!("BD1: forbidden declared {edge}")),
                    Some(format!("BD1: forbidden resolved {edge}"))
                ),
            );
        }
    }
}

#[test]
fn harness_stage_rejects_inactive_registry_tokio_declaration() {
    let mut metadata = stage_metadata();
    check_declared(&decode(metadata.clone())).expect("the approved stage declarations");
    let mut optional_tokio = registry_manifest("tokio", None);
    optional_tokio["optional"] = json!(true);
    append(&mut metadata["packages"][0]["dependencies"], optional_tokio);

    let metadata = decode(metadata);
    check_resolved(&metadata).expect("inactive Tokio is absent from the leaf's resolved edges");
    assert_eq!(
        violation(check_declared(&metadata)),
        Some(format!(
            "BD1: forbidden declared Normal dependency {CONTRACT} -> tokio (CratesIo)"
        ))
    );
}

#[test]
fn harness_stage_rejects_resolved_registry_tokio() {
    let mut metadata = stage_metadata();
    let mut optional_tokio = registry_manifest("tokio", None);
    optional_tokio["optional"] = json!(true);
    append(&mut metadata["packages"][0]["dependencies"], optional_tokio);
    append(
        &mut metadata["resolve"]["nodes"][0]["deps"],
        resolved_dependency("tokio", None),
    );

    assert_eq!(
        violation(check_resolved(&decode(metadata))),
        Some(format!(
            "BD1: forbidden resolved Normal dependency {CONTRACT} -> tokio (CratesIo)"
        ))
    );
}

#[test]
fn harness_stage_rejects_unrelated_carrier_external_dependency() {
    let mut metadata = stage_metadata();
    append(
        &mut metadata["packages"][1]["dependencies"],
        registry_manifest("serde", None),
    );
    append(
        &mut metadata["resolve"]["nodes"][1]["deps"],
        resolved_dependency("serde", None),
    );

    let metadata = decode(metadata);
    assert_eq!(
        (
            violation(check_declared(&metadata)),
            violation(check_resolved(&metadata))
        ),
        (
            Some(format!(
                "BD1: forbidden declared Normal dependency {CARRIER} -> serde (CratesIo)"
            )),
            Some(format!(
                "BD1: forbidden resolved Normal dependency {CARRIER} -> serde (CratesIo)"
            ))
        )
    );
}

#[test]
fn harness_stage_checks_allowed_dependency_source_and_kind() {
    let mut foreign_uuid = stage_metadata();
    let foreign_source = json!("git+https://example.invalid/uuid#8ac57");
    foreign_uuid["packages"][0]["dependencies"][0]["source"] = foreign_source.clone();
    foreign_uuid["packages"][2]["source"] = foreign_source;

    let mut normal_hegel = stage_metadata();
    normal_hegel["packages"][1]["dependencies"][3]["kind"] = Value::Null;
    normal_hegel["resolve"]["nodes"][1]["deps"][3] = resolved_dependency("hegeltest", None);

    for (metadata, owner, dependency, source) in [
        (foreign_uuid, CONTRACT, "uuid", "Other"),
        (normal_hegel, CARRIER, "hegeltest", "CratesIo"),
    ] {
        let metadata = decode(metadata);
        assert_eq!(
            (
                violation(check_declared(&metadata)),
                violation(check_resolved(&metadata))
            ),
            (
                Some(format!(
                    "BD1: forbidden declared Normal dependency {owner} -> {dependency} ({source})"
                )),
                Some(format!(
                    "BD1: forbidden resolved Normal dependency {owner} -> {dependency} ({source})"
                ))
            )
        );
    }
}

#[test]
fn harness_stage_rejects_tokio_through_an_enabled_feature() {
    let default = stage_metadata();
    workspace_boundaries::check_harness_layers(&NonDevDependencyGraph::from_metadata(decode(
        default.clone(),
    )))
    .expect("the default UUID closure is runtime-free");

    for forbidden_runtime in ["tokio", "tokio-util"] {
        let mut enabled = default.clone();
        enabled["resolve"]["nodes"][2]["features"] = json!(["fixture-runtime"]);
        append(
            &mut enabled["resolve"]["nodes"][2]["deps"],
            resolved_dependency(forbidden_runtime, None),
        );
        let enabled = decode(enabled);
        check_declared(&enabled).expect("the declared UUID edge remains allowed");
        check_resolved(&enabled).expect("the enabled edge is transitive");

        assert_eq!(
            violation(workspace_boundaries::check_harness_layers(
                &NonDevDependencyGraph::from_metadata(enabled)
            )),
            Some(format!(
                "BD1: forbidden reachable runtime {CONTRACT} -> {forbidden_runtime}"
            )),
        );
    }
}
