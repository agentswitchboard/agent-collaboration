use crate::identity_contract::{AuthorizationRevision, AuthorizationRevisionAcknowledgement};
use crate::organization_admin_contract::{
    ListWorkspacesResult, OrganizationAdminResult, OrganizationChangeRecord,
};
use crate::person_request::{
    AdminOperation, AdminRoute, CreateWorkspaceRequest, ListWorkspacesRequest,
    OrganizationAdminIntent, PersonClaims, PersonTokenHeader, ProofClaims, ProofHeader,
    ReadChangeRecordsRequest, WorkspaceLookupRequest,
};
use fleet_contracts::{
    AllowedWorkspaces, DecideDeviceLogin, DecideMachineToken, DeviceLoginDecision,
    DeviceLoginRefusal, DeviceLoginRequestId, EndedMachineClient, EnrollPermission,
    MachineEnrollmentRevocationResult, MachineTokenDecision, MachineTokenRefusal, PersonId,
    PresentedMachine, RevokeMachineEnrollmentRequest,
};
use fleet_contracts::{
    ClientId, MachinePrincipal, MachineTokenClaims, OrganizationStatementDigestReply,
    ProvisionOrganization, ProvisionOrganizationResult, ReadOrganizationStatementDigest,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId, UnixMillis, UnixSeconds};
use fleet_signin::{
    machine_token::{MachineAccessTokenClaims, MachineAccessTokenHeader},
    organization_authority::{OrganizationStampClaims, OrganizationStampHeader},
};
use message_board::MachineLabel;
use serde_json::{Value, json};
use specta::Types;
use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

#[path = "signin_schema_parity_test.rs"]
mod signin_schema_parity_test;
#[path = "signin_wire_specimens_test.rs"]
mod signin_wire_specimens_test;
use signin_wire_specimens_test::{
    checked_signed_specimens, description_codec_specimens, numeric_boundary_specimens,
    signed_intent_vector,
};

#[derive(Clone, Copy)]
enum WirePhase {
    Serialize,
    Deserialize,
}

impl WirePhase {
    fn suffix(self) -> &'static str {
        match self {
            Self::Serialize => "_Serialize",
            Self::Deserialize => "_Deserialize",
        }
    }
}

struct WireSpecimen {
    label: String,
    phase: WirePhase,
    value: Value,
}

struct WireRoot {
    name: &'static str,
    register: fn(Types) -> Types,
    specimens: fn(&str) -> Vec<WireSpecimen>,
    central_reads_only: bool,
}

macro_rules! wire_root {
    (read_only $wire:ident, $specimens:expr) => {
        WireRoot {
            central_reads_only: true,
            ..wire_root!($wire, $specimens)
        }
    };
    ($wire:ident, $specimens:expr) => {
        WireRoot {
            name: stringify!($wire),
            register: |types| types.register::<$wire>(),
            specimens: $specimens,
            central_reads_only: false,
        }
    };
}

use WirePhase::{Deserialize, Serialize};

// This is the common inventory for the exporter and the TypeScript specimens.
const SIGNIN_ROOTS: &[WireRoot] = &[
    wire_root!(DecideDeviceLogin, core_request),
    wire_root!(DeviceLoginDecision, core_reply),
    wire_root!(DeviceLoginRefusal, core_reply),
    wire_root!(DecideMachineToken, core_request),
    wire_root!(MachineTokenDecision, machine_decision_specimens),
    wire_root!(MachineTokenRefusal, core_reply),
    wire_root!(PresentedMachine, core_request),
    wire_root!(MachinePrincipal, core_request),
    wire_root!(read_only MachineTokenClaims, core_request),
    wire_root!(CreateWorkspaceRequest, core_request),
    wire_root!(WorkspaceLookupRequest, core_request),
    wire_root!(ListWorkspacesRequest, core_request),
    wire_root!(ListWorkspacesResult, core_reply),
    wire_root!(ReadChangeRecordsRequest, change_records_requests),
    wire_root!(OrganizationAdminResult, core_reply),
    wire_root!(OrganizationChangeRecord, change_records_replies),
    wire_root!(ProvisionOrganization, core_request),
    wire_root!(ProvisionOrganizationResult, core_reply),
    wire_root!(ReadOrganizationStatementDigest, core_request),
    wire_root!(OrganizationStatementDigestReply, core_reply),
    wire_root!(AuthorizationRevision, core_request),
    wire_root!(AuthorizationRevisionAcknowledgement, core_reply),
    wire_root!(RevokeMachineEnrollmentRequest, core_request),
    wire_root!(MachineEnrollmentRevocationResult, core_reply),
    wire_root!(AllowedWorkspaces, core_request),
    wire_root!(ClientId, core_request),
    wire_root!(DeviceLoginRequestId, core_request),
    wire_root!(EndedMachineClient, core_reply),
    wire_root!(EnrollPermission, core_request),
    wire_root!(PersonId, core_request),
    wire_root!(EnrollmentId, identity_specimens),
    wire_root!(MachineLabel, identity_specimens),
    wire_root!(OrganizationId, identity_specimens),
    wire_root!(UnixSeconds, identity_specimens),
    wire_root!(UnixMillis, identity_specimens),
    wire_root!(OrganizationAdminIntent, admin_intent_specimens),
    wire_root!(read_only PersonTokenHeader, checked_signed_specimens),
    wire_root!(read_only PersonClaims, checked_signed_specimens),
    wire_root!(read_only ProofHeader, checked_signed_specimens),
    wire_root!(read_only ProofClaims, checked_signed_specimens),
    wire_root!(read_only OrganizationStampHeader, checked_signed_specimens),
    wire_root!(read_only OrganizationStampClaims, checked_signed_specimens),
    wire_root!(read_only MachineAccessTokenHeader, checked_signed_specimens),
    wire_root!(read_only MachineAccessTokenClaims, checked_signed_specimens),
];

fn contract_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("contracts")
}

fn read_contract_json<Contract: serde::de::DeserializeOwned>(path: &Path) -> Contract {
    let input = fs::read(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_json::from_slice(&input).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn load_contract_fixtures(directory: &Path, name: &str, phase: WirePhase) -> Vec<WireSpecimen> {
    let path = directory.join(format!("{name}.fixtures.json"));
    let values: Vec<Value> = read_contract_json(&path);
    assert!(!values.is_empty(), "empty fixture set {name}");
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| WireSpecimen {
            label: format!("{name}.fixtures.json specimen {}", index + 1),
            phase,
            value,
        })
        .collect()
}

fn core_specimens(name: &str, phase: WirePhase) -> Vec<WireSpecimen> {
    let contracts = contract_directory();
    let mut specimens = load_contract_fixtures(&contracts, name, phase);
    let path = contracts.join(format!("{name}.reads.json"));
    if path.is_file() {
        let reads: Vec<Value> = read_contract_json(&path);
        for (index, read) in reads.into_iter().enumerate() {
            for (key, phase) in [("input", Deserialize), ("canonical", Serialize)] {
                specimens.push(WireSpecimen {
                    label: format!("{name}.reads.json {key} {}", index + 1),
                    phase,
                    value: read
                        .get(key)
                        .expect("accepted-read specimen member")
                        .clone(),
                });
            }
        }
    }
    specimens
}

fn core_request(name: &str) -> Vec<WireSpecimen> {
    core_specimens(name, Deserialize)
}

fn core_reply(name: &str) -> Vec<WireSpecimen> {
    core_specimens(name, Serialize)
}

fn identity_specimens(name: &str) -> Vec<WireSpecimen> {
    load_contract_fixtures(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../fleet-identity-protocol/contracts"),
        name,
        Deserialize,
    )
}

fn change_records_requests(name: &str) -> Vec<WireSpecimen> {
    let mut specimens = core_request(name);
    let mut omitted_cursor = specimens
        .first()
        .expect("change-record read fixture")
        .value
        .clone();
    omitted_cursor
        .as_object_mut()
        .expect("change-record read object")
        .remove("after");
    let read = serde_json::from_value::<ReadChangeRecordsRequest>(omitted_cursor.clone())
        .expect("omitting the read cursor is legal");
    assert_eq!(serde_json::to_value(read).unwrap()["after"], Value::Null);
    specimens.push(WireSpecimen {
        label: "change-record request omits after".into(),
        phase: Deserialize,
        value: omitted_cursor,
    });
    specimens
}

fn machine_decision_specimens(name: &str) -> Vec<WireSpecimen> {
    let mut specimens = core_reply(name);
    let mut nullable_grant = specimens
        .first()
        .expect("machine grant fixture")
        .value
        .clone();
    nullable_grant["superseded"] = Value::Null;
    let read = serde_json::from_value::<MachineTokenDecision>(nullable_grant.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(read).unwrap(),
        specimens.first().unwrap().value
    );
    specimens.push(WireSpecimen {
        label: "machine grant reader accepts null superseded".into(),
        phase: Deserialize,
        value: nullable_grant,
    });
    specimens
}

fn change_records_replies(_: &str) -> Vec<WireSpecimen> {
    // The canonical aggregate is already the authoritative corpus for these records.
    core_reply("OrganizationAdminResult")
        .into_iter()
        .flat_map(|specimen| {
            match serde_json::from_value::<OrganizationAdminResult>(specimen.value).unwrap() {
                OrganizationAdminResult::Records { records } => records,
                _ => Vec::new(),
            }
        })
        .flat_map(|record| {
            let record = serde_json::to_value(record).unwrap();
            [Serialize, Deserialize].map(|phase| WireSpecimen {
                label: format!("change record {} ({})", record["requestId"], phase.suffix()),
                phase,
                value: record.clone(),
            })
        })
        .collect()
}

fn admin_intent_specimens(name: &str) -> Vec<WireSpecimen> {
    let mut specimens = core_reply(name);
    let raw = json!({
        "v": 1,
        "operation": "RevokeMachineEnrollment",
        "organization": "019a3dce-2000-7000-8000-000000000001",
        "target": "019a3dce-2000-7000-8000-000000000002",
        "operationId": null,
        "params": {"actor": "019a3dce-2000-7000-8000-000000000003", "secondFactorAt": null}
    });
    let intent: OrganizationAdminIntent = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(serde_json::to_value(intent).unwrap(), raw);
    specimens.push(WireSpecimen {
        label: "revocation intent with a null operation ID".into(),
        phase: Serialize,
        value: raw,
    });
    specimens
}

fn exported_typescript() -> String {
    let types = SIGNIN_ROOTS
        .iter()
        .fold(Types::default(), |types, root| (root.register)(types));
    let mut header = concat!(
        "// Phase suffixes are from Central's perspective.\n",
        "// Use _Deserialize for requests and signed payloads Central only reads.\n",
        "// Use _Serialize for replies Central produces and the canonical admin intent it recomputes.\n",
        "// Use the unsuffixed type when there are no separate phase aliases.\n",
        "// Central only reads these signed roots:\n",
    )
    .to_owned();
    for root in SIGNIN_ROOTS.iter().filter(|root| root.central_reads_only) {
        writeln!(header, "//   {}", root.name).expect("append read-only root");
    }
    let exported = specta_typescript::Typescript::default()
        .header(header)
        .export(&types, specta_serde::PhasesFormat)
        .expect("export Central Sign-in wire types");
    let mut generated = String::with_capacity(exported.len());
    for line in exported.lines() {
        writeln!(generated, "{}", line.trim_end()).expect("append TypeScript line");
    }
    generated
        .push_str("\nexport type OrganizationChangeKind = OrganizationChangeRecord[\"kind\"];\n");
    generated
}

#[test]
fn central_signin_types_match_generated_serde_wire() {
    let artifact = contract_directory().join("central-sign-in.ts");
    let generated = exported_typescript();
    if std::env::var("UPDATE_CENTRAL_SIGNIN_TYPES").as_deref() == Ok("1") {
        fs::write(&artifact, generated.as_bytes()).expect("write Central Sign-in types");
    }
    assert!(
        fs::read(&artifact).is_ok_and(|current| current == generated.as_bytes()),
        "Central Sign-in types drifted. Run UPDATE_CENTRAL_SIGNIN_TYPES=1 cargo test -p collaboration-central-core --features wire-export --lib central_signin_types_match_generated_serde_wire"
    );
}

fn phase_alias(declarations: &str, name: &str, phase: WirePhase) -> String {
    let phased = format!("{name}{}", phase.suffix());
    if declarations.contains(&format!("export type {phased} =")) {
        return phased;
    }
    assert!(
        declarations.contains(&format!("export type {name} =")),
        "missing wire root {name}"
    );
    name.to_owned()
}

fn compile_specimens(filename: &str, declarations: &str, assignments: &str) -> Output {
    let scratch = tempfile::tempdir().expect("allocate TypeScript fixture directory");
    fs::write(scratch.path().join("central-sign-in.ts"), declarations)
        .expect("stage wire declarations");
    let compiler_input = scratch.path().join(filename);
    fs::write(&compiler_input, assignments).expect("stage TypeScript fixture assignments");
    Command::new("pnpm")
        .arg("--dir")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../verification"))
        .args([
            "exec",
            "tsc",
            "--ignoreConfig",
            "--strict",
            "--exactOptionalPropertyTypes",
            "--noUncheckedIndexedAccess",
            "--verbatimModuleSyntax",
            "--noEmit",
            "--target",
            "es2024",
            "--module",
            "nodenext",
            "--skipLibCheck",
            "--pretty",
            "false",
        ])
        .arg(compiler_input)
        .output()
        .expect("run the verification package's TypeScript compiler")
}

fn typescript_specimen_expression(root_name: &str, specimen: &Value) -> String {
    if root_name != "MachineTokenClaims" {
        return specimen.to_string();
    }
    // This root is the extension, with unrelated issuer claims ignored by Serde.
    // Spread those members to check the whole input as a structural assignment.
    let known_claims = serde_json::from_value::<MachineTokenClaims>(specimen.clone()).unwrap();
    let known_claims = serde_json::to_value(known_claims).unwrap();
    let mut extra_claims = specimen.as_object().unwrap().clone();
    let mut known_members = Vec::new();
    for key in known_claims.as_object().unwrap().keys() {
        let value = extra_claims.remove(key).unwrap();
        known_members.push(format!("{}: {value}", json!(key)));
    }
    format!(
        "{{ ...{}, {} }}",
        Value::Object(extra_claims),
        known_members.join(", ")
    )
}

#[test]
fn canonical_signin_fixtures_typecheck_in_typescript() {
    let declarations = fs::read_to_string(contract_directory().join("central-sign-in.ts")).unwrap();
    let mut assignments = String::from("import type * as Wire from './central-sign-in.js';\n");
    let mut specimen_count = 0;
    for root in SIGNIN_ROOTS {
        let specimens = (root.specimens)(root.name);
        assert!(!specimens.is_empty(), "missing specimens for {}", root.name);
        for specimen in specimens {
            let alias = phase_alias(&declarations, root.name, specimen.phase);
            let expression = typescript_specimen_expression(root.name, &specimen.value);
            writeln!(
                assignments,
                "// {}\n({expression} satisfies Wire.{alias});",
                specimen.label
            )
            .unwrap();
            specimen_count += 1;
        }
    }
    let output = compile_specimens("positive.ts", &declarations, &assignments);
    assert!(
        output.status.success(),
        "canonical wire fixture check failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!(
        "{specimen_count} canonical and accepted-read specimens across {} roots",
        SIGNIN_ROOTS.len()
    );
}

#[test]
fn invalid_signin_fixtures_fail_at_marked_specimens() {
    let declarations = fs::read_to_string(contract_directory().join("central-sign-in.ts")).unwrap();
    let mut assignments = String::from("import type * as Wire from './central-sign-in.js';\n");
    let mut expected_lines = Vec::new();
    for (name, specimen) in signin_wire_specimens_test::invalid_specimens() {
        let alias = phase_alias(&declarations, name, specimen.phase);
        writeln!(assignments, "// {}", specimen.label).unwrap();
        expected_lines.push(assignments.lines().count() + 1);
        writeln!(assignments, "({} satisfies Wire.{alias});", specimen.value).unwrap();
    }
    let output = compile_specimens("negative.ts", &declarations, &assignments);
    let diagnostics = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.code().is_some_and(|code| code != 0),
        "negative fixture compiler exit: {diagnostics}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut actual_lines = Vec::new();
    for diagnostic in diagnostics
        .lines()
        .filter(|line| line.contains(": error TS"))
    {
        let (location, _) = diagnostic
            .split_once("): error TS")
            .expect("TypeScript diagnostic location");
        let (_, location) = location
            .rsplit_once("negative.ts(")
            .expect("error at a negative specimen");
        let (line, _) = location
            .split_once(',')
            .expect("TypeScript line and column");
        actual_lines.push(line.parse::<usize>().unwrap());
    }
    actual_lines.dedup();
    assert_eq!(
        actual_lines, expected_lines,
        "one rejection per marked specimen: {diagnostics}"
    );
    println!("{} marked invalid specimens rejected", actual_lines.len());
}

#[test]
fn signin_wire_numbers_accept_only_safe_integers() {
    numeric_boundary_specimens();
}

#[test]
fn signin_description_codecs_preserve_wire_strings() {
    description_codec_specimens();
}

#[test]
fn canonical_signin_intent_hash_matches_shared_vector() {
    signed_intent_vector();
}
