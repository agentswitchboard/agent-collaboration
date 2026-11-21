use crate::identity_contract::{
    AuthorizationRevision, AuthorizationRevisionAcknowledgement, DecideDeviceLogin,
    DecideMachineToken, DeviceLoginDecision, DeviceLoginRefusal, MachineTokenDecision,
    MachineTokenRefusal, PresentedMachine,
};
use crate::organization_admin_contract::{
    MachineEnrollmentRevocationResult, OrganizationAdminResult,
};
use crate::person_request::{
    CreateWorkspaceRequest, ReadChangeRecordsRequest, RevokeMachineEnrollmentRequest,
    WorkspaceLookupRequest,
};
use fleet_contracts::{
    MachinePrincipal, MachineTokenClaims, OrganizationStatementDigestReply, ProvisionOrganization,
    ProvisionOrganizationResult, ReadOrganizationStatementDigest,
};
use fleet_signin::organization_change::OrganizationChangeRecord;
use specta::Types;
use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn signin_wire_types() -> Types {
    Types::default()
        .register::<DecideDeviceLogin>()
        .register::<DeviceLoginDecision>()
        .register::<DeviceLoginRefusal>()
        .register::<DecideMachineToken>()
        .register::<MachineTokenDecision>()
        .register::<MachineTokenRefusal>()
        .register::<PresentedMachine>()
        .register::<MachinePrincipal>()
        .register::<MachineTokenClaims>()
        .register::<CreateWorkspaceRequest>()
        .register::<WorkspaceLookupRequest>()
        .register::<ReadChangeRecordsRequest>()
        .register::<OrganizationAdminResult>()
        .register::<OrganizationChangeRecord>()
        .register::<ProvisionOrganization>()
        .register::<ProvisionOrganizationResult>()
        .register::<ReadOrganizationStatementDigest>()
        .register::<OrganizationStatementDigestReply>()
        .register::<AuthorizationRevision>()
        .register::<AuthorizationRevisionAcknowledgement>()
        .register::<RevokeMachineEnrollmentRequest>()
        .register::<MachineEnrollmentRevocationResult>()
}

fn contract_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("contracts")
}

#[test]
fn central_signin_types_match_generated_serde_wire() {
    let artifact = contract_directory().join("central-sign-in.ts");
    let exported = specta_typescript::Typescript::default()
        .export(&signin_wire_types(), specta_serde::PhasesFormat)
        .expect("export Central Sign-in wire types");

    // Specta leaves a space before line breaks in some unions.
    let mut generated = String::with_capacity(exported.len());
    for line in exported.lines() {
        writeln!(generated, "{}", line.trim_end()).expect("append TypeScript line");
    }

    if std::env::var("UPDATE_CENTRAL_SIGNIN_TYPES").as_deref() == Ok("1") {
        fs::write(&artifact, generated.as_bytes()).expect("write Central Sign-in types");
    }
    assert!(
        fs::read(&artifact).is_ok_and(|current| current == generated.as_bytes()),
        "Central Sign-in types drifted. Run UPDATE_CENTRAL_SIGNIN_TYPES=1 cargo test -p collaboration-central-core --features wire-export --lib central_signin_types_match_generated_serde_wire"
    );
}

// A phased type uses the write alias here because these are serialized values.
const CANONICAL_FIXTURES: &[(&str, &str)] = &[
    ("AllowedWorkspaces", "AllowedWorkspaces"),
    ("AuthorizationRevision", "AuthorizationRevision"),
    (
        "AuthorizationRevisionAcknowledgement",
        "AuthorizationRevisionAcknowledgement",
    ),
    ("ClientId", "ClientId"),
    ("CreateWorkspaceRequest", "CreateWorkspaceRequest_Serialize"),
    ("DecideDeviceLogin", "DecideDeviceLogin_Serialize"),
    ("DecideMachineToken", "DecideMachineToken"),
    ("DeviceLoginDecision", "DeviceLoginDecision"),
    ("DeviceLoginRefusal", "DeviceLoginRefusal"),
    ("DeviceLoginRequestId", "DeviceLoginRequestId"),
    ("EndedMachineClient", "EndedMachineClient"),
    ("EnrollPermission", "EnrollPermission"),
    ("MachinePrincipal", "MachinePrincipal"),
    ("MachineTokenClaims", "MachineTokenClaims"),
    ("MachineTokenDecision", "MachineTokenDecision_Serialize"),
    ("MachineTokenRefusal", "MachineTokenRefusal"),
    (
        "OrganizationAdminResult",
        "OrganizationAdminResult_Serialize",
    ),
    ("PersonId", "PersonId"),
    ("PresentedMachine", "PresentedMachine"),
    (
        "ReadChangeRecordsRequest",
        "ReadChangeRecordsRequest_Serialize",
    ),
    ("WorkspaceLookupRequest", "WorkspaceLookupRequest"),
];

#[test]
fn canonical_signin_fixtures_typecheck_in_typescript() {
    let contracts = contract_directory();
    let scratch = tempfile::tempdir().expect("allocate TypeScript fixture directory");
    fs::copy(
        contracts.join("central-sign-in.ts"),
        scratch.path().join("central-sign-in.ts"),
    )
    .expect("stage the committed contract");
    let compiler_input = scratch.path().join("fixtures.ts");
    let mut assignments = String::from("import type * as Wire from './central-sign-in.js';\n");
    let mut fixture_count = 0;
    for (fixture, wire_type) in CANONICAL_FIXTURES {
        let path = contracts.join(format!("{fixture}.fixtures.json"));
        let raw = fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        let values: Vec<serde_json::Value> = serde_json::from_slice(&raw)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        assert!(!values.is_empty(), "empty fixture set {fixture}");
        for (number, value) in values.iter().enumerate() {
            writeln!(
                assignments,
                "// {fixture}.fixtures.json specimen {}\n({value} satisfies Wire.{wire_type});",
                number + 1,
            )
            .expect("write TypeScript fixture assignment");
            fixture_count += 1;
        }
    }

    let digest =
        include_str!("../../fleet-contracts/tests/fixtures/organization-statement-digest.json");
    let digest: serde_json::Value = serde_json::from_str(digest).expect("read digest fixture");
    writeln!(
        assignments,
        "({digest} satisfies Wire.OrganizationStatementDigestReply);"
    )
    .expect("write digest fixture assignment");

    assignments.push_str(
        r#"
declare const grant: Extract<Wire.MachineTokenDecision_Serialize, { result: "grant" }>;
({ ...grant, superseded: null } satisfies Wire.MachineTokenDecision_Deserialize);
// @ts-expect-error Rust omits an absent superseded value when serializing.
({ ...grant, superseded: null } satisfies Wire.MachineTokenDecision_Serialize);
// @ts-expect-error JSON omission is distinct from an explicitly undefined property.
({ ...grant, superseded: undefined } satisfies Wire.MachineTokenDecision_Serialize);
// @ts-expect-error The wire uses a numeric timestamp.
({ ...grant, decidedAt: "1900000000" } satisfies Wire.MachineTokenDecision_Serialize);
"#,
    );

    fs::write(&compiler_input, assignments).expect("stage TypeScript fixture assignments");
    let verification = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../verification");
    let output = Command::new("pnpm")
        .arg("--dir")
        .arg(verification)
        .args([
            "exec",
            "tsc",
            "--ignoreConfig",
            "--strict",
            "--exactOptionalPropertyTypes",
            "--noEmit",
            "--target",
            "es2022",
            "--module",
            "nodenext",
            "--skipLibCheck",
        ])
        .arg(compiler_input)
        .output()
        .expect("run the verification package's TypeScript compiler");
    assert!(
        output.status.success(),
        "canonical wire fixture check failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    println!(
        "{} canonical specimens and three deliberate type errors checked",
        fixture_count + 1
    );
}
