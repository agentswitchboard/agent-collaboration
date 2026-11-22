use super::{
    ApprovalRequestId, AreaId, IdValidationError, MachineId, ProjectId, RouterRunId, SessionId,
    SessionOperationId, SessionRef, WorkspaceId,
};
use serde::de::DeserializeOwned;
use std::str::FromStr;

#[test]
fn router_run_id_requires_canonical_uuid_v7() {
    assert_canonical_approval_id::<RouterRunId>("RouterRunId");
}

#[test]
fn approval_request_id_requires_canonical_uuid_v7() {
    assert_canonical_approval_id::<ApprovalRequestId>("ApprovalRequestId");
}

fn assert_canonical_approval_id<Id: FromStr + DeserializeOwned>(identifier: &str) {
    let canonical = "01890f2e-7b4c-7cc0-98c4-000000000001";
    assert!(
        canonical.parse::<Id>().is_ok(),
        "{identifier} must accept {canonical}"
    );
    assert!(
        serde_json::from_value::<Id>(serde_json::json!(canonical)).is_ok(),
        "{identifier} must decode {canonical}"
    );

    for (reason, invalid) in [
        ("UUIDv4", "01890f2e-7b4c-4cc0-98c4-000000000001"),
        ("uppercase UUIDv7", "01890F2E-7B4C-7CC0-98C4-000000000001"),
        ("short UUIDv7", "01890f2e-7b4c-7cc0-98c4-00000000001"),
    ] {
        assert!(
            invalid.parse::<Id>().is_err(),
            "{identifier} must reject {reason}: {invalid}"
        );
        assert!(
            serde_json::from_value::<Id>(serde_json::json!(invalid)).is_err(),
            "{identifier} must reject {reason} on JSON decode: {invalid}"
        );
    }
}

#[test]
fn canonical_v7_is_required_on_construction_and_decode() {
    let good = "01890f2e-7b4c-7cc0-98c4-000000000001";
    assert!(good.parse::<AreaId>().is_ok());
    for invalid in [
        "",
        "01890f2e-7b4c-4cc0-98c4-000000000001",
        "01890f2e-7b4c-7cc0-08c4-000000000001",
        "01890F2E-7B4C-7CC0-98C4-000000000001",
        "01890f2e7b4c7cc098c4000000000001",
    ] {
        assert!(invalid.parse::<AreaId>().is_err());
        assert!(serde_json::from_str::<ProjectId>(&format!("\"{invalid}\"")).is_err());
    }
    assert!(serde_json::from_str::<WorkspaceId>(&format!("\"{good}\"")).is_ok());
}

/// The one machine id (owner, 2026-11-18: "there shodu be no v4 ever"):
/// any UUID decodes only when it is a canonical lowercase RFC UUIDv7.
#[hegel::test(test_cases = 300)]
fn a_machine_id_decodes_only_from_a_canonical_uuid_v7(tc: hegel::TestCase) {
    use hegel::generators as gs;
    let mut bytes = [0_u8; 16];
    for byte in &mut bytes {
        *byte = tc.draw(gs::integers::<u8>());
    }
    let uuid = uuid::Uuid::from_bytes(bytes);
    let text = uuid.hyphenated().to_string();
    let is_v7 = uuid.get_version_num() == 7 && uuid.get_variant() == uuid::Variant::RFC4122;

    assert_eq!(text.parse::<MachineId>().is_ok(), is_v7, "{text}");
    assert_eq!(
        serde_json::from_str::<MachineId>(&format!("\"{text}\"")).is_ok(),
        is_v7
    );
    assert!(
        text.to_uppercase().parse::<MachineId>().is_err() || !text.chars().any(char::is_alphabetic),
        "only canonical lowercase decodes"
    );
}

#[test]
fn a_session_ref_is_the_machine_and_the_provider_session_with_no_endpoint() {
    let wire = serde_json::json!({
        "machineId": "01890f2e-7b4c-7cc0-98c4-000000000001",
        "sessionId": "Codex thread 019a-anything",
    });
    let session: SessionRef = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(session.session_id.as_str(), "Codex thread 019a-anything");
    assert_eq!(serde_json::to_value(&session).unwrap(), wire);
    for bad in [
        serde_json::json!({"machineId": "550e8400-e29b-41d4-a716-446655440701", "sessionId": "s"}),
        serde_json::json!({"machineId": "01890f2e-7b4c-7cc0-98c4-000000000001", "sessionId": ""}),
        serde_json::json!({"machineId": "01890f2e-7b4c-7cc0-98c4-000000000001", "sessionId": "s", "endpoint": "codex"}),
    ] {
        assert!(
            serde_json::from_value::<SessionRef>(bad.clone()).is_err(),
            "{bad}"
        );
    }
    assert!("a\0b".parse::<SessionId>().is_err());
    assert!("x".repeat(4097).parse::<SessionId>().is_err());
}

#[test]
fn the_session_ref_schema_describes_two_strings() {
    let schema = serde_json::to_value(schemars::schema_for!(SessionRef)).unwrap();
    for (field, definition) in [("machineId", "MachineId"), ("sessionId", "SessionId")] {
        assert_eq!(
            schema["properties"][field]["$ref"],
            format!("#/$defs/{definition}"),
            "{schema}"
        );
        assert_eq!(schema["$defs"][definition]["type"], "string", "{schema}");
    }
    assert_eq!(
        schema["required"],
        serde_json::json!(["machineId", "sessionId"])
    );
    assert_eq!(schema["additionalProperties"], false, "{schema}");
}

// The UUID spellings and JSON bytes are independent of the identifier's serializer.
#[test]
fn session_operation_id_reads_only_canonical_uuid_v7() {
    let valid = "01890f2e-7b4c-7cc0-98c4-000000000001";
    let literal = r#""01890f2e-7b4c-7cc0-98c4-000000000001""#;
    let expected = valid.parse::<SessionOperationId>().unwrap();
    assert_eq!(expected.as_str(), valid);
    assert_eq!(
        serde_json::from_str::<SessionOperationId>(literal).unwrap(),
        expected
    );
    assert_eq!(serde_json::to_string(&expected).unwrap(), literal);

    for (reason, invalid) in [
        ("uppercase", "01890F2E-7B4C-7CC0-98C4-000000000001"),
        ("UUIDv4", "01890f2e-7b4c-4cc0-98c4-000000000001"),
        ("non-RFC variant", "01890f2e-7b4c-7cc0-08c4-000000000001"),
        ("braces", "{01890f2e-7b4c-7cc0-98c4-000000000001}"),
        ("empty", ""),
    ] {
        assert_eq!(
            invalid.parse::<SessionOperationId>(),
            Err(IdValidationError {
                field: "SessionOperationId"
            }),
            "SessionOperationId must reject {reason}"
        );
        let wire = serde_json::to_string(invalid).unwrap();
        let error = serde_json::from_str::<SessionOperationId>(&wire).unwrap_err();
        assert!(
            error.to_string().contains(
                &IdValidationError {
                    field: "SessionOperationId"
                }
                .to_string()
            ),
            "SessionOperationId JSON reader must reject {reason}"
        );
    }
}
