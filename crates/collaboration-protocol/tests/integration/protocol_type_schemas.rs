use collaboration_protocol::protocol_type_schemas;

#[test]
fn settlement_refusal_keeps_the_failed_request_and_the_predecessor_in_separate_fields() {
    use collaboration_protocol::{
        ConversationOperationFailure as Failure, ConversationOperationFailureKind as Kind,
        ConversationOperationFailureStage as Stage, OperationId, ProviderOperationEffect,
        SessionRef,
    };

    let request = OperationId::generate();
    let predecessor = OperationId::generate();
    let target: SessionRef = serde_json::from_value(serde_json::json!({
        "endpoint":{"serviceId":"0ff962c5-7fa3-4c18-a5ca-1bbe8db09e89","endpointId":"cursor-local"},
        "sessionId":"fenced-session"
    }))
    .unwrap();
    let guidance = collaboration_protocol::PREVIOUS_TURN_SETTLEMENT_GUIDANCE
        .to_owned()
        .try_into()
        .unwrap();
    let ordinary = Failure::new(
        Kind::Busy,
        Stage::Admission,
        ProviderOperationEffect::None,
        String::from("provider session is busy").try_into().unwrap(),
        Some(request.clone()),
        Some(target.clone()),
    );
    let fence = Failure::previous_turn_settlement_unresolved(
        request.clone(),
        predecessor.clone(),
        target,
        guidance,
    );
    let schema = serde_json::to_value(schemars::schema_for!(Failure)).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();

    let ordinary = serde_json::to_value(ordinary).unwrap();
    let wire = serde_json::to_value(fence).unwrap();
    assert!(validator.is_valid(&ordinary));
    assert!(validator.is_valid(&wire));
    assert!(ordinary.get("previousOperationId").is_none());
    assert_eq!(wire["kind"], "previousTurnSettlementUnresolved");
    assert_eq!(wire["operationId"], request.as_str());
    assert_eq!(wire["previousOperationId"], predecessor.as_str());
    let decoded: Failure = serde_json::from_value(wire).unwrap();
    assert_eq!(decoded.operation_id.as_ref(), Some(&request));
    assert_eq!(decoded.previous_operation_id(), Some(&predecessor));
}
use serde_json::json;

#[test]
fn endpoint_schema_matches_runtime_ascii_identity_boundaries() {
    let schema = serde_json::to_value(schemars::schema_for!(collaboration_protocol::EndpointId))
        .unwrap_or_else(|error| panic!("schema: {error}"));
    let validator =
        jsonschema::validator_for(&schema).unwrap_or_else(|error| panic!("validator: {error}"));
    for name in [
        "codex-local".to_owned(),
        "codex-local\n".to_owned(),
        "Codex".to_owned(),
        "1codex".to_owned(),
        "".to_owned(),
        "a".repeat(64),
        "a".repeat(65),
        "a_".to_owned(),
    ] {
        let accepted = collaboration_protocol::EndpointId::try_from(name.clone()).is_ok();
        assert_eq!(
            validator.is_valid(&json!(name)),
            accepted,
            "identity {name:?}"
        );
    }
}

#[test]
fn journal_schema_rejects_unsafe_positions_and_unknown_fields() {
    let schemas = protocol_type_schemas().unwrap_or_else(|error| panic!("schemas: {error}"));
    let schema = schemas
        .get("JournalPage")
        .unwrap_or_else(|| panic!("journal schema"));
    let validator =
        jsonschema::validator_for(schema).unwrap_or_else(|error| panic!("validator: {error}"));
    let id = "00000000-0000-4000-8000-000000000001";
    let mut page = json!({"bounds":{"journalId":id,"earliestSequence":1,"lastSequence":0},"records":[],"next":{"journalId":id,"sequence":0},"caughtUp":true});
    assert!(validator.is_valid(&page));
    page["bounds"]["lastSequence"] = json!(9007199254740992_u64);
    assert!(!validator.is_valid(&page));
    page["bounds"]["lastSequence"] = json!(0);
    page["surprise"] = json!(true);
    assert!(!validator.is_valid(&page));
}

#[test]
fn snapshot_schema_preserves_required_nullable_coverage_fields() {
    let schemas = protocol_type_schemas().unwrap_or_else(|error| panic!("schemas: {error}"));
    let schema = schemas
        .get("AddressPage")
        .unwrap_or_else(|| panic!("address schema"));
    let validator =
        jsonschema::validator_for(schema).unwrap_or_else(|error| panic!("validator: {error}"));
    let id = "00000000-0000-4000-8000-000000000001";
    let mut snapshot = json!({"snapshotId":id,"capturedAt":"2026-09-05T12:00:00Z","watermark":{"journalId":id,"sequence":0},"coverage":{"endpoint":{"serviceId":id,"endpointId":"codex-local"},"observerId":null,"generation":null,"state":"initializing","observedAt":"2026-09-05T12:00:00Z"},"entries":[],"nextCursor":null});
    assert!(validator.is_valid(&snapshot));
    snapshot["coverage"]
        .as_object_mut()
        .unwrap_or_else(|| panic!("coverage object"))
        .remove("generation");
    assert!(!validator.is_valid(&snapshot));
}

#[test]
fn permission_diagnostic_schema_exposes_closed_agent_recovery_contract() {
    let schemas = protocol_type_schemas().unwrap_or_else(|error| panic!("schemas: {error}"));
    let schema = schemas
        .get("PermissionDiagnostic")
        .unwrap_or_else(|| panic!("permission diagnostic schema"));
    let validator =
        jsonschema::validator_for(schema).unwrap_or_else(|error| panic!("validator: {error}"));
    let diagnostic = json!({
        "kind": "permissionDenied",
        "stage": "socketConnect",
        "message": "Request automated approval review through your tool, or ask the user to grant the required command/socket access. Retry only after access is granted.",
        "nextAction": "requestApproval"
    });
    assert!(validator.is_valid(&diagnostic));

    let mut invalid_action = diagnostic.clone();
    invalid_action["nextAction"] = json!("retryLater");
    assert!(!validator.is_valid(&invalid_action));

    let mut unknown_field = diagnostic;
    unknown_field["automaticApproval"] = json!(true);
    assert!(!validator.is_valid(&unknown_field));
}
