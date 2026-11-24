use collaboration_protocol::protocol_type_schemas;
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
