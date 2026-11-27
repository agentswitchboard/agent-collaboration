use serde_json::{Value, json};

pub(super) const TIMESTAMP_LEXICAL_EXCEPTION: &str = "first-seen-integer-token";

pub(super) fn manifest() -> Value {
    json!({
        "name": "session-read-contract",
        "version": 1,
        "roots": ["Item", "LoadRequest", "LoadQuery"],
        "deferredRoots": ["InspectResult", "LoadResult", "WatchRecord", "WatchEnd"],
        "plainValidatorLimitations": [
            {
                "rule": "checked-utf8-byte-limits",
                "detail": "A plain validator ignores x-maxUtf8Bytes. The contract test registers it and the consumer generates a UTF-8 byte-length check."
            },
            {
                "rule": TIMESTAMP_LEXICAL_EXCEPTION,
                "detail": "JSON Schema accepts an integral JSON number such as 1.0. The Rust timestamp reader requires an integer token."
            }
        ],
        "inherited": {
            "policy": "Inherited from workspace-ids, not restated here.",
            "types": [
                {
                    "name": "SessionOperationId",
                    "roots": ["LoadRequest", "LoadQuery"],
                    "schema": "String with no UUID pattern.",
                    "rustReader": "Requires a lower-case, hyphenated canonical RFC 4122 UUIDv7.",
                    "additionalRefusals": ["Malformed UUID", "Wrong UUID version", "Wrong UUID variant", "Noncanonical UUID spelling"]
                },
                {
                    "name": "SessionId",
                    "roots": ["Item", "LoadRequest", "LoadQuery"],
                    "schema": "String. The description mentions a limit and no NUL.",
                    "rustReader": "Requires 1 to 4096 UTF-8 bytes and refuses NUL.",
                    "additionalRefusals": ["Empty string", "More than 4096 UTF-8 bytes", "NUL"]
                },
                {
                    "name": "MachineId",
                    "roots": ["Item", "LoadRequest", "LoadQuery"],
                    "schema": "String with a lower-case, hyphenated RFC 4122 UUIDv7 pattern.",
                    "rustReader": "Requires the same UUID representation.",
                    "additionalRefusals": []
                },
                {
                    "name": "SessionRef",
                    "roots": ["Item", "LoadRequest", "LoadQuery"],
                    "schema": "Closed object requiring machineId and sessionId.",
                    "rustReader": "Requires the named pair and applies the inherited identifier readers.",
                    "additionalRefusals": ["See SessionId"]
                }
            ]
        },
        "jsonParserBoundary": "Duplicate keys and numeric token spellings inside raw values are handled by the JSON parser."
    })
}

#[test]
fn session_read_manifest_keeps_the_exact_scope_and_inherited_limits() {
    let manifest = manifest();
    assert_eq!(manifest.get("name"), Some(&json!("session-read-contract")));
    assert_eq!(manifest.get("version"), Some(&json!(1)));
    assert_eq!(
        manifest.get("roots"),
        Some(&json!(["Item", "LoadRequest", "LoadQuery"]))
    );
    assert_eq!(
        manifest.get("deferredRoots"),
        Some(&json!([
            "InspectResult",
            "LoadResult",
            "WatchRecord",
            "WatchEnd"
        ])),
    );
    let inherited = manifest
        .pointer("/inherited/types")
        .and_then(Value::as_array)
        .unwrap();
    let names = inherited
        .iter()
        .map(|entry| entry.get("name").and_then(Value::as_str).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["SessionOperationId", "SessionId", "MachineId", "SessionRef"]
    );
    let limits = manifest
        .get("plainValidatorLimitations")
        .and_then(Value::as_array)
        .unwrap();
    assert_eq!(limits.len(), 2);
}

#[test]
fn session_load_schemas_retain_the_inherited_workspace_id_contracts() {
    use switchboard_session_payloads::{LoadQuery, LoadRequest};

    assert_inherited_load_rules::<LoadRequest>();
    assert_inherited_load_rules::<LoadQuery>();
}

fn assert_inherited_load_rules<T: schemars::JsonSchema + serde::de::DeserializeOwned>() {
    use super::{
        LOAD, OPERATION_ID,
        contract_artifacts::replace_field,
        contract_schema::{contract_validator, schema_for},
    };

    let schema = schema_for::<T>();
    let validator = contract_validator(&schema);
    let load: Value = serde_json::from_str(LOAD).unwrap();
    assert!(validator.is_valid(&load));
    assert!(serde_json::from_value::<T>(load.clone()).is_ok());
    for definition in ["SessionOperationId", "SessionId", "MachineId"] {
        assert_eq!(
            schema
                .pointer(&format!("/$defs/{definition}/type"))
                .and_then(Value::as_str),
            Some("string"),
        );
    }
    assert!(
        schema
            .pointer("/$defs/SessionOperationId/pattern")
            .is_none()
    );
    assert!(schema.pointer("/$defs/SessionId/minLength").is_none());
    assert!(schema.pointer("/$defs/SessionId/maxLength").is_none());
    assert!(schema.pointer("/$defs/SessionId/x-maxUtf8Bytes").is_none());
    assert!(
        schema
            .pointer("/$defs/MachineId/pattern")
            .and_then(Value::as_str)
            .is_some()
    );
    assert_eq!(
        schema.pointer("/$defs/SessionRef/additionalProperties"),
        Some(&Value::Bool(false)),
    );

    for (pointer, value, accepted_by_schema) in [
        ("/operationId", json!("not-a-uuid"), true),
        ("/operationId", json!(OPERATION_ID.to_uppercase()), true),
        ("/target/sessionId", json!(""), true),
        ("/target/sessionId", json!("é".repeat(2049)), true),
        ("/target/sessionId", json!("fixture\0value"), true),
        (
            "/target/machineId",
            json!("01890f2e-7b4c-4cc0-98c4-000000000001"),
            false,
        ),
    ] {
        let mut input = load.clone();
        replace_field(&mut input, pointer, value);
        assert_eq!(validator.is_valid(&input), accepted_by_schema, "{pointer}");
        assert!(serde_json::from_value::<T>(input).is_err(), "{pointer}");
    }
}
