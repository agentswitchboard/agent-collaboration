use jsonschema::Validator;
use serde_json::{Value, json};
use switchboard_session_payloads::{
    AssistantBody, DecimalCount, FirstSeenAtMs, FirstSeenAtMsError, Item, PlanEntry,
    PlanEntryState, RawInput, SearchOver, ToolAction, ToolState, UsageAmount, UsageMetric,
};

use super::{
    assert_refused,
    contract_artifacts::replace_field,
    contract_item_fixtures::{
        assistant_item, file_read_item, money_item, tool_item, usage_item, user_item,
    },
    contract_schema::{contract_validator, schema_for},
};

fn assert_wire_parity(
    validator: &Validator,
    base: &Value,
    pointer: &str,
    replacement: Value,
    accepted: bool,
) {
    let mut input = base.clone();
    replace_field(&mut input, pointer, replacement);
    let wire = serde_json::to_string(&input).unwrap();
    let parsed = serde_json::from_str::<Item>(&wire);
    assert_eq!(
        parsed.is_ok(),
        accepted,
        "Rust at {pointer} in {wire}: {parsed:?}"
    );
    assert_eq!(
        validator.is_valid(&input),
        accepted,
        "schema at {pointer} in {wire}"
    );
}

fn decimal_sites() -> [(Value, &'static str); 5] {
    let counts = json!(usage_item(
        UsageMetric::InputTokens,
        UsageAmount::Count {
            value: 0.into(),
            capacity: Some(0.into())
        },
    ));
    [
        (json!(user_item()), "/order"),
        (
            json!(assistant_item(AssistantBody::Message {
                text: "Hello".into()
            })),
            "/body/group/order",
        ),
        (counts.clone(), "/body/body/reading/amount/value"),
        (counts, "/body/body/reading/amount/capacity"),
        (
            json!(tool_item(
                ToolAction::Other {
                    input: RawInput::TooLarge { bytes: 0.into() }
                },
                ToolState::Pending
            )),
            "/body/body/call/action/input/bytes",
        ),
    ]
}

#[test]
fn exported_item_schema_refuses_noncanonical_decimal_text() {
    let schema = schema_for::<Item>();
    assert_eq!(
        schema
            .pointer("/$defs/DecimalCount/maxLength")
            .and_then(Value::as_u64),
        Some(20)
    );
    assert_eq!(
        schema
            .pointer("/$defs/DecimalCount/type")
            .and_then(Value::as_str),
        Some("string")
    );
    let validator = contract_validator(&schema);
    for (base, pointer) in decimal_sites() {
        for text in ["00", "007", "", "７", "-1", "+1", "1.0", "7\n"] {
            assert_wire_parity(&validator, &base, pointer, json!(text), false);
        }
        assert_wire_parity(&validator, &base, pointer, json!(7), false);
    }
}

#[test]
fn exported_item_schema_enforces_the_full_u64_decimal_range() {
    let validator = contract_validator(&schema_for::<Item>());
    for (base, pointer) in decimal_sites() {
        for length in 1..20 {
            assert_wire_parity(&validator, &base, pointer, json!("9".repeat(length)), true);
        }
        for text in [
            "0",
            "9",
            "10",
            "9999999999999999999",
            "10000000000000000000",
            "17446744073709551615",
            "18400000000000000000",
            "18446744073709551000",
            "18446744073709551614",
            "18446744073709551615",
        ] {
            assert_wire_parity(&validator, &base, pointer, json!(text), true);
        }
        for text in [
            "18446744073709551616",
            "99999999999999999999",
            "100000000000000000000",
        ] {
            assert_wire_parity(&validator, &base, pointer, json!(text), false);
        }
        let maximum = u64::MAX.to_string();
        for index in 0..maximum.len() {
            for digit in 0..=9 {
                let mut text = maximum.clone();
                text.replace_range(index..=index, &digit.to_string());
                let accepted = text.parse::<DecimalCount>().is_ok();
                assert_wire_parity(&validator, &base, pointer, json!(text), accepted);
            }
        }
    }
}

#[test]
fn exported_item_schema_counts_identifier_utf8_bytes() {
    let schema = schema_for::<Item>();
    let validator = contract_validator(&schema);
    let plain = jsonschema::validator_for(&schema).unwrap();
    let base = json!(assistant_item(AssistantBody::Message {
        text: "Hello".into()
    }));
    for (definition, pointer) in [
        ("ItemId", "/id"),
        ("TurnId", "/turn"),
        ("GroupId", "/body/group/id"),
    ] {
        let keyword = format!("/$defs/{definition}/x-maxUtf8Bytes");
        assert_eq!(
            schema.pointer(&keyword).and_then(Value::as_u64),
            Some(256),
            "{definition}"
        );
        let keyword = format!("/$defs/{definition}/minLength");
        assert_eq!(
            schema.pointer(&keyword).and_then(Value::as_u64),
            Some(1),
            "{definition}"
        );
        assert_wire_parity(&validator, &base, pointer, json!("é".repeat(128)), true);
        assert_wire_parity(&validator, &base, pointer, json!(" printable/name. "), true);
        for text in ["", "a\t", "a\n", "a\0", "a\u{007f}", "a\u{009f}"] {
            assert_wire_parity(&validator, &base, pointer, json!(text), false);
        }
        let over = json!("é".repeat(129));
        assert_wire_parity(&validator, &base, pointer, over.clone(), false);
        let mut input = base.clone();
        replace_field(&mut input, pointer, over);
        assert!(
            plain.is_valid(&input),
            "{definition}: case must isolate the byte keyword"
        );
    }
}

#[test]
fn exported_item_schema_preserves_money_grammar_and_currency_limits() {
    let schema = schema_for::<Item>();
    let validator = contract_validator(&schema);
    assert_eq!(
        schema
            .pointer("/$defs/MoneyAmount/maxLength")
            .and_then(Value::as_u64),
        Some(40)
    );
    assert_eq!(
        schema
            .pointer("/$defs/MoneyCurrency/minLength")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        schema
            .pointer("/$defs/MoneyCurrency/x-maxUtf8Bytes")
            .and_then(Value::as_u64),
        Some(16)
    );
    for (definition, max_length) in [("MoneyAmount", 40), ("MoneyCurrency", 16)] {
        assert_eq!(
            schema
                .pointer(&format!("/$defs/{definition}/type"))
                .and_then(Value::as_str),
            Some("string"),
        );
        assert_eq!(
            schema
                .pointer(&format!("/$defs/{definition}/maxLength"))
                .and_then(Value::as_u64),
            Some(max_length),
        );
        assert!(
            schema
                .pointer(&format!("/$defs/{definition}/pattern"))
                .and_then(Value::as_str)
                .is_some()
        );
    }
    let base = json!(money_item("0.0100", "USD"));
    let amount = "/body/body/reading/amount/amount";
    let currency = "/body/body/reading/amount/currency";
    for text in ["0", "0.0100", "12", "12.300"] {
        assert_wire_parity(&validator, &base, amount, json!(text), true);
    }
    for text in ["", "01", "1.", ".5", "+1", "-1", "1e0", "１", "1\n"] {
        assert_wire_parity(&validator, &base, amount, json!(text), false);
    }
    assert_wire_parity(&validator, &base, amount, json!("9".repeat(40)), true);
    assert_wire_parity(&validator, &base, amount, json!("9".repeat(41)), false);

    assert_wire_parity(&validator, &base, currency, json!("é".repeat(8)), true);
    assert_wire_parity(&validator, &base, currency, json!("é".repeat(9)), false);
    assert_wire_parity(&validator, &base, currency, json!("Canadian dollar"), true);
    for text in ["", "a\t", "a\n", "a\0", "a\u{007f}", "a\u{009f}"] {
        assert_wire_parity(&validator, &base, currency, json!(text), false);
    }
}

#[test]
fn exported_item_schema_refuses_zero_in_either_line_range_field() {
    let schema = schema_for::<Item>();
    let validator = contract_validator(&schema);
    let base = json!(file_read_item());
    for field in ["firstLine", "lineCount"] {
        let schema_pointer = format!("/$defs/LineRange/properties/{field}");
        let field_schema = schema.pointer(&schema_pointer).unwrap();
        assert_eq!(
            field_schema.get("type").and_then(Value::as_str),
            Some("integer")
        );
        assert_eq!(field_schema.get("minimum").and_then(Value::as_u64), Some(1));
        assert_eq!(
            field_schema.get("maximum").and_then(Value::as_u64),
            Some(u64::from(u32::MAX))
        );
        let pointer = &format!("/body/body/call/action/lines/{field}");
        assert_wire_parity(&validator, &base, pointer, json!(1), true);
        assert_wire_parity(&validator, &base, pointer, json!(u32::MAX), true);
        assert_wire_parity(&validator, &base, pointer, json!(0), false);
        assert_wire_parity(
            &validator,
            &base,
            pointer,
            json!(u64::from(u32::MAX) + 1),
            false,
        );
    }
}

#[test]
fn exported_item_schema_requires_a_javascript_safe_integer_for_first_seen_time() {
    let schema = schema_for::<Item>();
    assert_eq!(
        schema
            .pointer("/$defs/FirstSeenAtMs/type")
            .and_then(Value::as_str),
        Some("integer")
    );
    assert_eq!(
        schema
            .pointer("/$defs/FirstSeenAtMs/minimum")
            .and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(
        schema
            .pointer("/$defs/FirstSeenAtMs/maximum")
            .and_then(Value::as_u64),
        Some(9_007_199_254_740_991)
    );
    assert!(
        schema.pointer("/$defs/FirstSeenAtMs/format").is_none(),
        "consumer must use its bounded integer branch"
    );
    assert_eq!(
        FirstSeenAtMs::try_from(9_007_199_254_740_992),
        Err(FirstSeenAtMsError)
    );

    let validator = contract_validator(&schema);
    let mut base = json!(user_item());
    let pointer = "/firstSeenAtMs";
    for value in [0, 1, 9_007_199_254_740_991_u64] {
        assert_wire_parity(&validator, &base, pointer, json!(value), true);
    }
    for value in [
        json!(-1),
        json!(1.5),
        json!(9_007_199_254_740_992_u64),
        json!("1"),
        Value::Null,
    ] {
        assert_wire_parity(&validator, &base, pointer, value, false);
    }
    base.as_object_mut().unwrap().remove("firstSeenAtMs");
    assert!(!validator.is_valid(&base));
    assert!(serde_json::from_value::<Item>(base).is_err());
}

#[test]
fn first_seen_integral_float_and_exponent_are_lexical_exceptions() {
    let validator = contract_validator(&schema_for::<Item>());
    let base = serde_json::to_string(&user_item()).unwrap();
    for token in ["1.0", "1e0"] {
        let wire = base.replace("1700000000000", token);
        assert_refused::<Item>(&wire, "firstSeenAtMs", "expected u64");
        assert!(
            validator.is_valid(&serde_json::from_str(&wire).unwrap()),
            "{wire}"
        );
    }
}

#[test]
fn exported_item_schema_reaches_the_exact_fieldless_enum_shapes() {
    let schema = schema_for::<Item>();
    let validator = contract_validator(&schema);
    let search_tool = json!(tool_item(
        ToolAction::Search {
            query: "notes".into(),
            over: SearchOver::Web
        },
        ToolState::Pending,
    ));
    let mut plan = json!(assistant_item(AssistantBody::Plan {
        entries: vec![PlanEntry {
            text: "check".into(),
            state: PlanEntryState::Pending,
            priority: Some(switchboard_session_payloads::PlanEntryPriority::High),
        }],
    }));
    let usage = json!(money_item("0.0100", "USD"));
    for (definition, base, pointer, choices) in [
        (
            "ToolKind",
            &search_tool,
            "/body/body/call/kind",
            &[
                "read", "edit", "delete", "move", "search", "execute", "think", "fetch", "other",
            ][..],
        ),
        (
            "SearchOver",
            &search_tool,
            "/body/body/call/action/over",
            &["fileNames", "fileContents", "web"][..],
        ),
        (
            "PlanEntryState",
            &plan,
            "/body/body/entries/0/state",
            &["pending", "inProgress", "completed"][..],
        ),
        (
            "PlanEntryPriority",
            &plan,
            "/body/body/entries/0/priority",
            &["high", "medium", "low"][..],
        ),
        (
            "UsageScope",
            &usage,
            "/body/body/reading/scope",
            &["turn", "session"][..],
        ),
    ] {
        assert_eq!(
            schema.pointer(&format!("/$defs/{definition}/enum")),
            Some(&json!(choices))
        );
        for choice in choices {
            assert_wire_parity(&validator, base, pointer, json!(choice), true);
        }
        assert_wire_parity(&validator, base, pointer, json!("futureChoice"), false);
    }
    // Removing an optional priority is also a valid plan entry.
    plan.pointer_mut("/body/body/entries/0")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove("priority");
    assert!(validator.is_valid(&plan));
}

#[test]
fn exported_raw_input_uses_an_empty_schema_for_opaque_json() {
    let schema = schema_for::<Item>();
    let variants = schema
        .pointer("/$defs/RawInput/oneOf")
        .and_then(Value::as_array)
        .unwrap();
    let raw = variants
        .iter()
        .find(|variant| {
            variant
                .pointer("/properties/kind/const")
                .and_then(Value::as_str)
                == Some("value")
        })
        .unwrap();
    assert_eq!(raw.pointer("/properties/value"), Some(&json!({})));
    assert_eq!(raw.get("additionalProperties"), Some(&Value::Bool(false)));

    let opaque = json!({"sequence": 9_007_199_254_740_993_u64, "epoch": 7});
    let expected = tool_item(
        ToolAction::Other {
            input: RawInput::Value {
                value: opaque.clone(),
            },
        },
        ToolState::Pending,
    );
    let wire = serde_json::to_string(&expected).unwrap();
    let decoded: Item = serde_json::from_str(&wire).unwrap();
    assert_eq!(decoded, expected);
    let encoded: Value = serde_json::from_str(&wire).unwrap();
    assert_eq!(
        encoded.pointer("/body/body/call/action/input/value"),
        Some(&opaque)
    );
    assert_eq!(encoded.get("order"), Some(&json!("0")));
    assert!(contract_validator(&schema).is_valid(&encoded));
}
