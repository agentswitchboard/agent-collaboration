use std::{fs, path::Path};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use switchboard_session_payloads::{
    AssistantBody, BoundedText, FirstSeenAtMs, Item, LoadQuery, LoadRequest, RawInput, ToolAction,
    ToolState,
};

use super::{
    OPERATION_ID,
    contract_item_fixtures::{
        ItemExamples, assistant_item, file_read_item, money_item, tool_item, user_item,
    },
    contract_manifest::{TIMESTAMP_LEXICAL_EXCEPTION, manifest},
    contract_schema::{contract_validator, schema_for},
    item_actions, item_examples, target,
};

#[derive(Serialize)]
struct RefusedExample {
    name: &'static str,
    root: &'static str,
    input: Value,
    path: String,
    reason: String,
    schema_expressible: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_exception: Option<&'static str>,
}

#[derive(Serialize)]
struct ReadExample {
    name: &'static str,
    input: Value,
    canonical: Value,
}

#[test]
fn session_read_contract_artifacts_match_generation() {
    let item_schema = schema_for::<Item>();
    let load_request_schema = schema_for::<LoadRequest>();
    let load_query_schema = schema_for::<LoadQuery>();
    let item_validator = contract_validator(&item_schema);
    let examples = accepted_items();
    check_item_fixture_inventory(&examples);

    for fixture in &examples.fixtures {
        check_accepted(&fixture.name, &fixture.value, &item_validator);
    }

    let negatives = refused_items(&item_validator);
    let reads = output_reads(&item_validator);
    check_read_and_refusal_inventory(&reads, &negatives);
    let load_request = LoadRequest {
        target: target(),
        operation_id: OPERATION_ID.parse().unwrap(),
    };
    let load_query = LoadQuery {
        target: target(),
        operation_id: OPERATION_ID.parse().unwrap(),
    };
    check_accepted(
        "load-request",
        &load_request,
        &contract_validator(&load_request_schema),
    );
    check_accepted(
        "load-query",
        &load_query,
        &contract_validator(&load_query_schema),
    );

    for (name, value) in [
        ("Item.schema.json", item_schema),
        ("LoadRequest.schema.json", load_request_schema),
        ("LoadQuery.schema.json", load_query_schema),
        ("Item.fixtures.json", json!(examples.fixtures)),
        ("Item.negatives.json", json!(negatives)),
        ("Item.reads.json", json!(reads)),
        (
            "LoadRequest.fixtures.json",
            json!([{"name":"load-request","value":load_request}]),
        ),
        (
            "LoadQuery.fixtures.json",
            json!([{"name":"load-query","value":load_query}]),
        ),
        ("session-read-contract.json", manifest()),
    ] {
        check_artifact(name, &value);
    }
}

fn check_accepted<T>(name: &str, expected: &T, validator: &jsonschema::Validator)
where
    T: Serialize + DeserializeOwned + Eq + std::fmt::Debug,
{
    let wire = serde_json::to_string(expected).unwrap();
    assert_eq!(
        serde_json::from_str::<T>(&wire).unwrap(),
        *expected,
        "{name}"
    );
    let encoded = serde_json::from_str(&wire).unwrap();
    assert!(
        validator.is_valid(&encoded),
        "{name} fails the schema: {encoded}"
    );
}

fn accepted_items() -> ItemExamples {
    let mut examples = ItemExamples::default();
    item_examples::collect_examples(&mut examples);
    item_actions::collect_examples(&mut examples);

    let mut zero = user_item();
    zero.first_seen_at_ms = FirstSeenAtMs::try_from(0).unwrap();
    examples.add("zero-order-and-zero-time", zero);

    let mut maximum = user_item();
    maximum.order = u64::MAX.into();
    maximum.first_seen_at_ms = FirstSeenAtMs::try_from(FirstSeenAtMs::MAX).unwrap();
    examples.add("maximum-order-and-maximum-time", maximum);

    let limit = "é".repeat(128);
    let mut at_id_limit = assistant_item(AssistantBody::Message {
        text: "Hello".into(),
    });
    at_id_limit.id = limit.parse().unwrap();
    at_id_limit.turn = Some(limit.parse().unwrap());
    if let switchboard_session_payloads::ItemBody::Assistant { group, .. } = &mut at_id_limit.body {
        group.id = limit.parse().unwrap();
    }
    examples.add("identifiers-at-utf8-byte-limit", at_id_limit);

    examples.add(
        "money-preserves-fractional-zeros",
        money_item("0.0100", "USD"),
    );
    let raw = examples.add(
        "raw-numeric-sequence-and-epoch",
        tool_item(
            ToolAction::Other {
                input: RawInput::Value {
                    value: json!({"sequence": 9_007_199_254_740_993_u64, "epoch": 7}),
                },
            },
            ToolState::Pending,
        ),
    );
    raw.description =
        Some("Raw sequence and epoch are ordinary JSON numbers. The item order uses decimal text.");
    examples
}

fn check_item_fixture_inventory(examples: &ItemExamples) {
    let names: std::collections::BTreeSet<_> = examples
        .fixtures
        .iter()
        .map(|fixture| fixture.name.as_str())
        .collect();
    assert_eq!(
        names.len(),
        examples.fixtures.len(),
        "duplicate fixture name"
    );
    let expected = std::collections::BTreeSet::from([
        "shell-completed",
        "file-change-old-and-new-and-path-only",
        "raw-opaque-object",
        "raw-null",
        "raw-array",
        "raw-too-large",
        "action-unreported",
        "raw-input-unreported",
        "raw-empty-object",
        "tool-pending",
        "tool-running",
        "tool-failed",
        "tool-declined",
        "completed-error-text",
        "output-absent",
        "output-empty",
        "output-cut",
        "two-assistant-groups-1",
        "two-assistant-groups-2",
        "tool-only-group",
        "user-message",
        "session-notice",
        "session-setting-change",
        "other-assistant",
        "other-session",
        "ordered-plan",
        "plan-priority-omitted",
        "usage-count",
        "usage-money",
        "other-tool-completed",
        "other-tool-failed",
        "file-read-range",
        "file-read-without-range",
        "search-file-names",
        "search-file-contents",
        "search-web",
        "fetch",
        "sub-agent-with-child",
        "sub-agent-without-child",
        "file-patch-and-move",
        "zero-order-and-zero-time",
        "maximum-order-and-maximum-time",
        "identifiers-at-utf8-byte-limit",
        "money-preserves-fractional-zeros",
        "raw-numeric-sequence-and-epoch",
    ]);
    assert_eq!(names, expected, "public Item fixture inventory");
}

fn check_read_and_refusal_inventory(reads: &[ReadExample], refusals: &[RefusedExample]) {
    assert_eq!(
        reads.iter().map(|case| case.name).collect::<Vec<_>>(),
        ["detail-omitted", "detail-null", "detail-empty"],
    );
    assert_eq!(
        refusals.iter().map(|case| case.name).collect::<Vec<_>>(),
        [
            "order-leading-zero",
            "order-overflow",
            "id-over-utf8-byte-limit",
            "id-control-character",
            "money-invalid-text",
            "currency-over-utf8-byte-limit",
            "line-range-zero-first-line",
            "line-range-zero-line-count",
            "first-seen-over-maximum",
            "first-seen-negative",
            "first-seen-fractional",
            "first-seen-integral-float-token",
            "first-seen-required",
        ],
    );
}

pub(super) fn replace_field(value: &mut Value, pointer: &str, changed: Value) {
    *value.pointer_mut(pointer).unwrap() = changed;
}

fn refused_items(validator: &jsonschema::Validator) -> Vec<RefusedExample> {
    let item = json!(user_item());
    let file = json!(file_read_item());
    let money = json!(money_item("0.0100", "USD"));
    let mut examples = Vec::new();
    for (name, base, pointer, changed, path, expected_reason) in [
        (
            "order-leading-zero",
            &item,
            "/order",
            json!("007"),
            "order",
            "decimal text has a leading zero",
        ),
        (
            "order-overflow",
            &item,
            "/order",
            json!("18446744073709551616"),
            "order",
            "decimal text exceeds the maximum",
        ),
        (
            "id-over-utf8-byte-limit",
            &item,
            "/id",
            json!("é".repeat(129)),
            "id",
            "identifier text exceeds 256 bytes",
        ),
        (
            "id-control-character",
            &item,
            "/id",
            json!("id\u{009f}"),
            "id",
            "identifier text contains a control character",
        ),
        (
            "money-invalid-text",
            &money,
            "/body/body/reading/amount/amount",
            json!("not-money"),
            "body",
            "money amount must be ASCII digits",
        ),
        (
            "currency-over-utf8-byte-limit",
            &money,
            "/body/body/reading/amount/currency",
            json!("é".repeat(9)),
            "body",
            "money currency exceeds 16 bytes",
        ),
        (
            "line-range-zero-first-line",
            &file,
            "/body/body/call/action/lines/firstLine",
            json!(0),
            "body",
            "firstLine must be at least 1",
        ),
        (
            "line-range-zero-line-count",
            &file,
            "/body/body/call/action/lines/lineCount",
            json!(0),
            "body",
            "lineCount must be at least 1",
        ),
        (
            "first-seen-over-maximum",
            &item,
            "/firstSeenAtMs",
            json!(FirstSeenAtMs::MAX + 1),
            "firstSeenAtMs",
            "first-seen time exceeds the maximum",
        ),
        (
            "first-seen-negative",
            &item,
            "/firstSeenAtMs",
            json!(-1),
            "firstSeenAtMs",
            "expected u64",
        ),
        (
            "first-seen-fractional",
            &item,
            "/firstSeenAtMs",
            json!(1.5),
            "firstSeenAtMs",
            "expected u64",
        ),
        (
            "first-seen-integral-float-token",
            &item,
            "/firstSeenAtMs",
            json!(1.0),
            "firstSeenAtMs",
            "expected u64",
        ),
    ] {
        let mut input = base.clone();
        replace_field(&mut input, pointer, changed);
        let schema_exception =
            (name == "first-seen-integral-float-token").then_some(TIMESTAMP_LEXICAL_EXCEPTION);
        examples.push(refused(
            name,
            input,
            path,
            expected_reason,
            schema_exception,
            validator,
        ));
    }

    let mut missing = item;
    missing.as_object_mut().unwrap().remove("firstSeenAtMs");
    examples.push(refused(
        "first-seen-required",
        missing,
        ".",
        "missing field `firstSeenAtMs`",
        None,
        validator,
    ));
    examples
}

fn refused(
    name: &'static str,
    input: Value,
    expected_path: &str,
    expected_reason: &str,
    schema_exception: Option<&'static str>,
    validator: &jsonschema::Validator,
) -> RefusedExample {
    let wire = serde_json::to_string(&input).unwrap();
    let mut reader = serde_json::Deserializer::from_str(&wire);
    let error = serde_path_to_error::deserialize::<_, Item>(&mut reader).unwrap_err();
    let path = error.path().to_string();
    let error_text = error.inner().to_string();
    let reason = error_text
        .rsplit_once(" at line ")
        .map_or(error_text.as_str(), |(message, _)| message);
    assert_eq!(path, expected_path, "{name}: {error}");
    assert!(reason.contains(expected_reason), "{name}: {error}");
    assert_eq!(
        validator.is_valid(&input),
        schema_exception.is_some(),
        "{name}: schema parity"
    );
    RefusedExample {
        name,
        root: "Item",
        input,
        path,
        reason: reason.to_owned(),
        schema_expressible: schema_exception.is_none(),
        schema_exception,
    }
}

fn output_reads(validator: &jsonschema::Validator) -> Vec<ReadExample> {
    let absent = tool_item(
        ToolAction::NotReported,
        ToolState::Completed {
            output: None,
            exit_code: None,
        },
    );
    let empty = tool_item(
        ToolAction::NotReported,
        ToolState::Completed {
            output: Some(BoundedText {
                text: String::new(),
                cut: false,
            }),
            exit_code: None,
        },
    );
    let mut null = json!(&absent);
    null.pointer_mut("/body/body/call/state")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("output".into(), Value::Null);
    [
        ("detail-omitted", json!(&absent), &absent),
        ("detail-null", null, &absent),
        ("detail-empty", json!(&empty), &empty),
    ]
    .into_iter()
    .map(|(name, input, canonical)| {
        assert!(
            validator.is_valid(&input),
            "{name}: schema refused the input"
        );
        let decoded: Item = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(&decoded, canonical, "{name}");
        ReadExample {
            name,
            input,
            canonical: json!(canonical),
        }
    })
    .collect()
}

fn check_artifact(name: &str, expected: &Value) {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("contracts");
    let path = directory.join(name);
    let expected_bytes = artifact_bytes(expected);
    if std::env::var_os("UPDATE_SESSION_READ_CONTRACT").as_deref()
        == Some(std::ffi::OsStr::new("1"))
    {
        fs::create_dir_all(directory).unwrap();
        fs::write(&path, &expected_bytes).unwrap();
    }

    let existing_result = fs::read(&path);
    assert!(
        existing_result.is_ok(),
        "{name}: missing contract file, regenerate with UPDATE_SESSION_READ_CONTRACT=1"
    );
    let existing_bytes = existing_result.unwrap();
    let actual = serde_json::from_slice::<Value>(&existing_bytes);
    assert!(actual.is_ok(), "{name}: invalid contract JSON");
    assert!(
        &actual.unwrap() == expected,
        "{name}: contract structure drift, regenerate with UPDATE_SESSION_READ_CONTRACT=1"
    );
    assert!(
        existing_bytes == expected_bytes,
        "{name}: contract byte drift, regenerate with UPDATE_SESSION_READ_CONTRACT=1"
    );
}

fn artifact_bytes(value: &Value) -> Vec<u8> {
    let output = if let Some(entries) = value.as_array() {
        let rows = entries
            .iter()
            .map(|entry| serde_json::to_string(entry).unwrap())
            .collect::<Vec<_>>();
        format!("[\n{}\n]\n", rows.join(",\n"))
    } else {
        format!("{}\n", serde_json::to_string_pretty(value).unwrap())
    };
    output.into_bytes()
}
