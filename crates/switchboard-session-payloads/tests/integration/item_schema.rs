use std::fmt::Debug;

use schemars::{JsonSchema, generate::SchemaSettings};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use switchboard_session_payloads::{
    AssistantBody, FileChangeKind, FileDiff, GroupPlace, Item, ItemBody, ItemOrder,
    PlanEntryPriority, PlanEntryState, RawInput, SearchOver, SessionBody, ToolAction, ToolKind,
    ToolState, UsageAmount, UsageMetric, UsageScope, UserBody,
};

use super::assert_refused;

fn schema<T: JsonSchema>() -> Value {
    let schema = SchemaSettings::draft2020_12()
        .with(|settings| settings.inline_subschemas = true)
        .into_generator()
        .into_root_schema_for::<T>();
    serde_json::to_value(schema).unwrap()
}

#[test]
fn schema_uses_strings_for_exact_positions_counts_and_money() {
    assert_eq!(schema::<ItemOrder>()["type"], "string");
    for item_shape in [schema::<Item>(), schema::<GroupPlace>()] {
        assert_eq!(item_shape["properties"]["order"]["type"], "string");
    }

    let usage = schema::<UsageAmount>();
    let variants = branches(&usage);
    let count = variant(variants, "count");
    assert_eq!(count["properties"]["value"]["type"], "string");
    assert_eq!(
        count["properties"]["capacity"]["type"],
        json!(["string", "null"])
    );
    assert_eq!(count["required"], json!(["kind", "value"]));
    let money = variant(variants, "money");
    assert_eq!(money["properties"]["amount"]["type"], "string");
    assert_eq!(money["properties"]["currency"]["type"], "string");

    let raw = schema::<RawInput>();
    let too_large = variant(branches(&raw), "tooLarge");
    assert_eq!(too_large["properties"]["bytes"]["type"], "string");
}

#[test]
fn schema_advertises_only_objects_tagged_kind_and_readers_refuse_unknown_kinds() {
    assert_tagged::<ItemBody>(&["user", "assistant", "session"]);
    assert_tagged::<UserBody>(&["message"]);
    assert_tagged::<AssistantBody>(&["message", "thinking", "toolCall", "plan", "other"]);
    assert_tagged::<SessionBody>(&["notice", "settingChange", "usage", "other"]);
    assert_tagged::<ToolAction>(&[
        "notReported",
        "shell",
        "fileChange",
        "fileRead",
        "search",
        "fetch",
        "subAgent",
        "other",
    ]);
    assert_tagged::<FileChangeKind>(&["added", "changed", "removed", "moved"]);
    assert_tagged::<FileDiff>(&["texts", "unifiedPatch"]);
    assert_tagged::<RawInput>(&["notReported", "value", "tooLarge"]);
    assert_tagged::<ToolState>(&["pending", "running", "completed", "failed", "declined"]);
    assert_tagged::<UsageMetric>(&[
        "contextUsed",
        "inputTokens",
        "outputTokens",
        "reasoningTokens",
        "cacheReadTokens",
        "cacheWriteTokens",
        "cost",
        "other",
    ]);
    assert_tagged::<UsageAmount>(&["count", "money"]);
}

#[test]
fn schema_advertises_exact_string_choices_for_fieldless_enums() {
    assert_string_choices::<ToolKind>(&[
        "read", "edit", "delete", "move", "search", "execute", "think", "fetch", "other",
    ]);
    assert_string_choices::<SearchOver>(&["fileNames", "fileContents", "web"]);
    assert_string_choices::<PlanEntryState>(&["pending", "inProgress", "completed"]);
    assert_string_choices::<PlanEntryPriority>(&["high", "medium", "low"]);
    assert_string_choices::<UsageScope>(&["turn", "session"]);
}

fn assert_string_choices<T: JsonSchema>(choices: &[&str]) {
    let shape = schema::<T>();
    assert_eq!(
        shape.get("type").and_then(Value::as_str),
        Some("string"),
        "{shape}"
    );
    assert_eq!(shape.get("enum"), Some(&json!(choices)), "{shape}");
}

fn branches(schema: &Value) -> &[Value] {
    schema["oneOf"]
        .as_array()
        .or_else(|| schema["anyOf"].as_array())
        .map_or_else(|| std::slice::from_ref(schema), Vec::as_slice)
}

fn variant<'shape>(branches: &'shape [Value], tag: &str) -> &'shape Value {
    branches
        .iter()
        .find(|variant| {
            variant
                .pointer("/properties/kind/const")
                .and_then(Value::as_str)
                == Some(tag)
        })
        .unwrap()
}

fn assert_tagged<T: JsonSchema + DeserializeOwned + Debug>(expected_tags: &[&str]) {
    let schema = schema::<T>();
    let variants = branches(&schema);
    assert_eq!(variants.len(), expected_tags.len(), "{schema}");
    for tag in expected_tags {
        let shape = variant(variants, tag);
        assert_eq!(shape["type"], "object", "{shape}");
        assert_eq!(
            shape
                .pointer("/properties/kind/type")
                .and_then(Value::as_str),
            Some("string"),
            "{shape}"
        );
        assert_eq!(shape["additionalProperties"], false, "{shape}");
        assert!(
            shape["required"]
                .as_array()
                .unwrap()
                .contains(&json!("kind")),
            "{shape}"
        );
    }
    assert_refused::<T>(
        r#"{"kind":"fixtureFutureKind"}"#,
        "kind",
        "unknown variant `fixtureFutureKind`",
    );
}
