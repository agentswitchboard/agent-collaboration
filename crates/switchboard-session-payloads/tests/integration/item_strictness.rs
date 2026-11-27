use std::fmt::Debug;

use serde::de::DeserializeOwned;
use switchboard_session_payloads::{
    AssistantBody, BoundedText, FileChange, FileDiff, FileLocation, GroupPlace, Item, LineRange,
    LineRangeError, PlanEntry, RawInput, ToolAction, ToolCall, ToolState, UsageAmount,
    UsageReading,
};

use super::assert_refused;

const VALID_CALL: &str = r#"{"kind":{"kind":"read"},"action":{"kind":"notReported"},"state":{"kind":"pending"},"locations":[]}"#;
const TOOL_ITEM: &str = r#"{"id":"fixture","order":"0","body":{"kind":"assistant","group":{"id":"group","order":"0"},"body":{"kind":"toolCall","call":{"kind":{"kind":"read"},"action":{"kind":"notReported"},"state":{"kind":"pending"},"locations":[]}}}}"#;

#[test]
fn user_and_session_reject_tool_calls_and_thinking() {
    for (role, valid_body) in [
        ("user", r#"{"kind":"message","text":"ok"}"#),
        ("session", r#"{"kind":"notice","text":"ok"}"#),
    ] {
        let wire = format!(
            r#"{{"id":"fixture","order":"0","body":{{"kind":"{role}","body":{valid_body}}}}}"#
        );
        serde_json::from_str::<Item>(&wire).unwrap();

        let tool_call = format!(r#"{{"kind":"toolCall","call":{VALID_CALL}}}"#);
        serde_json::from_str::<AssistantBody>(&tool_call).unwrap();
        assert_refused::<Item>(
            &wire.replace(valid_body, &tool_call),
            "body",
            "unknown variant `toolCall`",
        );

        let thinking = r#"{"kind":"thinking","text":"ok"}"#;
        serde_json::from_str::<AssistantBody>(thinking).unwrap();
        assert_refused::<Item>(
            &wire.replace(valid_body, thinking),
            "body",
            "unknown variant `thinking`",
        );
    }
}

#[test]
fn assistant_group_is_required() {
    serde_json::from_str::<Item>(TOOL_ITEM).unwrap();
    assert_refused::<Item>(
        &TOOL_ITEM.replace(r#""group":{"id":"group","order":"0"},"#, ""),
        "body",
        "missing field `group`",
    );
}

#[test]
fn extra_fields_are_refused_at_all_item_and_tool_levels() {
    serde_json::from_str::<Item>(TOOL_ITEM).unwrap();
    for (anchor, field, path) in [
        (r#""id":"fixture""#, "itemExtra", "itemExtra"),
        (r#""kind":"assistant""#, "bodyExtra", "body"),
        (r#""kind":"toolCall""#, "assistantExtra", "body"),
        (r#""id":"group""#, "groupExtra", "body"),
        (r#""locations":[]"#, "callExtra", "body"),
        (r#""kind":"notReported""#, "actionExtra", "body"),
        (r#""kind":"pending""#, "stateExtra", "body"),
    ] {
        let wire = TOOL_ITEM.replace(anchor, &format!(r#"{anchor},"{field}":null"#));
        assert_refused::<Item>(&wire, path, &format!("unknown field `{field}`"));
    }
}

#[test]
fn state_rejects_fields_owned_by_other_variants() {
    for (valid, extra, value) in [
        (
            r#"{"kind":"completed"}"#,
            "message",
            r#"{"text":"failure","cut":false}"#,
        ),
        (r#"{"kind":"declined"}"#, "exitCode", "7"),
        (
            r#"{"kind":"declined"}"#,
            "output",
            r#"{"text":"output","cut":false}"#,
        ),
    ] {
        serde_json::from_str::<ToolState>(valid).unwrap();
        let without_end = valid.strip_suffix('}').unwrap();
        let wire = format!(r#"{without_end},"{extra}":{value}}}"#);
        assert_refused::<ToolState>(&wire, ".", &format!("unknown field `{extra}`"));
    }
}

#[test]
fn too_large_raw_input_refuses_a_value() {
    let valid = r#"{"kind":{"kind":"other"},"action":{"kind":"other","input":{"kind":"tooLarge","bytes":"8192"}},"state":{"kind":"pending"},"locations":[]}"#;
    serde_json::from_str::<ToolCall>(valid).unwrap();
    assert_refused::<ToolCall>(
        &valid.replace(r#""bytes":"8192""#, r#""bytes":"8192","value":null"#),
        "action",
        "unknown field `value`",
    );
}

#[test]
fn line_range_refuses_zero_start_and_zero_count() {
    let positive = r#"{"kind":{"kind":"read"},"action":{"kind":"fileRead","path":"notes","lines":{"firstLine":1,"lineCount":2}},"state":{"kind":"pending"},"locations":[]}"#;
    serde_json::from_str::<ToolCall>(positive).unwrap();
    assert_eq!(LineRange::new(0, 2), Err(LineRangeError::FirstLineZero));
    assert_eq!(LineRange::new(1, 0), Err(LineRangeError::LineCountZero));
    assert_refused::<ToolCall>(
        &positive.replace(r#""firstLine":1"#, r#""firstLine":0"#),
        "action",
        &LineRangeError::FirstLineZero.to_string(),
    );
    assert_refused::<ToolCall>(
        &positive.replace(r#""lineCount":2"#, r#""lineCount":0"#),
        "action",
        &LineRangeError::LineCountZero.to_string(),
    );
}

fn assert_strict_object<T: DeserializeOwned + Debug>(valid: &str, positional: &str) {
    assert_strict_shape::<T>(valid, positional, "extraField");
}

fn assert_strict_enum<T: DeserializeOwned + Debug>(valid: &str, positional: &str) {
    assert_strict_shape::<T>(valid, positional, ".");
}

fn assert_strict_shape<T: DeserializeOwned + Debug>(
    valid: &str,
    positional: &str,
    extra_path: &str,
) {
    serde_json::from_str::<T>(valid).unwrap();
    let without_end = valid.strip_suffix('}').unwrap();
    let extra = format!(r#"{without_end},"extraField":null}}"#);
    assert_refused::<T>(&extra, extra_path, "unknown field `extraField`");
    assert_refused::<T>(positional, ".", "expected an object map");
}

#[test]
fn nested_objects_reject_extra_keys_and_positional_arrays() {
    assert_strict_object::<BoundedText>(r#"{"text":"result","cut":false}"#, r#"["result",false]"#);
    assert_strict_object::<FileLocation>(r#"{"path":"notes","line":5}"#, r#"["notes",5]"#);
    assert_strict_object::<LineRange>(r#"{"firstLine":4,"lineCount":2}"#, "[4,2]");
    assert_strict_object::<FileChange>(
        r#"{"path":"notes","change":{"kind":"changed"}}"#,
        r#"["notes",{"kind":"changed"},null]"#,
    );
    assert_strict_object::<PlanEntry>(
        r#"{"text":"check","state":{"kind":"pending"}}"#,
        r#"["check",{"kind":"pending"},null]"#,
    );
    assert_strict_object::<UsageReading>(
        r#"{"metric":{"kind":"inputTokens"},"scope":{"kind":"turn"},"amount":{"kind":"count","value":"1"}}"#,
        r#"[{"kind":"inputTokens"},{"kind":"turn"},{"kind":"count","value":"1"}]"#,
    );
    assert_strict_object::<GroupPlace>(r#"{"id":"group","order":"0"}"#, r#"["group","0"]"#);
    assert_strict_enum::<RawInput>(r#"{"kind":"value","value":null}"#, r#"["value",null]"#);
    assert_strict_enum::<FileDiff>(
        r#"{"kind":"unifiedPatch","patch":"diff"}"#,
        r#"["unifiedPatch","diff"]"#,
    );
    assert_strict_enum::<UsageAmount>(r#"{"kind":"count","value":"1"}"#, r#"["count","1",null]"#);
    assert_strict_enum::<ToolState>(r#"{"kind":"pending"}"#, r#"["pending"]"#);
    assert_strict_object::<Item>(
        TOOL_ITEM,
        r#"["fixture",null,"0",{"kind":"assistant","group":{"id":"group","order":"0"},"body":{"kind":"toolCall","call":{"kind":{"kind":"read"},"action":{"kind":"notReported"},"state":{"kind":"pending"},"locations":[]}}}]"#,
    );
}

#[test]
fn sub_agent_child_refuses_extra_keys_and_positional_arrays() {
    let pair =
        r#"{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000001","sessionId":"native/thread"}"#;
    let valid = format!(r#"{{"kind":"subAgent","child":{pair}}}"#);
    serde_json::from_str::<ToolAction>(&valid).unwrap();
    let extra = r#"{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000001","sessionId":"native/thread","endpoint":"extra"}"#;
    assert_refused::<ToolAction>(&valid.replace(pair, extra), ".", "unknown field `endpoint`");
    let positional = r#"["01890f2e-7b4c-7cc0-98c4-000000000001","native/thread"]"#;
    assert_refused::<ToolAction>(
        &valid.replace(pair, positional),
        ".",
        "expected a named session address",
    );
}
