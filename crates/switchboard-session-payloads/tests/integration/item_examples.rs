use serde_json::json;
use switchboard_session_payloads::{
    AssistantBody, BoundedText, FileChange, FileChangeKind, FileDiff, FileLocation, GroupPlace,
    Item, ItemBody, PlanEntry, PlanEntryPriority, PlanEntryState, RawInput, SessionBody,
    ToolAction, ToolCall, ToolKind, ToolState, UsageAmount, UsageMetric, UsageReading, UsageScope,
    UserBody,
};

use super::{assert_wire, assert_wire_normalized};

fn bounded(text: &str, cut: bool) -> BoundedText {
    BoundedText {
        text: text.into(),
        cut,
    }
}

fn assistant(id: &str, order: u64, group_id: &str, group_order: u64, body: AssistantBody) -> Item {
    Item {
        id: id.parse().unwrap(),
        turn: Some("turn-a".parse().unwrap()),
        order: order.into(),
        body: ItemBody::Assistant {
            group: GroupPlace {
                id: group_id.parse().unwrap(),
                order: group_order.into(),
            },
            body,
        },
    }
}

fn call(kind: ToolKind, action: ToolAction, state: ToolState) -> ToolCall {
    ToolCall {
        source_name: None,
        kind,
        title: None,
        action,
        state,
        locations: vec![],
    }
}

#[test]
fn completed_shell_keeps_command_folder_output_and_success() {
    assert_wire(
        r#"{"sourceName":"exec","kind":{"kind":"execute"},"title":"Show directory","action":{"kind":"shell","command":"pwd","folder":"/work"},"state":{"kind":"completed","output":{"text":"/work","cut":false},"exitCode":0},"locations":[{"path":"src/lib.rs","line":5}]}"#,
        ToolCall {
            source_name: Some("exec".into()),
            title: Some("Show directory".into()),
            locations: vec![FileLocation {
                path: "src/lib.rs".into(),
                line: Some(5),
            }],
            ..call(
                ToolKind::Execute,
                ToolAction::Shell {
                    command: "pwd".into(),
                    folder: Some("/work".into()),
                },
                ToolState::Completed {
                    output: Some(bounded("/work", false)),
                    exit_code: Some(0),
                },
            )
        },
    );
}

#[test]
fn file_changes_keep_reported_text_or_just_the_path() {
    assert_wire(
        r#"{"kind":"fileChange","changes":[{"path":"notes.txt","change":{"kind":"changed"},"diff":{"kind":"texts","old":"before","new":"after"}},{"path":"new.txt","change":{"kind":"added"}}]}"#,
        ToolAction::FileChange {
            changes: vec![
                FileChange {
                    path: "notes.txt".into(),
                    change: FileChangeKind::Changed,
                    diff: Some(FileDiff::Texts {
                        old: Some("before".into()),
                        new: "after".into(),
                    }),
                },
                FileChange {
                    path: "new.txt".into(),
                    change: FileChangeKind::Added,
                    diff: None,
                },
            ],
        },
    );
}

#[test]
fn unrecognized_tool_preserves_any_raw_json_or_a_size_report() {
    assert_wire(
        r#"{"kind":{"kind":"other"},"action":{"kind":"other","input":{"kind":"value","value":{"providerOnly":[null,2]}}},"state":{"kind":"pending"},"locations":[]}"#,
        call(
            ToolKind::Other,
            ToolAction::Other {
                input: RawInput::Value {
                    value: json!({"providerOnly":[null,2]}),
                },
            },
            ToolState::Pending,
        ),
    );
    assert_wire(
        r#"{"kind":"other","input":{"kind":"value","value":null}}"#,
        ToolAction::Other {
            input: RawInput::Value { value: json!(null) },
        },
    );
    assert_wire(
        r#"{"kind":"other","input":{"kind":"value","value":[null,2]}}"#,
        ToolAction::Other {
            input: RawInput::Value {
                value: json!([null, 2]),
            },
        },
    );
    assert_wire(
        r#"{"kind":"other","input":{"kind":"tooLarge","bytes":8192}}"#,
        ToolAction::Other {
            input: RawInput::TooLarge { bytes: 8192 },
        },
    );
}

#[test]
fn missing_action_missing_input_and_empty_input_have_distinct_wires() {
    assert_wire(r#"{"kind":"notReported"}"#, ToolAction::NotReported);
    assert_wire(
        r#"{"kind":"other","input":{"kind":"notReported"}}"#,
        ToolAction::Other {
            input: RawInput::NotReported,
        },
    );
    assert_wire(
        r#"{"kind":"other","input":{"kind":"value","value":{}}}"#,
        ToolAction::Other {
            input: RawInput::Value { value: json!({}) },
        },
    );
}

#[test]
fn tool_states_carry_only_their_reported_fields() {
    assert_wire(r#"{"kind":"pending"}"#, ToolState::Pending);
    assert_wire(
        r#"{"kind":"running","outputSoFar":{"text":"checked","cut":false}}"#,
        ToolState::Running {
            output_so_far: Some(bounded("checked", false)),
        },
    );
    assert_wire(
        r#"{"kind":"failed","output":{"text":"details","cut":false},"exitCode":17,"message":{"text":"failure","cut":false}}"#,
        ToolState::Failed {
            output: Some(bounded("details", false)),
            exit_code: Some(17),
            message: Some(bounded("failure", false)),
        },
    );
    assert_wire(
        r#"{"kind":"declined","message":{"text":"denied","cut":false}}"#,
        ToolState::Declined {
            message: Some(bounded("denied", false)),
        },
    );
    assert_wire(
        r#"{"kind":"completed","output":{"text":"Error: fixture","cut":false}}"#,
        ToolState::Completed {
            output: Some(bounded("Error: fixture", false)),
            exit_code: None,
        },
    );
}

#[test]
fn null_and_absent_output_are_unreported_and_empty_or_cut_output_is_preserved() {
    let absent = ToolState::Completed {
        output: None,
        exit_code: None,
    };
    assert_wire_normalized(
        r#"{"kind":"completed","output":null}"#,
        r#"{"kind":"completed"}"#,
        &absent,
    );
    assert_wire(r#"{"kind":"completed"}"#, absent);
    assert_wire(
        r#"{"kind":"completed","output":{"text":"","cut":false}}"#,
        ToolState::Completed {
            output: Some(bounded("", false)),
            exit_code: None,
        },
    );
    assert_wire(
        r#"{"kind":"completed","output":{"text":"prefix","cut":true}}"#,
        ToolState::Completed {
            output: Some(bounded("prefix", true)),
            exit_code: None,
        },
    );
}

#[test]
fn assistant_items_keep_their_group_and_a_tool_only_group_is_self_contained() {
    assert_wire(
        r#"[{"id":"item-a","turn":"turn-a","order":"1","body":{"kind":"assistant","group":{"id":"group-a","order":"1"},"body":{"kind":"message","text":"hello"}}},{"id":"item-b","turn":"turn-a","order":"3","body":{"kind":"assistant","group":{"id":"group-b","order":"2"},"body":{"kind":"thinking","text":"next"}}}]"#,
        vec![
            assistant(
                "item-a",
                1,
                "group-a",
                1,
                AssistantBody::Message {
                    text: "hello".into(),
                },
            ),
            assistant(
                "item-b",
                3,
                "group-b",
                2,
                AssistantBody::Thinking {
                    text: "next".into(),
                },
            ),
        ],
    );
    assert_wire(
        r#"{"id":"item-c","turn":"turn-a","order":"4","body":{"kind":"assistant","group":{"id":"group-c","order":"4"},"body":{"kind":"toolCall","call":{"kind":{"kind":"read"},"action":{"kind":"notReported"},"state":{"kind":"pending"},"locations":[]}}}}"#,
        assistant(
            "item-c",
            4,
            "group-c",
            4,
            AssistantBody::ToolCall {
                call: call(ToolKind::Read, ToolAction::NotReported, ToolState::Pending),
            },
        ),
    );
}

#[test]
fn user_message_and_session_notice_have_no_group() {
    assert_wire(
        r#"{"id":"item-a","turn":"turn-a","order":"0","body":{"kind":"user","body":{"kind":"message","text":"go"}}}"#,
        Item {
            id: "item-a".parse().unwrap(),
            turn: Some("turn-a".parse().unwrap()),
            order: 0.into(),
            body: ItemBody::User {
                body: UserBody::Message { text: "go".into() },
            },
        },
    );
    assert_wire(
        r#"{"id":"notice-a","order":"1","body":{"kind":"session","body":{"kind":"notice","text":"opened"}}}"#,
        Item {
            id: "notice-a".parse().unwrap(),
            turn: None,
            order: 1.into(),
            body: ItemBody::Session {
                body: SessionBody::Notice {
                    text: "opened".into(),
                },
            },
        },
    );
    assert_wire(
        r#"{"id":"setting-a","order":"2","body":{"kind":"session","body":{"kind":"settingChange","text":"quiet"}}}"#,
        Item {
            id: "setting-a".parse().unwrap(),
            turn: None,
            order: 2.into(),
            body: ItemBody::Session {
                body: SessionBody::SettingChange {
                    text: "quiet".into(),
                },
            },
        },
    );
}

#[test]
fn explicit_other_items_keep_identity_order_turn_and_group() {
    assert_wire(
        r#"{"id":"item-a","turn":"turn-a","order":"5","body":{"kind":"assistant","group":{"id":"group-a","order":"1"},"body":{"kind":"other","sourceKind":"providerEvent","text":"reported"}}}"#,
        assistant(
            "item-a",
            5,
            "group-a",
            1,
            AssistantBody::Other {
                source_kind: "providerEvent".into(),
                text: Some("reported".into()),
            },
        ),
    );
    assert_wire(
        r#"{"id":"item-b","turn":"turn-a","order":"6","body":{"kind":"session","body":{"kind":"other","sourceKind":"providerNotice"}}}"#,
        Item {
            id: "item-b".parse().unwrap(),
            turn: Some("turn-a".parse().unwrap()),
            order: 6.into(),
            body: ItemBody::Session {
                body: SessionBody::Other {
                    source_kind: "providerNotice".into(),
                    text: None,
                },
            },
        },
    );
}

#[test]
fn plan_keeps_entry_order_and_usage_keeps_reported_count_or_money() {
    assert_wire(
        r#"{"kind":"plan","entries":[{"text":"read","state":{"kind":"completed"},"priority":{"kind":"high"}},{"text":"edit","state":{"kind":"inProgress"},"priority":{"kind":"medium"}},{"text":"check","state":{"kind":"pending"},"priority":{"kind":"low"}}]}"#,
        AssistantBody::Plan {
            entries: vec![
                PlanEntry {
                    text: "read".into(),
                    state: PlanEntryState::Completed,
                    priority: Some(PlanEntryPriority::High),
                },
                PlanEntry {
                    text: "edit".into(),
                    state: PlanEntryState::InProgress,
                    priority: Some(PlanEntryPriority::Medium),
                },
                PlanEntry {
                    text: "check".into(),
                    state: PlanEntryState::Pending,
                    priority: Some(PlanEntryPriority::Low),
                },
            ],
        },
    );
    assert_wire(
        r#"{"text":"check","state":{"kind":"pending"}}"#,
        PlanEntry {
            text: "check".into(),
            state: PlanEntryState::Pending,
            priority: None,
        },
    );
    assert_wire(
        r#"{"kind":"usage","reading":{"metric":{"kind":"contextUsed"},"scope":{"kind":"turn"},"amount":{"kind":"count","value":"9007199254740993","capacity":"18446744073709551615"}}}"#,
        SessionBody::Usage {
            reading: UsageReading {
                metric: UsageMetric::ContextUsed,
                scope: UsageScope::Turn,
                amount: UsageAmount::Count {
                    value: 9_007_199_254_740_993,
                    capacity: Some(u64::MAX),
                },
            },
        },
    );
    assert_wire(
        r#"{"kind":"usage","reading":{"metric":{"kind":"cost"},"scope":{"kind":"session"},"amount":{"kind":"money","amount":"0.0400","currency":"USD"}}}"#,
        SessionBody::Usage {
            reading: UsageReading {
                metric: UsageMetric::Cost,
                scope: UsageScope::Session,
                amount: UsageAmount::Money {
                    amount: "0.0400".into(),
                    currency: "USD".into(),
                },
            },
        },
    );
}

#[test]
fn other_tool_actions_keep_completed_and_failed_output() {
    assert_wire(
        r#"{"kind":{"kind":"other"},"action":{"kind":"other","input":{"kind":"notReported"}},"state":{"kind":"completed","output":{"text":"result","cut":false}},"locations":[]}"#,
        call(
            ToolKind::Other,
            ToolAction::Other {
                input: RawInput::NotReported,
            },
            ToolState::Completed {
                output: Some(bounded("result", false)),
                exit_code: None,
            },
        ),
    );
    assert_wire(
        r#"{"kind":{"kind":"other"},"action":{"kind":"other","input":{"kind":"notReported"}},"state":{"kind":"failed","output":{"text":"details","cut":false}},"locations":[]}"#,
        call(
            ToolKind::Other,
            ToolAction::Other {
                input: RawInput::NotReported,
            },
            ToolState::Failed {
                output: Some(bounded("details", false)),
                exit_code: None,
                message: None,
            },
        ),
    );
}
