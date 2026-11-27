use serde_json::json;
use switchboard_session_payloads::{
    AssistantBody, BoundedText, FileChange, FileChangeKind, FileDiff, FileLocation, GroupPlace,
    Item, ItemBody, PlanEntry, PlanEntryPriority, PlanEntryState, RawInput, SessionBody,
    ToolAction, ToolCall, ToolKind, ToolState, UsageAmount, UsageMetric, UsageReading, UsageScope,
    UserBody,
};

use super::assert_wire_normalized;
use super::contract_item_fixtures::{fixture_time, item_scenarios};

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
        first_seen_at_ms: fixture_time(),
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

item_scenarios! {
    fn completed_shell_keeps_command_folder_output_and_success(examples) {
        examples.check("shell-completed",
            r#"{"sourceName":"exec","kind":"execute","title":"Show directory","action":{"kind":"shell","command":"pwd","folder":"/work"},"state":{"kind":"completed","output":{"text":"/work","cut":false},"exitCode":0},"locations":[{"path":"src/lib.rs","line":5}]}"#,
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

    fn file_changes_keep_reported_text_or_just_the_path(examples) {
        examples.check("file-change-old-and-new-and-path-only",
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

    fn unrecognized_tool_preserves_any_raw_json_or_a_size_report(examples) {
        examples.check("raw-opaque-object",
            r#"{"kind":"other","action":{"kind":"other","input":{"kind":"value","value":{"providerOnly":[null,2]}}},"state":{"kind":"pending"},"locations":[]}"#,
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
        examples.check(
            "raw-null",
            r#"{"kind":"other","input":{"kind":"value","value":null}}"#,
            ToolAction::Other {
                input: RawInput::Value { value: json!(null) },
            },
        );
        examples.check(
            "raw-array",
            r#"{"kind":"other","input":{"kind":"value","value":[null,2]}}"#,
            ToolAction::Other {
                input: RawInput::Value {
                    value: json!([null, 2]),
                },
            },
        );
        examples.check(
            "raw-too-large",
            r#"{"kind":"other","input":{"kind":"tooLarge","bytes":"8192"}}"#,
            ToolAction::Other {
                input: RawInput::TooLarge { bytes: 8192.into() },
            },
        );
    }

    fn missing_action_missing_input_and_empty_input_have_distinct_wires(examples) {
        examples.check(
            "action-unreported",
            r#"{"kind":"notReported"}"#,
            ToolAction::NotReported,
        );
        examples.check(
            "raw-input-unreported",
            r#"{"kind":"other","input":{"kind":"notReported"}}"#,
            ToolAction::Other {
                input: RawInput::NotReported,
            },
        );
        examples.check(
            "raw-empty-object",
            r#"{"kind":"other","input":{"kind":"value","value":{}}}"#,
            ToolAction::Other {
                input: RawInput::Value { value: json!({}) },
            },
        );
    }

    fn tool_states_carry_only_their_reported_fields(examples) {
        examples.check("tool-pending", r#"{"kind":"pending"}"#, ToolState::Pending);
        examples.check(
            "tool-running",
            r#"{"kind":"running","outputSoFar":{"text":"checked","cut":false}}"#,
            ToolState::Running {
                output_so_far: Some(bounded("checked", false)),
            },
        );
        examples.check("tool-failed",
            r#"{"kind":"failed","output":{"text":"details","cut":false},"exitCode":17,"message":{"text":"failure","cut":false}}"#,
            ToolState::Failed {
                output: Some(bounded("details", false)),
                exit_code: Some(17),
                message: Some(bounded("failure", false)),
            },
        );
        examples.check(
            "tool-declined",
            r#"{"kind":"declined","message":{"text":"denied","cut":false}}"#,
            ToolState::Declined {
                message: Some(bounded("denied", false)),
            },
        );
        examples.check(
            "completed-error-text",
            r#"{"kind":"completed","output":{"text":"Error: fixture","cut":false}}"#,
            ToolState::Completed {
                output: Some(bounded("Error: fixture", false)),
                exit_code: None,
            },
        );
    }

    fn null_and_absent_output_are_unreported_and_empty_or_cut_output_is_preserved(examples) {
        let absent = ToolState::Completed {
            output: None,
            exit_code: None,
        };
        assert_wire_normalized(
            r#"{"kind":"completed","output":null}"#,
            r#"{"kind":"completed"}"#,
            &absent,
        );
        examples.check("output-absent", r#"{"kind":"completed"}"#, absent);
        examples.check(
            "output-empty",
            r#"{"kind":"completed","output":{"text":"","cut":false}}"#,
            ToolState::Completed {
                output: Some(bounded("", false)),
                exit_code: None,
            },
        );
        examples.check(
            "output-cut",
            r#"{"kind":"completed","output":{"text":"prefix","cut":true}}"#,
            ToolState::Completed {
                output: Some(bounded("prefix", true)),
                exit_code: None,
            },
        );
    }

    fn assistant_items_keep_their_group_and_a_tool_only_group_is_self_contained(examples) {
        examples.check("two-assistant-groups",
            r#"[{"id":"item-a","turn":"turn-a","order":"1","firstSeenAtMs":1700000000000,"body":{"kind":"assistant","group":{"id":"group-a","order":"1"},"body":{"kind":"message","text":"hello"}}},{"id":"item-b","turn":"turn-a","order":"3","firstSeenAtMs":1700000000000,"body":{"kind":"assistant","group":{"id":"group-b","order":"2"},"body":{"kind":"thinking","text":"next"}}}]"#,
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
        examples.check("tool-only-group",
            r#"{"id":"item-c","turn":"turn-a","order":"4","firstSeenAtMs":1700000000000,"body":{"kind":"assistant","group":{"id":"group-c","order":"4"},"body":{"kind":"toolCall","call":{"kind":"read","action":{"kind":"notReported"},"state":{"kind":"pending"},"locations":[]}}}}"#,
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

    fn user_message_and_session_notice_have_no_group(examples) {
        examples.check("user-message",
            r#"{"id":"item-a","turn":"turn-a","order":"0","firstSeenAtMs":1700000000000,"body":{"kind":"user","body":{"kind":"message","text":"go"}}}"#,
            Item {
                id: "item-a".parse().unwrap(),
                turn: Some("turn-a".parse().unwrap()),
                order: 0.into(),
                first_seen_at_ms: fixture_time(),
                body: ItemBody::User {
                    body: UserBody::Message { text: "go".into() },
                },
            },
        );
        examples.check("session-notice",
            r#"{"id":"notice-a","order":"1","firstSeenAtMs":1700000000000,"body":{"kind":"session","body":{"kind":"notice","text":"opened"}}}"#,
            Item {
                id: "notice-a".parse().unwrap(),
                turn: None,
                order: 1.into(),
                first_seen_at_ms: fixture_time(),
                body: ItemBody::Session {
                    body: SessionBody::Notice {
                        text: "opened".into(),
                    },
                },
            },
        );
        examples.check("session-setting-change",
            r#"{"id":"setting-a","order":"2","firstSeenAtMs":1700000000000,"body":{"kind":"session","body":{"kind":"settingChange","text":"quiet"}}}"#,
            Item {
                id: "setting-a".parse().unwrap(),
                turn: None,
                order: 2.into(),
                first_seen_at_ms: fixture_time(),
                body: ItemBody::Session {
                    body: SessionBody::SettingChange {
                        text: "quiet".into(),
                    },
                },
            },
        );
    }

    fn explicit_other_items_keep_identity_order_turn_and_group(examples) {
        examples.check("other-assistant",
            r#"{"id":"item-a","turn":"turn-a","order":"5","firstSeenAtMs":1700000000000,"body":{"kind":"assistant","group":{"id":"group-a","order":"1"},"body":{"kind":"other","sourceKind":"providerEvent","text":"reported"}}}"#,
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
        examples.check("other-session",
            r#"{"id":"item-b","turn":"turn-a","order":"6","firstSeenAtMs":1700000000000,"body":{"kind":"session","body":{"kind":"other","sourceKind":"providerNotice"}}}"#,
            Item {
                id: "item-b".parse().unwrap(),
                turn: Some("turn-a".parse().unwrap()),
                order: 6.into(),
                first_seen_at_ms: fixture_time(),
                body: ItemBody::Session {
                    body: SessionBody::Other {
                        source_kind: "providerNotice".into(),
                        text: None,
                    },
                },
            },
        );
    }

    fn plan_keeps_entry_order_and_usage_keeps_reported_count_or_money(examples) {
        examples.check("ordered-plan",
            r#"{"kind":"plan","entries":[{"text":"read","state":"completed","priority":"high"},{"text":"edit","state":"inProgress","priority":"medium"},{"text":"check","state":"pending","priority":"low"}]}"#,
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
        examples.check(
            "plan-priority-omitted",
            r#"{"text":"check","state":"pending"}"#,
            PlanEntry {
                text: "check".into(),
                state: PlanEntryState::Pending,
                priority: None,
            },
        );
        examples.check("usage-count",
            r#"{"kind":"usage","reading":{"metric":{"kind":"contextUsed"},"scope":"turn","amount":{"kind":"count","value":"9007199254740993","capacity":"18446744073709551615"}}}"#,
            SessionBody::Usage {
                reading: UsageReading {
                    metric: UsageMetric::ContextUsed,
                    scope: UsageScope::Turn,
                    amount: UsageAmount::Count {
                        value: 9_007_199_254_740_993.into(),
                        capacity: Some(u64::MAX.into()),
                    },
                },
            },
        );
        examples.check("usage-money",
            r#"{"kind":"usage","reading":{"metric":{"kind":"cost"},"scope":"session","amount":{"kind":"money","amount":"0.0400","currency":"USD"}}}"#,
            SessionBody::Usage {
                reading: UsageReading {
                    metric: UsageMetric::Cost,
                    scope: UsageScope::Session,
                    amount: UsageAmount::Money {
                        amount: "0.0400".parse().unwrap(),
                        currency: "USD".parse().unwrap(),
                    },
                },
            },
        );
    }

    fn other_tool_actions_keep_completed_and_failed_output(examples) {
        examples.check("other-tool-completed",
            r#"{"kind":"other","action":{"kind":"other","input":{"kind":"notReported"}},"state":{"kind":"completed","output":{"text":"result","cut":false}},"locations":[]}"#,
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
        examples.check("other-tool-failed",
            r#"{"kind":"other","action":{"kind":"other","input":{"kind":"notReported"}},"state":{"kind":"failed","output":{"text":"details","cut":false}},"locations":[]}"#,
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
}
