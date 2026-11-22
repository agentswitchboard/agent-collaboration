//! Builds the provider-only export from the wire types and checks its public shapes.
use collaboration_protocol::{
    ObservationCursor, ObservationEventNotification, ProviderObservationEventTooLarge,
    ProviderObservationEventTooLargeKind, ProviderSnapshotMarker,
};
use schemars::generate::SchemaSettings;
use serde_json::{Map, Value, json};
use session_event_model::{SessionEvent, SessionItem, SessionItemKind, TurnOutcome};

const SCHEMA_FILE: &str = "src/mcp_server/snapshots/provider_observation.schema.json";
const PUBLISHED: &str = include_str!("snapshots/provider_observation.schema.json");

fn reference(name: &str) -> Value {
    json!({"$ref":format!("#/$defs/{name}")})
}

fn closed(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}

fn generated_schema() -> Value {
    let mut generator = SchemaSettings::draft2020_12()
        .for_serialize()
        .into_generator();
    let event: Value = generator.subschema_for::<SessionEvent>().into();
    let cursor: Value = generator.subschema_for::<ObservationCursor>().into();
    let snapshot_end: Value = generator.subschema_for::<ProviderSnapshotMarker>().into();
    let too_large: Value = generator
        .subschema_for::<ProviderObservationEventTooLarge>()
        .into();
    let mut definitions = generator.take_definitions(true);
    let sequence = reference("ObservationSequence");
    let event_record = closed(
        json!({"sequence":sequence,"event":event}),
        &["sequence", "event"],
    );
    let bare_gap = closed(json!({"kind":{"const":"resyncRequired"}}), &["kind"]);
    let record = json!({"oneOf":[event_record,snapshot_end,bare_gap,too_large]});
    // A reset is a numbered event on the old stream, but it cannot be resumed.
    let reset_event = json!({
        "required":["event"],"properties":{
            "event":{"required":["kind"],"properties":{"kind":{"const":"resyncRequired"}}}
        }
    });
    let mut notification = closed(
        json!({"event":reference("ProviderObservationRecord"),"cursor":{}}),
        &["event", "cursor"],
    );
    notification["if"] = json!({"properties":{"event":{"anyOf":[bare_gap,reset_event]}}});
    notification["then"] = json!({"properties":{"cursor":{"type":"null"}}});
    notification["else"] = json!({"properties":{"cursor":cursor}});
    definitions.insert(
        "ObservationSequence".into(),
        json!({"type":"integer","format":"uint64","minimum":0,"maximum":u64::MAX}),
    );
    definitions.insert("ProviderObservationRecord".into(), record);
    definitions.insert("ProviderObservationNotification".into(), notification);

    // Schemars adds the lower bound for u64. Publish the upper bound for exact decoders too.
    fn bound_positions(schema: &mut Value) {
        match schema {
            Value::Object(object) => {
                if object.get("format").and_then(Value::as_str) == Some("uint64") {
                    object.insert("maximum".into(), json!(u64::MAX));
                }
                for value in object.values_mut() {
                    bound_positions(value);
                }
            }
            Value::Array(children) => {
                for child in children {
                    bound_positions(child);
                }
            }
            _ => {}
        }
    }
    let mut schema = json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "$ref":"#/$defs/ProviderObservationRecord",
        "title":"Codex Router event-hub observation",
        "description":"Record root, with ProviderObservationNotification and SessionEvent available as definitions.",
        "x-router-version":env!("CARGO_PKG_VERSION"),
        "$defs":definitions,
    });
    bound_positions(&mut schema);
    schema
}

fn published_schema() -> Value {
    serde_json::from_str(PUBLISHED).expect("provider observation schema")
}

fn validate_definition(mut schema: Value, name: &str) -> jsonschema::Validator {
    schema["$ref"] = reference(name)["$ref"].clone();
    jsonschema::validator_for(&schema).expect("draft 2020-12 validator")
}

// Rejects checking in an export that has drifted from the Rust wire types.
#[test]
fn published_schema_matches_the_rust_types() {
    let generated = generated_schema();
    if std::env::var_os("BLESS_PROVIDER_OBSERVATION_SCHEMA").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(SCHEMA_FILE);
        let mut json = serde_json::to_string(&generated).expect("encoded schema");
        // Keep each definition on its own line, like the tool success schema snapshot.
        json = json.replace(",\"$defs\":{", ",\n\"$defs\":{\n");
        for name in generated["$defs"].as_object().expect("definitions").keys() {
            let definition = format!(",\"{name}\":");
            json = json.replace(&definition, &format!(",\n\"{name}\":"));
        }
        std::fs::write(path, format!("{json}\n")).expect("write provider observation export");
        return;
    }
    assert_eq!(generated, published_schema());
}

// Rejects schemas that silently add or drop a record branch, or infer an open record shape.
#[test]
fn published_record_branches_match_the_four_contract_kinds() {
    let schema = published_schema();
    let defs = schema["$defs"].as_object().expect("definitions");
    let branches = defs["ProviderObservationRecord"]["oneOf"]
        .as_array()
        .expect("record branches");
    let shapes = branches
        .iter()
        .map(|branch| {
            let resolved = dereference(defs, branch);
            let resolved = resolved.get("oneOf").map_or(resolved, |variants| {
                let [variant] = variants.as_array().expect("tagged marker").as_slice() else {
                    panic!("expected the one snapshot branch");
                };
                variant
            });
            match resolved["properties"]["kind"].get("const") {
                Some(kind) => kind.as_str().expect("kind").to_owned(),
                None if resolved["properties"].get("event").is_some() => "event".to_owned(),
                None => {
                    let kind = dereference(defs, &resolved["properties"]["kind"]);
                    kind["enum"]
                        .as_array()
                        .and_then(|values| values.first())
                        .or_else(|| kind.get("const"))
                        .and_then(Value::as_str)
                        .expect("record discriminant")
                        .to_owned()
                }
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        shapes,
        ["event", "snapshotEnd", "resyncRequired", "eventTooLarge"]
    );
}

fn dereference<'a>(defs: &'a Map<String, Value>, schema: &'a Value) -> &'a Value {
    schema
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| reference.strip_prefix("#/$defs/"))
        .map_or(schema, |name| &defs[name])
}

// Rejects schemas that accept malformed variants or lose actual Rust event, marker and gap forms.
#[test]
fn real_observation_records_validate_with_exact_wide_positions() {
    let schema = published_schema();
    let records = validate_definition(schema.clone(), "ProviderObservationRecord");
    let notifications = validate_definition(schema, "ProviderObservationNotification");
    let exact = u64::MAX - 1;
    let cursor = ObservationCursor {
        epoch: u64::MAX,
        after_sequence: exact,
    };
    let mut examples = event_examples()
        .into_iter()
        .map(|event| {
            let record = json!({"sequence":exact,"event":event});
            if matches!(event, SessionEvent::ResyncRequired { .. }) {
                ObservationEventNotification::resync_marker(record)
            } else {
                ObservationEventNotification {
                    event: record,
                    cursor: Some(cursor),
                }
            }
        })
        .collect::<Vec<_>>();
    examples.push(ObservationEventNotification::resync_marker(
        json!({"kind":"resyncRequired"}),
    ));
    examples.push(ObservationEventNotification {
        event: serde_json::to_value(ProviderSnapshotMarker::SnapshotEnd { cursor }).unwrap(),
        cursor: Some(cursor),
    });
    for item_id in [None, Some("oversized".to_owned())] {
        let event = serde_json::to_value(ProviderObservationEventTooLarge {
            kind: ProviderObservationEventTooLargeKind::EventTooLarge,
            sequence: exact,
            item_id,
        })
        .unwrap();
        examples.push(ObservationEventNotification {
            event,
            cursor: Some(cursor),
        });
    }
    if let Some(output) = std::env::var_os("PROVIDER_OBSERVATION_EXAMPLES") {
        let wire = serde_json::to_vec(&examples).expect("wire examples");
        std::fs::write(output, wire).expect("write wire examples for the external decoder check");
    }
    for notification in examples {
        let record = &notification.event;
        assert!(
            records.is_valid(record),
            "{record}: {:?}",
            records.validate(record)
        );
        let wire = serde_json::to_string(&notification).unwrap();
        let restored: ObservationEventNotification = serde_json::from_str(&wire).unwrap();
        assert_eq!(restored, notification);
        let wire_value = serde_json::from_str(&wire).unwrap();
        assert!(
            notifications.is_valid(&wire_value),
            "{wire}: {:?}",
            notifications.validate(&wire_value)
        );
    }
    for invalid in [
        json!({"kind":"unknown"}),
        json!({"kind":"snapshotEnd"}),
        json!({"kind":"eventTooLarge","sequence":1}),
        json!({"sequence":1,"event":{"kind":"turnEnded","turnId":"turn","outcome":{"kind":"invented"}}}),
        json!({"sequence":1,"event":{"kind":"itemStarted","item":{"itemId":"id","kind":{"kind":"invented"},"text":null}}}),
    ] {
        assert!(!records.is_valid(&invalid), "{invalid}");
    }
    assert!(!notifications.is_valid(&json!({"event":{"kind":"resyncRequired"},"cursor":cursor})));
    assert!(
        !notifications
            .is_valid(&json!({"event":{"kind":"snapshotEnd","cursor":cursor},"cursor":null}))
    );
}

fn event_examples() -> Vec<SessionEvent> {
    use session_event_model::{
        ConfigValue, ConfigValueState, InputId, LocalCause, SessionSettings, SessionState,
        StopReason, ToolCallStatus, TurnLostReason,
    };
    let input = InputId::new("fixture-input").unwrap();
    let mut examples = vec![
        SessionEvent::TurnStarted {
            turn_id: "turn".into(),
            input_id: input.clone(),
        },
        SessionEvent::InputAccepted {
            turn_id: "turn".into(),
            input_id: input,
        },
        SessionEvent::TurnEnded {
            turn_id: "turn".into(),
            outcome: TurnOutcome::Lost {
                reason: TurnLostReason::ProviderRetired,
            },
        },
        SessionEvent::ItemCompleted {
            item_id: "item".into(),
        },
        SessionEvent::InteractionResolved {
            request_id: "fixture-request".into(),
        },
        SessionEvent::CapabilitiesChanged {
            capabilities: Default::default(),
        },
        SessionEvent::SettingsChanged {
            settings: SessionSettings {
                config: vec![
                    ConfigValue {
                        id: "mode".into(),
                        value: ConfigValueState::Select("auto".into()),
                    },
                    ConfigValue {
                        id: "bool".into(),
                        value: ConfigValueState::Boolean(true),
                    },
                ],
                ..Default::default()
            },
        },
        SessionEvent::ResyncRequired {
            replay_epoch: u64::MAX,
        },
    ];
    for session_state in [
        SessionState::Idle,
        SessionState::Unloaded,
        SessionState::Running,
        SessionState::Closed,
        SessionState::AuthenticationRequired,
    ] {
        examples.push(SessionEvent::StateChanged {
            state: session_state,
        });
    }
    for reason in [
        StopReason::EndTurn,
        StopReason::MaxTokens,
        StopReason::MaxTurnRequests,
        StopReason::Refusal,
        StopReason::Cancelled,
        StopReason::Unknown("provider extra reason".into()),
    ] {
        examples.push(SessionEvent::TurnEnded {
            turn_id: "turn".into(),
            outcome: TurnOutcome::Ended {
                stop_reason: reason,
                local_cause: Some(LocalCause::Other("provider cause".into())),
            },
        });
    }
    for kind in [
        SessionItemKind::UserMessage,
        SessionItemKind::AgentMessage,
        SessionItemKind::AgentThought,
        SessionItemKind::ToolCall {
            tool_kind: "fixture".into(),
            status: ToolCallStatus::Completed,
        },
        SessionItemKind::Plan,
        SessionItemKind::Usage,
        SessionItemKind::ModeChange,
        SessionItemKind::ConfigChange,
        SessionItemKind::SessionInfo,
        SessionItemKind::Notice,
        SessionItemKind::Unknown {
            source_kind: "provider-extra-item".into(),
        },
    ] {
        let item = SessionItem {
            item_id: "item".into(),
            kind,
            text: None,
        };
        examples.push(SessionEvent::ItemStarted { item: item.clone() });
        examples.push(SessionEvent::ItemUpdated { item });
    }
    let question_fields = [
        json!({"kind":"text","fieldId":"some-text","label":"Text","description":null,"required":true}),
        json!({"kind":"number","fieldId":"count","label":"Count","description":null,"required":true}),
        json!({"kind":"boolean","fieldId":"confirm","label":"Confirm","description":null,"required":false}),
        json!({"kind":"singleChoice","fieldId":"choice","label":"Choice","description":null,"required":true,
            "options":[{"optionId":"item","label":"Item"}]}),
        json!({"kind":"multiChoice","fieldId":"choices","label":"Choices","description":null,"required":true,
            "options":[{"optionId":"item","label":"Item"}],"min":0,"max":1}),
    ];
    let question = json!({"kind":"question","approver":{"kind":"human","humanId":"owner"},
        "request":{"requestId":"question-1","prompt":"Choose","fields":question_fields}});
    for scope in [
        json!("once"),
        json!("session"),
        json!({"persistent":{"where_stored":"fixture-store"}}),
    ] {
        let approval: session_event_model::PendingInteraction = serde_json::from_value(json!({
            "kind":"approval","approver":{"kind":"human","humanId":"owner"},
            "request":{"requestId":"approval-1","title":"Approve",
                "options":[{"optionId":"allow","label":"Allow","choice":{"effect":"allow","scope":scope}}]}
        })).unwrap();
        examples.push(SessionEvent::InteractionRequested {
            interaction: approval,
        });
    }
    let pending: session_event_model::PendingInteraction =
        serde_json::from_value(question).unwrap();
    examples.push(SessionEvent::StateChanged {
        state: SessionState::RequiresAction {
            pending: session_event_model::PendingInteractions::new(vec![pending.clone()]).unwrap(),
        },
    });
    examples.push(SessionEvent::InteractionRequested {
        interaction: pending,
    });
    examples
}
