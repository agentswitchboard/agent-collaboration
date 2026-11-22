use super::{ConfigValue, ConfigValueState, SessionSettings};

#[test]
fn session_settings_keeps_its_wire_fields() {
    let literal = r#"{"mode":"ask","model":"sonnet","effort":null,"config":[]}"#;
    let settings = SessionSettings {
        mode: Some("ask".to_owned()),
        model: Some("sonnet".to_owned()),
        effort: None,
        config: Vec::new(),
    };

    let encoded = serde_json::to_string(&settings).expect("settings serialize");
    assert_eq!(encoded, literal, "session settings wire text");
    let decoded: SessionSettings = serde_json::from_str(literal).expect("literal settings parse");
    assert_eq!(decoded, settings);
}

#[test]
fn config_value_keeps_its_option_envelope() {
    let literal = r#"{"id":"fast","value":false}"#;
    let value = ConfigValue {
        id: "fast".to_owned(),
        value: ConfigValueState::Boolean(false),
    };

    let encoded = serde_json::to_string(&value).expect("option serializes");
    assert_eq!(encoded, literal, "config option wire text");
    let decoded: ConfigValue = serde_json::from_str(literal).expect("literal option parses");
    assert_eq!(decoded, value);
}

#[test]
fn config_value_state_keeps_both_untagged_wire_values() {
    for (literal, value) in [
        (r#""high""#, ConfigValueState::Select("high".to_owned())),
        ("true", ConfigValueState::Boolean(true)),
    ] {
        let encoded = serde_json::to_string(&value).expect("option state serializes");
        assert_eq!(encoded, literal, "untagged option state wire text");
        let decoded: ConfigValueState =
            serde_json::from_str(literal).expect("literal option state parses");
        assert_eq!(decoded, value);
    }
}
