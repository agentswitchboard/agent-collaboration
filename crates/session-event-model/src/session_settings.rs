use serde::{Deserialize, Serialize};

/// Why a caller is applying a setting; never persisted in Session history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettingApplicationIntent {
    ExplicitChange,
    TurnContext,
}

/// Current settings reported for one provider Session.
/// The harness adapter or controller builds this snapshot from the current settings observed for one provider session.
#[derive(schemars::JsonSchema, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionSettings {
    pub mode: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub config: Vec<ConfigValue>,
}

/// One current config option value, identified by the provider's option ID.
/// The settings projection pairs the provider's option ID with the current value reported for it.
#[derive(schemars::JsonSchema, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConfigValue {
    pub id: String,
    pub value: ConfigValueState,
}

/// ACP v1 config options currently report either a selected ID or a boolean.
#[derive(schemars::JsonSchema, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConfigValueState {
    Select(String),
    Boolean(bool),
}
