//! Settings, observations and accepted values at the harness boundary.

use crate::RunAttempt;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProviderSettingKind {
    Mode,
    Model,
    Effort,
    Fast,
}

impl ProviderSettingKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mode => "mode",
            Self::Model => "model",
            Self::Effort => "effort",
            Self::Fast => "fast",
        }
    }
}

/// A setting's value as the agent names it (an option or mode ID).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingValueId(pub String);

/// One change attempt's identity (a UUIDv7), so an older change's result
/// is recognised.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SettingApplication(pub uuid::Uuid);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnerSetting {
    pub change: RunAttempt<SettingApplication>,
    pub kind: ProviderSettingKind,
}

/// Explains how the provider's confirmed mode relates to requested Auto.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionModeReason {
    AutoNotOffered,
    AgentFellBack,
    AutoNotRealized,
    EscalationUnproven,
    AutoNotApplied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedSetting {
    Confirmed(SettingValueId),
    LastConfirmed(SettingValueId),
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationCause {
    NotOffered,
    Refused,
    NoAnswer,
    Unconfirmed,
    ReportedOther,
    FellBack,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingEvidence {
    pub value: ObservedSetting,
    pub cause: Option<ObservationCause>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestedProviderSettings {
    pub mode: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: FastRequest,
}

impl RequestedProviderSettings {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mode.is_none()
            && self.model.is_none()
            && self.effort.is_none()
            && self.fast == FastRequest::Leave
    }
}

/// What a request asks of the agent's fast option, and where it came from
/// (D1 spec R7, R10). The Router never turns fast on.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FastRequest {
    /// Don't touch fast: `--fast auto`, and every setter that isn't about fast.
    #[default]
    Leave,
    /// The create default: turn fast off when the agent offers it; a no-op
    /// when it doesn't.
    OffIfOffered,
    /// An explicit fast off: with no fast option it's an invalid setting.
    Off,
}

/// Current settings reported for one provider Session.
#[derive(schemars::JsonSchema, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionSettings {
    pub mode: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub config: Vec<ConfigValue>,
}

/// One current config option value, identified by the provider's option ID.
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

#[cfg(test)]
#[path = "harness_settings_test.rs"]
mod tests;
