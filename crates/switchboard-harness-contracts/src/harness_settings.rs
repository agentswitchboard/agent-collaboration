//! Settings, observations and accepted values at the harness boundary.

use crate::RunAttempt;

/// Controller requests and harness observations use this closed set to identify the setting they concern.
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
/// The Router's setting caller or harness adapter supplies this ID to correlate one setting change with its result.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SettingApplication(pub uuid::Uuid);

/// The controller pairs the setting kind with its run-bound change attempt before sending it to the harness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnerSetting {
    pub change: RunAttempt<SettingApplication>,
    pub kind: ProviderSettingKind,
}

/// Explains how the provider's confirmed mode relates to requested Auto.
/// The harness adapter or controller records why requested Auto was unavailable, used a fallback, or remained unconfirmed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionModeReason {
    AutoNotOffered,
    AgentFellBack,
    AutoNotRealized,
    EscalationUnproven,
    AutoNotApplied,
}

/// The harness adapter or controller records whether a setting is confirmed, only last confirmed, or unknown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedSetting {
    Confirmed(SettingValueId),
    LastConfirmed(SettingValueId),
    Unknown,
}

/// The harness adapter or controller records this reason when a setting observation needs an explanation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationCause {
    NotOffered,
    Refused,
    NoAnswer,
    Unconfirmed,
    ReportedOther,
    FellBack,
}

/// The harness adapter or controller pairs a setting observation with any reason it has evidence to report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingEvidence {
    pub value: ObservedSetting,
    pub cause: Option<ObservationCause>,
}
