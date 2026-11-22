//! Typed provider Session settings at the ACP client boundary.
use provider_session_machines::{EffectiveProviderSettings, ProviderSettingKind};

pub use switchboard_harness_contracts::{FastRequest, RequestedProviderSettings};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSettingApplication {
    pub effective_settings: EffectiveProviderSettings,
    /// The requested mode confirmed by this application without a fallback.
    pub confirmed_requested_mode: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedProviderSetting {
    pub kind: ProviderSettingKind,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSettingRequest {
    pub kind: ProviderSettingKind,
    pub value: String,
    pub intent: session_event_model::SettingApplicationIntent,
    pub expected_model: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailedProviderSetting {
    pub kind: ProviderSettingKind,
    pub value: String,
    pub reason: session_event_model::ProviderSettingFailureReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotAppliedProviderSetting {
    pub kind: ProviderSettingKind,
    pub value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidSettingSessionDisposition {
    Closed,
    RemainsCreated,
}
