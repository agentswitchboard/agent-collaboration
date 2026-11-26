//! Provider setting catalogue values shared with the session controller.

use crate::{ModelReasoningMetadata, ProviderSettingChoice, ProviderSettingKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderConfigValue {
    Select(String),
    Boolean(bool),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderConfigOption {
    pub id: String,
    pub name: String,
    pub category: Option<ProviderSettingKind>,
    pub current_value: ProviderConfigValue,
    pub choices: Vec<ProviderSettingChoice>,
}

/// Last options advertised by a provider Session. The Host supervisor may keep
/// this snapshot after the Session unloads for model/list and settings inspect.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProviderModeObservation {
    #[default]
    FromCatalogue,
    Unconfirmed,
}

/// The provider settings snapshot carried in a work readback.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProviderSettingsCatalogRecord {
    pub current_mode: Option<String>,
    pub mode_observation: ProviderModeObservation,
    pub modes: Vec<ProviderSettingChoice>,
    pub config_options: Vec<ProviderConfigOption>,
    pub model_reasoning: ModelReasoningMetadata,
}
