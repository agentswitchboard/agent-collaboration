//! Provider setting catalogue values shared with the session controller.

use crate::{ProviderSettingChoice, ProviderSettingKind};

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
