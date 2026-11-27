//! Protocol-neutral provider setting values and their advertised choices.
use crate::{ModelReasoningMetadata, ProviderSettingKind};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

/// A select option's vendor-provided ID for turning fast off.
pub fn is_fast_off_select_value(value: &str) -> bool {
    matches!(value, "false" | "off")
}

pub use switchboard_harness_contracts::{
    EffectiveProviderSettings, FastObserved, ProviderConfigOption, ProviderConfigValue,
    ProviderModeObservation, ProviderSettingsCatalogRecord, ProviderSettingsUpdate,
};

/// Last options advertised by a provider session, with its catalogue operations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProviderSettingsCatalog(Arc<ProviderSettingsCatalogRecord>);

impl Deref for ProviderSettingsCatalog {
    type Target = ProviderSettingsCatalogRecord;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ProviderSettingsCatalog {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

impl ProviderSettingsCatalog {
    #[must_use]
    pub fn from_record(record: impl Into<Arc<ProviderSettingsCatalogRecord>>) -> Self {
        Self(record.into())
    }

    #[must_use]
    pub fn into_record(self) -> Arc<ProviderSettingsCatalogRecord> {
        self.0
    }

    #[must_use]
    pub fn into_model_reasoning(self) -> ModelReasoningMetadata {
        match Arc::try_unwrap(self.0) {
            Ok(record) => record.model_reasoning,
            Err(record) => record.model_reasoning.clone(),
        }
    }
}

impl ProviderSettingsCatalog {
    pub fn apply_update(&mut self, update: ProviderSettingsUpdate) {
        match update {
            ProviderSettingsUpdate::CurrentMode(mode) => {
                self.current_mode = Some(mode.clone());
                self.mode_observation = ProviderModeObservation::FromCatalogue;
                if let Some(option) = self
                    .config_options
                    .iter_mut()
                    .find(|option| option.category == Some(ProviderSettingKind::Mode))
                {
                    option.current_value = ProviderConfigValue::Select(mode);
                }
            }
            ProviderSettingsUpdate::ConfigOptions {
                options,
                model_reasoning,
            } => {
                self.config_options = options;
                if let Some(option) = self.config_option(ProviderSettingKind::Mode)
                    && let ProviderConfigValue::Select(mode) = &option.current_value
                {
                    self.current_mode = Some(mode.clone());
                    self.mode_observation = ProviderModeObservation::FromCatalogue;
                }
                if let Some(reasoning) = model_reasoning {
                    self.model_reasoning = reasoning;
                }
            }
        }
    }

    pub(crate) fn apply_late_setting_readback(
        &mut self,
        kind: ProviderSettingKind,
        readback: &Self,
    ) {
        if kind == ProviderSettingKind::Mode {
            self.current_mode = readback.effective_settings().mode;
            self.mode_observation = readback.mode_observation;
            self.modes.clone_from(&readback.modes);
        }
        self.config_options
            .retain(|option| option.category != Some(kind));
        self.config_options.extend(
            readback
                .config_options
                .iter()
                .filter(|option| option.category == Some(kind))
                .cloned(),
        );
    }

    /// Snapshot the values currently reported by the agent for the ordered
    /// Session event stream. Choice catalogs remain client-owned.
    #[must_use]
    pub fn to_session_settings(&self) -> session_event_model::SessionSettings {
        use session_event_model::{ConfigValue, ConfigValueState, SessionSettings};

        let effective = self.effective_settings();
        SessionSettings {
            mode: effective.mode,
            model: effective.model,
            effort: effective.effort,
            config: self
                .config_options
                .iter()
                .filter(|option| {
                    option.category != Some(ProviderSettingKind::Mode)
                        || self.mode_observation != ProviderModeObservation::Unconfirmed
                })
                .map(|option| ConfigValue {
                    id: option.id.clone(),
                    value: match &option.current_value {
                        ProviderConfigValue::Select(value) => {
                            ConfigValueState::Select(value.clone())
                        }
                        ProviderConfigValue::Boolean(value) => ConfigValueState::Boolean(*value),
                    },
                })
                .collect(),
        }
    }

    #[must_use]
    pub fn advertised_values(&self, kind: ProviderSettingKind) -> Vec<String> {
        if kind == ProviderSettingKind::Mode
            && !self
                .config_options
                .iter()
                .any(|option| option.category == Some(kind))
        {
            return self
                .modes
                .iter()
                .map(|choice| choice.value.clone())
                .collect();
        }
        self.config_option(kind)
            .map(|option| {
                option
                    .choices
                    .iter()
                    .map(|choice| choice.value.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Lists the valid choices when an explicit catalog selection is unavailable.
    pub fn unavailable_selection(
        &self,
        kind: ProviderSettingKind,
        requested: &str,
    ) -> Option<Vec<String>> {
        match kind {
            ProviderSettingKind::Model | ProviderSettingKind::Effort => {
                let advertised = self.advertised_values(kind);
                (!advertised.iter().any(|value| value == requested)).then_some(advertised)
            }
            ProviderSettingKind::Fast if requested == "off" && !self.offers_fast() => {
                Some(Vec::new())
            }
            ProviderSettingKind::Mode | ProviderSettingKind::Fast => None,
        }
    }

    #[must_use]
    pub fn config_option(&self, kind: ProviderSettingKind) -> Option<&ProviderConfigOption> {
        if kind == ProviderSettingKind::Effort {
            let eligible = |option: &&ProviderConfigOption| {
                option.category == Some(kind)
                    && option.id != "thinking"
                    && matches!(option.current_value, ProviderConfigValue::Select(_))
            };
            let has_unique_config_id = |option: &ProviderConfigOption| {
                self.config_options
                    .iter()
                    .filter(|candidate| candidate.id == option.id)
                    .count()
                    == 1
            };
            let mut explicit = self
                .config_options
                .iter()
                .filter(eligible)
                .filter(|option| option.id == "effort");
            if let Some(option) = explicit.next() {
                return (explicit.next().is_none() && has_unique_config_id(option))
                    .then_some(option);
            }
            let mut fallback = self.config_options.iter().filter(eligible);
            let option = fallback.next()?;
            return (fallback.next().is_none() && has_unique_config_id(option)).then_some(option);
        }
        self.config_options
            .iter()
            .find(|option| option.category == Some(kind))
    }

    #[must_use]
    pub fn effective_settings(&self) -> EffectiveProviderSettings {
        let selected = |kind| {
            self.config_option(kind)
                .and_then(|option| match &option.current_value {
                    ProviderConfigValue::Select(value) => Some(value.clone()),
                    ProviderConfigValue::Boolean(_) => None,
                })
        };
        EffectiveProviderSettings {
            mode: match self.mode_observation {
                ProviderModeObservation::FromCatalogue => {
                    selected(ProviderSettingKind::Mode).or_else(|| self.current_mode.clone())
                }
                ProviderModeObservation::Unconfirmed => None,
            },
            model: selected(ProviderSettingKind::Model),
            effort: selected(ProviderSettingKind::Effort),
            fast: Some(self.fast_observed()),
        }
    }

    /// Whether this catalog offers a fast option at all.
    #[must_use]
    pub fn offers_fast(&self) -> bool {
        self.config_option(ProviderSettingKind::Fast).is_some()
    }

    /// Reads the vendor's off identifier or a boolean false as off.
    #[must_use]
    pub fn fast_observed(&self) -> FastObserved {
        match self.config_option(ProviderSettingKind::Fast) {
            None => FastObserved::NoOption,
            Some(option) => match &option.current_value {
                ProviderConfigValue::Select(value) if is_fast_off_select_value(value) => {
                    FastObserved::Off
                }
                ProviderConfigValue::Boolean(false) => FastObserved::Off,
                ProviderConfigValue::Select(_) | ProviderConfigValue::Boolean(true) => {
                    FastObserved::On
                }
            },
        }
    }
}

#[cfg(test)]
#[path = "provider_settings_catalog_test.rs"]
mod tests;
