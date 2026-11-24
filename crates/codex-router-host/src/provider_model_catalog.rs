//! Project agent-advertised model choices for one provider connection.

use collaboration_contracts::{ProviderModelEntry, ProviderReasoningChoice};
use provider_session_machines::ProviderSettingKind;
use provider_session_machines::{ModelReasoning, ProviderSettingsCatalog};

pub(crate) fn provider_default_model() -> Result<ProviderModelEntry, &'static str> {
    ProviderModelEntry::try_new(
        "provider-default".into(),
        "Use provider settings".into(),
        "Use provider settings".into(),
    )
    .map_err(|_| "invalid provider default model")
}

pub(crate) fn model_entries(catalog: &ProviderSettingsCatalog) -> Vec<ProviderModelEntry> {
    let advertised = catalog
        .config_options
        .iter()
        .find(|option| option.category == Some(ProviderSettingKind::Model))
        .map(|option| option.choices.as_slice())
        .unwrap_or_default();
    let entries = advertised
        .iter()
        .filter_map(|choice| {
            let entry = ProviderModelEntry::try_new(
                choice.value.clone(),
                choice.label.clone(),
                "Advertised by the provider".into(),
            )
            .ok()?;
            match catalog.model_reasoning.for_model(&choice.value) {
                ModelReasoning::Unknown(_) => Some(entry),
                ModelReasoning::Select(control) => entry
                    .with_reasoning(
                        control
                            .choices()
                            .iter()
                            .map(|choice| ProviderReasoningChoice {
                                value: choice.value.clone(),
                                label: choice.label.clone(),
                            })
                            .collect(),
                        control.default_value().to_owned(),
                    )
                    .ok(),
            }
        })
        .collect::<Vec<_>>();
    if entries.is_empty() {
        provider_default_model().into_iter().collect()
    } else {
        entries
    }
}

#[cfg(test)]
#[path = "provider_model_catalog_test.rs"]
mod tests;
