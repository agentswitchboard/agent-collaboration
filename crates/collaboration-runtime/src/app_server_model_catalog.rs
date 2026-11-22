//! Codex app-server model-list rendering over the provider catalogue.
use collaboration_contracts::{ProviderModelEntry, ProviderModelReasoning};
use serde_json::{Value, json};

fn provider_default() -> ProviderModelEntry {
    ProviderModelEntry::try_new(
        "provider-default".into(),
        "Use provider settings".into(),
        "Use provider settings".into(),
    )
    .expect("valid provider-default model")
}

fn render_app_server_model(entry: &ProviderModelEntry, is_default: bool) -> Value {
    let (choices, default_value) = match entry.reasoning() {
        ProviderModelReasoning::ProviderSettings => (
            vec![json!({"reasoningEffort":"provider","description":"Use provider settings"})],
            "provider",
        ),
        ProviderModelReasoning::Select {
            choices,
            default_value,
        } => (
            choices
                .iter()
                .map(|choice| json!({"reasoningEffort":choice.value,"description":choice.label}))
                .collect(),
            default_value.as_str(),
        ),
    };
    json!({
        "id":entry.id(),"model":entry.id(),
        "upgrade":null,"upgradeInfo":null,"availabilityNux":null,
        "displayName":entry.display_name(),"description":entry.description(),
        "hidden":false,"supportedReasoningEfforts":choices,
        "defaultReasoningEffort":default_value,"multiAgentVersion":null,
        "isDefault":is_default
    })
}

pub(crate) fn render_model_list(catalog: &[ProviderModelEntry]) -> Value {
    let models = if catalog.is_empty() {
        vec![provider_default()]
    } else {
        catalog.to_vec()
    };
    json!({
        "data":models.iter().enumerate()
            .map(|(index, model)| render_app_server_model(model, index == 0))
            .collect::<Vec<_>>(),
        "nextCursor":null
    })
}

#[cfg(test)]
#[path = "app_server_model_catalog_test.rs"]
mod tests;
