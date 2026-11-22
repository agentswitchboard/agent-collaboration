//! Codex app-server model-list rendering over the provider catalogue.
use collaboration_contracts::{ProviderModelEntry, ProviderModelReasoning};
use serde_json::{Value, json};

fn render_app_server_model(
    id: &str,
    display_name: &str,
    description: &str,
    reasoning: &ProviderModelReasoning,
    is_default: bool,
) -> Value {
    let (choices, default_value) = match reasoning {
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
        "id":id,"model":id,
        "upgrade":null,"upgradeInfo":null,"availabilityNux":null,
        "displayName":display_name,"description":description,
        "hidden":false,"supportedReasoningEfforts":choices,
        "defaultReasoningEffort":default_value,"multiAgentVersion":null,
        "isDefault":is_default
    })
}

pub(crate) fn render_model_list(catalog: &[ProviderModelEntry]) -> Value {
    let models = if catalog.is_empty() {
        vec![render_app_server_model(
            "provider-default",
            "Use provider settings",
            "Use provider settings",
            &ProviderModelReasoning::ProviderSettings,
            true,
        )]
    } else {
        catalog
            .iter()
            .enumerate()
            .map(|(index, model)| {
                render_app_server_model(
                    model.id(),
                    model.display_name(),
                    model.description(),
                    model.reasoning(),
                    index == 0,
                )
            })
            .collect()
    };
    json!({
        "data":models,
        "nextCursor":null
    })
}

#[cfg(test)]
#[path = "app_server_model_catalog_test.rs"]
mod tests;
