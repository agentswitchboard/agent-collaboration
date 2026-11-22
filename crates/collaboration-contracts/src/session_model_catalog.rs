//! Shared model catalogue entries and the session-scoped read port.
use std::{future::Future, pin::Pin};

pub trait SessionModelCatalog: Send + Sync {
    fn read_models(
        &self,
        session: message_board::SessionRef,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ProviderModelEntry>, SessionModelCatalogError>>
                + Send
                + '_,
        >,
    >;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionModelCatalogError {
    #[error("session model catalogue not found")]
    SessionNotFound,
    #[error("session model catalogue unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderModelReasoning {
    ProviderSettings,
    Select {
        choices: Vec<ProviderReasoningChoice>,
        default_value: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReasoningChoice {
    pub value: String,
    pub label: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderModelEntry {
    id: String,
    display_name: String,
    description: String,
    reasoning: ProviderModelReasoning,
}

#[derive(Debug, thiserror::Error)]
#[error("provider model entry requires a nonempty id and display name")]
pub struct InvalidProviderModelEntry;

impl ProviderModelEntry {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn try_new(
        id: String,
        display_name: String,
        description: String,
    ) -> Result<Self, InvalidProviderModelEntry> {
        if id.is_empty()
            || display_name.is_empty()
            || id.contains('\0')
            || display_name.contains('\0')
        {
            return Err(InvalidProviderModelEntry);
        }
        Ok(Self {
            id,
            display_name,
            description,
            reasoning: ProviderModelReasoning::ProviderSettings,
        })
    }

    pub fn with_reasoning(
        mut self,
        choices: Vec<ProviderReasoningChoice>,
        default_value: String,
    ) -> Result<Self, InvalidProviderModelEntry> {
        let mut seen = std::collections::HashSet::new();
        if choices.is_empty()
            || !choices.iter().any(|choice| choice.value == default_value)
            || choices.iter().any(|choice| {
                choice.value.is_empty()
                    || choice.label.is_empty()
                    || choice.value.contains('\0')
                    || choice.label.contains('\0')
                    || !seen.insert(&choice.value)
            })
        {
            return Err(InvalidProviderModelEntry);
        }
        // The reserved action cannot also name an actual provider value.
        if !choices.iter().any(|choice| choice.value == "provider") {
            self.reasoning = ProviderModelReasoning::Select {
                choices,
                default_value,
            };
        }
        Ok(self)
    }

    #[must_use]
    pub fn reasoning(&self) -> &ProviderModelReasoning {
        &self.reasoning
    }

    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }
}
