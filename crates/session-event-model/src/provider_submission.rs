//! An input's original content and explicit skill selections, carried together.

use crate::{PromptContent, SelectedSkill};
use std::sync::Arc;
use switchboard_harness_contracts::HarnessContent;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSubmission {
    pub content: Vec<PromptContent>,
    pub selected: Vec<SelectedSkill>,
}

impl ProviderSubmission {
    pub fn plain(content: Vec<PromptContent>) -> Self {
        Self {
            content,
            selected: Vec::new(),
        }
    }

    #[must_use]
    pub fn submitted_text(&self) -> String {
        self.content
            .iter()
            .filter_map(|block| match block {
                PromptContent::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderTurnInput {
    Prompt(ProviderSubmission),
    Compact,
}

/// Retained prompt blocks and the original input used for history and admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedPrompt {
    Input {
        blocks: Arc<[PromptContent]>,
        submitted_text: String,
        selected_skills: Vec<SelectedSkill>,
    },
    Compaction {
        blocks: Arc<[PromptContent]>,
    },
}

impl PreparedPrompt {
    #[must_use]
    pub fn plain(blocks: Vec<PromptContent>) -> Self {
        let submission = ProviderSubmission::plain(blocks);
        let submitted_text = submission.submitted_text();
        Self::Input {
            blocks: submission.content.into(),
            submitted_text,
            selected_skills: submission.selected,
        }
    }

    #[must_use]
    pub fn as_harness_content(&self) -> HarnessContent<'_> {
        match self {
            Self::Input { blocks, .. } => HarnessContent::Input(blocks),
            Self::Compaction { blocks } => HarnessContent::Compaction(blocks),
        }
    }

    #[must_use]
    pub fn submitted_text(&self) -> Option<&str> {
        match self {
            Self::Input { submitted_text, .. } => Some(submitted_text),
            Self::Compaction { .. } => None,
        }
    }

    #[must_use]
    pub fn selected_skills(&self) -> &[SelectedSkill] {
        match self {
            Self::Input {
                selected_skills, ..
            } => selected_skills,
            Self::Compaction { .. } => &[],
        }
    }
}
