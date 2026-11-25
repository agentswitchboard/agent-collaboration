//! Validate ACP prompt blocks against a Session's advertised content types.

use crate::ProviderCapabilityReport;
use crate::provider_client_contract::ExternalProviderRuntimeError;
use agent_client_protocol::schema::v1::{
    AudioContent, BlobResourceContents, ContentBlock, EmbeddedResource, EmbeddedResourceResource,
    ImageContent, ResourceLink, TextContent, TextResourceContents,
};
use session_event_model::{PromptContent, PromptEmbeddedSource};
use std::sync::Arc;
use switchboard_harness_contracts::HarnessContent;

pub(crate) fn join_submitted_text_blocks<'a>(blocks: impl Iterator<Item = &'a str>) -> String {
    blocks.collect()
}

/// Translate validated Session content only at the ACP client edge.
pub(crate) fn acp_blocks_from_prompt_content(contents: &[PromptContent]) -> Vec<ContentBlock> {
    contents
        .iter()
        .map(|content| match content {
            PromptContent::Text { text } => {
                ContentBlock::Text(TextContent::new(text.as_str().to_owned()))
            }
            PromptContent::ResourceLink {
                uri,
                name,
                mime_type,
            } => ContentBlock::ResourceLink(
                ResourceLink::new(name.as_str().to_owned(), uri.as_str().to_owned())
                    .mime_type(mime_type.as_ref().map(|value| value.as_str().to_owned())),
            ),
            PromptContent::Image {
                mime_type,
                data,
                uri,
            } => ContentBlock::Image(
                ImageContent::new(data.as_str().to_owned(), mime_type.as_str().to_owned())
                    .uri(uri.as_ref().map(|value| value.as_str().to_owned())),
            ),
            PromptContent::Audio { mime_type, data } => ContentBlock::Audio(AudioContent::new(
                data.as_str().to_owned(),
                mime_type.as_str().to_owned(),
            )),
            PromptContent::EmbeddedResource {
                uri,
                mime_type,
                source,
            } => {
                let resource_uri = uri.as_str().to_owned();
                let resource_mime_type = mime_type.as_ref().map(|value| value.as_str().to_owned());
                let resource = match source {
                    PromptEmbeddedSource::Text(text) => {
                        EmbeddedResourceResource::TextResourceContents(
                            TextResourceContents::new(text.as_str().to_owned(), resource_uri)
                                .mime_type(resource_mime_type),
                        )
                    }
                    PromptEmbeddedSource::Blob(blob) => {
                        EmbeddedResourceResource::BlobResourceContents(
                            BlobResourceContents::new(blob.as_str().to_owned(), resource_uri)
                                .mime_type(resource_mime_type),
                        )
                    }
                };
                ContentBlock::Resource(EmbeddedResource::new(resource))
            }
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct ProviderPromptContent {
    contents: Arc<[PromptContent]>,
    history_text: Option<String>,
    selected_skills: Vec<session_event_model::SelectedSkill>,
}

impl ProviderPromptContent {
    pub fn from_session_contents(
        contents: Vec<PromptContent>,
        capabilities: &ProviderCapabilityReport,
    ) -> Result<Self, ExternalProviderRuntimeError> {
        for content in &contents {
            let unsupported = match content {
                PromptContent::Image { .. } if !capabilities.accepts_image => Some("image"),
                PromptContent::Audio { .. } if !capabilities.accepts_audio => Some("audio"),
                PromptContent::EmbeddedResource { .. }
                    if !capabilities.accepts_embedded_resource =>
                {
                    Some("embeddedResource")
                }
                PromptContent::Text { .. }
                | PromptContent::ResourceLink { .. }
                | PromptContent::Image { .. }
                | PromptContent::Audio { .. }
                | PromptContent::EmbeddedResource { .. } => None,
            };
            if let Some(content_type) = unsupported {
                return Err(ExternalProviderRuntimeError::UnsupportedContent { content_type });
            }
        }
        let history_text =
            join_submitted_text_blocks(contents.iter().filter_map(|content| match content {
                PromptContent::Text { text } => Some(text.as_str()),
                _ => None,
            }));
        Ok(Self {
            contents: contents.into(),
            history_text: Some(history_text),
            selected_skills: Vec::new(),
        })
    }

    #[must_use]
    pub fn as_harness_content(&self) -> HarnessContent<'_> {
        if self.is_compaction() {
            HarnessContent::Compaction(&self.contents)
        } else {
            HarnessContent::Input(&self.contents)
        }
    }

    pub fn submitted_text(&self) -> Option<&str> {
        self.history_text.as_deref()
    }

    fn is_compaction(&self) -> bool {
        self.history_text.is_none()
    }

    pub fn selected_skills(&self) -> &[session_event_model::SelectedSkill] {
        &self.selected_skills
    }

    pub(crate) fn set_input_context(
        &mut self,
        text: Option<String>,
        selected: Vec<session_event_model::SelectedSkill>,
    ) {
        self.history_text = text;
        self.selected_skills = selected;
    }
}

#[cfg(test)]
#[path = "provider_prompt_content_test.rs"]
mod tests;
