//! Validate ACP prompt blocks against a Session's advertised content types.

use crate::ProviderCapabilityReport;
use crate::provider_client_contract::ExternalProviderRuntimeError;
use agent_client_protocol::schema::v1::{
    AudioContent, BlobResourceContents, ContentBlock, EmbeddedResource, EmbeddedResourceResource,
    ImageContent, ResourceLink, TextContent, TextResourceContents,
};
use session_event_model::{PromptContent, PromptEmbeddedSource};

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

pub(crate) fn validate_prompt_content(
    contents: &[PromptContent],
    capabilities: &ProviderCapabilityReport,
) -> Result<(), ExternalProviderRuntimeError> {
    for content in contents {
        let unsupported = match content {
            PromptContent::Image { .. } if !capabilities.accepts_image => Some("image"),
            PromptContent::Audio { .. } if !capabilities.accepts_audio => Some("audio"),
            PromptContent::EmbeddedResource { .. } if !capabilities.accepts_embedded_resource => {
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
    Ok(())
}

#[cfg(test)]
#[path = "provider_prompt_content_test.rs"]
mod tests;
