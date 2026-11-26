//! Validated, provider-neutral content blocks for provider prompts and steers.

use base64::Engine as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidPromptContent {
    field: &'static str,
}

impl std::fmt::Display for InvalidPromptContent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid prompt {}", self.field)
    }
}

impl std::error::Error for InvalidPromptContent {}

macro_rules! prompt_string {
    ($name:ident, $field:literal) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name(String);

        impl $name {
            pub fn try_new(value: String) -> Result<Self, InvalidPromptContent> {
                if value.is_empty() || value.contains('\0') {
                    Err(InvalidPromptContent { field: $field })
                } else {
                    Ok(Self(value))
                }
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }
    };
}

prompt_string!(PromptText, "text");
prompt_string!(PromptUri, "uri");
prompt_string!(PromptName, "name");
prompt_string!(PromptMimeType, "mime type");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptData(String);

impl PromptData {
    pub fn try_new(value: String) -> Result<Self, InvalidPromptContent> {
        if value.is_empty()
            || value.contains('\0')
            || base64::engine::general_purpose::STANDARD
                .decode(value.as_bytes())
                .is_err()
        {
            Err(InvalidPromptContent {
                field: "base64 data",
            })
        } else {
            Ok(Self(value))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromptEmbeddedSource {
    Text(PromptText),
    Blob(PromptData),
}

/// Images always carry base64 data; an optional URI preserves ACP provenance.
/// Embedded resources carry exactly one text or blob source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromptContent {
    Text {
        text: PromptText,
    },
    ResourceLink {
        uri: PromptUri,
        name: PromptName,
        mime_type: Option<PromptMimeType>,
    },
    Image {
        mime_type: PromptMimeType,
        data: PromptData,
        uri: Option<PromptUri>,
    },
    Audio {
        mime_type: PromptMimeType,
        data: PromptData,
    },
    EmbeddedResource {
        uri: PromptUri,
        mime_type: Option<PromptMimeType>,
        source: PromptEmbeddedSource,
    },
}

impl PromptContent {
    pub fn text(text: String) -> Result<Self, InvalidPromptContent> {
        Ok(Self::Text {
            text: PromptText::try_new(text)?,
        })
    }

    pub fn resource_link(
        uri: String,
        name: String,
        mime_type: Option<String>,
    ) -> Result<Self, InvalidPromptContent> {
        Ok(Self::ResourceLink {
            uri: PromptUri::try_new(uri)?,
            name: PromptName::try_new(name)?,
            mime_type: mime_type.map(PromptMimeType::try_new).transpose()?,
        })
    }

    pub fn image(
        mime_type: String,
        data: String,
        uri: Option<String>,
    ) -> Result<Self, InvalidPromptContent> {
        Ok(Self::Image {
            mime_type: PromptMimeType::try_new(mime_type)?,
            data: PromptData::try_new(data)?,
            uri: uri.map(PromptUri::try_new).transpose()?,
        })
    }

    pub fn audio(mime_type: String, data: String) -> Result<Self, InvalidPromptContent> {
        Ok(Self::Audio {
            mime_type: PromptMimeType::try_new(mime_type)?,
            data: PromptData::try_new(data)?,
        })
    }

    pub fn embedded_text(
        uri: String,
        mime_type: Option<String>,
        text: String,
    ) -> Result<Self, InvalidPromptContent> {
        Ok(Self::EmbeddedResource {
            uri: PromptUri::try_new(uri)?,
            mime_type: mime_type.map(PromptMimeType::try_new).transpose()?,
            source: PromptEmbeddedSource::Text(PromptText::try_new(text)?),
        })
    }

    pub fn embedded_blob(
        uri: String,
        mime_type: Option<String>,
        blob: String,
    ) -> Result<Self, InvalidPromptContent> {
        Ok(Self::EmbeddedResource {
            uri: PromptUri::try_new(uri)?,
            mime_type: mime_type.map(PromptMimeType::try_new).transpose()?,
            source: PromptEmbeddedSource::Blob(PromptData::try_new(blob)?),
        })
    }
}

/// The profile wire is a codec over the same validated content vocabulary the
/// Host and ACP client use internally.
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum PromptContentWire {
    Text {
        text: String,
    },
    ResourceLink {
        uri: String,
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
    },
    Image {
        mime_type: String,
        data: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        uri: Option<String>,
    },
    Audio {
        mime_type: String,
        data: String,
    },
    EmbeddedResource {
        uri: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        blob: Option<String>,
    },
}

impl Serialize for PromptContent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let wire = match self {
            Self::Text { text } => PromptContentWire::Text {
                text: text.as_str().to_owned(),
            },
            Self::ResourceLink {
                uri,
                name,
                mime_type,
            } => PromptContentWire::ResourceLink {
                uri: uri.as_str().to_owned(),
                name: name.as_str().to_owned(),
                mime_type: mime_type.as_ref().map(|value| value.as_str().to_owned()),
            },
            Self::Image {
                mime_type,
                data,
                uri,
            } => PromptContentWire::Image {
                mime_type: mime_type.as_str().to_owned(),
                data: data.as_str().to_owned(),
                uri: uri.as_ref().map(|value| value.as_str().to_owned()),
            },
            Self::Audio { mime_type, data } => PromptContentWire::Audio {
                mime_type: mime_type.as_str().to_owned(),
                data: data.as_str().to_owned(),
            },
            Self::EmbeddedResource {
                uri,
                mime_type,
                source,
            } => {
                let (text, blob) = match source {
                    crate::PromptEmbeddedSource::Text(text) => {
                        (Some(text.as_str().to_owned()), None)
                    }
                    crate::PromptEmbeddedSource::Blob(data) => {
                        (None, Some(data.as_str().to_owned()))
                    }
                };
                PromptContentWire::EmbeddedResource {
                    uri: uri.as_str().to_owned(),
                    mime_type: mime_type.as_ref().map(|value| value.as_str().to_owned()),
                    text,
                    blob,
                }
            }
        };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PromptContent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = PromptContentWire::deserialize(deserializer)?;
        let content = match wire {
            PromptContentWire::Text { text } => Self::text(text),
            PromptContentWire::ResourceLink {
                uri,
                name,
                mime_type,
            } => Self::resource_link(uri, name, mime_type),
            PromptContentWire::Image {
                mime_type,
                data,
                uri,
            } => Self::image(mime_type, data, uri),
            PromptContentWire::Audio { mime_type, data } => Self::audio(mime_type, data),
            PromptContentWire::EmbeddedResource {
                uri,
                mime_type,
                text: Some(text),
                blob: None,
            } => Self::embedded_text(uri, mime_type, text),
            PromptContentWire::EmbeddedResource {
                uri,
                mime_type,
                text: None,
                blob: Some(blob),
            } => Self::embedded_blob(uri, mime_type, blob),
            PromptContentWire::EmbeddedResource { .. } => {
                return Err(serde::de::Error::custom(
                    "embedded resource requires exactly one of text or blob",
                ));
            }
        };
        content.map_err(serde::de::Error::custom)
    }
}
