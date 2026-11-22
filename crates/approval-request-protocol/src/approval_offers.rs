use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, RouterRunId, SessionRef};

use crate::approval_values::{
    CallOperationName, Description, InvocationDigest, Label, OfferedOptionId, Subject, Title,
};
use crate::question_fields::{InvalidOfferedOptions, QuestionFields, validate_options};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RequestKind {
    ProviderApproval,
    ProviderQuestion,
    UnprovenCall,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum Raise {
    ProviderApproval {
        request_id: ApprovalRequestId,
        holder: ProviderHolder,
        offer: ApprovalOffer,
    },
    ProviderQuestion {
        request_id: ApprovalRequestId,
        holder: ProviderHolder,
        offer: QuestionOffer,
    },
    UnprovenCall {
        request_id: ApprovalRequestId,
        holder: ParkedCallHolder,
        offer: CallOffer,
    },
}

impl Raise {
    #[must_use]
    pub const fn kind(&self) -> RequestKind {
        match self {
            Self::ProviderApproval { .. } => RequestKind::ProviderApproval,
            Self::ProviderQuestion { .. } => RequestKind::ProviderQuestion,
            Self::UnprovenCall { .. } => RequestKind::UnprovenCall,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> &ApprovalRequestId {
        match self {
            Self::ProviderApproval { request_id, .. }
            | Self::ProviderQuestion { request_id, .. }
            | Self::UnprovenCall { request_id, .. } => request_id,
        }
    }

    #[must_use]
    pub fn offer_view(&self) -> OfferView {
        match self {
            Self::ProviderApproval { offer, .. } => OfferView::ProviderApproval {
                offer: offer.clone(),
            },
            Self::ProviderQuestion { offer, .. } => OfferView::ProviderQuestion {
                offer: offer.clone(),
            },
            Self::UnprovenCall { offer, .. } => OfferView::UnprovenCall {
                offer: offer.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum OfferView {
    ProviderApproval { offer: ApprovalOffer },
    ProviderQuestion { offer: QuestionOffer },
    UnprovenCall { offer: CallOffer },
}

impl OfferView {
    #[must_use]
    pub const fn kind(&self) -> RequestKind {
        match self {
            Self::ProviderApproval { .. } => RequestKind::ProviderApproval,
            Self::ProviderQuestion { .. } => RequestKind::ProviderQuestion,
            Self::UnprovenCall { .. } => RequestKind::UnprovenCall,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ProviderHolder {
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = crate::typescript_shapes::SessionRef))]
    pub session: SessionRef,
    pub router_run: RouterRunId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ParkedCallHolder {
    pub router_run: RouterRunId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ApprovalOffer {
    pub title: Title,
    pub description: Option<Description>,
    pub subject: Option<Subject>,
    pub options: OfferedOptions,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct QuestionOffer {
    pub prompt: Description,
    pub fields: QuestionFields,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct CallOffer {
    pub operation: CallOperationName,
    pub summary: Description,
    pub invocation: InvocationDigest,
    pub options: OfferedOptions,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct OfferedOption {
    pub id: OfferedOptionId,
    pub label: Label,
    pub effect: OptionEffect,
    pub lasts: OptionDuration,
    pub kept: KeptWhere,
}

impl OfferedOption {
    #[must_use]
    pub const fn needs_person_with_fresh_factor(&self) -> bool {
        matches!(
            self.lasts,
            OptionDuration::KeptUntilChanged | OptionDuration::NotVerified
        ) || matches!(self.kept, KeptWhere::NotVerified)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum OptionEffect {
    Allow,
    Decline,
    Abort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum OptionDuration {
    ThisRequest,
    ThisTurn,
    ThisSession,
    KeptUntilChanged,
    NotVerified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum KeptWhere {
    Nowhere,
    AgentMemory,
    AgentSettingsFile,
    AgentRulesFile,
    NotVerified,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<OfferedOption>", into = "Vec<OfferedOption>")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct OfferedOptions(Vec<OfferedOption>);

impl OfferedOptions {
    pub fn new(options: Vec<OfferedOption>) -> Result<Self, InvalidOfferedOptions> {
        Self::try_from(options)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[OfferedOption] {
        &self.0
    }
}

impl TryFrom<Vec<OfferedOption>> for OfferedOptions {
    type Error = InvalidOfferedOptions;

    fn try_from(options: Vec<OfferedOption>) -> Result<Self, Self::Error> {
        validate_options(options.len(), options.iter().map(|option| &option.id))?;
        Ok(Self(options))
    }
}

impl From<OfferedOptions> for Vec<OfferedOption> {
    fn from(options: OfferedOptions) -> Self {
        options.0
    }
}
