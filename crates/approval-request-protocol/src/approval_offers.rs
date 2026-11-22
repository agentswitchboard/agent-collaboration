use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;
use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, CentralSessionId, RouterRunId};

use crate::approval_values::{
    CallOperationName, Description, Label, OfferedOptionId, Subject, Title,
};
use crate::question_fields::{InvalidOfferedOptions, QuestionFields, validate_options};
use crate::{ApprovalDigestError, InvocationDigest, RequestContentsDigest};

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

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
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
        invocation: InvocationDigest,
        offer: CallOffer,
    },
}
}

impl Raise {
    pub const fn router_run(&self) -> &RouterRunId {
        match self {
            Self::ProviderApproval { holder, .. } | Self::ProviderQuestion { holder, .. } => {
                &holder.router_run
            }
            Self::UnprovenCall { holder, .. } => &holder.router_run,
        }
    }

    #[must_use]
    pub const fn requesting_session(&self) -> Option<&CentralSessionId> {
        match self {
            Self::ProviderApproval { holder, .. } | Self::ProviderQuestion { holder, .. } => {
                Some(&holder.session_id)
            }
            Self::UnprovenCall { .. } => None,
        }
    }

    pub fn contents_digest(&self) -> Result<RequestContentsDigest, ApprovalDigestError> {
        RequestContentsDigest::from_wire(self)
    }

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
    pub fn offer_view(&self) -> Option<OfferView> {
        match self {
            Self::ProviderApproval { offer, .. } => Some(OfferView::ProviderApproval {
                offer: offer.clone(),
            }),
            Self::ProviderQuestion { offer, .. } => Some(OfferView::ProviderQuestion {
                offer: offer.clone(),
            }),
            Self::UnprovenCall { .. } => None,
        }
    }
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum OfferView {
    ProviderApproval { offer: ApprovalOffer },
    ProviderQuestion { offer: QuestionOffer },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ProviderHolder {
    pub session_id: CentralSessionId,
    pub router_run: RouterRunId,
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ParkedCallHolder {
    pub router_run: RouterRunId,
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
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
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct QuestionOffer {
    pub prompt: Description,
    pub fields: QuestionFields,
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct CallOffer {
    pub operation: CallOperationName,
    pub summary: Description,
    pub options: OfferedOptions,
}
}

approval_wire_record! {
    [deny_unknown_fields]
    #[derive(Clone, Debug, Eq, PartialEq)]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    pub struct OfferedOption {
        pub id: OfferedOptionId,
        pub label: Label,
        pub effect: OptionEffect,
        pub lasts: OptionLifetime,
    }
}

impl OfferedOption {
    #[must_use]
    pub const fn needs_person_with_fresh_factor(&self) -> bool {
        matches!(
            self.lasts,
            OptionLifetime::KeptUntilChanged { .. } | OptionLifetime::NotVerified {}
        )
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

approval_wire_union! {
/// A verified lifetime determines which storage targets can be named.
/// A file cannot be declared as the storage for a turn-only option.
///
/// ```compile_fail,E0308
/// use approval_request_protocol::{OptionLifetime, RetainedOptionStorage};
/// let _ = OptionLifetime::ThisTurn { kept: RetainedOptionStorage::AgentRulesFile };
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum OptionLifetime {
    /// Only the current operation is authorized; no authorization is kept.
    ThisRequest {},
    ThisTurn {
        kept: TransientOptionStorage,
    },
    ThisSession {
        kept: TransientOptionStorage,
    },
    KeptUntilChanged {
        kept: RetainedOptionStorage,
    },
    /// The adapter cannot verify the duration or the destination.
    NotVerified {},
}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum TransientOptionStorage {
    Nowhere,
    AgentMemory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RetainedOptionStorage {
    AgentSettingsFile,
    AgentRulesFile,
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
