//! Public rejection for a provider observation call.
use crate::collaboration_rejection::{
    CollaborationRejection, CollaborationRejectionReason, PublishedRejection,
};
use serde_json::json;

/// Why a provider observation failed: `{kind, stage, message}` on the wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ObservationFailure {
    #[error("Invalid provider observation request")]
    InvalidField,
    #[error("Provider Session was not found")]
    NotFound,
    #[error("Provider observation unavailable")]
    Unavailable,
}

impl ObservationFailure {
    #[must_use]
    pub const fn kind(self) -> &'static str {
        match self {
            Self::InvalidField => "invalidField",
            Self::NotFound => "notFound",
            Self::Unavailable => "unavailable",
        }
    }
}

impl serde::Serialize for ObservationFailure {
    fn serialize<TSerializer: serde::Serializer>(
        &self,
        serializer: TSerializer,
    ) -> Result<TSerializer::Ok, TSerializer::Error> {
        json!({"kind":self.kind(),"stage":"inspect","message":self.to_string()})
            .serialize(serializer)
    }
}

impl CollaborationRejection for ObservationFailure {
    fn rejection_reason(&self) -> Option<CollaborationRejectionReason> {
        match self {
            Self::InvalidField => Some(CollaborationRejectionReason::InvalidShape),
            Self::NotFound | Self::Unavailable => None,
        }
    }

    fn published_rejection(&self) -> PublishedRejection {
        let code = match self {
            Self::InvalidField => PublishedRejection::INVALID_PARAMS,
            Self::NotFound => PublishedRejection::NOT_FOUND,
            Self::Unavailable => PublishedRejection::OPERATION_FAILED,
        };
        PublishedRejection::typed(code, self.to_string(), self)
    }
}
