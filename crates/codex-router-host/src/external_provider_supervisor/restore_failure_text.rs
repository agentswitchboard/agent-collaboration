//! Project coded restore failures into the rejection or unavailable text callers see.

use collaboration_protocol::DeliveryCorrelationId;
use provider_session_machines::session_owner::{ProviderWireFailure, ProviderWireFailureKind};

pub(crate) enum CodedRestoreFailure {
    Rejected {
        kind: RestoreRejectionKind,
        code: i64,
        correlation_id: DeliveryCorrelationId,
    },
    Unavailable(String),
}

pub(crate) enum RestoreRejectionKind {
    SessionNotFound,
    ProviderRejected,
}

impl CodedRestoreFailure {
    pub(crate) fn from_wire(failure: ProviderWireFailure) -> Self {
        let ProviderWireFailure {
            kind,
            code,
            correlation_id,
        } = failure;
        let Ok(correlation_id) =
            DeliveryCorrelationId::try_from(correlation_id.hyphenated().to_string())
        else {
            return Self::Unavailable(
                "provider error reference was invalid, so retry this session".to_owned(),
            );
        };

        let explanation = match kind {
            ProviderWireFailureKind::SessionNotFound => {
                return Self::Rejected {
                    kind: RestoreRejectionKind::SessionNotFound,
                    code,
                    correlation_id,
                };
            }
            ProviderWireFailureKind::RestoreRefused
            | ProviderWireFailureKind::AuthenticationRequired
            | ProviderWireFailureKind::Rejected => {
                return Self::Rejected {
                    kind: RestoreRejectionKind::ProviderRejected,
                    code,
                    correlation_id,
                };
            }
            ProviderWireFailureKind::ResourceNotFound => {
                "provider resource was not found, so check the provider before retrying"
            }
            ProviderWireFailureKind::RestoreFailed => {
                "the provider failed while restoring this session, so retry it"
            }
            ProviderWireFailureKind::UnsupportedMethod => {
                "provider ACP method is unsupported, so check the provider's restore support before retrying"
            }
            ProviderWireFailureKind::InvalidParams => {
                "provider ACP parameters are invalid, so check the session setup before retrying"
            }
            ProviderWireFailureKind::RequestCancelled => {
                "provider ACP request was cancelled, so retry the restore when ready"
            }
        };
        Self::Unavailable(format!(
            "{explanation} (ACP code {code}; reference {})",
            correlation_id.as_str()
        ))
    }

    pub(crate) fn into_text(self) -> String {
        match self {
            Self::Rejected {
                kind,
                code,
                correlation_id,
            } => kind.text(code, &correlation_id),
            Self::Unavailable(reason) => reason,
        }
    }
}

impl RestoreRejectionKind {
    pub(crate) fn text(&self, code: i64, correlation_id: &DeliveryCorrelationId) -> String {
        let explanation = match self {
            Self::SessionNotFound => {
                "the provider no longer knows this session, so start a new session with the same setup"
            }
            Self::ProviderRejected => {
                "the provider refused to restore this session, so check the provider before retrying"
            }
        };
        format!(
            "{explanation} (provider code {code}; reference {})",
            correlation_id.as_str()
        )
    }
}
