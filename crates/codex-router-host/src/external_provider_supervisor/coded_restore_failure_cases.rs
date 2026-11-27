//! Shared inputs and literal expectations for the two coded restore projections.

use collaboration_protocol::{
    ConversationOperationFailureKind as Kind, ConversationOperationFailureStage as Stage,
    ProviderOperationEffect as Effect,
};
use provider_session_machines::session_owner::ProviderWireFailureKind as Wire;
use uuid::Uuid;

pub(crate) const REFERENCE: Uuid = uuid::uuid!("01981f2b-d79c-7abc-8000-000000000001");
pub(crate) const CODE: i64 = -32017;

type CodedRestoreCase = (
    Wire,
    Kind,
    Stage,
    Effect,
    &'static str,
    &'static str,
    Option<bool>,
);

pub(crate) const CASES: [CodedRestoreCase; 9] = [
    (
        Wire::AuthenticationRequired,
        Kind::AuthenticationRequired,
        Stage::Binding,
        Effect::None,
        "provider authentication is required",
        "the provider refused to restore this session, so check the provider before retrying",
        Some(false),
    ),
    (
        Wire::SessionNotFound,
        Kind::ProviderSessionNotFound,
        Stage::Binding,
        Effect::None,
        "the provider no longer knows this session, so start a new session with the same setup",
        "the provider no longer knows this session, so start a new session with the same setup",
        Some(true),
    ),
    (
        Wire::ResourceNotFound,
        Kind::NotFound,
        Stage::Binding,
        Effect::None,
        "provider resource was not found",
        "provider resource was not found, so check the provider before retrying",
        None,
    ),
    (
        Wire::UnsupportedMethod,
        Kind::UnsupportedCapability,
        Stage::Validation,
        Effect::None,
        "provider ACP method is unsupported",
        "provider ACP method is unsupported, so check the provider's restore support before retrying",
        None,
    ),
    (
        Wire::InvalidParams,
        Kind::InvalidRequest,
        Stage::Validation,
        Effect::None,
        "provider ACP parameters are invalid",
        "provider ACP parameters are invalid, so check the session setup before retrying",
        None,
    ),
    (
        Wire::RequestCancelled,
        Kind::ProviderRejected,
        Stage::Settlement,
        Effect::Unknown,
        "provider ACP request was cancelled",
        "provider ACP request was cancelled, so retry the restore when ready",
        None,
    ),
    (
        Wire::Rejected,
        Kind::ProviderRejected,
        Stage::Settlement,
        Effect::Unknown,
        "provider rejected the operation after dispatch",
        "the provider refused to restore this session, so check the provider before retrying",
        Some(false),
    ),
    (
        Wire::RestoreFailed,
        Kind::OutcomeUnknown,
        Stage::Settlement,
        Effect::Unknown,
        "the provider failed while restoring this session, so retry it",
        "the provider failed while restoring this session, so retry it",
        None,
    ),
    (
        Wire::RestoreRefused,
        Kind::ProviderRejected,
        Stage::Settlement,
        Effect::Unknown,
        "the provider refused to restore this session, so check the provider before retrying",
        "the provider refused to restore this session, so check the provider before retrying",
        Some(false),
    ),
];
