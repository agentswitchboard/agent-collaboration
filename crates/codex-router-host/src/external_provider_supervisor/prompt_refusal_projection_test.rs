use super::cause_tests::assert_failure;
use super::{OperationFailureCause, runtime_failure};
use crate::external_provider_runtime::prompt_refusal;
use crate::external_provider_supervisor::{PromptRefusal, PromptSelectionRefusal};
use acp_client_runtime::{ExternalProviderRuntimeError, ProviderError};
use collaboration_protocol::{
    ConversationOperationFailureKind, ConversationOperationFailureStage, OperationId,
    ProviderOperationEffect,
};

#[test]
fn prompt_refusal_is_classified_before_projection() {
    use ConversationOperationFailureKind as Kind;
    use ConversationOperationFailureStage as Stage;
    use ProviderOperationEffect as Effect;
    for (error, selection, kind, message) in [
        (
            ProviderError::InvalidSelection,
            PromptSelectionRefusal::InvalidSelection,
            Kind::InvalidRequest,
            "invalid or stale skill selection",
        ),
        (
            ProviderError::MalformedMetadata,
            PromptSelectionRefusal::MalformedMetadata,
            Kind::InvalidRequest,
            "provider metadata is malformed",
        ),
        (
            ProviderError::Unsupported,
            PromptSelectionRefusal::Unsupported,
            Kind::UnsupportedCapability,
            "provider does not support this operation or input form",
        ),
        (
            ProviderError::SourceUnavailable,
            PromptSelectionRefusal::SourceUnavailable,
            Kind::UnsupportedCapability,
            "provider skill source is unavailable",
        ),
    ] {
        let refusal = prompt_refusal(ExternalProviderRuntimeError::Behavior(error));
        assert_eq!(
            refusal,
            PromptRefusal::Selection {
                kind: selection,
                message: message.to_owned()
            }
        );
        let failure = runtime_failure(
            OperationId::generate(),
            None,
            OperationFailureCause::PromptRefused(refusal),
        );
        assert_failure(
            &failure,
            (kind, Stage::Dispatch, Effect::None, None, message),
        );
    }

    for (media, text) in [
        ("image", "unsupportedContent{image}"),
        ("audio", "unsupportedContent{audio}"),
        ("embeddedResource", "unsupportedContent{embeddedResource}"),
        ("other", "unsupportedContent{unknown}"),
    ] {
        let refusal = prompt_refusal(ExternalProviderRuntimeError::UnsupportedContent {
            content_type: media,
        });
        assert_eq!(
            refusal,
            PromptRefusal::UnsupportedContent {
                content_type: media
            }
        );
        let failure = runtime_failure(
            OperationId::generate(),
            None,
            OperationFailureCause::PromptRefused(refusal),
        );
        assert_failure(
            &failure,
            (
                Kind::UnsupportedCapability,
                Stage::Validation,
                Effect::None,
                None,
                text,
            ),
        );
    }

    let refusal = prompt_refusal(ExternalProviderRuntimeError::TransportFailure);
    assert_eq!(refusal, PromptRefusal::Unclassified);
    let failure = runtime_failure(
        OperationId::generate(),
        None,
        OperationFailureCause::PromptRefused(refusal),
    );
    assert_failure(
        &failure,
        (
            Kind::OutcomeUnknown,
            Stage::Settlement,
            Effect::Unknown,
            None,
            "provider operation response was not available after dispatch",
        ),
    );
}
