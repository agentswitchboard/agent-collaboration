//! Map provider admission and prompt loss into the operation failure contract.

use super::{OperationFailureCause, PromptRefusal, PromptSelectionRefusal};
use super::{failure, failure_with_provider_code};
use collaboration_protocol::{
    ConversationOperationFailure, ConversationOperationFailureKind,
    ConversationOperationFailureStage, DeliveryCorrelationId, InvalidProviderSetting,
    InvalidSettingSessionDisposition, NonEmptyText, OperationId, ProviderOperationEffect,
    SessionRef,
};
use provider_session_machines::session_owner::{ProviderWireFailure, ProviderWireFailureKind};

const MAX_ADVERTISED_CHOICES: usize = 32;
const MAX_ADVERTISED_SUMMARY_BYTES: usize = 2048;
const MAX_SETTING_VALUE_DISPLAY_CHARS: usize = 120;

#[derive(Clone, Copy, Debug)]
struct ProviderErrorReference {
    provider_code: i64,
    correlation_id: uuid::Uuid,
}

struct CorrelatedProviderFailureProps {
    kind: ConversationOperationFailureKind,
    stage: ConversationOperationFailureStage,
    effect: ProviderOperationEffect,
    explanation: &'static str,
    operation_id: OperationId,
    target: Option<SessionRef>,
    provider_error: ProviderErrorReference,
}

pub(super) fn invalid_setting_failure(
    operation_id: OperationId,
    target: Option<SessionRef>,
    setting: provider_session_machines::ProviderSettingKind,
    value: String,
    advertised: Vec<String>,
    disposition: acp_client_runtime::InvalidSettingSessionDisposition,
) -> ConversationOperationFailure {
    let (session_status, disposition, fallback_message) = match disposition {
        acp_client_runtime::InvalidSettingSessionDisposition::Closed => (
            "new Session was closed",
            InvalidSettingSessionDisposition::Closed,
            "invalid provider setting; see advertised values; new Session was closed",
        ),
        acp_client_runtime::InvalidSettingSessionDisposition::RemainsCreated => (
            "new Session remains created and idle",
            InvalidSettingSessionDisposition::RemainsCreated,
            "invalid provider setting; see advertised values; new Session remains created and idle",
        ),
    };
    let advertised_values = format_bounded_advertised_values(&advertised);
    let displayed_value = format_bounded_option_label(&value);
    let message = format!(
        "invalid provider setting {}={displayed_value}; advertised: {advertised_values}; {session_status}",
        setting.as_str()
    );
    let mut result = failure(
        ConversationOperationFailureKind::InvalidSetting,
        ConversationOperationFailureStage::Settlement,
        ProviderOperationEffect::Applied,
        fallback_message,
        operation_id,
        target,
    );
    if let Ok(message) = NonEmptyText::try_from(message) {
        result.message = message;
    }
    result.invalid_setting = Some(InvalidProviderSetting {
        setting: crate::provider_operation_settlement::provider_setting_name(setting),
        value,
        advertised,
        session_disposition: disposition,
    });
    result
}

fn format_bounded_option_label(label: &str) -> String {
    let mut label_characters = label.chars();
    let displayed_label = label_characters
        .by_ref()
        .take(MAX_SETTING_VALUE_DISPLAY_CHARS)
        .collect::<String>();
    let truncation_marker = if label_characters.next().is_some() {
        "…"
    } else {
        ""
    };
    format!("{displayed_label:?}{truncation_marker}")
}

fn format_bounded_advertised_values(advertised: &[String]) -> String {
    if advertised.is_empty() {
        return "none".to_owned();
    }

    let mut displayed_values = String::new();
    let mut displayed_count = 0;
    for option in advertised.iter().take(MAX_ADVERTISED_CHOICES) {
        let displayed_option = format_bounded_option_label(option);
        let separator = if displayed_count == 0 { "" } else { ", " };
        let omitted_after_option = advertised.len() - displayed_count - 1;
        let reserved_truncation_marker = if omitted_after_option == 0 {
            String::new()
        } else {
            format!(", … (+{omitted_after_option} more; see invalidSetting.advertised)")
        };
        let candidate_bytes = displayed_values.len()
            + separator.len()
            + displayed_option.len()
            + reserved_truncation_marker.len();
        if candidate_bytes > MAX_ADVERTISED_SUMMARY_BYTES {
            break;
        }

        displayed_values.push_str(separator);
        displayed_values.push_str(&displayed_option);
        displayed_count += 1;
    }

    let omitted_count = advertised.len() - displayed_count;
    if omitted_count > 0 {
        if displayed_count > 0 {
            displayed_values.push_str(", ");
        }
        displayed_values.push_str(&format!(
            "… (+{omitted_count} more; see invalidSetting.advertised)"
        ));
    }
    displayed_values
}

pub(super) fn admission_failure(
    operation_id: OperationId,
    message: &'static str,
    target: Option<SessionRef>,
) -> ConversationOperationFailure {
    failure(
        ConversationOperationFailureKind::Unavailable,
        ConversationOperationFailureStage::Admission,
        ProviderOperationEffect::None,
        message,
        operation_id,
        target,
    )
}

pub(super) fn restore_operation_failure(
    operation_id: OperationId,
    target: SessionRef,
    failure: ProviderWireFailure,
) -> ConversationOperationFailure {
    let reason = crate::provider_acp_session_loading::load_error_reason(failure);
    let mut result = runtime_failure(
        operation_id,
        Some(target),
        OperationFailureCause::HarnessCoded(failure),
    );
    if let Ok(message) = NonEmptyText::try_from(reason) {
        result.message = message;
    }
    result
}

pub(super) fn runtime_failure(
    operation_id: OperationId,
    target: Option<SessionRef>,
    cause: OperationFailureCause,
) -> ConversationOperationFailure {
    match cause {
        OperationFailureCause::HarnessCoded(failure) => {
            coded_runtime_failure(operation_id, target, failure)
        }
        OperationFailureCause::PromptRefused(PromptRefusal::Selection { kind, message }) => {
            let kind = match kind {
                PromptSelectionRefusal::InvalidSelection
                | PromptSelectionRefusal::MalformedMetadata => {
                    ConversationOperationFailureKind::InvalidRequest
                }
                PromptSelectionRefusal::Unsupported | PromptSelectionRefusal::SourceUnavailable => {
                    ConversationOperationFailureKind::UnsupportedCapability
                }
            };
            failure(
                kind,
                ConversationOperationFailureStage::Dispatch,
                ProviderOperationEffect::None,
                &message,
                operation_id,
                target,
            )
        }
        OperationFailureCause::CompactionUnconfirmed => failure(
            ConversationOperationFailureKind::ProviderRejected,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Applied,
            "explicit compaction was not confirmed",
            operation_id,
            target,
        ),
        OperationFailureCause::PromptOutputLimitExceeded => failure(
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
            "provider prompt output exceeded the retained output limit; cancellation was requested and settled",
            operation_id,
            target,
        ),
        OperationFailureCause::MessageTooLarge => failure(
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
            "provider frame exceeded the configured transport limit",
            operation_id,
            target,
        ),
        OperationFailureCause::PromptRefused(PromptRefusal::UnsupportedContent {
            content_type,
        }) => failure(
            ConversationOperationFailureKind::UnsupportedCapability,
            ConversationOperationFailureStage::Validation,
            ProviderOperationEffect::None,
            match content_type {
                "image" => "unsupportedContent{image}",
                "audio" => "unsupportedContent{audio}",
                "embeddedResource" => "unsupportedContent{embeddedResource}",
                _ => "unsupportedContent{unknown}",
            },
            operation_id,
            target,
        ),
        OperationFailureCause::ResponseUnavailable
        | OperationFailureCause::PromptRefused(PromptRefusal::Unclassified)
        | OperationFailureCause::InvalidSetting { .. }
        | OperationFailureCause::CreatedWithoutSettings { .. } => failure(
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
            "provider operation response was not available after dispatch",
            operation_id,
            target,
        ),
    }
}

fn coded_runtime_failure(
    operation_id: OperationId,
    target: Option<SessionRef>,
    failure: ProviderWireFailure,
) -> ConversationOperationFailure {
    let ProviderWireFailure {
        kind,
        code,
        correlation_id,
    } = failure;
    match kind {
        ProviderWireFailureKind::AuthenticationRequired => {
            correlated_provider_failure(CorrelatedProviderFailureProps {
                kind: ConversationOperationFailureKind::AuthenticationRequired,
                stage: ConversationOperationFailureStage::Binding,
                effect: ProviderOperationEffect::None,
                explanation: "provider authentication is required",
                operation_id,
                target,
                provider_error: ProviderErrorReference {
                    provider_code: code,
                    correlation_id,
                },
            })
        }
        ProviderWireFailureKind::SessionNotFound => {
            correlated_provider_failure(CorrelatedProviderFailureProps {
                kind: ConversationOperationFailureKind::ProviderSessionNotFound,
                stage: ConversationOperationFailureStage::Binding,
                effect: ProviderOperationEffect::None,
                explanation: "the provider no longer knows this session, so start a new session with the same setup",
                operation_id,
                target,
                provider_error: ProviderErrorReference {
                    provider_code: code,
                    correlation_id,
                },
            })
        }
        ProviderWireFailureKind::ResourceNotFound => {
            correlated_provider_failure(CorrelatedProviderFailureProps {
                kind: ConversationOperationFailureKind::NotFound,
                stage: ConversationOperationFailureStage::Binding,
                effect: ProviderOperationEffect::None,
                explanation: "provider resource was not found",
                operation_id,
                target,
                provider_error: ProviderErrorReference {
                    provider_code: code,
                    correlation_id,
                },
            })
        }
        ProviderWireFailureKind::RestoreFailed => {
            correlated_provider_failure(CorrelatedProviderFailureProps {
                kind: ConversationOperationFailureKind::OutcomeUnknown,
                stage: ConversationOperationFailureStage::Settlement,
                effect: ProviderOperationEffect::Unknown,
                explanation: "the provider failed while restoring this session, so retry it",
                operation_id,
                target,
                provider_error: ProviderErrorReference {
                    provider_code: code,
                    correlation_id,
                },
            })
        }
        ProviderWireFailureKind::RestoreRefused => {
            correlated_provider_failure(CorrelatedProviderFailureProps {
                kind: ConversationOperationFailureKind::ProviderRejected,
                stage: ConversationOperationFailureStage::Settlement,
                effect: ProviderOperationEffect::Unknown,
                explanation: "the provider refused to restore this session, so check the provider before retrying",
                operation_id,
                target,
                provider_error: ProviderErrorReference {
                    provider_code: code,
                    correlation_id,
                },
            })
        }
        ProviderWireFailureKind::UnsupportedMethod => {
            correlated_provider_failure(CorrelatedProviderFailureProps {
                kind: ConversationOperationFailureKind::UnsupportedCapability,
                stage: ConversationOperationFailureStage::Validation,
                effect: ProviderOperationEffect::None,
                explanation: "provider ACP method is unsupported",
                operation_id,
                target,
                provider_error: ProviderErrorReference {
                    provider_code: code,
                    correlation_id,
                },
            })
        }
        ProviderWireFailureKind::InvalidParams => {
            correlated_provider_failure(CorrelatedProviderFailureProps {
                kind: ConversationOperationFailureKind::InvalidRequest,
                stage: ConversationOperationFailureStage::Validation,
                effect: ProviderOperationEffect::None,
                explanation: "provider ACP parameters are invalid",
                operation_id,
                target,
                provider_error: ProviderErrorReference {
                    provider_code: code,
                    correlation_id,
                },
            })
        }
        ProviderWireFailureKind::RequestCancelled => {
            correlated_provider_failure(CorrelatedProviderFailureProps {
                kind: ConversationOperationFailureKind::ProviderRejected,
                stage: ConversationOperationFailureStage::Settlement,
                effect: ProviderOperationEffect::Unknown,
                explanation: "provider ACP request was cancelled",
                operation_id,
                target,
                provider_error: ProviderErrorReference {
                    provider_code: code,
                    correlation_id,
                },
            })
        }
        ProviderWireFailureKind::Rejected => {
            correlated_provider_failure(CorrelatedProviderFailureProps {
                kind: ConversationOperationFailureKind::ProviderRejected,
                stage: ConversationOperationFailureStage::Settlement,
                effect: ProviderOperationEffect::Unknown,
                explanation: "provider rejected the operation after dispatch",
                operation_id,
                target,
                provider_error: ProviderErrorReference {
                    provider_code: code,
                    correlation_id,
                },
            })
        }
    }
}

fn correlated_provider_failure(
    props: CorrelatedProviderFailureProps,
) -> ConversationOperationFailure {
    let CorrelatedProviderFailureProps {
        kind,
        stage,
        effect,
        explanation,
        operation_id,
        target,
        provider_error,
    } = props;
    let correlation_id = match DeliveryCorrelationId::try_from(
        provider_error.correlation_id.hyphenated().to_string(),
    ) {
        Ok(correlation_id) => correlation_id,
        Err(_) => {
            return failure_with_provider_code(
                kind,
                stage,
                effect,
                "provider failure was classified, but its diagnostic reference could not be represented",
                operation_id,
                target,
                Some(provider_error.provider_code),
            );
        }
    };
    let mut failure = failure_with_provider_code(
        kind,
        stage,
        effect,
        explanation,
        operation_id,
        target,
        Some(provider_error.provider_code),
    );
    if let Ok(message) = NonEmptyText::try_from(format!(
        "{explanation} (provider code {}; reference {})",
        provider_error.provider_code,
        correlation_id.as_str()
    )) {
        failure.message = message;
    }
    failure
}

#[cfg(test)]
#[path = "provider_operation_failure_test.rs"]
mod tests;

#[cfg(test)]
#[path = "operation_failure_cause_test.rs"]
mod cause_tests;
