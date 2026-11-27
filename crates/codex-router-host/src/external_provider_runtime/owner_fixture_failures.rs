//! ACP-shaped failure projections used by the owner fixtures.

use super::OperationFailureCause;
#[cfg(test)]
use super::failure;
use super::provider_create_result::provider_create_failure;
use crate::ExternalProviderRuntimeError;
use crate::session_owner_door::{OwnerCreateFailure, OwnerCreateReport};
use collaboration_contracts::CommandFailure;
use provider_session_machines::harness_lifecycle::OpeningFailure;

#[cfg(test)]
use super::provider_operation_failure::runtime_failure;
#[cfg(test)]
use super::provider_settings_control::setting_outcome_unknown;
#[cfg(test)]
use crate::external_provider_runtime::prompt_refusal;
#[cfg(test)]
use collaboration_protocol::{
    ConversationOperationFailure, ConversationOperationFailureKind,
    ConversationOperationFailureStage, OperationId, ProviderOperationEffect, ProviderSettingName,
    ProviderSettingsFailure, ProviderSettingsFailureKind, ProviderSettingsSetRequest, SessionRef,
};

pub(crate) fn fixture_created_session(
    result: Result<OwnerCreateReport, OwnerCreateFailure>,
) -> Result<String, ExternalProviderRuntimeError> {
    result.map(|report| report.session.0).map_err(|failure| {
        let diagnostic = match &failure {
            OwnerCreateFailure::Activation { .. } => {
                Some("the provider created the session but could not activate it".to_owned())
            }
            OwnerCreateFailure::Operation(message)
            | OwnerCreateFailure::Opening(OpeningFailure::Unavailable {
                diagnostic: message,
            }) => Some(message.clone()),
            _ => None,
        };
        match provider_create_failure(failure) {
            OperationFailureCause::HarnessCoded(failure) => failure.into(),
            OperationFailureCause::InvalidSetting {
                setting,
                value,
                advertised,
                provider_session_id,
                disposition,
            } => ExternalProviderRuntimeError::InvalidSetting {
                setting,
                value,
                advertised,
                provider_session_id,
                disposition,
            },
            _ => diagnostic.map_or(
                ExternalProviderRuntimeError::TransportFailure,
                ExternalProviderRuntimeError::Operation,
            ),
        }
    })
}

#[cfg(test)]
fn fixture_operation_cause(
    error: ExternalProviderRuntimeError,
) -> Result<OperationFailureCause, ExternalProviderRuntimeError> {
    if let Some(failure) = error.coded_wire_failure() {
        return Ok(OperationFailureCause::HarnessCoded(failure));
    }
    Ok(match error {
        error @ (ExternalProviderRuntimeError::Behavior(_)
        | ExternalProviderRuntimeError::UnsupportedContent { .. }) => {
            OperationFailureCause::PromptRefused(prompt_refusal(error))
        }
        ExternalProviderRuntimeError::TransportFailure
        | ExternalProviderRuntimeError::Operation(_) => OperationFailureCause::ResponseUnavailable,
        ExternalProviderRuntimeError::CompactionUnconfirmed => {
            OperationFailureCause::CompactionUnconfirmed
        }
        ExternalProviderRuntimeError::PromptOutputLimitExceeded => {
            OperationFailureCause::PromptOutputLimitExceeded
        }
        ExternalProviderRuntimeError::FrameLimitExceeded => OperationFailureCause::MessageTooLarge,
        ExternalProviderRuntimeError::InvalidSetting {
            setting,
            value,
            advertised,
            provider_session_id,
            disposition,
        } => OperationFailureCause::InvalidSetting {
            setting,
            value,
            advertised,
            provider_session_id,
            disposition,
        },
        ExternalProviderRuntimeError::CreatedWithoutSettings {
            provider_session_id,
            applied,
            failed,
            not_applied,
            permission_mode,
        } => OperationFailureCause::CreatedWithoutSettings {
            provider_session_id,
            applied,
            failed,
            not_applied,
            permission_mode,
        },
        other => return Err(other),
    })
}

#[cfg(test)]
pub(crate) fn legacy_runtime_failure(
    operation_id: OperationId,
    target: Option<SessionRef>,
    error: ExternalProviderRuntimeError,
) -> ConversationOperationFailure {
    let error = match fixture_operation_cause(error) {
        Ok(cause) => return runtime_failure(operation_id, target, cause),
        Err(error) => error,
    };
    match error {
        ExternalProviderRuntimeError::HistoryReplayAdmissionFailed => failure(
            ConversationOperationFailureKind::Unavailable,
            ConversationOperationFailureStage::Dispatch,
            ProviderOperationEffect::None,
            "session history replay admission failed before provider load was dispatched",
            operation_id,
            target,
        ),
        ExternalProviderRuntimeError::LocalBusy { answer } => failure(
            ConversationOperationFailureKind::Busy,
            ConversationOperationFailureStage::Validation,
            ProviderOperationEffect::None,
            &answer,
            operation_id,
            target,
        ),
        ExternalProviderRuntimeError::SettingsUnresolved { answer } => failure(
            ConversationOperationFailureKind::SettingsUnresolved,
            ConversationOperationFailureStage::Validation,
            ProviderOperationEffect::None,
            &answer,
            operation_id,
            target,
        ),
        // The session's state refused the request: its own answer names the
        // state and the valid next steps (D1 M8).
        ExternalProviderRuntimeError::SessionStateRefused { answer } => failure(
            ConversationOperationFailureKind::InvalidRequest,
            ConversationOperationFailureStage::Validation,
            ProviderOperationEffect::None,
            &answer,
            operation_id,
            target,
        ),
        // The Router closed the session while a load or resume was in
        // flight; the same refusal as one checked before it (D1 R14).
        ExternalProviderRuntimeError::SessionReleased => failure(
            ConversationOperationFailureKind::InvalidRequest,
            ConversationOperationFailureStage::Validation,
            ProviderOperationEffect::None,
            "the session is closed",
            operation_id,
            target,
        ),
        ExternalProviderRuntimeError::LocalNotFound
        | ExternalProviderRuntimeError::LocalCancelTargetMismatch => failure(
            ConversationOperationFailureKind::NotFound,
            ConversationOperationFailureStage::Validation,
            ProviderOperationEffect::None,
            "provider conversation operation is not active",
            operation_id,
            target,
        ),
        error @ (ExternalProviderRuntimeError::RestoreNoAnswer
        | ExternalProviderRuntimeError::RestoreDisconnected) => failure(
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
            &error.to_string(),
            operation_id,
            target,
        ),
        ExternalProviderRuntimeError::FrameDecodeFailure => failure(
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
            "provider frame could not be decoded or classified",
            operation_id,
            target,
        ),
        _ => failure(
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
            "provider operation response was not available after dispatch",
            operation_id,
            target,
        ),
    }
}

#[cfg(test)]
pub(crate) fn legacy_prompt_runtime_failure(
    operation_id: OperationId,
    target: Option<SessionRef>,
    error: ExternalProviderRuntimeError,
) -> ConversationOperationFailure {
    if matches!(error, ExternalProviderRuntimeError::TransportFailure) {
        return failure(
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
            "provider connection lost before the agent ended the turn (providerRetired)",
            operation_id,
            target,
        );
    }
    legacy_runtime_failure(operation_id, target, error)
}

#[cfg(test)]
pub(crate) fn setting_error(
    request: &ProviderSettingsSetRequest,
    error: ExternalProviderRuntimeError,
) -> ProviderSettingsFailure {
    match error {
        // Setting and value come from the runtime error, so a fast failure
        // after a model change says fast and off (D1 R9).
        ExternalProviderRuntimeError::InvalidSetting {
            setting,
            value,
            advertised,
            ..
        } => ProviderSettingsFailure {
            kind: ProviderSettingsFailureKind::InvalidSetting,
            stage: None,
            target: Some(request.target.clone()),
            message: "setting value requires an unambiguous advertised provider option".into(),
            setting: Some(crate::provider_operation_settlement::provider_setting_name(
                setting,
            )),
            value: Some(value),
            advertised,
        },
        error @ ExternalProviderRuntimeError::FastNotOffered => ProviderSettingsFailure {
            kind: ProviderSettingsFailureKind::ProviderRejected,
            stage: None,
            target: Some(request.target.clone()),
            message: error.to_string(),
            setting: Some(ProviderSettingName::Fast),
            value: Some("off".to_owned()),
            advertised: Vec::new(),
        },
        ExternalProviderRuntimeError::SettingFailed { setting, value, .. } => {
            ProviderSettingsFailure {
                kind: ProviderSettingsFailureKind::ProviderRejected,
                stage: None,
                target: Some(request.target.clone()),
                message: "provider rejected the setting; current reported values remain effective"
                    .into(),
                setting: Some(crate::provider_operation_settlement::provider_setting_name(
                    setting,
                )),
                value: Some(value),
                advertised: Vec::new(),
            }
        }
        ExternalProviderRuntimeError::SettingOutcomeUnknown { setting, value, .. } => {
            setting_outcome_unknown(&request.target, setting, value)
        }
        other => {
            let mut failure = plain_error(&request.target, other);
            if failure.kind == ProviderSettingsFailureKind::OutcomeUnknown {
                failure.setting = Some(request.setting);
                failure.value = Some(request.value.clone());
            }
            failure
        }
    }
}

#[cfg(test)]
fn plain_error(
    target: &SessionRef,
    error: ExternalProviderRuntimeError,
) -> ProviderSettingsFailure {
    let busy_answer;
    let (kind, message) = match error {
        // The session's answer names its state and the valid next steps (D1 M8).
        ExternalProviderRuntimeError::LocalBusy { answer } => {
            busy_answer = answer;
            (ProviderSettingsFailureKind::Busy, busy_answer.as_str())
        }
        ExternalProviderRuntimeError::LocalNotFound => (
            ProviderSettingsFailureKind::NotFound,
            "provider Session is not loaded",
        ),
        ExternalProviderRuntimeError::TransportFailure
        | ExternalProviderRuntimeError::FrameDecodeFailure
        | ExternalProviderRuntimeError::FrameLimitExceeded => (
            ProviderSettingsFailureKind::OutcomeUnknown,
            "provider setting outcome is unknown; Session remains gated",
        ),
        _ => (
            ProviderSettingsFailureKind::Unavailable,
            "provider settings action is unavailable",
        ),
    };
    settings_failure(kind, target, message)
}

#[cfg(test)]
fn settings_failure(
    kind: ProviderSettingsFailureKind,
    target: &SessionRef,
    message: &str,
) -> ProviderSettingsFailure {
    ProviderSettingsFailure {
        kind,
        stage: None,
        target: Some(target.clone()),
        message: message.into(),
        setting: None,
        value: None,
        advertised: Vec::new(),
    }
}

pub(crate) fn map_runtime_failure(error: ExternalProviderRuntimeError) -> CommandFailure {
    match error {
        ExternalProviderRuntimeError::Behavior(
            acp_client_runtime::ProviderError::InvalidSelection,
        ) => CommandFailure::InvalidSelection,
        ExternalProviderRuntimeError::Behavior(
            acp_client_runtime::ProviderError::Unsupported
            | acp_client_runtime::ProviderError::SourceUnavailable,
        ) => CommandFailure::Unsupported,
        ExternalProviderRuntimeError::LocalBusy { .. } => CommandFailure::Busy,
        // An agent that can't steer: a busy ACP prompt queues instead (D1 R12).
        ExternalProviderRuntimeError::UnsupportedSteering => {
            CommandFailure::UnsupportedOperation { operation: "steer" }
        }
        ExternalProviderRuntimeError::LocalNotFound
        | ExternalProviderRuntimeError::SessionReleased
        | ExternalProviderRuntimeError::ProviderSessionNotFound { .. } => {
            CommandFailure::SessionNotFound
        }
        ExternalProviderRuntimeError::SettingsUnresolved { .. } => {
            CommandFailure::SettingsUnresolved
        }
        ExternalProviderRuntimeError::UnsupportedContent { content_type } => {
            CommandFailure::UnsupportedContent(content_type)
        }
        ExternalProviderRuntimeError::UnsupportedCapability { capability } => {
            CommandFailure::UnsupportedOperation {
                operation: capability,
            }
        }
        _ => CommandFailure::ProviderFailure,
    }
}

#[cfg(test)]
fn legacy_load_error_outcome(
    failure: &acp_client_runtime::RestoreFailure,
) -> crate::provider_acp_session_loading::ProviderSessionLoadOutcome {
    use crate::provider_acp_session_loading::{
        ProviderSessionLoadOutcome, coded_load_outcome, load_failure,
    };
    use acp_client_runtime::RestorePhase;
    use provider_session_machines::session_owner::RestorePhase as OwnerPhase;

    if let Some(coded) = failure.error().coded_wire_failure() {
        return coded_load_outcome(coded);
    }
    let phase = failure.phase;
    let owner_phase = match phase {
        RestorePhase::Attempted => OwnerPhase::Attempted,
        RestorePhase::Preflight => OwnerPhase::Preflight,
    };
    match failure.error() {
        ExternalProviderRuntimeError::SessionReleased => {
            ProviderSessionLoadOutcome::ClosedWhileLoading { phase: owner_phase }
        }
        error => {
            let reason = match (error, phase) {
                (ExternalProviderRuntimeError::SinkClosed | ExternalProviderRuntimeError::HistoryReplayUnavailable, RestorePhase::Attempted) =>
                    "the Router could not publish the restored session's replay or settings, so reconnect and retry this session".to_owned(),
                (ExternalProviderRuntimeError::RestoreNoAnswer | ExternalProviderRuntimeError::RestoreDisconnected, _)
                | (_, RestorePhase::Preflight) => error.to_string(),
                (_, RestorePhase::Attempted) => format!("{error}, so retry this session"),
            };
            load_failure(owner_phase, reason)
        }
    }
}

#[cfg(test)]
#[path = "owner_fixture_failures_test.rs"]
mod tests;
