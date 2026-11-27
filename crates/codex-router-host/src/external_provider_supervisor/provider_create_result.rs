//! Project neutral create settlements into the current provider API.

use super::OperationFailureCause;
use crate::session_owner_door::{OwnerCreateFailure, OwnerCreateReport};
use acp_client_runtime::{ExternalProviderCreatedSession, InvalidSettingSessionDisposition};
use provider_session_machines::harness_lifecycle::{
    CreatedSettingsFailure, InvalidRequestedSetting, OpeningFailure,
};

pub(crate) struct ProviderCreatedOwnerSetup {
    pub(crate) created: ExternalProviderCreatedSession,
    pub(crate) settings_failure: Option<OperationFailureCause>,
}

pub(crate) fn project_owner_create(
    result: Result<OwnerCreateReport, OwnerCreateFailure>,
) -> Result<ProviderCreatedOwnerSetup, OperationFailureCause> {
    result
        .map(provider_created_setup)
        .map_err(provider_create_failure)
}

fn provider_created_setup(report: OwnerCreateReport) -> ProviderCreatedOwnerSetup {
    let OwnerCreateReport {
        session,
        effective_settings,
        permission_mode,
        settings_failure,
        ..
    } = report;
    let settings_failure = settings_failure.map(|failure| match failure {
        CreatedSettingsFailure::Invalid(failure) => {
            invalid_setting(failure, InvalidSettingSessionDisposition::RemainsCreated)
        }
        CreatedSettingsFailure::Partial {
            applied,
            failed,
            not_applied,
        } => OperationFailureCause::CreatedWithoutSettings {
            provider_session_id: session.0.clone(),
            applied,
            failed,
            not_applied,
            permission_mode: Box::new(permission_mode.clone()),
        },
    });
    ProviderCreatedOwnerSetup {
        created: ExternalProviderCreatedSession {
            provider_session_id: session.0,
            effective_settings,
            permission_mode,
        },
        settings_failure,
    }
}

pub(crate) fn provider_create_failure(failure: OwnerCreateFailure) -> OperationFailureCause {
    match failure {
        // Transport loss, failed activation, harness diagnostics, and uncoded openings
        // share the same unavailable-response row of the caller’s failure table.
        OwnerCreateFailure::TransportUnavailable => OperationFailureCause::ResponseUnavailable,
        OwnerCreateFailure::Operation(diagnostic) => {
            drop(diagnostic);
            OperationFailureCause::ResponseUnavailable
        }
        OwnerCreateFailure::Activation { reason, .. } => {
            tracing::debug!(?reason, "session harness refused create activation");
            OperationFailureCause::ResponseUnavailable
        }
        OwnerCreateFailure::ClosedAfterInvalidSetting(failure) => {
            invalid_setting(failure, InvalidSettingSessionDisposition::Closed)
        }
        OwnerCreateFailure::Opening(failure) => match failure {
            OpeningFailure::Lost {
                reported: Some(failure),
            }
            | OpeningFailure::Refused(failure)
            | OpeningFailure::ProviderError(failure) => {
                OperationFailureCause::HarnessCoded(failure)
            }
            OpeningFailure::Lost { reported: None }
            | OpeningFailure::Disconnected
            | OpeningFailure::OutcomeUnknown
            | OpeningFailure::NoAnswer
            | OpeningFailure::ReplayNotKnown
            | OpeningFailure::Closed
            | OpeningFailure::Unavailable { .. } => OperationFailureCause::ResponseUnavailable,
        },
    }
}

fn invalid_setting(
    failure: InvalidRequestedSetting,
    disposition: InvalidSettingSessionDisposition,
) -> OperationFailureCause {
    OperationFailureCause::InvalidSetting {
        setting: failure.kind,
        value: failure.value,
        advertised: failure.advertised,
        provider_session_id: failure.session.0,
        disposition,
    }
}
