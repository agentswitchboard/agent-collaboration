//! Project neutral create settlements into the current provider API.

use super::{
    SessionOwnerDoor,
    creation::{OwnerCreateFailure, OwnerCreateReport},
};
use acp_client_runtime::{
    ExternalProviderCreatedSession, ExternalProviderRuntimeError, InvalidSettingSessionDisposition,
};
use collaboration_protocol::{EndpointRef, OperationId};
use provider_session_machines::harness_lifecycle::{
    CreatedSettingsFailure, InvalidRequestedSetting, OpenSetup, OpeningFailure,
    RequestedProviderSettings,
};
use std::sync::Arc;

pub(crate) struct ProviderCreatedOwnerSetup {
    pub(crate) created: ExternalProviderCreatedSession,
    pub(crate) settings_failure: Option<ExternalProviderRuntimeError>,
}

impl SessionOwnerDoor {
    pub(crate) async fn create(
        self: &Arc<Self>,
        endpoint: EndpointRef,
        operation: OperationId,
        setup: OpenSetup,
        requested: RequestedProviderSettings,
        work_roots: crate::provider_work_roots::ProviderWorkRoots,
    ) -> Result<ProviderCreatedOwnerSetup, ExternalProviderRuntimeError> {
        self.create_with_harness(endpoint, operation, setup, requested, work_roots)
            .await
            .map(provider_created_setup)
            .map_err(provider_create_failure)
    }
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
        } => ExternalProviderRuntimeError::CreatedWithoutSettings {
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

fn provider_create_failure(failure: OwnerCreateFailure) -> ExternalProviderRuntimeError {
    match failure {
        OwnerCreateFailure::TransportUnavailable => ExternalProviderRuntimeError::TransportFailure,
        OwnerCreateFailure::Operation(message) => ExternalProviderRuntimeError::Operation(message),
        OwnerCreateFailure::ClosedAfterInvalidSetting(failure) => {
            invalid_setting(failure, InvalidSettingSessionDisposition::Closed)
        }
        OwnerCreateFailure::Opening(failure) => match failure {
            OpeningFailure::Lost {
                reported: Some(failure),
            }
            | OpeningFailure::Refused(failure)
            | OpeningFailure::ProviderError(failure) => failure.into(),
            OpeningFailure::Unavailable { diagnostic } => {
                ExternalProviderRuntimeError::Operation(diagnostic)
            }
            OpeningFailure::Lost { reported: None }
            | OpeningFailure::Disconnected
            | OpeningFailure::NoAnswer
            | OpeningFailure::ReplayNotKnown
            | OpeningFailure::Closed => ExternalProviderRuntimeError::TransportFailure,
        },
    }
}

fn invalid_setting(
    failure: InvalidRequestedSetting,
    disposition: InvalidSettingSessionDisposition,
) -> ExternalProviderRuntimeError {
    ExternalProviderRuntimeError::InvalidSetting {
        setting: failure.kind,
        value: failure.value,
        advertised: failure.advertised,
        provider_session_id: failure.session.0,
        disposition,
    }
}
