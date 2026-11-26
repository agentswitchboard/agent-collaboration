//! Configure a newly acquired ACP session under its adopted owner.

use crate::provider_setting_application::{FAST_OFF, fast_off_requested};
use crate::{
    ExternalProviderCreatedSession, ExternalProviderRuntimeError, FailedProviderSetting,
    InvalidSettingSessionDisposition, RequestedProviderSettings,
    owner_session_open::OwnerOpenedSession,
    provider_setting_application::{
        InitialSettingTarget, InitialSettingsOutcome, SettingSetupFailure, apply_initial_settings,
    },
};
use agent_client_protocol::{Agent, schema::v1::CloseSessionRequest};
use provider_session_machines::{
    ProviderSettingKind,
    harness_lifecycle::{
        CreateAttempt, CreatedSettingsFailure, CreatedSetupFact, CreatedSetupReport,
        HarnessSessionId, SessionCreatedSetupFact, SetupReport,
    },
    session_owner::{
        CreatedSessionFacts, InitialSettingFailure, InitialSettingObservation, ObservedSetting,
        SettingEvidence,
    },
    session_setting_machine::{SettingApplication, SettingValueId},
};
use router_clock::RuntimeClock;
use session_event_model::ProviderSettingFailureReason;
use std::collections::BTreeMap;
use std::sync::Arc;
use switchboard_harness_contracts::InvalidRequestedSetting;
use uuid::Uuid;

pub struct OwnerCreatedSetup {
    pub created: ExternalProviderCreatedSession,
    pub settings_failure: Option<ExternalProviderRuntimeError>,
    keep_fast_off: bool,
    failed_setting: Option<InitialSettingFailure>,
}

struct ConfiguredCreate {
    permission_mode: crate::PermissionModeReport,
    keep_fast_off: bool,
    failed_setting: Option<InitialSettingFailure>,
    failure: Option<CreatedSettingsFailure<ProviderSettingFailureReason>>,
}

impl OwnerCreatedSetup {
    #[must_use]
    pub fn owner_facts(&self, session: &OwnerOpenedSession) -> CreatedSessionFacts {
        CreatedSessionFacts {
            capabilities: session.capabilities().to_session_model(),
            settings: session.settings(),
            observed: catalogue_evidence(session.catalog()),
            catalogue: Arc::new(session.catalog().clone()),
            router_auto_requested: self.created.permission_mode.requested.as_deref()
                == Some(crate::AUTO_PERMISSION_MODE),
            mode_reason: self.created.permission_mode.reason,
            keep_fast_off: self.keep_fast_off,
            failed_setting: self.failed_setting.clone(),
        }
    }
}

impl OwnerOpenedSession {
    pub async fn configure_create(
        &mut self,
        requested: &RequestedProviderSettings,
        clock: &dyn RuntimeClock,
    ) -> Result<OwnerCreatedSetup, ExternalProviderRuntimeError> {
        let ConfiguredCreate {
            permission_mode,
            keep_fast_off,
            failed_setting,
            failure,
        } = self
            .configure_create_setup(requested, clock)
            .await
            .map_err(|failure| {
                invalid_setting_error(failure, InvalidSettingSessionDisposition::Closed)
            })?;
        let provider_session_id = self.provider_id();
        let settings_failure = failure.map(|failure| match failure {
            CreatedSettingsFailure::Invalid(failure) => {
                invalid_setting_error(failure, InvalidSettingSessionDisposition::RemainsCreated)
            }
            CreatedSettingsFailure::Partial {
                applied,
                failed,
                not_applied,
            } => ExternalProviderRuntimeError::CreatedWithoutSettings {
                provider_session_id: provider_session_id.clone(),
                applied,
                failed,
                not_applied,
                permission_mode: Box::new(permission_mode.clone()),
            },
        });
        Ok(OwnerCreatedSetup {
            created: ExternalProviderCreatedSession {
                provider_session_id,
                effective_settings: self.catalog.effective_settings(),
                permission_mode,
            },
            settings_failure,
            keep_fast_off,
            failed_setting,
        })
    }

    pub(crate) async fn configure_lifecycle_create(
        &mut self,
        attempt: CreateAttempt,
        requested: &RequestedProviderSettings,
        clock: &dyn RuntimeClock,
    ) -> SessionCreatedSetupFact {
        let ConfiguredCreate {
            permission_mode,
            keep_fast_off,
            failed_setting,
            failure,
        } = match self.configure_create_setup(requested, clock).await {
            Ok(configured) => configured,
            Err(failure) => {
                return CreatedSetupFact::ClosedAfterInvalidSetting { attempt, failure };
            }
        };
        let report = Box::new(CreatedSetupReport {
            setup: SetupReport {
                capabilities: self.capabilities().to_session_model(),
                settings: self.settings(),
                observed: catalogue_evidence(self.catalog()),
            },
            catalogue: self.catalog.clone().into_record(),
            effective_settings: self.catalog.effective_settings(),
            router_auto_requested: permission_mode.requested.as_deref()
                == Some(crate::AUTO_PERMISSION_MODE),
            permission_mode,
            keep_fast_off,
            failed_setting,
        });
        match failure {
            Some(failure) => CreatedSetupFact::RemainsCreated {
                attempt,
                report,
                failure,
            },
            None => CreatedSetupFact::Configured { attempt, report },
        }
    }

    async fn configure_create_setup(
        &mut self,
        requested: &RequestedProviderSettings,
        clock: &dyn RuntimeClock,
    ) -> Result<ConfiguredCreate, InvalidRequestedSetting> {
        let keep_fast_off = fast_off_requested(requested.fast, &self.catalog);
        let InitialSettingsOutcome {
            applied,
            failure,
            permission_mode,
        } = apply_initial_settings(
            InitialSettingTarget {
                connection: &self.connection,
                session_id: &self.provider_id,
            },
            requested,
            &mut self.catalog,
            self.behavior.as_ref(),
            clock,
        )
        .await;
        let failed_setting = failure.as_ref().map(|failure| {
            let (kind, attempted, observation) = match failure {
                SettingSetupFailure::Invalid { kind, value, .. } => (
                    *kind,
                    value,
                    InitialSettingObservation::Known {
                        confirmed_current: false,
                    },
                ),
                SettingSetupFailure::Agent {
                    kind,
                    value,
                    confirmed_current,
                    ..
                } => (
                    *kind,
                    value,
                    InitialSettingObservation::Known {
                        confirmed_current: *confirmed_current,
                    },
                ),
                SettingSetupFailure::Uncertain { kind, value, .. } => {
                    (*kind, value, InitialSettingObservation::Unknown)
                }
            };
            InitialSettingFailure {
                kind,
                attempted: SettingValueId(attempted.clone()),
                application: SettingApplication(Uuid::now_v7()),
                observation,
            }
        });
        if failed_setting.as_ref().is_some_and(|failure| {
            failure.kind == ProviderSettingKind::Mode
                && failure.observation == InitialSettingObservation::Unknown
        }) {
            self.catalog.mode_observation =
                provider_session_machines::ProviderModeObservation::Unconfirmed;
        }
        let failure = match failure {
            None => None,
            Some(SettingSetupFailure::Invalid {
                kind,
                value,
                advertised,
            }) if applied.is_empty() => {
                let closed = if self.capabilities.supports_close {
                    self.connection
                        .send_request_to(Agent, CloseSessionRequest::new(self.provider_id.clone()))
                        .block_task()
                        .await
                        .is_ok()
                } else {
                    false
                };
                let failure = InvalidRequestedSetting {
                    session: HarnessSessionId(self.provider_id()),
                    kind,
                    value,
                    advertised,
                };
                if closed {
                    return Err(failure);
                }
                Some(CreatedSettingsFailure::Invalid(failure))
            }
            Some(failure) => {
                let failed = match failure {
                    SettingSetupFailure::Invalid { kind, value, .. } => FailedProviderSetting {
                        kind,
                        value,
                        reason: ProviderSettingFailureReason::InvalidSetting,
                    },
                    SettingSetupFailure::Agent {
                        kind,
                        value,
                        reason,
                        ..
                    } => FailedProviderSetting {
                        kind,
                        value,
                        reason: reason.into(),
                    },
                    SettingSetupFailure::Uncertain { kind, value, .. } => FailedProviderSetting {
                        kind,
                        value,
                        reason: ProviderSettingFailureReason::OutcomeUnknown,
                    },
                };
                Some(CreatedSettingsFailure::Partial {
                    not_applied: untried_settings(requested, &self.catalog, failed.kind)
                        .into_boxed_slice(),
                    applied: applied.into_boxed_slice(),
                    failed,
                })
            }
        };
        Ok(ConfiguredCreate {
            permission_mode,
            keep_fast_off,
            failed_setting,
            failure,
        })
    }
}

fn invalid_setting_error(
    failure: InvalidRequestedSetting,
    disposition: InvalidSettingSessionDisposition,
) -> ExternalProviderRuntimeError {
    ExternalProviderRuntimeError::InvalidSetting {
        provider_session_id: failure.session.0,
        setting: failure.kind,
        value: failure.value,
        advertised: failure.advertised,
        disposition,
    }
}

pub fn catalogue_evidence(
    catalog: &provider_session_machines::ProviderSettingsCatalog,
) -> BTreeMap<ProviderSettingKind, SettingEvidence> {
    let observed = catalog.effective_settings();
    [
        (ProviderSettingKind::Mode, observed.mode),
        (ProviderSettingKind::Model, observed.model),
        (ProviderSettingKind::Effort, observed.effort),
        (
            ProviderSettingKind::Fast,
            match catalog.fast_observed() {
                provider_session_machines::FastObserved::Off => Some("off".to_owned()),
                provider_session_machines::FastObserved::On => Some("on".to_owned()),
                provider_session_machines::FastObserved::NoOption => None,
            },
        ),
    ]
    .into_iter()
    .filter_map(|(kind, value)| {
        value.map(|value| {
            (
                kind,
                SettingEvidence {
                    value: ObservedSetting::Confirmed(SettingValueId(value)),
                    cause: None,
                },
            )
        })
    })
    .collect()
}

fn untried_settings(
    requested: &crate::RequestedProviderSettings,
    catalog: &provider_session_machines::ProviderSettingsCatalog,
    failed_kind: provider_session_machines::ProviderSettingKind,
) -> Vec<crate::NotAppliedProviderSetting> {
    let fast_off = FAST_OFF.to_owned();
    let fast = fast_off_requested(requested.fast, catalog).then_some(&fast_off);
    let mut after_failure = false;
    let mut untried = Vec::new();
    for (kind, value) in [
        (
            provider_session_machines::ProviderSettingKind::Mode,
            requested.mode.as_ref(),
        ),
        (
            provider_session_machines::ProviderSettingKind::Model,
            requested.model.as_ref(),
        ),
        (
            provider_session_machines::ProviderSettingKind::Effort,
            requested.effort.as_ref(),
        ),
        (provider_session_machines::ProviderSettingKind::Fast, fast),
    ] {
        if after_failure && let Some(value) = value {
            untried.push(crate::NotAppliedProviderSetting {
                kind,
                value: value.clone(),
            });
        }
        if kind == failed_kind {
            after_failure = true;
        }
    }
    untried
}
