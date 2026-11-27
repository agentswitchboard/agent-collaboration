//! Immediate settings actions use the durable Session inventory for authority.

use provider_session_machines::ProviderSettingKind;

use super::ExternalProviderSupervisor;
use crate::provider_operation_settlement::effective_settings;
use collaboration_protocol::{
    ProviderIdentity, ProviderSettingName, ProviderSettingsAcceptRequest, ProviderSettingsFailure,
    ProviderSettingsFailureKind, ProviderSettingsMappingStatus, ProviderSettingsResult,
    ProviderSettingsSetRequest, SessionRef,
};
use provider_session_machines::session_lifecycle_answers::state_answer;
use switchboard_session_store::ProviderSessionRecord;

#[expect(
    clippy::result_large_err,
    reason = "The typed failure retains the full published operation payload."
)]
pub(super) async fn set(
    supervisor: &ExternalProviderSupervisor,
    request: ProviderSettingsSetRequest,
) -> Result<ProviderSettingsResult, ProviderSettingsFailure> {
    match apply(
        supervisor,
        request,
        session_event_model::SettingApplicationIntent::ExplicitChange,
        None,
    )
    .await
    {
        Ok(result) => Ok(result),
        Err(SettingControlFailure::Provider(error)) => Err(*error),
        Err(
            SettingControlFailure::ContextChanged(target)
            | SettingControlFailure::Unresolved(target),
        ) => Err(failure(
            ProviderSettingsFailureKind::Unavailable,
            &target,
            "setting context unavailable",
        )),
    }
}

pub(super) enum SettingControlFailure {
    Provider(Box<ProviderSettingsFailure>),
    ContextChanged(SessionRef),
    Unresolved(SessionRef),
}
impl From<ProviderSettingsFailure> for SettingControlFailure {
    fn from(error: ProviderSettingsFailure) -> Self {
        Self::Provider(Box::new(error))
    }
}

pub(super) async fn apply(
    supervisor: &ExternalProviderSupervisor,
    request: ProviderSettingsSetRequest,
    intent: session_event_model::SettingApplicationIntent,
    expected_model: Option<String>,
) -> Result<ProviderSettingsResult, SettingControlFailure> {
    let record = authorized_session(supervisor, &request.target, &request.actor).await?;
    // One check for every caller, before anything reaches the agent (D1 R7).
    if request.setting == ProviderSettingName::Fast
        && !matches!(request.value.as_str(), "off" | "auto")
    {
        return Err(ProviderSettingsFailure {
            kind: ProviderSettingsFailureKind::InvalidSetting,
            stage: None,
            target: Some(request.target.clone()),
            message: "fast accepts off or auto".into(),
            setting: Some(ProviderSettingName::Fast),
            value: Some(request.value.clone()),
            advertised: vec!["off".into(), "auto".into()],
        }
        .into());
    }
    if request.setting == ProviderSettingName::Mode
        && supervisor
            .provider_kind_for(&request.target.endpoint)
            .is_some_and(|provider| {
                super::provider_full_access_guard::is_full_access_mode(provider, &request.value)
            })
    {
        return Err(ProviderSettingsFailure {
            kind: ProviderSettingsFailureKind::InvalidSetting,
            stage: None,
            target: Some(request.target.clone()),
            message: super::provider_full_access_guard::FULL_ACCESS_REFUSAL.into(),
            setting: Some(ProviderSettingName::Mode),
            value: Some(request.value.clone()),
            advertised: Vec::new(),
        }
        .into());
    }
    if request.value.trim().is_empty() {
        return Err(failure(
            ProviderSettingsFailureKind::InvalidSetting,
            &request.target,
            "setting value must be nonempty",
        )
        .into());
    }
    use provider_session_machines::{
        session_lifecycle_machine::SessionLifecycle,
        session_owner::{
            OwnerSetting, RemoteEffect, RunAttempt, SessionOwnerCommand, SessionOwnerRefusal,
            SessionOwnerRejection, SessionOwnerReply,
        },
        session_setting_machine::{SettingApplication, SettingValueId},
    };
    let owner = supervisor.session_owner_for(&request.target).map_err(|_| {
        failure(
            ProviderSettingsFailureKind::Unavailable,
            &request.target,
            "provider connection is unavailable",
        )
    })?;
    let lifecycle = owner.snapshots().borrow().lifecycle;
    let SessionLifecycle::Live { generation } = lifecycle else {
        return Err(failure(
            ProviderSettingsFailureKind::NotFound,
            &request.target,
            &state_answer(&lifecycle),
        )
        .into());
    };
    let kind = match request.setting {
        ProviderSettingName::Mode => provider_session_machines::ProviderSettingKind::Mode,
        ProviderSettingName::Model => provider_session_machines::ProviderSettingKind::Model,
        ProviderSettingName::Effort => provider_session_machines::ProviderSettingKind::Effort,
        ProviderSettingName::Fast => provider_session_machines::ProviderSettingKind::Fast,
    };
    if intent == session_event_model::SettingApplicationIntent::ExplicitChange
        && !(kind == provider_session_machines::ProviderSettingKind::Mode
            && request.value == acp_client_runtime::AUTO_PERMISSION_MODE)
        && let Some(advertised) = owner
            .catalog()
            .await
            .and_then(|catalog| catalog.unavailable_selection(kind, &request.value))
    {
        return Err(ProviderSettingsFailure {
            kind: ProviderSettingsFailureKind::InvalidSetting,
            stage: Some(collaboration_protocol::ConversationOperationFailureStage::Validation),
            target: Some(request.target),
            message: if kind == provider_session_machines::ProviderSettingKind::Fast {
                "fast is not offered for the current model"
            } else {
                "setting value is not offered for this session"
            }
            .to_owned(),
            setting: Some(request.setting),
            value: Some(request.value),
            advertised,
        }
        .into());
    }
    let correlation = uuid::Uuid::now_v7();
    let setting = OwnerSetting {
        change: RunAttempt {
            generation,
            attempt: SettingApplication(correlation),
        },
        kind,
    };
    let input = session_event_model::InputId::generate();
    let value = SettingValueId(request.value.clone());
    let command = match intent {
        session_event_model::SettingApplicationIntent::ExplicitChange => {
            owner.expect_confirmed_mode(setting, request.value.clone());
            SessionOwnerCommand::Set {
                input,
                setting,
                value,
                expected_model: expected_model.map(SettingValueId),
            }
        }
        session_event_model::SettingApplicationIntent::TurnContext => {
            SessionOwnerCommand::SetInContext {
                input,
                setting,
                value,
                expected_model: expected_model.map(SettingValueId),
                reuse_confirmed: supervisor
                    .inner
                    .bindings
                    .get(&request.target.endpoint)
                    .is_some_and(|binding| binding.connection.reuses_setting_confirmation(kind)),
            }
        }
    };
    let applied = owner
        .settle(correlation, command)
        .await
        .map_err(|error| match error {
            crate::session_owner_door::OwnerDoorError::Refused {
                rejection:
                    SessionOwnerRejection::Refused(SessionOwnerRefusal::SettingContextChanged),
                ..
            } => SettingControlFailure::ContextChanged(request.target.clone()),
            crate::session_owner_door::OwnerDoorError::Refused {
                rejection: SessionOwnerRejection::Refused(SessionOwnerRefusal::SettingsUnresolved),
                ..
            } => SettingControlFailure::Unresolved(request.target.clone()),
            other => failure(
                ProviderSettingsFailureKind::Busy,
                &request.target,
                &super::provider_owner_operations::derived_owner_refusal(&other)
                    .unwrap_or_else(|| other.to_string()),
            )
            .into(),
        })?;
    let remote = match applied.reply {
        SessionOwnerReply::SettingKept { .. } => {
            let observed = snapshot_settings(&owner.snapshots().borrow());
            return Ok(result(request.target, record, observed));
        }
        SessionOwnerReply::SettingFinished { remote, .. } => remote,
        SessionOwnerReply::ModelFastFinished { fast, .. } => {
            use provider_session_machines::session_owner::ModelFastOutcome;
            let failure = match fast {
                ModelFastOutcome::NotOffered => Some((
                    ProviderSettingsFailureKind::InvalidSetting,
                    "fast is not offered for the current model",
                )),
                ModelFastOutcome::Refused => Some((
                    ProviderSettingsFailureKind::ProviderRejected,
                    "the provider refused fast off after the model change",
                )),
                ModelFastOutcome::Unconfirmed => Some((
                    ProviderSettingsFailureKind::OutcomeUnknown,
                    "the provider did not confirm fast off after the model change",
                )),
                ModelFastOutcome::ConfirmedOff => None,
            };
            if let Some((kind, message)) = failure {
                return Err(ProviderSettingsFailure {
                    kind,
                    stage: Some(
                        collaboration_protocol::ConversationOperationFailureStage::Settlement,
                    ),
                    target: Some(request.target),
                    message: message.to_owned(),
                    setting: Some(ProviderSettingName::Fast),
                    value: Some("off".to_owned()),
                    advertised: Vec::new(),
                }
                .into());
            }
            RemoteEffect::Applied
        }
        SessionOwnerReply::SettingNotSent {
            refusal: SessionOwnerRefusal::SettingContextChanged,
            ..
        } => return Err(SettingControlFailure::ContextChanged(request.target)),
        SessionOwnerReply::SettingNotSent {
            refusal: SessionOwnerRefusal::SettingsUnresolved,
            ..
        } => return Err(SettingControlFailure::Unresolved(request.target)),
        SessionOwnerReply::SettingNotSent { .. } => RemoteEffect::None,
        _ => {
            return Err(failure(
                ProviderSettingsFailureKind::Unavailable,
                &request.target,
                "the session owner returned another result",
            )
            .into());
        }
    };
    match remote {
        RemoteEffect::Unknown => {
            return Err(setting_outcome_unknown(&request.target, kind, request.value).into());
        }
        RemoteEffect::None
            if !(request.setting == ProviderSettingName::Fast && request.value == "auto") =>
        {
            return Err(failure(
                ProviderSettingsFailureKind::ProviderRejected,
                &request.target,
                "the provider did not apply the setting",
            )
            .into());
        }
        _ => {}
    }
    let observed = snapshot_settings(&owner.snapshots().borrow());
    Ok(result(request.target, record, observed))
}
#[expect(
    clippy::result_large_err,
    reason = "The typed failure retains the full published operation payload."
)]
pub(super) async fn accept(
    supervisor: &ExternalProviderSupervisor,
    request: ProviderSettingsAcceptRequest,
) -> Result<ProviderSettingsResult, ProviderSettingsFailure> {
    let record = authorized_session(supervisor, &request.target, &request.actor).await?;
    use provider_session_machines::{
        session_lifecycle_machine::SessionLifecycle, session_owner::SessionOwnerCommand,
    };
    let owner = supervisor.session_owner_for(&request.target).map_err(|_| {
        failure(
            ProviderSettingsFailureKind::Unavailable,
            &request.target,
            "provider connection is unavailable",
        )
    })?;
    let lifecycle = owner.snapshots().borrow().lifecycle;
    let SessionLifecycle::Live { generation } = lifecycle else {
        return Err(failure(
            ProviderSettingsFailureKind::NotFound,
            &request.target,
            &state_answer(&lifecycle),
        ));
    };
    owner
        .admit(SessionOwnerCommand::AcceptSettings { generation })
        .await
        .map_err(|error| {
            failure(
                ProviderSettingsFailureKind::OutcomeUnknown,
                &request.target,
                &error.to_string(),
            )
        })?;
    let observed = snapshot_settings(&owner.snapshots().borrow());
    Ok(result(request.target, record, observed))
}

#[expect(
    clippy::result_large_err,
    reason = "The typed failure retains the full published operation payload."
)]
async fn authorized_session(
    supervisor: &ExternalProviderSupervisor,
    target: &SessionRef,
    actor: &ProviderIdentity,
) -> Result<ProviderSessionRecord, ProviderSettingsFailure> {
    let record = supervisor
        .inner
        .store
        .lock()
        .await
        .session_record(target)
        .await
        .map_err(|_| {
            failure(
                ProviderSettingsFailureKind::Unavailable,
                target,
                "provider Session inventory is unavailable",
            )
        })?
        .ok_or_else(|| {
            failure(
                ProviderSettingsFailureKind::NotFound,
                target,
                "provider Session is unknown",
            )
        })?;
    if actor != &record.created_by && actor != &record.approver {
        return Err(failure(
            ProviderSettingsFailureKind::WrongActor,
            target,
            "only the creator or Approver may change Session settings",
        ));
    }
    Ok(record)
}

fn result(
    target: SessionRef,
    record: ProviderSessionRecord,
    observed: provider_session_machines::EffectiveProviderSettings,
) -> ProviderSettingsResult {
    let mut effective = effective_settings(record.requested_policy);
    effective.mapping_status = ProviderSettingsMappingStatus::Verified;
    effective.mode = observed.mode;
    effective.model = observed.model;
    effective.effort = observed.effort;
    effective.fast = observed
        .fast
        .map(crate::provider_operation_settlement::provider_fast_state);
    ProviderSettingsResult {
        target,
        effective_settings: effective,
    }
}

pub(super) fn setting_outcome_unknown(
    target: &SessionRef,
    setting: ProviderSettingKind,
    value: String,
) -> ProviderSettingsFailure {
    ProviderSettingsFailure {
        kind: ProviderSettingsFailureKind::OutcomeUnknown,
        stage: None,
        target: Some(target.clone()),
        message: "provider setting outcome is unknown; Session remains gated".into(),
        setting: Some(crate::provider_operation_settlement::provider_setting_name(
            setting,
        )),
        value: Some(value),
        advertised: Vec::new(),
    }
}

fn failure(
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

impl ExternalProviderSupervisor {
    pub(crate) async fn apply_setting_command(
        &self,
        request: ProviderSettingsSetRequest,
        intent: session_event_model::SettingApplicationIntent,
        expected_model: Option<String>,
    ) -> Result<(), collaboration_contracts::CommandFailure> {
        use collaboration_contracts::CommandFailure;

        apply(self, request, intent, expected_model)
            .await
            .map(|_| ())
            .map_err(|error| match error {
                SettingControlFailure::ContextChanged(_) => CommandFailure::SettingsContextChanged,
                SettingControlFailure::Unresolved(_) => CommandFailure::SettingsUnresolved,
                SettingControlFailure::Provider(error) => match error.kind {
                    collaboration_protocol::ProviderSettingsFailureKind::WrongActor => {
                        CommandFailure::UnauthorizedActor
                    }
                    collaboration_protocol::ProviderSettingsFailureKind::NotFound => {
                        CommandFailure::SessionNotFound
                    }
                    collaboration_protocol::ProviderSettingsFailureKind::Busy => {
                        CommandFailure::Busy
                    }
                    collaboration_protocol::ProviderSettingsFailureKind::InvalidSetting => {
                        CommandFailure::InvalidSetting {
                            advertised: error.advertised,
                        }
                    }
                    _ => CommandFailure::ProviderFailure,
                },
            })
    }
}

#[cfg(test)]
#[path = "provider_settings_control_test.rs"]
mod tests;

pub(crate) fn snapshot_settings(
    snapshot: &provider_session_machines::session_owner::SessionSnapshot,
) -> provider_session_machines::EffectiveProviderSettings {
    use provider_session_machines::{ProviderSettingKind, session_owner::ObservedSetting};
    let current = |kind| {
        snapshot
            .settings
            .get(&kind)
            .and_then(|evidence| match &evidence.value {
                ObservedSetting::Confirmed(value) => Some(value.0.clone()),
                _ => None,
            })
    };
    provider_session_machines::EffectiveProviderSettings {
        mode: current(ProviderSettingKind::Mode),
        model: current(ProviderSettingKind::Model),
        effort: current(ProviderSettingKind::Effort),
        fast: current(ProviderSettingKind::Fast).map(|value| {
            if matches!(value.as_str(), "false" | "off") {
                provider_session_machines::FastObserved::Off
            } else {
                provider_session_machines::FastObserved::On
            }
        }),
    }
}
