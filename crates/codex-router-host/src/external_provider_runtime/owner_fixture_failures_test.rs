use super::*;
use crate::provider_acp_session_loading::ProviderSessionLoadOutcome;
use acp_client_runtime::RestoreFailure;
use provider_session_machines::session_owner::RestorePhase;

#[test]
fn fixture_only_restore_wording_tracks_the_original_phases() {
    for (failure, phase, expected) in [
        (
            RestoreFailure::preflight(ExternalProviderRuntimeError::SinkClosed),
            RestorePhase::Preflight,
            "session event consumer closed",
        ),
        (
            RestoreFailure::attempted(ExternalProviderRuntimeError::HistoryReplayUnavailable),
            RestorePhase::Attempted,
            "the Router could not publish the restored session's replay or settings, so reconnect and retry this session",
        ),
        (
            RestoreFailure::attempted(ExternalProviderRuntimeError::RestoreNoAnswer),
            RestorePhase::Attempted,
            "the provider did not answer the restore in time, so retry this session",
        ),
        (
            RestoreFailure::attempted(ExternalProviderRuntimeError::Operation(
                "lost restore".to_owned(),
            )),
            RestorePhase::Attempted,
            "provider ACP operation failed: lost restore, so retry this session",
        ),
    ] {
        assert_eq!(
            legacy_load_error_outcome(&failure),
            ProviderSessionLoadOutcome::Unavailable {
                phase,
                reason: expected.to_owned()
            }
        );
    }
}

#[test]
fn post_send_transport_loss_names_unknown_setting_state() {
    use serde_json::json;

    let request: ProviderSettingsSetRequest = serde_json::from_value(json!({
            "target":{"endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"claude-local"},"sessionId":"provider-session"},
            "actor":{"endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"codex-local"},"sessionId":"creator"},
            "setting":"mode","value":"ask"
        })).expect("set request");
    let failure = setting_error(&request, ExternalProviderRuntimeError::TransportFailure);
    assert_eq!(failure.kind, ProviderSettingsFailureKind::OutcomeUnknown);
    assert_eq!(failure.setting, Some(ProviderSettingName::Mode));
    assert_eq!(failure.value.as_deref(), Some("ask"));
    let precise = setting_error(
        &request,
        ExternalProviderRuntimeError::SettingOutcomeUnknown {
            provider_session_id: "provider-session".into(),
            setting: provider_session_machines::ProviderSettingKind::Mode,
            value: "ask".into(),
        },
    );
    assert_eq!(precise.kind, ProviderSettingsFailureKind::OutcomeUnknown);
    assert_eq!(precise.setting, Some(ProviderSettingName::Mode));
}

#[test]
fn replay_failure_mapping_preserves_pre_and_post_dispatch_provenance() {
    let target: SessionRef = serde_json::from_value(serde_json::json!({"endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"cursor-local"},"sessionId":"known"})).expect("target");
    for (error, kind, stage, effect) in [
        (
            ExternalProviderRuntimeError::HistoryReplayAdmissionFailed,
            ConversationOperationFailureKind::Unavailable,
            ConversationOperationFailureStage::Dispatch,
            ProviderOperationEffect::None,
        ),
        (
            ExternalProviderRuntimeError::HistoryReplayUnavailable,
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
        ),
    ] {
        // Arrange/Act: mapping must retain the exact caller operation and target on either path.
        let operation = OperationId::generate();
        let failure = legacy_runtime_failure(operation.clone(), Some(target.clone()), error);
        // Assert: post-dispatch uncertainty cannot be weakened by the new admission arm.
        assert_eq!(failure.operation_id, Some(operation));
        assert_eq!(failure.target, Some(target.clone()));
        assert_eq!(failure.kind, kind);
        assert_eq!(failure.stage, stage);
        assert_eq!(failure.effect, effect);
        if effect == ProviderOperationEffect::None {
            assert!(String::from(failure.message).contains("before provider load"));
        }
    }
}
