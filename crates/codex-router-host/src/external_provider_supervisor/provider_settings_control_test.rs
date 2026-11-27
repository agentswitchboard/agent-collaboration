use super::*;
use crate::ExternalProviderRuntimeError;
use crate::external_provider_supervisor::owner_fixture_failures::setting_error;
use serde_json::json;

#[test]
fn post_send_transport_loss_names_unknown_setting_state() {
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
fn unknown_setting_outcome_preserves_the_complete_public_failure() {
    let target = crate::external_provider_supervisor::tests::requester();
    let failure = setting_outcome_unknown(&target, ProviderSettingKind::Mode, "ask".to_owned());

    assert_eq!(failure.kind, ProviderSettingsFailureKind::OutcomeUnknown);
    assert!(failure.stage.is_none());
    assert_eq!(failure.target, Some(target));
    assert_eq!(failure.setting, Some(ProviderSettingName::Mode));
    assert_eq!(failure.value.as_deref(), Some("ask"));
    assert!(failure.advertised.is_empty());
    assert_eq!(
        failure.message,
        "provider setting outcome is unknown; Session remains gated"
    );
}
