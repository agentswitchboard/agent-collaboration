use super::*;

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
