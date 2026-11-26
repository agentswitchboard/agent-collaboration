use super::*;
use crate::session_owner::FastReadback;
use crate::session_setting_machine::SettingValueId;
use crate::{
    ModelReasoningMetadata, ModelReasoningUnknown, ProviderConfigOption, ProviderConfigValue,
    ProviderModeObservation, ProviderSettingChoice, ProviderSettingKind,
    ProviderSettingsCatalogRecord,
};

fn advertised_record(
    current_mode: &str,
    reasoning: ModelReasoningUnknown,
) -> ProviderSettingsCatalogRecord {
    ProviderSettingsCatalogRecord {
        current_mode: Some(current_mode.into()),
        mode_observation: ProviderModeObservation::Unconfirmed,
        modes: vec![ProviderSettingChoice {
            value: current_mode.into(),
            label: format!("Fixture {current_mode}"),
        }],
        config_options: vec![ProviderConfigOption {
            id: "fast".into(),
            name: "Fast".into(),
            category: Some(ProviderSettingKind::Fast),
            current_value: ProviderConfigValue::Boolean(true),
            choices: Vec::new(),
        }],
        model_reasoning: ModelReasoningMetadata::Unknown(reasoning),
    }
}

#[test]
fn applied_setting_rebuilds_the_entire_catalogue_record() {
    let record = advertised_record("agent", ModelReasoningUnknown::UnsupportedSource);
    let provider_catalogue = ProviderSettingsCatalog::from_record(record.clone());
    let provider_record = provider_catalogue.into_record();
    let value = SettingValueId("agent".into());
    let incoming = SettingObservation::AppliedWithReadback(Box::new(SettingReadback {
        value: value.clone(),
        catalogue: Arc::clone(&provider_record),
        fast: FastReadback::Off,
        mode_reason: None,
    }));

    let SettingObservation::AppliedWithReadback(rebuilt) = wrap_setting_catalogue(incoming) else {
        panic!("applied readback must remain applied");
    };

    assert_eq!(rebuilt.value, value);
    assert_eq!(&**rebuilt.catalogue, &record);
    assert!(std::ptr::eq(&**rebuilt.catalogue, provider_record.as_ref()));
}

#[test]
fn refused_setting_rebuilds_the_entire_catalogue_record() {
    let record = advertised_record("plan", ModelReasoningUnknown::Malformed);
    let provider_catalogue = ProviderSettingsCatalog::from_record(record.clone());
    let provider_record = provider_catalogue.into_record();
    let value = SettingValueId("on".into());
    let incoming = SettingObservation::Refused {
        current: Some(value.clone()),
        catalogue: Some(Arc::clone(&provider_record)),
    };

    let SettingObservation::Refused {
        current,
        catalogue: Some(rebuilt),
    } = wrap_setting_catalogue(incoming)
    else {
        panic!("refusal must retain its catalogue");
    };

    assert_eq!(current, Some(value));
    assert_eq!(&**rebuilt, &record);
    assert!(std::ptr::eq(&**rebuilt, provider_record.as_ref()));
}
