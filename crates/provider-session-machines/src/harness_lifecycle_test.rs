use super::*;
use switchboard_harness_contracts::{
    ObservationCause, ObservedSetting, ProviderSettingKind, RestoreModeOutcome, SessionGeneration,
    SettingApplication, SettingEvidence,
};
use uuid::Uuid;

#[test]
fn queued_restore_settings_use_the_existing_local_unsent_transition() {
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let mode = OwnerSetting {
        change: RunAttempt {
            generation: run.generation,
            attempt: SettingApplication(Uuid::now_v7()),
        },
        kind: ProviderSettingKind::Mode,
    };
    let fast = OwnerSetting {
        change: RunAttempt {
            generation: run.generation,
            attempt: SettingApplication(Uuid::now_v7()),
        },
        kind: ProviderSettingKind::Fast,
    };
    let expected_mode = OwnerEvent::Wire(SessionWireResult::RestoreMode {
        run,
        setting: mode,
        outcome: RestoreModeOutcome::Unconfirmed,
        catalogue: None,
    });
    let expected_fast = OwnerEvent::Prepare(RestorePreparation::FastOff {
        run,
        setting: fast,
        catalogue: None,
        observation: SettingEvidence {
            value: ObservedSetting::Unknown,
            cause: Some(ObservationCause::Unconfirmed),
        },
    });
    for (setting, expected) in [(mode, expected_mode), (fast, expected_fast)] {
        assert_eq!(
            owner_event_for_restore_setting(SettingFact::NotSent {
                restore: run,
                setting
            }),
            expected
        );
        assert_eq!(
            owner_event_for_unsent_restore_setting(run, setting),
            expected
        );
    }
}
