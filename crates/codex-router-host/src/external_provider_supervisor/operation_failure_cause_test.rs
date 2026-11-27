use super::super::provider_create_result::{project_owner_create, provider_create_failure};
use super::*;
use crate::external_provider_supervisor::coded_restore_failure_cases::{CASES, CODE, REFERENCE};
use crate::session_owner_door::OwnerCreateFailure;
use provider_session_machines::ProviderSettingKind;
use provider_session_machines::harness_lifecycle::{
    ActivationRefusal, HarnessSessionId, InvalidRequestedSetting, OpeningFailure,
};

pub(crate) fn assert_failure(
    failure: &ConversationOperationFailure,
    expected: (
        ConversationOperationFailureKind,
        ConversationOperationFailureStage,
        ProviderOperationEffect,
        Option<i64>,
        &str,
    ),
) {
    assert_eq!(
        (
            failure.kind,
            failure.stage,
            failure.effect,
            failure.provider_code,
            String::from(failure.message.clone()).as_str()
        ),
        expected,
    );
}

#[test]
fn coded_projection_preserves_the_base_tuple_and_attempted_restore_wording() {
    let target = super::super::tests::requester();
    let operation = OperationId::generate();

    for (wire, kind, stage, effect, base, restore, rejected) in CASES {
        let failure = ProviderWireFailure {
            kind: wire,
            code: CODE,
            correlation_id: REFERENCE,
        };
        let cause = OperationFailureCause::HarnessCoded(failure);
        let result = runtime_failure(operation.clone(), Some(target.clone()), cause);
        let base = format!(
            "{base} (provider code -32017; reference 01981f2b-d79c-7abc-8000-000000000001)"
        );
        assert_failure(&result, (kind, stage, effect, Some(CODE), &base));

        let restore = if rejected.is_some() {
            format!(
                "{restore} (provider code -32017; reference 01981f2b-d79c-7abc-8000-000000000001)"
            )
        } else {
            format!("{restore} (ACP code -32017; reference 01981f2b-d79c-7abc-8000-000000000001)")
        };
        let result = restore_operation_failure(operation.clone(), target.clone(), failure);
        assert_failure(&result, (kind, stage, effect, Some(CODE), &restore));
        assert_eq!(result.target, Some(target.clone()));
        assert_eq!(result.operation_id, Some(operation.clone()));
    }
}

#[test]
fn create_unavailable_answers_share_the_public_fallback() {
    use ConversationOperationFailureKind as Kind;
    use ConversationOperationFailureStage as Stage;
    use ProviderOperationEffect as Effect;
    let session = HarnessSessionId("fixture-session".to_owned());
    let cases = [
        OwnerCreateFailure::TransportUnavailable,
        OwnerCreateFailure::Activation {
            reason: ActivationRefusal::ConnectionRetired,
            session,
            permission_mode: Default::default(),
        },
        OwnerCreateFailure::Operation("local diagnostic".to_owned()),
        OwnerCreateFailure::Opening(OpeningFailure::Unavailable {
            diagnostic: "harness diagnostic".to_owned(),
        }),
        OwnerCreateFailure::Opening(OpeningFailure::Lost { reported: None }),
        OwnerCreateFailure::Opening(OpeningFailure::Disconnected),
        OwnerCreateFailure::Opening(OpeningFailure::NoAnswer),
        OwnerCreateFailure::Opening(OpeningFailure::OutcomeUnknown),
        OwnerCreateFailure::Opening(OpeningFailure::ReplayNotKnown),
        OwnerCreateFailure::Opening(OpeningFailure::Closed),
    ];
    for input in cases {
        let result = project_owner_create(Err(input));
        let Err(cause) = result else {
            panic!("expected converted create failure")
        };
        assert_eq!(cause, OperationFailureCause::ResponseUnavailable);
        let result = runtime_failure(OperationId::generate(), None, cause);
        assert_failure(
            &result,
            (
                Kind::OutcomeUnknown,
                Stage::Settlement,
                Effect::Unknown,
                None,
                "provider operation response was not available after dispatch",
            ),
        );
        assert!(result.target.is_none());
    }
}

#[test]
fn coded_opening_on_create_keeps_the_provider_code_and_reference() {
    use ConversationOperationFailureKind as Kind;
    use ConversationOperationFailureStage as Stage;
    use ProviderOperationEffect as Effect;
    let wire = ProviderWireFailure {
        kind: ProviderWireFailureKind::AuthenticationRequired,
        code: CODE,
        correlation_id: REFERENCE,
    };
    for input in [
        OpeningFailure::Lost {
            reported: Some(wire),
        },
        OpeningFailure::Refused(wire),
        OpeningFailure::ProviderError(wire),
    ] {
        let Err(cause) = project_owner_create(Err(OwnerCreateFailure::Opening(input))) else {
            panic!("expected the coded failure from the create projection");
        };
        assert_eq!(cause, OperationFailureCause::HarnessCoded(wire));
        let result = runtime_failure(OperationId::generate(), None, cause);
        assert_failure(
            &result,
            (
                Kind::AuthenticationRequired,
                Stage::Binding,
                Effect::None,
                Some(CODE),
                "provider authentication is required (provider code -32017; reference 01981f2b-d79c-7abc-8000-000000000001)",
            ),
        );
    }
}

#[test]
fn create_invalid_setting_keeps_the_neutral_details_and_disposition() {
    let cause = provider_create_failure(OwnerCreateFailure::ClosedAfterInvalidSetting(
        InvalidRequestedSetting {
            session: HarnessSessionId("fixture-session".to_owned()),
            kind: ProviderSettingKind::Model,
            value: "requested".to_owned(),
            advertised: vec!["advertised".to_owned()],
        },
    ));
    assert_eq!(
        cause,
        OperationFailureCause::InvalidSetting {
            setting: ProviderSettingKind::Model,
            value: "requested".to_owned(),
            advertised: vec!["advertised".to_owned()],
            provider_session_id: "fixture-session".to_owned(),
            disposition: provider_session_machines::InvalidSettingSessionDisposition::Closed,
        }
    );
}

#[test]
fn created_report_preserves_partial_settings_and_idle_invalid_setting_causes() {
    use crate::session_owner_door::OwnerCreateReport;
    use provider_session_machines::InvalidSettingSessionDisposition;
    use provider_session_machines::harness_lifecycle::CreatedSettingsFailure;
    use provider_session_machines::harness_lifecycle::PermissionModeReport;
    use provider_session_machines::harness_lifecycle::{
        AppliedProviderSetting, NotAppliedProviderSetting,
    };
    use session_event_model::FailedProviderSetting;
    use session_event_model::ProviderSettingFailureReason;
    let session = HarnessSessionId("fixture-session".to_owned());
    let permission_mode = PermissionModeReport {
        requested: Some("ask".to_owned()),
        actual: Some("ask".to_owned()),
        reason: None,
    };
    let report = |settings_failure| OwnerCreateReport {
        session: session.clone(),
        effective_settings: Default::default(),
        permission_mode: permission_mode.clone(),
        settings_failure,
    };

    let applied: Box<[AppliedProviderSetting]> = Box::from([AppliedProviderSetting {
        kind: ProviderSettingKind::Mode,
        value: "ask".to_owned(),
    }]);
    let failed = FailedProviderSetting {
        kind: ProviderSettingKind::Model,
        value: "requested".to_owned(),
        reason: ProviderSettingFailureReason::OutcomeUnknown,
    };
    let not_applied: Box<[NotAppliedProviderSetting]> = Box::from([NotAppliedProviderSetting {
        kind: ProviderSettingKind::Effort,
        value: "low".to_owned(),
    }]);
    let Ok(partial) = project_owner_create(Ok(report(Some(CreatedSettingsFailure::Partial {
        applied: applied.clone(),
        failed: failed.clone(),
        not_applied: not_applied.clone(),
    })))) else {
        panic!("expected the created session with partial settings");
    };
    assert_eq!(partial.created.provider_session_id, session.0);
    assert_eq!(
        partial.settings_failure,
        Some(OperationFailureCause::CreatedWithoutSettings {
            provider_session_id: session.0.clone(),
            applied,
            failed,
            not_applied,
            permission_mode: Box::new(permission_mode.clone()),
        })
    );

    let Ok(invalid) = project_owner_create(Ok(report(Some(CreatedSettingsFailure::Invalid(
        InvalidRequestedSetting {
            session: session.clone(),
            kind: ProviderSettingKind::Model,
            value: "wrong".to_owned(),
            advertised: vec!["a".to_owned(), "b".to_owned()],
        },
    ))))) else {
        panic!("expected the created session to stay idle");
    };
    assert_eq!(invalid.created.provider_session_id, session.0);
    assert_eq!(
        invalid.settings_failure,
        Some(OperationFailureCause::InvalidSetting {
            setting: ProviderSettingKind::Model,
            provider_session_id: session.0,
            value: "wrong".to_owned(),
            advertised: vec!["a".to_owned(), "b".to_owned()],
            disposition: InvalidSettingSessionDisposition::RemainsCreated,
        })
    );
}
