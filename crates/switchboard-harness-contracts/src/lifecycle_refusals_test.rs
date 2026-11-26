//! The complete move list for each reason also rules out every omitted move.

use crate::LifecycleMove::*;
use crate::{
    ActivationRefusal, CloseAttempt, CloseFact, CloseUncertainty, CreatedSettingsFailure,
    FailedProviderSetting, HarnessSessionId, InvalidRequestedSetting, LifecycleMove,
    LifecycleRefusal, OpeningFailure, ProviderSettingKind, ProviderWireFailure,
    ProviderWireFailureKind, RestoreModeOutcome, RestoreSettingRefusal, RunAttempt,
    SessionGeneration, SettingValueId, WriterState,
};
use uuid::Uuid;

#[test]
fn immediate_refusals_name_only_the_moves_available_from_their_state() {
    use LifecycleRefusal::*;
    for refusal in [
        Encoding,
        Writer(WriterState::Full),
        Writer(WriterState::Stopped),
        RouteUnavailable,
        RouteClosing,
        AttemptMismatch,
        OpeningInProgress,
        OpeningNotHeld,
        SettingValueNotOffered,
        SettingSetterUnavailable,
        CatalogueUnavailable,
        InvalidCatalogueReply,
        OperationNotOffered,
    ] {
        let moves: &[LifecycleMove] = match refusal {
            Encoding => &[CorrectRequest],
            Writer(WriterState::Full) => &[WaitForWriter],
            Writer(WriterState::Stopped) => &[StopOwner],
            RouteUnavailable => &[Reopen],
            RouteClosing => &[WaitForClose],
            AttemptMismatch => &[CorrectRequest],
            OpeningInProgress => &[WaitForOpening],
            OpeningNotHeld => &[Reopen],
            SettingValueNotOffered => &[CorrectRequest],
            SettingSetterUnavailable => &[ReportUnavailable],
            CatalogueUnavailable => &[UseSavedCatalogue, ReportUnavailable],
            InvalidCatalogueReply => &[UseSavedCatalogue, ReportUnavailable],
            OperationNotOffered => &[ReportUnavailable],
        };
        assert_eq!(
            refusal.valid_moves(),
            moves,
            "immediate refusal {refusal:?}"
        );
        assert_eq!(
            RestoreSettingRefusal::Harness(refusal).valid_moves(),
            moves,
            "restore setter must retain the harness's exact guidance for {refusal:?}"
        );
    }
    for writer in [WriterState::Full, WriterState::Stopped] {
        let refusal = RestoreSettingRefusal::ReplyReservation(writer);
        let moves: &[LifecycleMove] = match writer {
            WriterState::Full | WriterState::Stopped => &[StopOwner],
        };
        assert_eq!(refusal.valid_moves(), moves, "reply reservation {writer:?}");
    }
}

#[test]
fn opening_and_activation_refusals_name_only_reachable_moves() {
    use OpeningFailure::*;
    let refusal = ProviderWireFailure {
        kind: ProviderWireFailureKind::InvalidParams,
        code: -32602,
        correlation_id: Uuid::now_v7(),
    };
    for failure in [
        Lost { reported: None },
        Lost {
            reported: Some(ProviderWireFailure {
                kind: ProviderWireFailureKind::SessionNotFound,
                ..refusal
            }),
        },
        Refused(refusal),
        ProviderError(ProviderWireFailure {
            kind: ProviderWireFailureKind::RestoreFailed,
            ..refusal
        }),
        Disconnected,
        NoAnswer,
        OutcomeUnknown,
        ReplayNotKnown,
        Closed,
        Unavailable {
            diagnostic: "fixture".into(),
        },
    ] {
        let moves: &[LifecycleMove] = match &failure {
            Lost { .. } => &[CreateFromSavedSetup],
            Refused(_) => &[CorrectRequest, ReportUnavailable],
            ProviderError(_) => &[Reopen],
            Disconnected => &[StopOwner],
            NoAnswer => &[KeepOutcomeUnknown],
            OutcomeUnknown => &[KeepOutcomeUnknown],
            ReplayNotKnown => &[Reopen],
            Closed => &[WaitForClose],
            Unavailable { .. } => &[KeepOutcomeUnknown],
        };
        assert_eq!(failure.valid_moves(), moves, "opening refusal {failure:?}");
    }
    for refusal in [
        ActivationRefusal::ConnectionRetired,
        ActivationRefusal::AttemptMismatch,
        ActivationRefusal::OpeningNotHeld,
        ActivationRefusal::AlreadyActive,
        ActivationRefusal::RouteUnavailable,
        ActivationRefusal::ReaderUnavailable,
    ] {
        let moves: &[LifecycleMove] = match refusal {
            ActivationRefusal::ConnectionRetired => &[StopOwner],
            ActivationRefusal::AttemptMismatch => &[CorrectRequest],
            ActivationRefusal::OpeningNotHeld => &[ReportUnavailable],
            ActivationRefusal::AlreadyActive => &[KeepActiveRun, Retire],
            ActivationRefusal::RouteUnavailable | ActivationRefusal::ReaderUnavailable => {
                &[ReportUnavailable]
            }
        };
        assert_eq!(
            refusal.valid_moves(),
            moves,
            "activation refusal {refusal:?}"
        );
    }
}

#[test]
fn close_reports_distinguish_confirmed_release_refusal_and_uncertainty() {
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    for fact in [
        CloseFact::Closed { run },
        CloseFact::NotOffered { run },
        CloseFact::RefusedStillLive { run },
        CloseFact::Unknown {
            run,
            cause: CloseUncertainty::ReplyLost,
        },
        CloseFact::Unknown {
            run,
            cause: CloseUncertainty::InvalidReply,
        },
        CloseFact::Unknown {
            run,
            cause: CloseUncertainty::EffectUnconfirmed,
        },
    ] {
        let moves: &[LifecycleMove] = match fact {
            CloseFact::Closed { .. } => &[Retire],
            CloseFact::NotOffered { .. } => &[Retire],
            CloseFact::RefusedStillLive { .. } => &[AdmitAgain, Retire],
            CloseFact::Unknown { cause, .. } => match cause {
                CloseUncertainty::ReplyLost
                | CloseUncertainty::InvalidReply
                | CloseUncertainty::EffectUnconfirmed => &[KeepOutcomeUnknown],
            },
        };
        assert_eq!(fact.valid_moves(), moves, "close fact {fact:?}");
    }
}

#[test]
fn setting_refusals_preserve_what_is_known_about_the_attempt() {
    let invalid = InvalidRequestedSetting {
        session: HarnessSessionId("created-session".into()),
        kind: ProviderSettingKind::Mode,
        value: "missing".into(),
        advertised: vec!["default".into()],
    };
    assert_eq!(invalid.valid_moves(), &[CorrectRequest]);
    for failure in [
        CreatedSettingsFailure::Invalid(invalid),
        CreatedSettingsFailure::Partial {
            applied: Box::new([]),
            failed: FailedProviderSetting {
                kind: ProviderSettingKind::Mode,
                value: "offered".into(),
                reason: (),
            },
            not_applied: Box::new([]),
        },
    ] {
        let moves: &[LifecycleMove] = match &failure {
            CreatedSettingsFailure::Invalid(reason) => reason.valid_moves(),
            CreatedSettingsFailure::Partial { .. } => &[CorrectRequest, KeepOutcomeUnknown],
        };
        assert_eq!(failure.valid_moves(), moves);
    }
    let value = SettingValueId("default".into());
    for outcome in [
        RestoreModeOutcome::Confirmed(value.clone()),
        RestoreModeOutcome::Refused { current: None },
        RestoreModeOutcome::Refused {
            current: Some(value.clone()),
        },
        RestoreModeOutcome::ReportedOther(value.clone()),
        RestoreModeOutcome::FellBack(value),
        RestoreModeOutcome::Unconfirmed,
    ] {
        let moves: Option<&[LifecycleMove]> = match &outcome {
            RestoreModeOutcome::Refused { .. } => Some(&[CorrectRequest, UseSavedCatalogue]),
            RestoreModeOutcome::Confirmed(_)
            | RestoreModeOutcome::ReportedOther(_)
            | RestoreModeOutcome::FellBack(_)
            | RestoreModeOutcome::Unconfirmed => None,
        };
        assert_eq!(
            outcome.refusal_moves(),
            moves,
            "restore mode outcome {outcome:?}"
        );
    }
}
