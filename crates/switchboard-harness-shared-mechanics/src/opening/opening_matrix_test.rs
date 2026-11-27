//! Independent expectations for the opening declaration and owned effects.

use super::super::RestoreReceiptStage;
use super::*;
use crate::opening::opening_test_adapter::{
    DisposalProbe, DisposalToken, HeldTable, TestOpenedSession, TestOpening, TestRestoreSignal,
    ignore_setting_reply,
};
use crate::run_ledger::ReadmitOutcome;
use Edge as E;
use OpeningPhase as P;
use std::{cell::RefCell, sync::Arc};
use switchboard_harness_contracts::{
    AcceptedRunSettings, CloseAttempt, CloseUncertainty, ConnectionEpoch, CreateAttempt,
    LoadAttempt, OpenAttempt, OwnerAddress, OwnerIncarnation, OwnerSetting, ProviderSettingChoice,
    ProviderSettingKind, ProviderSettingsCatalogRecord, RunAttempt, SessionGeneration,
    SettingApplication, SettingAttempt, SettingValueId,
};
use tokio_util::sync::CancellationToken;

#[path = "opening_matrix_expectations.rs"]
mod expectations;
#[path = "opening_late_receipts_test.rs"]
mod late_receipts;
#[path = "opening_payloads_test.rs"]
mod payloads;
#[path = "opening_worker_receipts_test.rs"]
mod worker_receipts;
use expectations::{expected, expected_actions};

struct Fixture {
    run: super::super::RunResources,
    attempt: OpenAttempt,
    create: OpenAttempt,
    restore: RunAttempt<LoadAttempt>,
    close: RunAttempt<CloseAttempt>,
    setter: OpeningOffer,
    catalogue: Arc<ProviderSettingsCatalogRecord>,
    stopped: CancellationToken,
    table: HeldTable,
    disposals: RefCell<Vec<DisposalProbe>>,
}

impl Fixture {
    fn new() -> Self {
        let owner = OwnerAddress {
            incarnation: OwnerIncarnation("019f0000-0000-7000-8000-000000001595".parse().unwrap()),
            connection_epoch: ConnectionEpoch(
                "019f0000-0000-7000-8000-000000001659".parse().unwrap(),
            ),
        };
        let generation = SessionGeneration::from_number(1);
        let restore = RunAttempt {
            generation,
            attempt: LoadAttempt("019f0000-0000-7000-8000-000000001843".parse().unwrap()),
        };
        let close = RunAttempt {
            generation,
            attempt: CloseAttempt("019f0000-0000-7000-8000-000000001961".parse().unwrap()),
        };
        let stopped = CancellationToken::new();
        Self {
            run: super::super::RunResources::new(owner, generation, &stopped),
            attempt: OpenAttempt::Restore(restore),
            create: OpenAttempt::Create(CreateAttempt(
                "019f0000-0000-7000-8000-000000002236".parse().unwrap(),
            )),
            restore,
            close,
            setter: OpeningOffer::default(),
            catalogue: Arc::new(ProviderSettingsCatalogRecord {
                modes: vec![ProviderSettingChoice {
                    value: "ask".into(),
                    label: "Ask".into(),
                }],
                ..Default::default()
            }),
            stopped,
            table: HeldTable::default(),
            disposals: RefCell::default(),
        }
    }

    fn session(&self) -> Box<TestOpenedSession> {
        Box::new(TestOpenedSession::new(self.disposal_token()))
    }

    fn disposal_token(&self) -> DisposalToken {
        let probe = DisposalProbe::default();
        let token = probe.bind(&self.table);
        self.disposals.borrow_mut().push(probe);
        token
    }

    fn restore_signal(
        &self,
        sender: tokio::sync::oneshot::Sender<RestoreDecision>,
    ) -> TestRestoreSignal {
        TestRestoreSignal::new(sender, self.disposal_token())
    }

    fn assert_disposals(&self) {
        for probe in self.disposals.borrow_mut().drain(..) {
            probe.assert_disposed_outside_table();
        }
    }

    fn setter_request(&self) -> SettingAttempt {
        SettingAttempt {
            restore: self.restore,
            setting: OwnerSetting {
                change: RunAttempt {
                    generation: self.restore.generation,
                    attempt: SettingApplication(
                        "019f0000-0000-7000-8000-000000003705".parse().unwrap(),
                    ),
                },
                kind: ProviderSettingKind::Mode,
            },
            value: SettingValueId("ask".into()),
            catalogue: Arc::clone(&self.catalogue),
        }
    }

    fn pending_setter(&self) -> super::super::PendingSetter<TestOpening> {
        let attempt = self.setter_request();
        super::super::PendingSetter {
            claim: self.setter.clone(),
            plan: Some(SetterPlan {
                restore: attempt.restore,
                setting: attempt.setting,
                wire: TestOpening::plan_setting_put_back("matrix-session".into(), attempt, None)
                    .expect("setter offered in test catalogue"),
            }),
        }
    }

    fn owned_restore(&self) -> RoutedRestore<TestOpening> {
        RoutedRestore {
            run: self.run.clone(),
            held: Box::new(HeldRestore {
                session: self.session(),
                accepted_mode_fallback: None,
                skills: None,
                setters: vec![self.pending_setter()],
            }),
        }
    }
}

impl Fixture {
    fn state(&self, phase: P, event: E) -> Option<OpeningState<TestOpening>> {
        use super::super::PendingCreate;
        use OpeningState as S;
        let run = self.run.clone();
        let pending = || PendingCreate {
            requested: Box::default(),
            stopped: self.stopped.clone(),
        };
        let offer = || {
            let offer = OpeningOffer::default();
            if event == E::WriterStoppedAfterOffer {
                assert!(offer.take());
            }
            offer
        };
        let state = match phase {
            P::QueuedCreate => S::OfferingCreate {
                offer: offer(),
                pending: pending(),
            },
            P::AwaitingCreate => S::AwaitingCreate(pending()),
            P::HeldCreate => S::HeldCreate(HeldCreate {
                session: self.session(),
                requested: Box::default(),
            }),
            P::ConfiguringCreate => S::ConfiguringCreate(self.stopped.clone()),
            P::ConfiguredCreate => S::ConfiguredCreate(self.session()),
            P::QueuedRestore => S::OfferingRestore {
                offer: offer(),
                run,
            },
            P::AwaitingRestore => S::AwaitingRestore(run),
            P::CapturingReplay => S::PublishingReplay(self.owned_restore()),
            P::HeldRestore => S::HeldRestore(self.owned_restore()),
            P::PreparedRestore => S::PreparedRestore(self.owned_restore()),
            P::StartingReader => S::StartingReader(run),
            P::Active => S::Active(run),
            P::OfferingClose => {
                let origin = match event {
                    E::CloseAcceptedLive | E::CloseUnsentActive | E::ReaderStopped => {
                        S::Active(run)
                    }
                    E::CloseUnsentReplay => S::PublishingReplay(self.owned_restore()),
                    E::CloseUnsentHeld => S::HeldRestore(self.owned_restore()),
                    E::CloseUnsentPrepared => S::PreparedRestore(self.owned_restore()),
                    E::OpenCreate
                    | E::OpenRestore
                    | E::WriterAccepted
                    | E::WriterRefused
                    | E::ProviderCreated
                    | E::Configure
                    | E::Configured
                    | E::ClosedDuringConfigure
                    | E::RestoreLoadReply
                    | E::RestoreResumeReply
                    | E::ReplayAdmitted
                    | E::ReplayRejected
                    | E::OpenFailed
                    | E::PutSettingBack
                    | E::SetterReserved
                    | E::SetterDone
                    | E::AcceptSettings
                    | E::AcceptCreatedSettings
                    | E::Activate
                    | E::ReaderAccepted
                    | E::ActivationFailed
                    | E::CloseAsked
                    | E::CloseAcceptedRestore
                    | E::DeferRestore
                    | E::CloseUnsentAwaiting
                    | E::CloseConfirmed
                    | E::CloseRefused
                    | E::CloseUnknown
                    | E::AdmitAgain
                    | E::Readmitted
                    | E::ReadmitFailed
                    | E::Retire
                    | E::RouteRemoved
                    | E::FinishOnStoppedWriter
                    | E::RestoreAfterLocalLoss
                    | E::LateOpenReply
                    | E::WorkerExited
                    | E::WriterStoppedAfterOffer
                    | E::WriterStoppedWithOlderRestore
                    | E::WriterStoppedWithDeferredRestore
                    | E::WriterStoppedWithoutReceipt
                    | E::RetainReceiptAfterConnectionLoss
                    | E::WaitOpening
                    | E::WaitClose
                    | E::KeepActive
                    | E::ReportRouteLost
                    | E::StopOnWriterLoss => S::AwaitingRestore(run),
                    E::WriterStopped | E::ConnectionRetired => S::Active(run),
                };
                let deferred =
                    (event == E::WriterStoppedWithDeferredRestore).then(|| DeferredRestore {
                        milestone: RestoreMilestone::held(*self.session(), None, None, true),
                        outcome: self.restore_signal(tokio::sync::oneshot::channel().0),
                    });
                S::OfferingClose {
                    origin: Box::new(origin),
                    attempt: self.close.attempt,
                    offer: offer(),
                    deferred,
                }
            }
            P::ClosingRestore => S::ClosingRestore {
                run,
                attempt: self.close.attempt,
            },
            P::ClosingLive => S::ClosingLive {
                run,
                attempt: self.close.attempt,
            },
            P::CloseRefusedLive => S::CloseRefusedLive {
                run,
                attempt: self.close.attempt,
            },
            P::Readmitting => S::Readmitting {
                run,
                attempt: self.close.attempt,
            },
            P::RouteLost => {
                let receipt = match event {
                    E::CloseConfirmed | E::CloseRefused | E::CloseUnknown => {
                        LostReceipt::Close(self.close.attempt)
                    }
                    E::WriterStoppedWithoutReceipt | E::ReportRouteLost | E::ConnectionRetired => {
                        LostReceipt::None
                    }
                    E::OpenCreate
                    | E::OpenRestore
                    | E::WriterAccepted
                    | E::WriterRefused
                    | E::ProviderCreated
                    | E::Configure
                    | E::Configured
                    | E::ClosedDuringConfigure
                    | E::RestoreLoadReply
                    | E::RestoreResumeReply
                    | E::ReplayAdmitted
                    | E::ReplayRejected
                    | E::OpenFailed
                    | E::PutSettingBack
                    | E::SetterReserved
                    | E::SetterDone
                    | E::AcceptSettings
                    | E::AcceptCreatedSettings
                    | E::Activate
                    | E::ReaderAccepted
                    | E::ReaderStopped
                    | E::ActivationFailed
                    | E::CloseAsked
                    | E::CloseAcceptedRestore
                    | E::CloseAcceptedLive
                    | E::DeferRestore
                    | E::CloseUnsentAwaiting
                    | E::CloseUnsentReplay
                    | E::CloseUnsentHeld
                    | E::CloseUnsentPrepared
                    | E::CloseUnsentActive
                    | E::AdmitAgain
                    | E::Readmitted
                    | E::ReadmitFailed
                    | E::Retire
                    | E::RouteRemoved
                    | E::FinishOnStoppedWriter
                    | E::RestoreAfterLocalLoss
                    | E::LateOpenReply
                    | E::WorkerExited
                    | E::WriterStopped
                    | E::WriterStoppedAfterOffer
                    | E::WriterStoppedWithOlderRestore
                    | E::WriterStoppedWithDeferredRestore
                    | E::RetainReceiptAfterConnectionLoss
                    | E::WaitOpening
                    | E::WaitClose
                    | E::KeepActive
                    | E::StopOnWriterLoss => LostReceipt::Open(RestoreReceiptStage::AwaitingPeer),
                };
                S::RouteLost { run, receipt }
            }
            P::StoppedAwaitingReply if event == E::FinishOnStoppedWriter => {
                S::StoppedAwaitingReply(StoppedReceipt::OpenCreate(self.stopped.clone()))
            }
            P::StoppedAwaitingReply => S::StoppedAwaitingReply(StoppedReceipt::OpenRestore(
                run,
                RestoreReceiptStage::AwaitingPeer,
            )),
            P::UnboundIdle | P::BoundIdle | P::RetiredAwaitingReply | P::Terminal => return None,
        };
        Some(state)
    }

    fn command(&self, phase: P, event: E) -> Option<Command<TestOpening>> {
        let command = match event {
            E::WriterAccepted => Command::WriterAccepted,
            E::WriterRefused => Command::WriterRefused,
            E::RestoreAfterLocalLoss => Command::RestoreProgress(
                RestoreMilestone::held(*self.session(), None, None, true),
                Some(self.restore_signal(tokio::sync::oneshot::channel().0)),
            ),
            E::ProviderCreated => Command::Created(self.session()),
            E::Configure => Command::Configure(self.stopped.clone()),
            E::Configured => Command::Configured(self.session()),
            E::ClosedDuringConfigure => Command::ConfigureClosed,
            E::RestoreLoadReply => Command::RestoreProgress(
                RestoreMilestone::held(*self.session(), None, None, true),
                None,
            ),
            E::RestoreResumeReply | E::LateOpenReply => Command::RestoreProgress(
                RestoreMilestone::held(*self.session(), None, None, false),
                None,
            ),
            E::ReplayAdmitted => Command::RestoreProgress(RestoreMilestone::ReplayAdmitted, None),
            E::ReplayRejected => Command::RestoreProgress(RestoreMilestone::ReplayRejected, None),
            E::DeferRestore => Command::RestoreProgress(
                RestoreMilestone::ReplayAdmitted,
                Some(self.restore_signal(tokio::sync::oneshot::channel().0)),
            ),
            E::OpenFailed | E::FinishOnStoppedWriter
                if matches!(
                    phase,
                    P::AwaitingRestore | P::CapturingReplay | P::RouteLost
                ) =>
            {
                Command::RestoreProgress(RestoreMilestone::Failed, None)
            }
            E::OpenFailed | E::FinishOnStoppedWriter => Command::OpenFailed,
            E::PutSettingBack => Command::PutSettingBack {
                attempt: Box::new(self.setter_request()),
                session: "matrix-session".into(),
            },
            E::SetterReserved => Command::SetterReserved(
                self.setter.clone(),
                ignore_setting_reply(self.disposal_token()),
            ),
            E::SetterDone => Command::SetterDone(self.setter.clone()),
            E::AcceptSettings | E::AcceptCreatedSettings => {
                Command::AcceptSettings(AcceptedRunSettings {
                    catalogue: Arc::clone(&self.catalogue),
                    router_auto_requested: false,
                })
            }
            E::Activate => Command::Activate {
                run: self.run.clone(),
                created: phase == P::ConfiguredCreate,
            },
            E::ReaderAccepted => Command::ReaderAccepted,
            E::ReaderStopped => Command::ReaderStopped(ReaderEnd::Retired),
            E::ActivationFailed => Command::ActivationFailed,
            E::CloseAsked => Command::CloseAsked(self.close),
            E::CloseAcceptedRestore | E::CloseAcceptedLive => {
                Command::CloseOffered(self.close, true)
            }
            E::CloseUnsentAwaiting
            | E::CloseUnsentReplay
            | E::CloseUnsentHeld
            | E::CloseUnsentPrepared
            | E::CloseUnsentActive => Command::CloseOffered(self.close, false),
            E::CloseConfirmed => Command::CloseSettled(CloseFact::Closed { run: self.close }),
            E::CloseRefused => {
                Command::CloseSettled(CloseFact::RefusedStillLive { run: self.close })
            }
            E::CloseUnknown => Command::CloseSettled(CloseFact::Unknown {
                run: self.close,
                cause: CloseUncertainty::EffectUnconfirmed,
            }),
            E::AdmitAgain => Command::AdmitAgain(self.close.attempt),
            E::Readmitted => Command::ReadmitResult(self.close.attempt, ReadmitOutcome::Reopened),
            E::ReadmitFailed => {
                Command::ReadmitResult(self.close.attempt, ReadmitOutcome::DifferentMarker)
            }
            E::Retire => Command::Retire,
            E::RouteRemoved => Command::RouteRemoved(self.run.route.clone()),
            E::WorkerExited
                if matches!(
                    phase,
                    P::CapturingReplay | P::AwaitingRestore | P::RouteLost
                ) =>
            {
                Command::RestoreProgress(RestoreMilestone::WorkerExited, None)
            }
            E::WorkerExited => Command::WorkerExited,
            E::WriterStopped
            | E::WriterStoppedAfterOffer
            | E::WriterStoppedWithoutReceipt
            | E::WriterStoppedWithOlderRestore
            | E::WriterStoppedWithDeferredRestore => Command::WriterStopped,
            E::ConnectionRetired
                if matches!(
                    phase,
                    P::AwaitingCreate
                        | P::AwaitingRestore
                        | P::CapturingReplay
                        | P::ConfiguringCreate
                        | P::ClosingLive
                        | P::ClosingRestore
                        | P::StoppedAwaitingReply
                ) =>
            {
                return None;
            }
            E::RetainReceiptAfterConnectionLoss if phase != P::StoppedAwaitingReply => return None,
            E::ConnectionRetired | E::RetainReceiptAfterConnectionLoss => {
                Command::ConnectionRetired
            }
            // These are admission at the vacant table or read-only guidance
            // labels. The declaration test covers all their pairs as well.
            E::OpenCreate
            | E::OpenRestore
            | E::WaitOpening
            | E::WaitClose
            | E::KeepActive
            | E::ReportRouteLost
            | E::StopOnWriterLoss => return None,
        };
        Some(command)
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Action {
    Hold,
    Remove,
    ReleaseAfter,
    Configure,
    ReportSettings,
    Reader,
    ReaderEnded,
    ReserveSetter,
    SendSetter,
    FenceSetter,
    OfferClose,
    ConfirmClose,
    Readmit,
    Wait,
    Discard,
    ResolveRestore,
}

fn actual_actions(
    effect: &Effect<TestOpening>,
    fixture: &Fixture,
    instance: &OpeningInstance,
) -> Vec<Action> {
    use Action as A;
    match effect {
        Effect::None | Effect::Rollback(None) => vec![A::Hold],
        Effect::Release(release) => {
            assert!(release.instance.is_same_as(instance));
            vec![if release.retire {
                A::Remove
            } else {
                A::ReleaseAfter
            }]
        }
        Effect::Configure { session, .. } => {
            assert_eq!(session.provider_id(), "matrix-session");
            vec![A::Configure]
        }
        Effect::Settings(_) => vec![A::ReportSettings],
        Effect::StartReader(handoff) => {
            assert!(handoff.run.route.is_same_as(&fixture.run.route));
            vec![A::Reader]
        }
        Effect::ReaderEnded {
            release,
            report_loss,
        } => {
            assert!(release.instance.is_same_as(instance));
            assert!(!report_loss);
            vec![A::ReaderEnded]
        }
        Effect::ReserveSetter(_) => vec![A::ReserveSetter],
        Effect::WriteSetter(write) => {
            assert!(write.route.is_same_as(&fixture.run.route));
            assert_eq!(write.restore, fixture.restore);
            vec![A::SendSetter]
        }
        Effect::WithdrawSetters(setters, next) => {
            assert!(
                setters
                    .iter()
                    .all(|setter| setter.claim.is_same_as(&fixture.setter))
            );
            let mut actions = vec![A::FenceSetter];
            actions.extend(actual_actions(next, fixture, instance));
            actions
        }
        Effect::CloseOffer(route, _) => {
            assert!(route.is_same_as(&fixture.run.route));
            vec![A::OfferClose]
        }
        Effect::CloseConfirmed(release, _) => {
            assert!(release.instance.is_same_as(instance));
            assert!(!release.retire);
            vec![A::ConfirmClose]
        }
        Effect::Readmit(route, close) => {
            assert!(route.is_same_as(&fixture.run.route));
            assert_eq!(*close, fixture.close.attempt);
            vec![A::Readmit]
        }
        Effect::Deferred => vec![A::Wait],
        Effect::Discarded(_) => vec![A::Discard],
        Effect::DiscardAfter(_, next) => {
            let mut actions = vec![A::Discard];
            actions.extend(actual_actions(next, fixture, instance));
            actions
        }
        Effect::ResolveDeferredRestore { release, .. }
        | Effect::ResolveRestore {
            release: Some(release),
            ..
        } => {
            assert!(release.instance.is_same_as(instance));
            vec![A::ResolveRestore]
        }
        Effect::DropSignal(_, next) => actual_actions(next, fixture, instance),
        Effect::OfferCreate(_)
        | Effect::OfferRestore(_)
        | Effect::Fault(_)
        | Effect::ReleaseBefore(..)
        | Effect::Rollback(_)
        | Effect::RefuseSetter(_)
        | Effect::ResolveRestore { .. } => panic!("unstated effect in owned matrix"),
    }
}

#[tokio::test]
async fn every_applicable_owned_step_matches_the_independent_effect_matrix() {
    let fixture = Fixture::new();
    for &phase in P::ALL {
        for &event in E::ALL {
            if !expectations::canonical_stimulus(phase, event) {
                continue;
            }
            let (Some(state), Some(command)) =
                (fixture.state(phase, event), fixture.command(phase, event))
            else {
                continue;
            };
            let declared = expected(phase, event);
            let instance = OpeningInstance::default();
            let identity = if matches!(
                phase,
                P::QueuedCreate
                    | P::AwaitingCreate
                    | P::HeldCreate
                    | P::ConfiguringCreate
                    | P::ConfiguredCreate
            ) || (phase == P::StoppedAwaitingReply
                && event == E::FinishOnStoppedWriter)
            {
                fixture.create
            } else {
                fixture.attempt
            };
            let row: super::super::OpeningRow<TestOpening> = super::super::OpeningRow {
                attempt: identity,
                instance: instance.clone(),
                state,
            };
            let result = {
                let _table = fixture.table.lock();
                row.apply(&instance, command)
            };
            match (declared, result) {
                (Some(declared), Ok((stored, effects))) => {
                    let expected_phase =
                        if matches!(declared, P::Terminal | P::RetiredAwaitingReply) {
                            None
                        } else {
                            Some(declared)
                        };
                    assert_eq!(
                        stored.as_ref().map(super::super::OpeningRow::phase),
                        expected_phase,
                        "{phase:?} + {event:?}"
                    );
                    if let Some(stored) = stored {
                        assert!(stored.instance().is_same_as(&instance));
                        assert_eq!(stored.attempt(), identity);
                        if let Some(run) = stored.state.run() {
                            assert!(run.route.is_same_as(&fixture.run.route));
                        }
                    }
                    assert_eq!(
                        actual_actions(&effects, &fixture, &instance),
                        expected_actions(phase, event, declared),
                        "{phase:?} + {event:?}"
                    );
                }
                (None, Err(refused)) => {
                    assert_eq!(refused.row.phase(), phase);
                    assert_eq!(refused.rejection.reason, expectations::refusal(phase));
                    assert!(refused.row.instance().is_same_as(&instance));
                    assert_eq!(refused.row.attempt(), identity);
                }
                _ => panic!("independent owned matrix disagrees at {phase:?} + {event:?}"),
            }
            fixture.assert_disposals();
        }
    }
}

#[tokio::test]
async fn successful_reopen_receipt_cannot_revive_an_invalidated_route() {
    let fixture = Fixture::new();
    let instance = OpeningInstance::default();
    let row: super::super::OpeningRow<TestOpening> = super::super::OpeningRow {
        attempt: fixture.attempt,
        instance: instance.clone(),
        state: OpeningState::Readmitting {
            run: fixture.run.clone(),
            attempt: fixture.close.attempt,
        },
    };
    fixture.run.route.mark_removed();
    let (next, effect) = row
        .apply(
            &instance,
            Command::ReadmitResult(fixture.close.attempt, ReadmitOutcome::Reopened),
        )
        .unwrap_or_else(|_| panic!("settle the successful reopen on the invalidated exact route"));
    assert_eq!(next.map(|row| row.phase()), Some(P::RouteLost));
    assert_eq!(
        actual_actions(&effect, &fixture, &instance),
        vec![Action::Remove]
    );
}

#[tokio::test]
async fn marker_absent_receipt_cannot_revive_an_invalidated_route() {
    let fixture = Fixture::new();
    let instance = OpeningInstance::default();
    let row: super::super::OpeningRow<TestOpening> = super::super::OpeningRow {
        attempt: fixture.attempt,
        instance: instance.clone(),
        state: OpeningState::Readmitting {
            run: fixture.run.clone(),
            attempt: fixture.close.attempt,
        },
    };
    fixture.run.route.mark_removed();
    let (next, effect) = row
        .apply(
            &instance,
            Command::ReadmitResult(fixture.close.attempt, ReadmitOutcome::MarkerAbsent),
        )
        .unwrap_or_else(|_| {
            panic!("settle the marker-absent readmission on the invalidated exact route")
        });
    assert_eq!(next.map(|row| row.phase()), Some(P::RouteLost));
    assert_eq!(
        actual_actions(&effect, &fixture, &instance),
        vec![Action::Remove]
    );
}

#[tokio::test]
async fn stopped_writer_only_releases_the_matching_owed_receipt() {
    let fixture = Fixture::new();
    for owed in 0..4 {
        for arrived in 0..4 {
            let run = fixture.run.clone();
            let receipt = match owed {
                0 => StoppedReceipt::OpenCreate(fixture.stopped.clone()),
                1 => StoppedReceipt::OpenRestore(run, RestoreReceiptStage::AwaitingPeer),
                2 => StoppedReceipt::Configure(fixture.stopped.clone()),
                _ => StoppedReceipt::Close(run, fixture.close.attempt),
            };
            let event = match arrived {
                0 => Command::OpenFailed,
                1 => Command::RestoreProgress(RestoreMilestone::Failed, None),
                2 => Command::ConfigureClosed,
                _ => Command::CloseSettled(CloseFact::Closed { run: fixture.close }),
            };
            let instance = OpeningInstance::default();
            let row: super::super::OpeningRow<TestOpening> = super::super::OpeningRow {
                attempt: fixture.attempt,
                instance: instance.clone(),
                state: OpeningState::StoppedAwaitingReply(receipt),
            };
            match row.apply(&instance, event) {
                Ok((None, Effect::Release(release))) => {
                    assert_eq!(owed, arrived, "wrong receipt destroyed stopped opening");
                    assert!(release.instance.is_same_as(&instance));
                }
                Err(refused) => {
                    assert_ne!(owed, arrived, "exact owed result did not settle");
                    assert_eq!(refused.row.phase(), P::StoppedAwaitingReply);
                    assert!(refused.row.instance().is_same_as(&instance));
                }
                _ => panic!("a stopped receipt must settle or preserve its exact row"),
            }
        }
    }
}

#[tokio::test]
async fn a_deferred_load_is_applied_only_when_close_was_unoffered() {
    let fixture = Fixture::new();
    for writer_took_close in [false, true] {
        let instance = OpeningInstance::default();
        let (outcome, _waiting) = tokio::sync::oneshot::channel();
        let row: super::super::OpeningRow<TestOpening> = super::super::OpeningRow {
            attempt: fixture.attempt,
            instance: instance.clone(),
            state: OpeningState::OfferingClose {
                origin: Box::new(OpeningState::AwaitingRestore(fixture.run.clone())),
                attempt: fixture.close.attempt,
                offer: OpeningOffer::default(),
                deferred: Some(DeferredRestore {
                    milestone: RestoreMilestone::held(*fixture.session(), None, None, true),
                    outcome: fixture.restore_signal(outcome),
                }),
            },
        };
        let result = {
            let _table = fixture.table.lock();
            row.apply(
                &instance,
                Command::CloseOffered(fixture.close, writer_took_close),
            )
        };
        let Ok((Some(row), effect)) = result else {
            panic!("exact close admission result did not settle its offer");
        };
        if writer_took_close {
            assert_eq!(row.phase(), P::ClosingRestore);
            assert!(
                matches!(effect, Effect::CloseConfirmed(_, Some(_))),
                "accepted close discards the deferred opening"
            );
        } else {
            assert_eq!(row.phase(), P::CapturingReplay);
            assert!(
                matches!(
                    effect,
                    Effect::ResolveRestore {
                        decision: RestoreDecision::Continue,
                        ..
                    }
                ),
                "unoffered close applies the original reply before publication"
            );
        }
        drop((row, effect));
        fixture.assert_disposals();
    }
}

#[path = "opening_effect_order_test.rs"]
mod effect_order;
