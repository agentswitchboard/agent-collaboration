//! Independent expectations for the opening declaration and owned effects.

use super::*;
use Edge as E;
use OpeningPhase as P;
use agent_client_protocol::{Agent, Client, ConnectionTo, schema::v1::SessionId};
use provider_session_machines::{
    ProviderSettingChoice, ProviderSettingKind, ProviderSettingsCatalogRecord, SessionGeneration,
    harness_lifecycle::{CreateAttempt, OpenAttempt, SettingAttempt},
    session_lifecycle_machine::{CloseAttempt, LoadAttempt},
    session_owner::{ConnectionEpoch, OwnerAddress, OwnerIncarnation, OwnerSetting, RunAttempt},
    session_setting_machine::{SettingApplication, SettingValueId},
};
use std::sync::Arc;
use switchboard_harness_contracts::{CloseUncertainty, ReservedReply};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// The expected machine is expressed by lifecycle responsibility, independently
// of the declaration macro and without using a production transition adapter.
fn expected(from: P, event: E) -> Option<P> {
    use E as Event;
    use P as Phase;
    let phase = match event {
        Event::ConnectionRetired
            if !matches!(
                from,
                Phase::UnboundIdle | Phase::BoundIdle | Phase::Terminal
            ) =>
        {
            Phase::Terminal
        }
        Event::RouteRemoved
            if matches!(
                from,
                Phase::QueuedRestore
                    | Phase::AwaitingRestore
                    | Phase::CapturingReplay
                    | Phase::HeldRestore
                    | Phase::PreparedRestore
                    | Phase::StartingReader
                    | Phase::Active
                    | Phase::OfferingClose
                    | Phase::ClosingRestore
                    | Phase::ClosingLive
                    | Phase::CloseRefusedLive
                    | Phase::Readmitting
            ) =>
        {
            Phase::RouteLost
        }
        Event::Retire => match from {
            Phase::AwaitingCreate | Phase::AwaitingRestore | Phase::ClosingRestore => {
                Phase::RetiredAwaitingReply
            }
            Phase::UnboundIdle
            | Phase::BoundIdle
            | Phase::RetiredAwaitingReply
            | Phase::Terminal => return None,
            _ => Phase::Terminal,
        },
        Event::WriterStopped => match from {
            Phase::AwaitingCreate
            | Phase::AwaitingRestore
            | Phase::ConfiguringCreate
            | Phase::ClosingRestore
            | Phase::ClosingLive
            | Phase::RouteLost => Phase::StoppedAwaitingReply,
            Phase::QueuedCreate
            | Phase::QueuedRestore
            | Phase::HeldCreate
            | Phase::ConfiguredCreate
            | Phase::CapturingReplay
            | Phase::HeldRestore
            | Phase::PreparedRestore
            | Phase::StartingReader
            | Phase::Active
            | Phase::OfferingClose
            | Phase::CloseRefusedLive
            | Phase::Readmitting => Phase::Terminal,
            Phase::RetiredAwaitingReply => from,
            _ => return None,
        },
        Event::WriterStoppedAfterOffer
            if matches!(
                from,
                Phase::QueuedCreate | Phase::QueuedRestore | Phase::OfferingClose
            ) =>
        {
            Phase::StoppedAwaitingReply
        }
        Event::WriterStoppedWithoutReceipt if from == Phase::RouteLost => Phase::Terminal,
        Event::WaitOpening
            if matches!(
                from,
                Phase::QueuedCreate
                    | Phase::QueuedRestore
                    | Phase::AwaitingCreate
                    | Phase::ConfiguringCreate
                    | Phase::AwaitingRestore
                    | Phase::CapturingReplay
                    | Phase::StartingReader
            ) =>
        {
            from
        }
        Event::WaitClose
            if matches!(
                from,
                Phase::OfferingClose
                    | Phase::ClosingRestore
                    | Phase::ClosingLive
                    | Phase::Readmitting
            ) =>
        {
            from
        }
        Event::CloseAsked
            if matches!(
                from,
                Phase::AwaitingRestore
                    | Phase::CapturingReplay
                    | Phase::HeldRestore
                    | Phase::PreparedRestore
                    | Phase::Active
            ) =>
        {
            Phase::OfferingClose
        }
        Event::CloseConfirmed | Event::CloseUnknown
            if matches!(
                from,
                Phase::ClosingRestore | Phase::ClosingLive | Phase::RouteLost
            ) =>
        {
            Phase::Terminal
        }
        Event::CloseRefused if matches!(from, Phase::ClosingRestore | Phase::RouteLost) => {
            Phase::Terminal
        }
        Event::ReaderStopped
            if matches!(
                from,
                Phase::ClosingLive | Phase::OfferingClose | Phase::Readmitting
            ) =>
        {
            Phase::RouteLost
        }
        Event::OpenFailed
            if matches!(
                from,
                Phase::AwaitingCreate | Phase::AwaitingRestore | Phase::CapturingReplay
            ) =>
        {
            Phase::Terminal
        }
        Event::WorkerExited if matches!(from, Phase::AwaitingCreate | Phase::AwaitingRestore) => {
            Phase::RetiredAwaitingReply
        }
        Event::WorkerExited
            if matches!(from, Phase::ConfiguringCreate | Phase::CapturingReplay) =>
        {
            Phase::Terminal
        }
        Event::OpenFailed | Event::WorkerExited if from == Phase::RouteLost => Phase::RouteLost,
        Event::ActivationFailed
            if matches!(
                from,
                Phase::ConfiguredCreate | Phase::PreparedRestore | Phase::StartingReader
            ) =>
        {
            Phase::Terminal
        }
        Event::Activate if matches!(from, Phase::ConfiguredCreate | Phase::PreparedRestore) => {
            Phase::StartingReader
        }
        _ => match (from, event) {
            (Phase::UnboundIdle, Event::OpenCreate) => Phase::QueuedCreate,
            (Phase::UnboundIdle, Event::OpenRestore) => Phase::QueuedRestore,
            (Phase::BoundIdle, Event::OpenRestore) => Phase::QueuedRestore,
            (Phase::QueuedCreate, Event::WriterAccepted) => Phase::AwaitingCreate,
            (Phase::QueuedRestore, Event::WriterAccepted) => Phase::AwaitingRestore,
            (Phase::QueuedCreate | Phase::QueuedRestore, Event::WriterRefused) => Phase::Terminal,
            (Phase::AwaitingCreate, Event::ProviderCreated) => Phase::HeldCreate,
            (Phase::HeldCreate, Event::Configure) => Phase::ConfiguringCreate,
            (Phase::ConfiguringCreate, Event::Configured) => Phase::ConfiguredCreate,
            (Phase::ConfiguringCreate, Event::ClosedDuringConfigure) => Phase::Terminal,
            (Phase::ConfiguredCreate, Event::AcceptCreatedSettings) => from,
            (Phase::AwaitingRestore, Event::RestoreLoadReply) => Phase::CapturingReplay,
            (Phase::AwaitingRestore, Event::RestoreResumeReply) => Phase::HeldRestore,
            (Phase::CapturingReplay, Event::ReplayAdmitted) => Phase::HeldRestore,
            (Phase::CapturingReplay, Event::ReplayRejected) => Phase::Terminal,
            (
                Phase::HeldRestore,
                Event::PutSettingBack | Event::SetterReserved | Event::SetterDone,
            ) => from,
            (Phase::HeldRestore, Event::AcceptSettings) => Phase::PreparedRestore,
            (Phase::StartingReader, Event::ReaderAccepted) => Phase::Active,
            (Phase::Active, Event::KeepActive) => from,
            (Phase::Active, Event::ReaderStopped) => Phase::Terminal,
            (Phase::ClosingLive, Event::CloseRefused) => Phase::CloseRefusedLive,
            (Phase::ClosingRestore, Event::LateOpenReply) => from,
            (Phase::OfferingClose, Event::CloseAcceptedRestore) => Phase::ClosingRestore,
            (Phase::OfferingClose, Event::CloseAcceptedLive) => Phase::ClosingLive,
            (Phase::OfferingClose, Event::DeferRestore) => from,
            (Phase::OfferingClose, Event::CloseUnsentAwaiting) => Phase::AwaitingRestore,
            (Phase::OfferingClose, Event::CloseUnsentReplay) => Phase::CapturingReplay,
            (Phase::OfferingClose, Event::CloseUnsentHeld) => Phase::HeldRestore,
            (Phase::OfferingClose, Event::CloseUnsentPrepared) => Phase::PreparedRestore,
            (Phase::OfferingClose, Event::CloseUnsentActive) => Phase::Active,
            (Phase::CloseRefusedLive, Event::AdmitAgain) => Phase::Readmitting,
            (Phase::Readmitting, Event::Readmitted) => Phase::Active,
            (Phase::Readmitting, Event::ReadmitFailed) => Phase::CloseRefusedLive,
            (Phase::RetiredAwaitingReply, Event::LateOpenReply) => Phase::Terminal,
            (Phase::RouteLost, Event::ReportRouteLost) => from,
            (Phase::StoppedAwaitingReply, Event::FinishOnStoppedWriter) => Phase::Terminal,
            (Phase::StoppedAwaitingReply, Event::WriterAccepted | Event::StopOnWriterLoss) => from,
            _ => return None,
        },
    };
    Some(phase)
}

#[test]
fn every_declared_pair_matches_the_independent_opening_matrix() {
    for &phase in P::ALL {
        for &edge in E::ALL {
            assert_eq!(
                OpeningPolicy::transition(&phase, edge)
                    .ok()
                    .map(|(next, _)| next),
                expected(phase, edge),
                "independent declaration for {phase:?} + {edge:?}",
            );
        }
    }
}

struct Fixture {
    connection: ConnectionTo<Agent>,
    run: super::super::RunResources,
    attempt: OpenAttempt,
    create: OpenAttempt,
    restore: RunAttempt<LoadAttempt>,
    close: RunAttempt<CloseAttempt>,
    setter: OpeningOffer,
    catalogue: Arc<ProviderSettingsCatalogRecord>,
    stopped: CancellationToken,
}

impl Fixture {
    async fn new() -> Self {
        use agent_client_protocol::Lines;
        use futures_util::{SinkExt as _, sink, stream};

        let outgoing = sink::drain().sink_map_err(|infallible| match infallible {});
        let incoming = stream::empty::<std::io::Result<String>>();
        let connection = Client
            .builder()
            .connect_with(Lines::new(outgoing, incoming), async |connection| {
                Ok(connection)
            })
            .await
            .expect("construct an inert SDK resource for the pure transition");
        let owner = OwnerAddress {
            incarnation: OwnerIncarnation(Uuid::now_v7()),
            connection_epoch: ConnectionEpoch(Uuid::now_v7()),
        };
        let generation = SessionGeneration::from_number(1);
        let restore = RunAttempt {
            generation,
            attempt: LoadAttempt(Uuid::now_v7()),
        };
        let close = RunAttempt {
            generation,
            attempt: CloseAttempt(Uuid::now_v7()),
        };
        let stopped = CancellationToken::new();
        Self {
            connection,
            run: super::super::RunResources::new(owner, generation, &stopped),
            attempt: OpenAttempt::Restore(restore),
            create: OpenAttempt::Create(CreateAttempt(Uuid::now_v7())),
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
        }
    }

    fn session(&self) -> Box<OwnerOpenedSession> {
        Box::new(OwnerOpenedSession {
            provider_id: SessionId::new("matrix-session"),
            connection: self.connection.clone(),
            route: None,
            capabilities: Default::default(),
            catalog: ProviderSettingsCatalog::from_record(Arc::clone(&self.catalogue)),
            behavior: Arc::new(crate::owner_session_setting::tests::ConnectionCatalogueBehavior),
            buffer: Arc::default(),
            projection: None,
            in_folder: None,
            observed_tool_calls: Arc::default(),
        })
    }

    fn setter_request(&self) -> SettingAttempt {
        SettingAttempt {
            restore: self.restore,
            setting: OwnerSetting {
                change: RunAttempt {
                    generation: self.restore.generation,
                    attempt: SettingApplication(Uuid::now_v7()),
                },
                kind: ProviderSettingKind::Mode,
            },
            value: SettingValueId("ask".into()),
            catalogue: Arc::clone(&self.catalogue),
        }
    }

    fn pending_setter(&self) -> super::super::PendingSetter {
        let attempt = self.setter_request();
        super::super::PendingSetter {
            claim: self.setter.clone(),
            plan: Some(SetterPlan {
                restore: attempt.restore,
                setting: attempt.setting,
                wire: OwnerSettingWirePlan::from_work_catalog(
                    "matrix-session".into(),
                    &ProviderSettingsCatalog::from_record(attempt.catalogue),
                    attempt.setting.kind,
                    attempt.value.0,
                    None,
                )
                .expect("setter offered in test catalogue"),
            }),
        }
    }

    fn owned_restore(&self) -> RoutedRestore {
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

struct IgnoreSettingReply;
impl ReservedReply<provider_session_machines::harness_lifecycle::SettingFact>
    for IgnoreSettingReply
{
    fn send(self: Box<Self>, _: provider_session_machines::harness_lifecycle::SettingFact) {}
}

impl Fixture {
    fn state(&self, phase: P, event: E) -> Option<OpeningState> {
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
                    _ => S::AwaitingRestore(run),
                };
                S::OfferingClose {
                    origin: Box::new(origin),
                    attempt: self.close.attempt,
                    offer: offer(),
                    deferred: None,
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
                    E::WriterStoppedWithoutReceipt | E::ReportRouteLost => LostReceipt::None,
                    _ => LostReceipt::Open,
                };
                S::RouteLost { run, receipt }
            }
            P::StoppedAwaitingReply if event == E::FinishOnStoppedWriter => {
                S::StoppedAwaitingReply(StoppedReceipt::OpenCreate(self.stopped.clone()))
            }
            P::StoppedAwaitingReply => S::StoppedAwaitingReply(StoppedReceipt::OpenRestore(run)),
            P::UnboundIdle | P::BoundIdle | P::RetiredAwaitingReply | P::Terminal => return None,
        };
        Some(state)
    }

    fn command(&self, phase: P, event: E) -> Option<Command> {
        let command = match event {
            E::WriterAccepted => Command::WriterAccepted,
            E::WriterRefused => Command::WriterRefused,
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
                Some(tokio::sync::oneshot::channel().0),
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
            E::SetterReserved => {
                Command::SetterReserved(self.setter.clone(), Box::new(IgnoreSettingReply))
            }
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
                Command::ReadmitResult(self.close.attempt, ReadmitOutcome::StaleMarker)
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
            E::WriterStopped | E::WriterStoppedAfterOffer | E::WriterStoppedWithoutReceipt => {
                Command::WriterStopped
            }
            E::ConnectionRetired => Command::ConnectionRetired,
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
}

fn expected_actions(phase: P, event: E, next: P) -> Vec<Action> {
    use Action as A;
    match event {
        E::Configure => vec![A::Configure],
        E::AcceptSettings => vec![A::FenceSetter, A::ReportSettings],
        E::AcceptCreatedSettings => vec![A::ReportSettings],
        E::PutSettingBack => vec![A::ReserveSetter],
        E::SetterReserved => vec![A::SendSetter],
        E::SetterDone => vec![A::FenceSetter, A::Hold],
        E::Activate => vec![A::Reader],
        E::ReaderStopped => vec![A::ReaderEnded],
        E::CloseAsked if phase == P::HeldRestore => vec![A::FenceSetter, A::OfferClose],
        E::CloseAsked => vec![A::OfferClose],
        E::CloseAcceptedLive | E::CloseAcceptedRestore => vec![A::ConfirmClose],
        E::DeferRestore => vec![A::Wait],
        E::AdmitAgain => vec![A::Readmit],
        E::LateOpenReply => vec![A::Discard],
        E::OpenFailed | E::WorkerExited if phase == P::RouteLost => vec![A::Discard],
        E::RouteRemoved => vec![A::Remove],
        _ if next == P::StoppedAwaitingReply && event != E::WriterAccepted => vec![A::ReleaseAfter],
        _ if matches!(next, P::Terminal | P::RetiredAwaitingReply) => vec![A::Remove],
        _ => vec![A::Hold],
    }
}

fn actual_actions(effect: &Effect, fixture: &Fixture, instance: &OpeningInstance) -> Vec<Action> {
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
        Effect::DropSignal(_, next) => actual_actions(next, fixture, instance),
        _ => panic!("unstated effect in owned matrix"),
    }
}

#[tokio::test]
async fn every_applicable_owned_step_matches_the_independent_effect_matrix() {
    let fixture = Fixture::new().await;
    for &phase in P::ALL {
        for &event in E::ALL {
            let (Some(state), Some(command)) =
                (fixture.state(phase, event), fixture.command(phase, event))
            else {
                continue;
            };
            let declared = expected(phase, event);
            // Several observer commands choose an edge using the current
            // payload. Other spellings of that same command are covered by
            // their own cases, with separately constructed payloads.
            let taken = event == E::WriterStoppedAfterOffer
                && matches!(phase, P::QueuedCreate | P::QueuedRestore | P::OfferingClose);
            if let Some(projected) = edge(&state, &command, taken)
                && projected != event
            {
                assert!(
                    declared.is_none(),
                    "{phase:?} + {event:?} projected as {projected:?}"
                );
                continue;
            }
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
            let row = super::super::OpeningRow {
                attempt: identity,
                instance: instance.clone(),
                state,
            };
            match (declared, row.apply(&instance, command)) {
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
                    }
                    assert_eq!(
                        actual_actions(&effects, &fixture, &instance),
                        expected_actions(phase, event, declared),
                        "{phase:?} + {event:?}"
                    );
                }
                (None, Err(refused)) => {
                    assert_eq!(refused.row.phase(), phase);
                    assert!(refused.row.instance().is_same_as(&instance));
                    assert_eq!(refused.row.attempt(), identity);
                }
                _ => panic!("independent owned matrix disagrees at {phase:?} + {event:?}"),
            }
        }
    }
}

#[tokio::test]
async fn stopped_writer_only_releases_the_matching_owed_receipt() {
    let fixture = Fixture::new().await;
    for owed in 0..4 {
        for arrived in 0..4 {
            let run = fixture.run.clone();
            let receipt = match owed {
                0 => StoppedReceipt::OpenCreate(fixture.stopped.clone()),
                1 => StoppedReceipt::OpenRestore(run),
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
            let row = super::super::OpeningRow {
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
    let fixture = Fixture::new().await;
    for writer_took_close in [false, true] {
        let instance = OpeningInstance::default();
        let (outcome, _waiting) = tokio::sync::oneshot::channel();
        let row = super::super::OpeningRow {
            attempt: fixture.attempt,
            instance: instance.clone(),
            state: OpeningState::OfferingClose {
                origin: Box::new(OpeningState::AwaitingRestore(fixture.run.clone())),
                attempt: fixture.close.attempt,
                offer: OpeningOffer::default(),
                deferred: Some(DeferredRestore {
                    milestone: RestoreMilestone::held(*fixture.session(), None, None, true),
                    outcome,
                }),
            },
        };
        let Ok((Some(row), effect)) = row.apply(
            &instance,
            Command::CloseOffered(fixture.close, writer_took_close),
        ) else {
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
                matches!(effect, Effect::ResolveRestore { accepted: true, .. }),
                "unoffered close applies the original reply before publication"
            );
        }
    }
}
