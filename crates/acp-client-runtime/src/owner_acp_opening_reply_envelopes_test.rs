//! Check two real reservations in each generated opening and setter schedule.

use super::{
    Command, Effect, OpenCreateRequest, OpenSetup, OpeningSlot, OwnerOpenings, RestoreMilestone,
    RestoreOpening, lock,
};
use crate::{
    owner_session_open::OwnerOpenedSession,
    owner_session_setting::tests::ConnectionCatalogueBehavior,
    owner_wire::{OpeningPausePoint, OwnerAcpWire, output_stop_tests},
};
use agent_client_protocol::{Agent, Client, ConnectionTo, Lines, schema::v1::SessionId};
use codex_router_test_support::reply_ledger::{ReplyLedger, ReplySite};
use futures_util::{SinkExt as _, sink, stream};
use provider_session_machines::harness_lifecycle::{
    CreatedSetupFact, EventPort, EventSendFuture, LifecycleReply, SessionCreatedSetupFact,
    SessionLifecycleFact, SettingFact,
};
use provider_session_machines::{
    ProviderSettingChoice, ProviderSettingKind, ProviderSettingsCatalog,
    ProviderSettingsCatalogRecord,
};
use std::sync::Arc;
use std::{num::NonZeroUsize, sync::OnceLock};
use switchboard_harness_contracts::{
    ConnectionEpoch, CreateAttempt, HarnessSessionId, LoadAttempt, OwnerAddress, OwnerIncarnation,
    OwnerSetting, RestoreVia, RunAttempt, SessionGeneration, SettingApplication, SettingAttempt,
    SettingValueId,
};
use switchboard_harness_contracts::{OpenRequest, OpeningFact, OpeningFailure, ReplyAdmission};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ReplyId {
    Create(Uuid),
    Configure(Uuid),
    Restore(Uuid),
    Setter { restore: Uuid, application: Uuid },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Holder {
    CreateWorker,
    ConfigureWorker,
    RestoreWorker,
    OrderedCarrier,
}

#[derive(Clone, Copy, Debug)]
enum Request {
    Create,
    Configure,
    Restore,
    Mode,
    FastOff,
}

impl Request {
    fn holder(self) -> Holder {
        match self {
            Self::Create => Holder::CreateWorker,
            Self::Configure => Holder::ConfigureWorker,
            Self::Restore => Holder::RestoreWorker,
            Self::Mode | Self::FastOff => Holder::OrderedCarrier,
        }
    }
}

enum PendingReply {
    Opening(oneshot::Receiver<OpeningFact>),
    Setup(oneshot::Receiver<SessionCreatedSetupFact>),
    Setting(oneshot::Receiver<SettingFact>),
}

impl PendingReply {
    fn assert_pending(&mut self) {
        fn empty<Fact>(receipt: &mut oneshot::Receiver<Fact>) {
            assert!(matches!(
                receipt.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        match self {
            Self::Opening(receipt) => empty(receipt),
            Self::Setup(receipt) => empty(receipt),
            Self::Setting(receipt) => empty(receipt),
        }
    }

    async fn observed_id(self) -> ReplyId {
        match self {
            Self::Opening(receipt) => {
                match receipt.await.expect("the opening envelope was admitted") {
                    OpeningFact::CreateFailed {
                        attempt,
                        failure: OpeningFailure::OutcomeUnknown,
                        ..
                    } => ReplyId::Create(attempt.0),
                    OpeningFact::RestoreFailed {
                        run,
                        failure: OpeningFailure::OutcomeUnknown,
                    } => ReplyId::Restore(run.attempt.0),
                    OpeningFact::Created { .. }
                    | OpeningFact::CreateFailed { .. }
                    | OpeningFact::Restored { .. }
                    | OpeningFact::RestoreFailed { .. } => {
                        panic!("unexpected failure for a queued opening")
                    }
                }
            }
            Self::Setup(receipt) => {
                let CreatedSetupFact::Configured { attempt, .. } = receipt
                    .await
                    .expect("the configure worker owns its buffered result")
                else {
                    panic!("unexpected configure result");
                };
                ReplyId::Configure(attempt.0)
            }
            Self::Setting(receipt) => {
                let SettingFact::NotSent { restore, setting } = receipt
                    .await
                    .expect("the ordered carrier owes its accepted setter")
                else {
                    panic!("the accepted queued setter must settle as not sent");
                };
                ReplyId::Setter {
                    restore: restore.attempt.0,
                    application: setting.change.attempt.0,
                }
            }
        }
    }
}

struct AcceptEvents;

impl EventPort<SessionLifecycleFact> for AcceptEvents {
    fn send<'a>(&'a self, _: SessionLifecycleFact) -> EventSendFuture<'a>
    where
        SessionLifecycleFact: 'a,
    {
        Box::pin(async { Ok(()) })
    }
}

struct Admission {
    binding: OwnerOpenings,
    id: ReplyId,
    receipt: PendingReply,
    release_setup: Option<std::sync::mpsc::Sender<()>>,
}

fn reserve<Fact: Send + 'static>() -> (LifecycleReply<Fact>, oneshot::Receiver<Fact>) {
    let (sender, receiver) = oneshot::channel();
    (Box::new(output_stop_tests::ReplyRecorder(sender)), receiver)
}

async fn admit(
    request: Request,
    wire: &Arc<OwnerAcpWire>,
    tasks: &TaskTracker,
    connection: &CancellationToken,
    ledger: &mut ReplyLedger<ReplyId, Holder>,
) -> Admission {
    let owner = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    let create = CreateAttempt(Uuid::now_v7());
    let restore = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let mut release_setup = None;
    let binding = OwnerOpenings::new(
        owner,
        Arc::clone(wire),
        Arc::<OnceLock<String>>::default(),
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        tasks.clone(),
        connection.clone(),
    );
    let setup = || OpenSetup {
        cwd: "/".into(),
        work_roots: Default::default(),
        scratch_folder: None,
    };
    let (id, receipt, site) = match request {
        Request::Create | Request::Restore => {
            let (id, opening) = match request {
                Request::Create => (
                    ReplyId::Create(create.0),
                    OpenRequest::Create {
                        owner,
                        attempt: create,
                        requested: Default::default(),
                        setup: setup(),
                    },
                ),
                Request::Restore => (
                    ReplyId::Restore(restore.attempt.0),
                    OpenRequest::Restore {
                        owner,
                        run: restore,
                        session: switchboard_harness_contracts::HarnessSessionId(
                            "queued-session".into(),
                        ),
                        via: provider_session_machines::session_lifecycle_machine::RestoreVia::Load,
                        setup: setup(),
                        requested_mode: None,
                    },
                ),
                Request::Configure | Request::Mode | Request::FastOff => {
                    panic!("this request does not use the open carrier")
                }
            };
            let (reply, receipt) = reserve();
            ledger.reserve(id);
            binding
                .open(
                    opening,
                    reply,
                    Arc::new(AcceptEvents),
                    Box::pin(std::future::pending()),
                )
                .expect("admit exact opening to carrier");
            (
                id,
                PendingReply::Opening(receipt),
                ReplySite::Admitted(request.holder()),
            )
        }
        Request::Configure => {
            let session = Box::new(fixture_session().await);
            let mut attempts = lock(&binding.attempts);
            let (instance, offer) = attempts
                .begin_create(
                    false,
                    create,
                    OpenCreateRequest {
                        setup: setup(),
                        requested: Default::default(),
                        stopped: CancellationToken::new(),
                    },
                )
                .unwrap_or_else(|_| panic!("create fixture admission"));
            assert!(matches!(offer, Effect::OfferCreate(_)));
            let slot = OpeningSlot::Create(create);
            attempts
                .apply(slot, &instance, Command::WriterAccepted)
                .unwrap_or_else(|_| panic!("fixture create was taken"));
            attempts
                .apply(slot, &instance, Command::Created(session))
                .unwrap_or_else(|_| panic!("fixture create returned a held session"));
            drop(attempts);
            let id = ReplyId::Configure(create.0);
            let (reply, receipt) = reserve();
            ledger.reserve(id);
            let buffered = wire.pause_opening_for_test(OpeningPausePoint::ConfigureReceiptBuffered);
            binding
                .configure_created(create, reply, Box::pin(std::future::pending()))
                .expect("admit exact configure worker");
            buffered
                .reached
                .await
                .expect("real configure worker holds the original reply");
            release_setup = Some(buffered.release);
            (
                id,
                PendingReply::Setup(receipt),
                ReplySite::WorkerResult(request.holder()),
            )
        }
        Request::Mode | Request::FastOff => {
            let kind = if let Request::Mode = request {
                ProviderSettingKind::Mode
            } else {
                ProviderSettingKind::Fast
            };
            binding
                .session
                .set("queued-session".into())
                .expect("bind preexisting provider session");
            let mut session = fixture_session().await;
            if kind == ProviderSettingKind::Fast {
                session.catalog =
                    ProviderSettingsCatalog::from_record(Arc::new(ProviderSettingsCatalogRecord {
                        config_options: vec![switchboard_harness_contracts::ProviderConfigOption {
                            id: "fast".into(),
                            name: "Fast".into(),
                            category: Some(kind),
                            current_value:
                                switchboard_harness_contracts::ProviderConfigValue::Boolean(true),
                            choices: Vec::new(),
                        }],
                        ..Default::default()
                    }));
            }
            let catalogue = session.catalog.clone().into_record();
            let route = {
                let mut attempts = lock(&binding.attempts);
                let (instance, offer) = attempts
                    .begin_restore(
                        Some("queued-session"),
                        owner,
                        RestoreOpening {
                            run: restore,
                            session: HarnessSessionId("queued-session".into()),
                            via: RestoreVia::Resume,
                            setup: setup(),
                            requested_mode: None,
                        },
                        CancellationToken::new(),
                    )
                    .unwrap_or_else(|_| panic!("restore fixture admission"));
                let Effect::OfferRestore(start) = offer else {
                    panic!("restore fixture requires a real route");
                };
                let slot = OpeningSlot::Run(restore.generation);
                attempts
                    .apply(slot, &instance, Command::WriterAccepted)
                    .unwrap_or_else(|_| panic!("fixture restore was taken"));
                attempts
                    .apply(
                        slot,
                        &instance,
                        Command::RestoreProgress(
                            RestoreMilestone::held(session, None, None, false),
                            None,
                        ),
                    )
                    .unwrap_or_else(|_| panic!("fixture restore returned a held session"));
                start.resources.route
            };
            wire.install_opening_route(&route, "queued-session".into())
                .expect("register actual ordered route");
            let setting = OwnerSetting {
                change: RunAttempt {
                    generation: restore.generation,
                    attempt: SettingApplication(Uuid::now_v7()),
                },
                kind,
            };
            let id = ReplyId::Setter {
                restore: restore.attempt.0,
                application: setting.change.attempt.0,
            };
            ledger.reserve(id);
            let (reply, receipt) = reserve();
            let mut reply = Some(reply);
            binding
                .put_setting_back(
                    SettingAttempt {
                        restore,
                        setting,
                        value: SettingValueId(
                            if kind == ProviderSettingKind::Fast {
                                "off"
                            } else {
                                "ask"
                            }
                            .into(),
                        ),
                        catalogue,
                    },
                    &mut || {
                        ReplyAdmission::Reserved(
                            reply.take().expect("unique actual setter reservation"),
                        )
                    },
                )
                .expect("setter has been taken by the actual ordered channel");
            (
                id,
                PendingReply::Setting(receipt),
                ReplySite::Admitted(request.holder()),
            )
        }
    };
    ledger.move_to(id, ReplySite::Reserved, site);
    let mut admission = Admission {
        binding,
        id,
        receipt,
        release_setup,
    };
    admission.receipt.assert_pending();
    // The settled signal later checks the same envelope's original identity.
    admission
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_opening_histories_drain_two_exact_reply_envelopes() {
    let kinds = [
        Request::Create,
        Request::Configure,
        Request::Restore,
        Request::Mode,
        Request::FastOff,
    ];
    let mut visited = [[[false; 2]; 5]; 5];
    for (left, older) in kinds.into_iter().enumerate() {
        for (right, newer) in kinds.into_iter().enumerate() {
            for (cut, connection_lost) in [false, true].into_iter().enumerate() {
                let (wire, writer) = OwnerAcpWire::new(
                    NonZeroUsize::new(4).expect("two admissions"),
                    Arc::new(ConnectionCatalogueBehavior),
                );
                let wire = Arc::new(wire);
                let connection = CancellationToken::new();
                let tasks = TaskTracker::new();
                let mut ledger = ReplyLedger::default();
                let first = admit(older, &wire, &tasks, &connection, &mut ledger).await;
                let before = ledger.owed();
                let second = admit(newer, &wire, &tasks, &connection, &mut ledger).await;
                let newly = [(second.id, newer.holder())];
                ledger.assert_conserved(
                    before.clone(),
                    &newly,
                    &[(first.id, older.holder()), newly[0]],
                    &[],
                );
                let output_stop = CancellationToken::new();
                output_stop.cancel();
                if connection_lost {
                    connection.cancel();
                }
                writer
                    .run(
                        output_stop_tests::inert_connection().await,
                        output_stop_tests::configuration(),
                        output_stop,
                        CancellationToken::new(),
                    )
                    .await;

                let mut delivered = Vec::new();
                for (admission, request) in [(first, older), (second, newer)] {
                    let _ = admission.binding.observe_health();
                    let site = if let Some(release) = admission.release_setup {
                        release
                            .send(())
                            .expect("drain the actual held configure effect");
                        ReplySite::WorkerResult(request.holder())
                    } else {
                        ReplySite::Admitted(request.holder())
                    };
                    let observed = admission.receipt.observed_id().await;
                    ledger.delivered(admission.id, observed, site);
                    delivered.push(observed);
                    drop(admission.binding);
                }
                ledger.assert_conserved(before, &newly, &[], &delivered);
                tasks.close();
                tasks.wait().await;
                visited[left][right][cut] = true;
            }
        }
    }
    assert_eq!(visited, [[[true; 2]; 5]; 5]);
}

async fn fixture_session() -> OwnerOpenedSession {
    let outgoing = sink::drain().sink_map_err(|infallible| match infallible {});
    let incoming = stream::empty::<std::io::Result<String>>();
    let connection: ConnectionTo<Agent> = Client
        .builder()
        .connect_with(Lines::new(outgoing, incoming), async |connection| {
            Ok(connection)
        })
        .await
        .expect("construct the held ACP fixture session");
    OwnerOpenedSession {
        provider_id: SessionId::new("matrix-session"),
        connection,
        route: None,
        capabilities: Default::default(),
        catalog: ProviderSettingsCatalog::from_record(ProviderSettingsCatalogRecord {
            modes: vec![ProviderSettingChoice {
                value: "ask".into(),
                label: "Ask".into(),
            }],
            ..Default::default()
        }),
        behavior: Arc::new(ConnectionCatalogueBehavior),
        buffer: Arc::default(),
        projection: None,
        in_folder: None,
        observed_tool_calls: Arc::default(),
        opening_disposal: None,
    }
}
