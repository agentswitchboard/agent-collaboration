//! Observe the ACP binding's exact opening and activation answers.

use super::*;
use crate::session_owner_lifecycle_reports::SessionHarnessBinding;

use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        AcceptedRunSettings, ActivationRefusal, ActivationSetup, CallbackAdmission, CreateAttempt,
        CreatedSettingsFailure, CreatedSetupFact, EventPort, EventSendFuture, HarnessOwnerEvents,
        HarnessProvider, HarnessSessionId, Held, LifecycleMove, OpenAttempt, OpenRequest,
        OpenSetup, OpeningFact, RequestedProviderSettings, ReservedReply, RestoredOpening,
        SessionLifecycleFact, SessionLifecyclePort,
    },
    session_lifecycle_machine::{LoadAttempt, RestoreVia},
    session_owner::{ConnectionEpoch, OwnerAddress, OwnerIncarnation, RunAttempt},
};
use session_event_model::ProviderAuthStatus;
use tokio::sync::oneshot;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

struct AcceptEvents;
impl HarnessOwnerEvents<ProviderAuthStatus> for AcceptEvents {
    fn report_harness_gone(&self) -> EventSendFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    fn report_connection_auth(&self, _: ProviderAuthStatus) -> CallbackAdmission {
        CallbackAdmission::Admitted
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acp_invalid_create_setting_is_known_unsent_and_offers_only_correction() {
    let fixture = hub_runtime().await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let attempt = CreateAttempt(Uuid::now_v7());
    let value = "unadvertised-mode";
    let stop = CancellationToken::new();
    let (opened, opening) = oneshot::channel();
    binding
        .open(
            OpenRequest::Create {
                owner,
                attempt,
                setup: OpenSetup {
                    cwd: fixture._root.path().to_owned(),
                    work_roots: Default::default(),
                    scratch_folder: None,
                },
                requested: RequestedProviderSettings {
                    mode: Some(value.into()),
                    ..Default::default()
                },
            },
            Box::new(Reply(opened)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("offer the create");
    let OpeningFact::Created {
        attempt: created,
        session,
    } = opening.await.expect("ACP created")
    else {
        panic!("expected a held create");
    };
    assert_eq!(created, attempt);
    let (reply, answer) = oneshot::channel();
    binding
        .configure_created(
            attempt,
            Box::new(Reply(reply)),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("configure the held creation");
    let CreatedSetupFact::RemainsCreated {
        attempt: configured,
        failure,
        ..
    } = answer.await.expect("ACP reports create settings")
    else {
        panic!("expected a created session with a known invalid setting");
    };
    assert_eq!(configured, attempt);
    let CreatedSettingsFailure::Invalid(reason) = &failure else {
        panic!("a local catalogue rejection must be reported as invalid");
    };
    assert_eq!(reason.session, session);
    assert_eq!(
        reason.kind,
        provider_session_machines::ProviderSettingKind::Mode
    );
    assert_eq!(reason.value, value);
    assert!(!reason.advertised.iter().any(|offered| offered == value));
    assert_eq!(reason.valid_moves(), &[LifecycleMove::CorrectRequest]);
    assert_eq!(failure.valid_moves(), reason.valid_moves());
    assert_eq!(recorded_requests(&fixture.receipt, "session/new"), 1);
    assert_eq!(
        recorded_requests(&fixture.receipt, "session/set_config_option"),
        0
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/set_mode"), 0);
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);

    drop(binding);
    stop.cancel();
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
impl EventPort<SessionLifecycleFact> for AcceptEvents {
    fn send<'a>(&'a self, _: SessionLifecycleFact) -> EventSendFuture<'a>
    where
        SessionLifecycleFact: 'a,
    {
        Box::pin(async { Ok(()) })
    }
}
struct Reply<Fact>(oneshot::Sender<Fact>);
impl<Fact: Send> ReservedReply<Fact> for Reply<Fact> {
    fn send(self: Box<Self>, fact: Fact) {
        assert!(
            self.0.send(fact).is_ok(),
            "the test still holds the opening reply"
        );
    }
}

fn bind_fixture(
    runtime: &ExternalProviderRuntime,
    tasks: &TaskTracker,
) -> (OwnerAddress, SessionHarnessBinding) {
    let provider = acp_client_runtime::owner_acp_binding::OwnerAcpProvider::new(
        runtime.client.owner_wire(),
        runtime.client.owner_approval_bindings(),
        runtime.client.runtime_clock(),
        tasks.clone(),
        runtime.retirement(),
    );
    let owner = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    (owner, provider.bind(owner, Arc::new(AcceptEvents)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acp_activation_preserves_the_exact_held_and_active_attempts() {
    let fixture = hub_runtime().await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let events: SessionLifecyclePort = Arc::new(AcceptEvents);
    let first = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let second = RunAttempt {
        generation: SessionGeneration::from_number(2),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let stop = CancellationToken::new();
    let activate = |binding: &SessionHarnessBinding, run| {
        binding.activate(
            ActivationSetup::Restored(run),
            AcceptedRunSettings {
                catalogue: Default::default(),
                router_auto_requested: false,
            },
            events.clone(),
            Box::pin(stop.clone().cancelled_owned()),
        )
    };
    assert_eq!(
        activate(&binding, first),
        Err(ActivationRefusal::OpeningNotHeld)
    );
    for run in [first, second] {
        let (reply, answer) = oneshot::channel();
        binding
            .open(
                OpenRequest::Restore {
                    owner,
                    run,
                    session: HarnessSessionId("reactive-session-1".into()),
                    via: RestoreVia::Load,
                    setup: OpenSetup {
                        cwd: fixture._root.path().to_owned(),
                        work_roots: Default::default(),
                        scratch_folder: None,
                    },
                    requested_mode: None,
                },
                Box::new(Reply(reply)),
                events.clone(),
                Box::pin(stop.clone().cancelled_owned()),
            )
            .expect("real ACP load offered");
        assert!(
            matches!(answer.await.expect("real ACP opening answer"), OpeningFact::Restored {
            run: exact, opening: RestoredOpening::Loaded { .. }
        } if exact == run)
        );
        let wrong = RunAttempt {
            attempt: LoadAttempt(Uuid::now_v7()),
            ..run
        };
        assert_eq!(
            activate(&binding, wrong),
            Err(ActivationRefusal::AttemptMismatch)
        );
        if run == second {
            assert_eq!(
                activate(&binding, first),
                Err(ActivationRefusal::OpeningNotHeld)
            );
        }
        assert_eq!(
            binding.opening_is_held(OpenAttempt::Restore(run)),
            Held::Held
        );
        assert_eq!(activate(&binding, run), Ok(()));
        assert_eq!(
            activate(&binding, wrong),
            Err(ActivationRefusal::AttemptMismatch)
        );
        assert_eq!(
            activate(&binding, run),
            Err(ActivationRefusal::AlreadyActive)
        );
        binding.retire(run.generation);
        assert_eq!(
            activate(&binding, run),
            Err(ActivationRefusal::OpeningNotHeld)
        );
    }
    drop(binding);
    stop.cancel();
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
