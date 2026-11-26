//! Observe the ACP binding's exact opening and activation answers.

#[path = "activation_handoff_test.rs"]
mod activation_handoff;
#[path = "opening_wait_test.rs"]
mod opening_wait;

use super::*;
use crate::session_owner_lifecycle_reports::SessionHarnessBinding;

use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        AcceptedRunSettings, ActivationRefusal, ActivationSetup, CallbackAdmission, CreateAttempt,
        CreatedSettingsFailure, CreatedSetupFact, EventPort, EventSendFuture, HarnessOwnerEvents,
        HarnessProvider, HarnessSessionId, Held, LifecycleMove, OpenAttempt, OpenRequest,
        OpenSetup, OpeningFact, ReplayCompleteness, RequestedProviderSettings, ReservedReply,
        RestoreLoadReadback, RestoredOpening, SessionLifecycleFact, SessionLifecyclePort,
    },
    session_lifecycle_machine::{LoadAttempt, RestoreVia},
    session_owner::{ConnectionEpoch, OwnerAddress, OwnerIncarnation, RunAttempt},
};
use session_event_model::ProviderAuthStatus;
use std::num::NonZeroUsize;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acp_replay_completeness_reaches_the_owner_without_changing_visible_history() {
    use provider_session_machines::{
        session_lifecycle_machine::SessionLifecycle, session_owner::RestoreDisposition,
    };

    let cases: [(&[&str], ReplayCompleteness, &[&str]); 3] = [
        (
            &["saved-history"],
            ReplayCompleteness::Complete,
            &["first saved update", "second saved update"],
        ),
        (
            &["saved-history", "malformed-saved-update"],
            ReplayCompleteness::Incomplete {
                skipped_updates: NonZeroUsize::MIN,
            },
            &["first saved update", "second saved update"],
        ),
        (
            &["saved-history", "opaque-saved-update"],
            ReplayCompleteness::Complete,
            &[
                "first saved update",
                "opaque saved update",
                "second saved update",
            ],
        ),
    ];
    for (behaviours, completeness, expected_history) in cases {
        let fixture = hub_runtime_with(behaviours).await;
        let tasks = TaskTracker::new();
        let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
        let direct_run = RunAttempt {
            generation: SessionGeneration::from_number(1),
            attempt: LoadAttempt(Uuid::now_v7()),
        };
        let (_, direct) = load_fixture_binding(
            &fixture,
            &mut binding,
            owner,
            direct_run,
            CancellationToken::new(),
        )
        .await;
        binding.retire(direct_run.generation);
        drop(binding);
        tasks.close();
        tasks.wait().await;

        let saved = "reactive-session-1";
        fixture
            .runtime
            .load_session(saved.to_owned(), fixture._root.path().to_owned())
            .await
            .expect("the public restore still succeeds");
        let (inbox, worker) = fixture
            .hub
            .owner_inbox(fixture.hub_target.clone(), NonZeroUsize::MIN);
        assert!(
            worker.is_none(),
            "the real owner registered the publication inbox"
        );
        let mut publication = inbox.capacity_returned();
        let retained = loop {
            let attached = fixture
                .hub
                .attach(fixture.hub_target.clone())
                .await
                .expect("retained history");
            if attached.snapshot.iter().any(|event| {
                matches!(
                    event.event,
                    SessionEvent::StateChanged {
                        state: SessionState::Idle
                    }
                )
            }) {
                break attached.snapshot;
            }
            publication.changed().await.expect("activation publication");
        };
        let history: Vec<_> = retained
            .iter()
            .filter_map(|observed| match &observed.event {
                SessionEvent::ItemStarted { item } => item.text.as_deref(),
                _ => None,
            })
            .collect();
        assert_eq!(
            history, expected_history,
            "{behaviours:?} keeps every decodable update in order"
        );
        assert_eq!(
            direct, completeness,
            "{behaviours:?} exact ACP opening must report replay completeness"
        );
        let restore = fixture
            .runtime
            .owner_door
            .get()
            .expect("production door")
            .existing_provider_session(saved)
            .expect("restored owner")
            .snapshots();
        let snapshot = restore.borrow().clone();
        let answer = snapshot
            .last_restore
            .expect("owner recorded the exact restore result");
        assert_eq!(
            answer.replay,
            Some(completeness),
            "{behaviours:?} Host retained the ACP replay claim"
        );
        assert!(matches!(
            answer.disposition,
            RestoreDisposition::Restored { .. }
        ));
        assert_eq!(
            snapshot.lifecycle,
            SessionLifecycle::Live {
                generation: answer.attempt.generation
            }
        );
        fixture.runtime.shutdown().await;
    }
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
async fn acp_unsupported_restore_setter_refuses_before_reserving_a_full_completion_carrier() {
    use acp_client_runtime::session_owner_task::{OwnerTaskError, SessionOwnerTask};
    use provider_session_machines::{
        ProviderSettingKind,
        harness_lifecycle::{
            LifecycleRefusal, RestoreSettingRefusal, SettingAttempt,
            owner_event_for_restore_setting,
        },
        session_owner::{OwnerSetting, ReplyAdmission, WriterState},
        session_setting_machine::{SettingApplication, SettingValueId},
    };
    use std::cell::Cell;

    let fixture = hub_runtime_with(&["fast-on-only"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let (readback, _) =
        load_fixture_binding(&fixture, &mut binding, owner, run, CancellationToken::new()).await;
    let unavailable_off = readback
        .fast_on
        .expect("the real ACP catalogue reads fast on");
    let slots = NonZeroUsize::new(4).expect("completion slots");
    let (_task, handle) = SessionOwnerTask::new(
        owner,
        fixture.runtime.client.runtime_clock(),
        NonZeroUsize::MIN,
        slots,
    );
    let completions = handle.completion_port();
    let held: Vec<_> = (0..slots.get())
        .map(|_| {
            completions
                .reserve(owner)
                .expect("reserve each real owner slot")
        })
        .collect();
    assert!(matches!(
        completions.reserve(owner),
        Err(OwnerTaskError::CompletionCapacity)
    ));
    let reserve_calls = Cell::new(0);
    let mut reserve = || {
        reserve_calls.set(reserve_calls.get() + 1);
        match completions.reserve(owner) {
            Ok(reply) => ReplyAdmission::Reserved(reply.map(owner_event_for_restore_setting)),
            Err(OwnerTaskError::CompletionCapacity) => ReplyAdmission::Refused(WriterState::Full),
            Err(OwnerTaskError::ExecutorStopped) => ReplyAdmission::Refused(WriterState::Stopped),
        }
    };
    let setting = |kind, attempt, value: &str| SettingAttempt {
        restore: run,
        setting: OwnerSetting {
            change: RunAttempt {
                generation: run.generation,
                attempt,
            },
            kind,
        },
        value: SettingValueId(value.to_owned()),
        catalogue: readback.catalogue.clone(),
    };

    assert_eq!(
        binding.put_setting_back(
            setting(ProviderSettingKind::Fast, unavailable_off, "off"),
            &mut reserve
        ),
        Err(RestoreSettingRefusal::Harness(
            LifecycleRefusal::SettingValueNotOffered
        )),
        "a locally unsupported setter is refused while every completion slot is held",
    );
    assert_eq!(
        reserve_calls.get(),
        0,
        "the invalid setter must not request a completion slot"
    );
    assert_eq!(
        binding.put_setting_back(
            setting(
                ProviderSettingKind::Mode,
                SettingApplication(Uuid::now_v7()),
                "agent"
            ),
            &mut reserve
        ),
        Err(RestoreSettingRefusal::ReplyReservation(WriterState::Full)),
        "a writable local setting reaches the full reply reservation",
    );
    assert_eq!(reserve_calls.get(), 1);
    assert_eq!(
        recorded_requests(&fixture.receipt, "session/set_config_option"),
        0
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/set_mode"), 0);

    drop(held);
    binding.retire(run.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}

async fn load_fixture_binding(
    fixture: &HubRuntime,
    binding: &mut SessionHarnessBinding,
    owner: OwnerAddress,
    run: RunAttempt<LoadAttempt>,
    stop: CancellationToken,
) -> (RestoreLoadReadback, ReplayCompleteness) {
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
            Arc::new(AcceptEvents),
            Box::pin(stop.cancelled_owned()),
        )
        .expect("real ACP load offered");
    let OpeningFact::Restored {
        run: exact,
        opening: RestoredOpening::Loaded {
            readback, replay, ..
        },
    } = answer.await.expect("real ACP opening answer")
    else {
        panic!("ACP did not report a load opening");
    };
    assert_eq!(exact, run);
    (readback, replay)
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
        assert_eq!(
            load_fixture_binding(&fixture, &mut binding, owner, run, stop.clone())
                .await
                .1,
            ReplayCompleteness::Complete
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
