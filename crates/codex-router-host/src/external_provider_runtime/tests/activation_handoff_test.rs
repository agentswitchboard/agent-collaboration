//! A real created SDK reader can lose its exact handoff on a live connection.

use super::*;
use acp_client_runtime::{acp_transport::TransportRun, owner_wire::OpeningPausePoint};
use provider_session_machines::{
    harness_lifecycle::LifecycleRefusal, session_lifecycle_machine::CloseAttempt,
};

#[tokio::test]
async fn acp_create_refuses_activation_when_its_reader_handoff_was_closed() {
    exercise_failed_created_handoff(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn retirement_fences_failed_activation_before_its_result_reaches_the_row() {
    exercise_failed_created_handoff(true).await;
}

async fn exercise_failed_created_handoff(retire_before_result: bool) {
    let fixture = hub_runtime_with(&["advertise-close"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let retirement = fixture.runtime.retirement();
    let stop = CancellationToken::new();
    let attempt = CreateAttempt(Uuid::now_v7());
    let generation = SessionGeneration::from_number(1);
    let scratch = fixture._root.path().join("scratch");
    std::fs::create_dir(&scratch).expect("create session scratch for the local grant");
    let wire = fixture.runtime.client.owner_wire();
    let (close_handoff, closed) = wire.close_created_handoff_for_test();
    let (reply, opening) = oneshot::channel();
    binding
        .open(
            OpenRequest::Create {
                owner,
                attempt,
                setup: OpenSetup {
                    cwd: fixture._root.path().to_owned(),
                    work_roots: Default::default(),
                    scratch_folder: Some(scratch),
                },
                requested: Default::default(),
            },
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("offer a real create");
    assert!(matches!(opening.await.expect("provider create reply"),
        OpeningFact::Created { attempt: exact, .. } if exact == attempt));
    let (reply, configured) = oneshot::channel();
    binding
        .configure_created(
            attempt,
            Box::new(Reply(reply)),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("configure the real create");
    assert!(matches!(configured.await.expect("provider setup complete"),
        CreatedSetupFact::Configured { attempt: exact, .. } if exact == attempt));
    close_handoff
        .send(())
        .expect("close only this create's reader receiver");
    closed
        .await
        .expect("the matching SDK handoff receiver has closed");
    assert!(
        !retirement.is_cancelled(),
        "the provider connection is still live"
    );
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Create(attempt)),
        Held::Held
    );

    let start_reader = move |binding: &SessionHarnessBinding, stop: CancellationToken| {
        binding.activate(
            ActivationSetup::Created {
                attempt,
                generation,
            },
            AcceptedRunSettings {
                catalogue: Default::default(),
                router_auto_requested: false,
            },
            Arc::new(AcceptEvents),
            Box::pin(stop.cancelled_owned()),
        )
    };
    let binding = Arc::new(binding);
    let route = TransportRun {
        incarnation: owner.incarnation,
        generation,
    };
    let activation = if retire_before_result {
        let failure = wire.pause_opening_for_test(OpeningPausePoint::BeforeFailedHandoffCommit);
        let activating = tokio::task::spawn_blocking({
            let binding = Arc::clone(&binding);
            let stop = stop.clone();
            move || start_reader(&binding, stop)
        });
        failure
            .reached
            .await
            .expect("the staged SDK reader has refused");
        assert!(wire.opening_route_is_registered_for_test(route));
        binding.retire(generation);
        assert!(
            !wire.opening_route_is_registered_for_test(route),
            "the retirement step must fence the staged route before the failed actor returns"
        );
        failure
            .release
            .send(())
            .expect("deliver the late exact activation failure");
        activating.await.expect("activation call completes")
    } else {
        start_reader(&binding, stop.clone())
    };
    assert_eq!(
        activation,
        Err(ActivationRefusal::ReaderUnavailable),
        "created activation must refuse synchronously when the exact SDK handoff is closed",
    );
    assert!(!wire.opening_route_is_registered_for_test(route));
    let mut binding = Arc::into_inner(binding).expect("the activating actor released its binding");
    let (reply, _answer) = oneshot::channel();
    assert_eq!(
        binding.close(
            RunAttempt {
                generation,
                attempt: CloseAttempt(Uuid::now_v7())
            },
            Box::new(Reply(reply)),
        ),
        Err(LifecycleRefusal::BoundSessionNotLoaded),
        "the failed handoff must clear the staged live route and its grant",
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);
    assert!(!retirement.is_cancelled());
    let follow_up = RunAttempt {
        generation: SessionGeneration::from_number(2),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    assert_eq!(
        load_fixture_binding(&fixture, &mut binding, owner, follow_up, stop.clone())
            .await
            .1,
        ReplayCompleteness::Complete,
        "the same live writer can reopen the session",
    );
    binding.retire(follow_up.generation);
    assert_eq!(recorded_requests(&fixture.receipt, "session/new"), 1);
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), 1);
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);
    drop(binding);
    stop.cancel();
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn accepted_created_sdk_handoff_cannot_commit_after_connection_retirement() {
    exercise_created_handoff(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn created_reader_delivers_its_waiting_update_after_activation_commits() {
    exercise_created_handoff(false).await;
}

struct RecordLifecycle(tokio::sync::mpsc::UnboundedSender<SessionLifecycleFact>);

impl EventPort<SessionLifecycleFact> for RecordLifecycle {
    fn send<'a>(&'a self, fact: SessionLifecycleFact) -> EventSendFuture<'a>
    where
        SessionLifecycleFact: 'a,
    {
        Box::pin(async move {
            let _test_ended = self.0.send(fact);
            Ok(())
        })
    }
}

async fn exercise_created_handoff(retire_before_commit: bool) {
    let fixture = hub_runtime_with(&["hold-created-update"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let attempt = CreateAttempt(Uuid::now_v7());
    let generation = SessionGeneration::from_number(1);
    let stop = CancellationToken::new();
    let (reply, opened) = oneshot::channel();
    binding
        .open(
            OpenRequest::Create {
                owner,
                attempt,
                requested: Default::default(),
                setup: OpenSetup {
                    cwd: fixture._root.path().into(),
                    work_roots: Default::default(),
                    scratch_folder: None,
                },
            },
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("send actual create");
    assert!(
        matches!(opened.await.expect("provider create result"), OpeningFact::Created { attempt: exact, .. } if exact == attempt)
    );
    let (reply, configured) = oneshot::channel();
    binding
        .configure_created(
            attempt,
            Box::new(Reply(reply)),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("configure actual create");
    assert!(
        matches!(configured.await.expect("setup settled"), CreatedSetupFact::Configured { attempt: exact, .. } if exact == attempt)
    );
    let wire = fixture.runtime.client.owner_wire();
    let paused = wire.pause_opening_for_test(OpeningPausePoint::ReaderHandoffAccepted);
    let ingress = wire.pause_opening_for_test(OpeningPausePoint::CreatedUpdateIngress);
    let (record, mut facts) = tokio::sync::mpsc::unbounded_channel();
    let binding = Arc::new(binding);
    let activating = tokio::task::spawn_blocking({
        let binding = Arc::clone(&binding);
        move || {
            binding.activate(
                ActivationSetup::Created {
                    attempt,
                    generation,
                },
                AcceptedRunSettings {
                    catalogue: Default::default(),
                    router_auto_requested: false,
                },
                Arc::new(RecordLifecycle(record)),
                Box::pin(stop.cancelled_owned()),
            )
        }
    });
    paused
        .reached
        .await
        .expect("created SDK receiver accepted the real handoff");
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Create(attempt)),
        Held::NotHeld(LifecycleRefusal::OpeningInProgress)
    );
    node_test_fixture::release_fixture(&fixture.receipt, ".release-created-update")
        .expect("the real provider may now send one update");
    ingress
        .reached
        .await
        .expect("the provider update reached ACP while activation was held");
    ingress
        .release
        .send(())
        .expect("let SDK route the waiting provider update");
    assert!(matches!(
        facts.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    if retire_before_commit {
        fixture.runtime.retirement().cancel();
    }
    paused
        .release
        .send(())
        .expect("return the actual handoff result to the table");
    let activation = activating.await.expect("activation call");
    if retire_before_commit {
        assert_eq!(activation, Err(ActivationRefusal::ConnectionRetired));
        assert_eq!(
            binding.opening_is_held(OpenAttempt::Create(attempt)),
            Held::NotHeld(LifecycleRefusal::ConnectionRetired)
        );
        assert!(
            facts.try_recv().is_err(),
            "a stale created reader cannot publish its waiting frame"
        );
    } else {
        assert_eq!(activation, Ok(()));
        let observation = facts
            .recv()
            .await
            .expect("the active SDK reader processes the real waiting update");
        assert!(
            matches!(observation, SessionLifecycleFact::CatalogueObserved { run, .. } if run == generation)
        );
        binding.retire(generation);
    }
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
