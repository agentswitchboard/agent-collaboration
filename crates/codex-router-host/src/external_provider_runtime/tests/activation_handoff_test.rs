//! A real created SDK reader can lose its exact handoff on a live connection.

use super::*;
use provider_session_machines::{
    harness_lifecycle::LifecycleRefusal, session_lifecycle_machine::CloseAttempt,
};

#[tokio::test]
async fn acp_create_refuses_activation_when_its_reader_handoff_was_closed() {
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

    assert_eq!(
        binding.activate(
            ActivationSetup::Created {
                attempt,
                generation
            },
            AcceptedRunSettings {
                catalogue: Default::default(),
                router_auto_requested: false
            },
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        ),
        Err(ActivationRefusal::ReaderUnavailable),
        "created activation must refuse synchronously when the exact SDK handoff is closed",
    );
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
