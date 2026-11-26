//! A run-local retirement reaches the live row while its ACP writer still lives.

use super::*;
use acp_client_runtime::owner_wire::OpeningPausePoint;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn reader_retirement_ends_liveness_before_another_binding_call() {
    reader_retirement_schedule().await;
}

pub(super) async fn reader_retirement_schedule() {
    let fixture = hub_runtime_with(&["advertise-close"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let (readback, _) =
        load_fixture_binding(&fixture, &mut binding, owner, run, CancellationToken::new()).await;
    let accepted = AcceptedRunSettings {
        catalogue: readback.catalogue,
        router_auto_requested: false,
    };
    binding
        .accept_settings(OpenAttempt::Restore(run), &accepted)
        .expect("prepare actual reader");
    let reader_retired = CancellationToken::new();
    binding
        .activate(
            ActivationSetup::Restored(run),
            accepted.clone(),
            Arc::new(AcceptEvents),
            Box::pin(reader_retired.clone().cancelled_owned()),
        )
        .expect("install an active ACP reader");

    let observed = fixture
        .runtime
        .client
        .owner_wire()
        .pause_opening_for_test(OpeningPausePoint::ReaderStoppedDelivered);
    reader_retired.cancel();
    observed
        .reached
        .await
        .expect("the reader worker delivered its exact retirement");
    assert!(
        !fixture.runtime.retirement().is_cancelled(),
        "the ACP connection stays open"
    );
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(LifecycleRefusal::BoundSessionNotLoaded),
        "the retired reader may no longer advertise the live run",
    );
    assert_eq!(
        binding.activate(
            ActivationSetup::Restored(run),
            accepted,
            Arc::new(AcceptEvents),
            Box::pin(std::future::pending()),
        ),
        Err(ActivationRefusal::OpeningState(
            LifecycleRefusal::BoundSessionNotLoaded
        ))
    );
    observed
        .release
        .send(())
        .expect("let the reader worker finish");

    let replacement = RunAttempt {
        attempt: LoadAttempt(Uuid::now_v7()),
        ..run
    };
    let (_, replay) = load_fixture_binding(
        &fixture,
        &mut binding,
        owner,
        replacement,
        CancellationToken::new(),
    )
    .await;
    assert_eq!(replay, ReplayCompleteness::Complete);
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);
    binding.retire(replacement.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
