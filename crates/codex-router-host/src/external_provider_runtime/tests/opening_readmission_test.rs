//! Observe the committed re-admission separately from the real close marker.

use super::*;
use acp_client_runtime::{acp_transport::TransportRun, owner_wire::OpeningPausePoint};

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn readmission_commits_before_the_exact_wire_marker_is_cleared() {
    readmission_schedule(false).await;
    readmission_schedule(true).await;
}

pub(super) async fn readmission_schedule(lose_route: bool) {
    exercise_readmission(lose_route, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_reader_that_ends_after_refused_close_cannot_be_readmitted() {
    exercise_readmission(false, true).await;
    exercise_readmission(false, false).await;
}

async fn exercise_readmission(lose_route: bool, reader_stops: bool) {
    let fixture = hub_runtime_with_launch(|receipt| {
        crate::external_provider_runtime::acp_scripted_fixture::launch_fixture_agent(
            "permission-mode-agent.ts",
            [
                receipt.display().to_string(),
                "cursor".into(),
                "no-directories".into(),
                "advertise-restore".into(),
                "advertise-close".into(),
                "hold-close-refusal".into(),
            ]
            .into(),
        )
    })
    .await;
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
        .expect("prepare real ACP");
    let retired = CancellationToken::new();
    binding
        .activate(
            ActivationSetup::Restored(run),
            accepted,
            Arc::new(AcceptEvents),
            Box::pin(retired.clone().cancelled_owned()),
        )
        .expect("activate one restore");

    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let (reply, refused) = oneshot::channel();
    binding
        .close(close, Box::new(Reply(reply)))
        .expect("queue first close");
    node_test_fixture::release_fixture(&fixture.receipt, ".release-close")
        .expect("permit the real close denial");
    assert_eq!(
        refused.await.expect("real refusal"),
        CloseFact::RefusedStillLive { run: close }
    );

    let route = TransportRun {
        incarnation: owner.incarnation,
        generation: run.generation,
    };
    let wire = fixture.runtime.client.owner_wire();
    if reader_stops {
        let ended = wire.pause_opening_for_test(OpeningPausePoint::ReaderStoppedDelivered);
        retired.cancel();
        ended
            .reached
            .await
            .expect("deliver the actual reader ending after the close refusal");
        binding.admit_again(close);
        assert_eq!(
            binding.opening_is_held(OpenAttempt::Restore(run)),
            Held::NotHeld(LifecycleRefusal::BoundRouteUnavailable),
            "a refused close no longer owns a live reader after its ending"
        );
        assert_eq!(
            wire.closing_marker_for_test(route),
            Some(close.attempt),
            "retirement must not reopen the wire marker"
        );
        ended.release.send(()).expect("finish reader disposal");
        binding.retire(run.generation);
        drop(binding);
        fixture.runtime.shutdown().await;
        tasks.close();
        tasks.wait().await;
        return;
    }
    let paused = wire.pause_opening_for_test(OpeningPausePoint::BeforeReadmitWire);
    let binding = Arc::new(binding);
    let admission = tokio::task::spawn_blocking({
        let binding = Arc::clone(&binding);
        move || binding.admit_again(close)
    });
    paused
        .reached
        .await
        .expect("re-admission committed before effect");
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(LifecycleRefusal::RouteClosing)
    );
    assert_eq!(
        wire.closing_marker_for_test(route),
        Some(close.attempt),
        "the real closing marker must survive until the committed re-admission effect"
    );
    if lose_route {
        fixture
            .runtime
            .retire_provider_route_for_test(owner.incarnation, run.generation);
    }
    paused
        .release
        .send(())
        .expect("let the effect touch its exact route");
    admission.await.expect("re-admission completes");
    assert_eq!(wire.closing_marker_for_test(route), None);
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(if lose_route {
            LifecycleRefusal::BoundRouteUnavailable
        } else {
            LifecycleRefusal::OpeningActive
        })
    );

    if !lose_route {
        let next = RunAttempt {
            attempt: CloseAttempt(Uuid::now_v7()),
            ..close
        };
        let (reply, result) = oneshot::channel();
        binding
            .close(next, Box::new(Reply(reply)))
            .expect("readmitted route accepts a distinct real close");
        assert_eq!(
            result.await.expect("distinct wire result"),
            CloseFact::RefusedStillLive { run: next }
        );
    }
    binding.retire(run.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
