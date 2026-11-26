//! A real provider receipt survives the loss of its local ordered writer.

use super::*;
use acp_client_runtime::{acp_transport::TransportRun, owner_wire::OpeningPausePoint};
use provider_session_machines::harness_lifecycle::OpeningFailure;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffered_load_and_resume_keep_their_exact_receipt_across_writer_stop() {
    for (via, method) in [
        (RestoreVia::Load, "session/load"),
        (RestoreVia::Resume, "session/resume"),
    ] {
        for behind_untaken_close in [false, true] {
            assert_peer_receipt_after_writer_stop(via, method, behind_untaken_close).await;
        }
    }
}

async fn assert_peer_receipt_after_writer_stop(
    via: RestoreVia,
    method: &str,
    behind_untaken_close: bool,
) {
    let fixture = hub_runtime_with_launch(|receipt| {
        crate::external_provider_runtime::acp_scripted_fixture::launch_fixture_agent(
            "permission-mode-agent.ts",
            [
                receipt.display().to_string(),
                "cursor".into(),
                "no-directories".into(),
                "advertise-restore".into(),
                "advertise-close".into(),
            ]
            .into(),
        )
    })
    .await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let wire = fixture.runtime.client.owner_wire();
    let receipt = wire.pause_opening_for_test(OpeningPausePoint::RestoreReceiptBuffered);
    let stopped = wire.pause_opening_for_test(OpeningPausePoint::WriterStopDelivered);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let OpenRequest::Restore { session, setup, .. } = restore_request(&fixture, owner, run) else {
        panic!("expected the saved session setup");
    };
    let (reply, mut answer) = oneshot::channel();
    binding
        .open(
            OpenRequest::Restore {
                owner,
                run,
                session,
                via,
                setup,
                requested_mode: None,
            },
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(std::future::pending()),
        )
        .expect("the ordered carrier took the exact restore");
    receipt
        .reached
        .await
        .expect("the restore worker owns the provider's successful receipt");
    let read_provider_receipt = || -> serde_json::Value {
        serde_json::from_slice(
            &std::fs::read(&fixture.receipt).expect("read the permission-mode agent receipt"),
        )
        .expect("parse the permission-mode agent receipt")
    };
    let provider_receipt = read_provider_receipt();
    assert_eq!(provider_receipt["restores"][0]["method"], method);
    assert_eq!(
        provider_receipt["restores"]
            .as_array()
            .expect("restore count")
            .len(),
        1
    );
    assert!(matches!(
        answer.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let binding = Arc::new(binding);
    let close = if behind_untaken_close {
        let before_offer = wire.pause_opening_for_test(OpeningPausePoint::BeforeCloseOffer);
        let deferred = wire.pause_opening_for_test(OpeningPausePoint::RestoreDeferred);
        let close = RunAttempt {
            generation: run.generation,
            attempt: CloseAttempt(Uuid::now_v7()),
        };
        let (reply, mut result) = oneshot::channel();
        let caller = tokio::task::spawn_blocking({
            let binding = Arc::clone(&binding);
            move || binding.close(close, Box::new(Reply(reply)))
        });
        before_offer
            .reached
            .await
            .expect("the exact close is reserved but still unoffered");
        receipt
            .release
            .send(())
            .expect("submit the provider success to the older opening");
        deferred
            .reached
            .await
            .expect("the offering-close row owns the deferred restore");
        assert_eq!(result.try_recv(), Err(oneshot::error::TryRecvError::Empty));
        Some((caller, result, before_offer.release, deferred.release))
    } else {
        None
    };
    assert!(matches!(
        answer.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    wire.stop_ordered_writer_for_test(TransportRun {
        incarnation: owner.incarnation,
        generation: run.generation,
    })
    .expect("stop the real ordered writer");
    stopped
        .reached
        .await
        .expect("the binding received the actual writer stop");
    stopped
        .release
        .send(())
        .expect("finish the writer observation");
    if let Some((caller, result, offer, deferred)) = close {
        deferred
            .send(())
            .expect("drain the deferred restore receipt");
        offer
            .send(())
            .expect("try to offer the close on the stopped writer");
        assert!(caller.await.expect("close caller completes").is_err());
        assert!(
            result.await.is_err(),
            "the unadmitted close has no reply debt"
        );
        assert!(
            read_provider_receipt()["closes"]
                .as_array()
                .expect("close count")
                .is_empty()
        );
    } else {
        receipt
            .release
            .send(())
            .expect("drain the buffered restore worker");
    }
    let fact = answer
        .await
        .expect("the admitted restore receipt must settle");
    let OpeningFact::RestoreFailed {
        run: exact,
        failure: OpeningFailure::Unavailable { diagnostic },
    } = fact
    else {
        panic!("buffered peer success must settle as locally unavailable");
    };
    assert_eq!(exact, run);
    assert!(diagnostic.contains(method), "{diagnostic}");
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
