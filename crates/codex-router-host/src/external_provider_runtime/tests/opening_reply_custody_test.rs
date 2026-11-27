//! Generated reply custody across two real bindings on the same ordered writer.

use super::*;
use acp_client_runtime::{acp_transport::TransportRun, owner_wire::OpeningPausePoint};
use codex_router_test_support::reply_ledger::{ReplyLedger, ReplySite};
use provider_session_machines::harness_lifecycle::OpeningFailure;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ReplyId {
    Restore(Uuid),
    Close(Uuid),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Holder {
    RestoreWorker,
    RestoreWorkerWaitingForRow,
}

#[derive(Clone, Copy, Debug)]
enum LocalLoss {
    Writer,
    Connection,
    Route,
}

struct UntakenClose {
    id: ReplyId,
    caller: tokio::task::JoinHandle<Result<(), LifecycleRefusal>>,
    answer: oneshot::Receiver<CloseFact>,
    release_offer: std::sync::mpsc::Sender<()>,
}

struct PendingRestore {
    id: ReplyId,
    site: ReplySite<Holder>,
    holder: Holder,
    run: RunAttempt<LoadAttempt>,
    route: TransportRun,
    binding: Arc<SessionHarnessBinding>,
    answer: oneshot::Receiver<OpeningFact>,
    release_result: std::sync::mpsc::Sender<()>,
    unoffered_close: Option<UntakenClose>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffered_load_and_resume_keep_their_exact_receipt_across_writer_stop() {
    let mut visited = [[[false; 2]; 2]; 3];
    for (loss_index, loss) in [LocalLoss::Writer, LocalLoss::Connection, LocalLoss::Route]
        .into_iter()
        .enumerate()
    {
        for (via_index, (via, method)) in [
            (RestoreVia::Load, "session/load"),
            (RestoreVia::Resume, "session/resume"),
        ]
        .into_iter()
        .enumerate()
        {
            for (site_index, deferred) in [false, true].into_iter().enumerate() {
                check_paired_restores_after_loss(via, method, deferred, loss).await;
                visited[loss_index][via_index][site_index] = true;
            }
        }
    }
    assert_eq!(visited, [[[true; 2]; 2]; 3]);
}

async fn check_paired_restores_after_loss(
    via: RestoreVia,
    method: &str,
    deferred: bool,
    loss: LocalLoss,
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
    let wire = fixture.runtime.client.owner_wire();
    let mut ledger = ReplyLedger::default();
    let first = buffer_exact_restore(&fixture, &tasks, via, deferred, &mut ledger).await;
    let before = ledger.owed();
    let second = buffer_exact_restore(&fixture, &tasks, via, deferred, &mut ledger).await;
    let newly_admitted = [(second.id, second.holder)];
    let held = [(first.id, first.holder), newly_admitted[0]];
    let pending = [first, second];
    let read_provider_receipt = || -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(&fixture.receipt).expect("read provider receipt"))
            .expect("parse provider receipt")
    };
    let peer = read_provider_receipt();
    let restores = peer["restores"].as_array().expect("restore receipts");
    assert_eq!(restores.len(), 2);
    for (restore, pending) in restores.iter().zip(&pending) {
        assert_eq!(restore["method"], method);
        assert_eq!(restore["sessionId"], saved_id(pending.run));
    }

    let refusal = match loss {
        LocalLoss::Writer => {
            let stopped = wire.pause_opening_for_test(OpeningPausePoint::WriterStopDelivered);
            wire.stop_ordered_writer_for_test(pending[0].route)
                .expect("stop the real ordered writer");
            stopped
                .reached
                .await
                .expect("a binding observed actual writer stop");
            stopped.release.send(()).expect("finish writer observation");
            LifecycleRefusal::Writer(WriterState::Stopped)
        }
        LocalLoss::Connection => {
            fixture.runtime.retirement().cancel();
            LifecycleRefusal::ConnectionRetired
        }
        LocalLoss::Route => {
            for pending in &pending {
                fixture.runtime.retire_provider_route_for_test(
                    pending.route.incarnation,
                    pending.route.generation,
                );
            }
            LifecycleRefusal::BoundRouteUnavailable
        }
    };
    let mut pending = pending;
    for restore in &mut pending {
        assert_eq!(
            restore
                .binding
                .opening_is_held(OpenAttempt::Restore(restore.run)),
            Held::NotHeld(refusal)
        );
        assert!(matches!(
            restore.answer.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    ledger.assert_conserved(before.clone(), &newly_admitted, &held, &[]);

    let mut delivered = Vec::new();
    for pending in pending {
        pending
            .release_result
            .send(())
            .expect("release this real restore holder");
        let fact = pending
            .answer
            .await
            .expect("the admitted restore envelope must settle");
        let OpeningFact::RestoreFailed {
            run,
            failure: OpeningFailure::Unavailable { diagnostic },
        } = fact
        else {
            panic!("buffered peer success must settle as locally unavailable");
        };
        assert_eq!(run.generation, pending.run.generation);
        assert!(diagnostic.contains(method), "{diagnostic}");
        let observed = ReplyId::Restore(run.attempt.0);
        ledger.delivered(pending.id, observed, pending.site);
        delivered.push(observed);
        if let Some(close) = pending.unoffered_close {
            close
                .release_offer
                .send(())
                .expect("finish the unoffered close");
            assert!(close.caller.await.expect("close caller completes").is_err());
            assert!(
                close.answer.await.is_err(),
                "the unadmitted close has no reply debt"
            );
            ledger.move_to(close.id, ReplySite::Reserved, ReplySite::Unoffered);
        }
        drop(pending.binding);
    }
    ledger.assert_conserved(before, &newly_admitted, &[], &delivered);
    assert!(
        read_provider_receipt()["closes"]
            .as_array()
            .expect("close count")
            .is_empty()
    );
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}

async fn buffer_exact_restore(
    fixture: &HubRuntime,
    tasks: &TaskTracker,
    via: RestoreVia,
    defer: bool,
    ledger: &mut ReplyLedger<ReplyId, Holder>,
) -> PendingRestore {
    let wire = fixture.runtime.client.owner_wire();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let route = TransportRun {
        incarnation: owner.incarnation,
        generation: run.generation,
    };
    let id = ReplyId::Restore(run.attempt.0);
    let (reply, mut answer) = oneshot::channel();
    let buffered = wire.pause_opening_for_test(OpeningPausePoint::RestoreReceiptBuffered);
    let OpenRequest::Restore { setup, .. } = restore_request(fixture, owner, run) else {
        panic!("exact saved setup");
    };
    ledger.reserve(id);
    binding
        .open(
            OpenRequest::Restore {
                owner,
                run,
                session: HarnessSessionId(saved_id(run)),
                via,
                setup,
                requested_mode: None,
            },
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(std::future::pending()),
        )
        .expect("the exact restore was admitted");
    let admitted = ReplySite::Admitted(Holder::RestoreWorker);
    ledger.move_to(id, ReplySite::Reserved, admitted);
    buffered
        .reached
        .await
        .expect("the actual worker holds the successful peer receipt");
    let peer_buffer = ReplySite::BufferedFromPeer(Holder::RestoreWorker);
    ledger.move_to(id, admitted, peer_buffer);
    let binding = Arc::new(binding);
    let (release_result, unoffered_close, site, holder) = if defer {
        let before_offer = wire.pause_opening_for_test(OpeningPausePoint::BeforeCloseOffer);
        let deferred = wire.pause_opening_for_test(OpeningPausePoint::RestoreDeferred);
        let close = RunAttempt {
            generation: run.generation,
            attempt: CloseAttempt(Uuid::now_v7()),
        };
        let close_id = ReplyId::Close(close.attempt.0);
        let (reply, mut close_answer) = oneshot::channel();
        ledger.reserve(close_id);
        let caller = tokio::task::spawn_blocking({
            let binding = Arc::clone(&binding);
            move || binding.close(close, Box::new(Reply(reply)))
        });
        before_offer
            .reached
            .await
            .expect("the exact close reservation is still unoffered");
        buffered
            .release
            .send(())
            .expect("hand the actual receipt to its opening row");
        deferred
            .reached
            .await
            .expect("worker is waiting on its owned deferred milestone");
        assert_eq!(
            close_answer.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
        let owner = Holder::RestoreWorkerWaitingForRow;
        let site = ReplySite::Deferred(owner);
        ledger.move_to(id, peer_buffer, site);
        (
            deferred.release,
            Some(UntakenClose {
                id: close_id,
                caller,
                answer: close_answer,
                release_offer: before_offer.release,
            }),
            site,
            owner,
        )
    } else {
        (buffered.release, None, peer_buffer, Holder::RestoreWorker)
    };
    assert!(matches!(
        answer.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    PendingRestore {
        id,
        site,
        holder,
        run,
        route,
        binding,
        answer,
        release_result,
        unoffered_close,
    }
}

fn saved_id(run: RunAttempt<LoadAttempt>) -> String {
    format!("fixture-restore-{}", run.attempt.0)
}
