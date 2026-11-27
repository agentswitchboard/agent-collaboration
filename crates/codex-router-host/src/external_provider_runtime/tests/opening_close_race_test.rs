//! Hold a real close offer across the original restore's replay admission.

use super::*;
use acp_client_runtime::owner_wire::OpeningPausePoint;

struct HoldReplayUntil {
    entered: Mutex<Option<oneshot::Sender<RunAttempt<LoadAttempt>>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl EventPort<SessionLifecycleFact> for HoldReplayUntil {
    fn send<'a>(&'a self, fact: SessionLifecycleFact) -> EventSendFuture<'a>
    where
        SessionLifecycleFact: 'a,
    {
        Box::pin(async move {
            if let SessionLifecycleFact::Replay { run, .. } = fact {
                let entered = self.entered.lock().expect("replay progress").take();
                let release = self.release.lock().expect("replay release").take();
                if let Some(entered) = entered {
                    let _test_gone = entered.send(run);
                }
                if let Some(release) = release {
                    let _test_gone = release.await;
                }
            }
            Ok(())
        })
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum CloseRace {
    WriterFull,
    Accepted,
    RetiredFirst,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_early_close_reply_is_held_until_close_owns_the_row() {
    run_close_race(CloseRace::Accepted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_close_restores_the_origin_and_applies_its_pending_replay() {
    run_close_race(CloseRace::WriterFull).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_blanks_a_deferred_fact_and_withdraws_the_untaken_close() {
    run_close_race(CloseRace::RetiredFirst).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_close_disposes_the_deferred_sdk_and_replies_after_unlocking() {
    use acp_client_runtime::owner_wire::OpeningDisposalKind;

    let fixture = hub_runtime_with(&["advertise-close", "hold-load"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let wire = fixture.runtime.client.owner_wire();
    let probes = [
        OpeningDisposalKind::SdkSession,
        OpeningDisposalKind::RestoreSignal,
        OpeningDisposalKind::OpeningReply,
        OpeningDisposalKind::CloseReply,
    ]
    .map(|kind| wire.watch_opening_disposal_for_test(kind));
    let before_offer = wire.pause_opening_for_test(OpeningPausePoint::BeforeCloseOffer);
    let restore_deferred = wire.pause_opening_for_test(OpeningPausePoint::RestoreDeferred);
    let stop = CancellationToken::new();
    let (reply, restored) = oneshot::channel();
    binding
        .open(
            restore_request(&fixture, owner, run),
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("offer the older load to the real provider");
    let (reply, closed) = oneshot::channel();
    let binding = Arc::new(binding);
    let offering = tokio::task::spawn_blocking({
        let binding = Arc::clone(&binding);
        move || binding.close(close, Box::new(Reply(reply)))
    });
    before_offer
        .reached
        .await
        .expect("close owns the row without a wire offer");
    node_test_fixture::release_fixture(&fixture.receipt, ".release-load")
        .expect("allow the provider to answer the load");
    restore_deferred
        .reached
        .await
        .expect("the successful load with the SDK session is deferred");
    before_offer
        .release
        .send(())
        .expect("let the writer take the close");
    assert_eq!(offering.await.expect("close call completed"), Ok(()));
    restore_deferred
        .release
        .send(())
        .expect("let the restore worker process disposal");
    assert_eq!(
        closed.await.expect("correlated close result"),
        CloseFact::Closed { run: close }
    );
    assert!(
        restored.await.is_err(),
        "accepted close supersedes the opening"
    );
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
    for probe in probes {
        probe.assert_disposed_outside_table();
    }
}

pub(super) async fn run_close_race(outcome: CloseRace) {
    let fixture = hub_runtime_with(&["advertise-close"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let stop = CancellationToken::new();
    let (entered, replay) = oneshot::channel();
    let (finish_replay, release) = oneshot::channel();
    let (reply, mut original) = oneshot::channel();
    binding
        .open(
            restore_request(&fixture, owner, run),
            Box::new(Reply(reply)),
            Arc::new(HoldReplayUntil {
                entered: Mutex::new(Some(entered)),
                release: Mutex::new(Some(release)),
            }),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("the load reaches the real ordered wire");
    assert_eq!(replay.await.expect("load holds its replay"), run);

    let binding = Arc::new(binding);
    let wire = fixture.runtime.client.owner_wire();
    let before_offer = wire.pause_opening_for_test(OpeningPausePoint::BeforeCloseOffer);
    let deferred = wire.pause_opening_for_test(OpeningPausePoint::RestoreDeferred);
    let before_commit = (outcome == CloseRace::Accepted)
        .then(|| wire.pause_opening_for_test(OpeningPausePoint::BeforeCloseCommit));
    let early_reply = (outcome == CloseRace::Accepted)
        .then(|| wire.pause_opening_for_test(OpeningPausePoint::CloseReceiptPublished));
    let (reply, mut close_result) = oneshot::channel();
    let offering = tokio::task::spawn_blocking({
        let binding = Arc::clone(&binding);
        move || binding.close(close, Box::new(Reply(reply)))
    });
    before_offer
        .reached
        .await
        .expect("row owns close before actual writer admission");
    finish_replay
        .send(())
        .expect("actually admit the original replay");
    deferred
        .reached
        .await
        .expect("exact replay admission is deferred on the close offer");
    assert!(matches!(
        original.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    if outcome == CloseRace::RetiredFirst {
        binding.retire(run.generation);
    }
    deferred
        .release
        .send(())
        .expect("release the worker to consume the close's decision");
    let capacity = (outcome == CloseRace::WriterFull).then(|| wire.hold_writer_capacity_for_test());
    before_offer
        .release
        .send(())
        .expect("run the physical writer decision");

    if let (Some(commit), Some(early)) = (before_commit, early_reply) {
        commit
            .reached
            .await
            .expect("actual writer accepted but row has not committed Closing");
        early
            .reached
            .await
            .expect("the real close response already arrived at the private relay");
        assert!(matches!(
            close_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            original.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        early
            .release
            .send(())
            .expect("release real close response task");
        commit
            .release
            .send(())
            .expect("let Closing take the private relay");
    }
    let offered = offering.await.expect("close caller completed");
    drop(capacity);
    match outcome {
        CloseRace::WriterFull => {
            assert_eq!(offered, Err(LifecycleRefusal::Writer(WriterState::Full)));
            assert_eq!(
                binding.opening_is_held(OpenAttempt::Restore(run)),
                Held::Held
            );
            assert!(
                matches!(original.await.expect("original load still answers"), OpeningFact::Restored { run: exact, .. } if exact == run)
            );
            assert!(close_result.await.is_err());
            assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);
            binding.retire(run.generation);
        }
        CloseRace::Accepted => {
            assert_eq!(offered, Ok(()));
            assert_eq!(
                close_result.await.expect("one actual close result"),
                CloseFact::Closed { run: close }
            );
            assert!(
                original.await.is_err(),
                "accepted close cannot publish the original restore success"
            );
            assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 1);
        }
        CloseRace::RetiredFirst => {
            assert!(offered.is_err());
            assert!(close_result.await.is_err());
            assert!(original.await.is_err());
            assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);
        }
    }
    let mut binding =
        Arc::try_unwrap(binding).unwrap_or_else(|_| panic!("close still holds the binding"));
    let replacement = RunAttempt {
        attempt: LoadAttempt(Uuid::now_v7()),
        ..run
    };
    let (_, replay) = load_fixture_binding(&fixture, &mut binding, owner, replacement, stop).await;
    assert_eq!(replay, ReplayCompleteness::Complete);
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), 2);
    binding.retire(replacement.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resume_cannot_offer_a_held_move_before_the_reader_is_installed() {
    let fixture = hub_runtime_with_launch(|receipt| {
        crate::external_provider_runtime::acp_scripted_fixture::launch_fixture_agent(
            "permission-mode-agent.ts",
            [
                receipt.display().to_string(),
                "cursor".into(),
                "no-directories".into(),
                "advertise-restore".into(),
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
    let stop = CancellationToken::new();
    let OpenRequest::Restore { setup, session, .. } = restore_request(&fixture, owner, run) else {
        panic!("expected restore setup");
    };
    let paused = fixture
        .runtime
        .client
        .owner_wire()
        .pause_opening_for_test(OpeningPausePoint::ResumeReaderBeforeCommit);
    let (reply, mut response) = oneshot::channel();
    binding
        .open(
            OpenRequest::Restore {
                owner,
                run,
                setup,
                session,
                via: RestoreVia::Resume,
                requested_mode: None,
            },
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("send an actual resume");
    paused
        .reached
        .await
        .expect("actual SDK reader exists before table commit");
    let settings = AcceptedRunSettings {
        catalogue: Default::default(),
        router_auto_requested: false,
    };
    let Err(refusal) = binding.accept_settings(OpenAttempt::Restore(run), &settings) else {
        panic!("resume installed settings before the row had a reader");
    };
    assert_eq!(
        refusal.valid_moves(),
        &[LifecycleMove::WaitForOpening],
        "held-before-payload: a started resume still owns the original answer"
    );
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    paused.release.send(()).expect("allow atomic held receipt");
    let OpeningFact::Restored {
        run: exact,
        opening: RestoredOpening::Resumed { readback, .. },
    } = response.await.expect("one original resume reply")
    else {
        panic!("resume did not hold its reader");
    };
    assert_eq!(exact, run);
    let settings = AcceptedRunSettings {
        catalogue: readback.catalogue,
        router_auto_requested: readback.router_auto_requested,
    };
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::Held
    );
    binding
        .accept_settings(OpenAttempt::Restore(run), &settings)
        .expect("the held reader now supports its advertised continuation");
    binding
        .activate(
            ActivationSetup::Restored(run),
            settings,
            Arc::new(AcceptEvents),
            Box::pin(stop.cancelled_owned()),
        )
        .expect("start the actual resumed reader");
    binding.retire(run.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
