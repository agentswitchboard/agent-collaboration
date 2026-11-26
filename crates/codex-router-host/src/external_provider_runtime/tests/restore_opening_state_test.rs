//! Run-only cleanup and held-restore guidance on the actual ACP binding.

#[path = "opening_close_race_test.rs"]
mod close_race;
#[path = "opening_binding_model_test.rs"]
mod model;
#[path = "restore_setter_claim_test.rs"]
mod setter_claim;

use super::*;
use provider_session_machines::{
    harness_lifecycle::{CloseFact, HeldOpeningKind, LifecycleRefusal},
    session_lifecycle_machine::CloseAttempt,
    session_owner::WriterState,
};
use std::sync::Mutex;

fn restore_request(
    fixture: &HubRuntime,
    owner: OwnerAddress,
    run: RunAttempt<LoadAttempt>,
) -> OpenRequest {
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
    }
}

#[tokio::test]
async fn a_pending_restore_waits_and_a_held_restore_needs_its_owner_to_continue() {
    let fixture = hub_runtime().await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let stop = CancellationToken::new();
    let (reply, mut answer) = oneshot::channel();
    binding
        .open(
            restore_request(&fixture, owner, run),
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("open on the live writer");
    let (duplicate, _no_answer) = oneshot::channel();
    let pending = binding
        .open(
            restore_request(&fixture, owner, run),
            Box::new(Reply(duplicate)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect_err("the original load owns the pending opening");
    assert_eq!(pending, LifecycleRefusal::OpeningInProgress);
    assert_eq!(pending.valid_moves(), &[LifecycleMove::WaitForOpening]);
    assert!(matches!(
        answer.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let OpeningFact::Restored {
        run: exact,
        opening: RestoredOpening::Loaded { readback, .. },
    } = answer.await.expect("waiting yields the original load")
    else {
        panic!("the existing restore did not load");
    };
    assert_eq!(exact, run);
    let (duplicate, _no_answer) = oneshot::channel();
    let held = binding
        .open(
            restore_request(&fixture, owner, run),
            Box::new(Reply(duplicate)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect_err("the loaded result is already held");
    assert_eq!(
        held,
        LifecycleRefusal::OpeningAlreadyHeld(HeldOpeningKind::Restore),
        "a held restore must continue its setup, not wait for a second opening reply"
    );
    assert_eq!(held.valid_moves(), &[LifecycleMove::ContinueHeldOpening]);
    let load_with_wrong_session = OpenRequest::Restore {
        owner,
        run,
        session: HarnessSessionId("other-session".into()),
        via: RestoreVia::Load,
        setup: OpenSetup {
            cwd: fixture._root.path().to_owned(),
            work_roots: Default::default(),
            scratch_folder: None,
        },
        requested_mode: None,
    };
    let (wrong, _no_wrong_answer) = oneshot::channel();
    let mismatch = binding
        .open(
            load_with_wrong_session,
            Box::new(Reply(wrong)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect_err("same generation and attempt do not excuse the wrong session ID");
    assert_eq!(mismatch, LifecycleRefusal::AttemptMismatch);
    assert_eq!(
        mismatch.valid_moves(),
        &[LifecycleMove::CorrectRequest],
        "wrong-session guidance must never offer to continue another session's setup"
    );
    let accepted = AcceptedRunSettings {
        catalogue: readback.catalogue,
        router_auto_requested: readback.router_auto_requested,
    };
    binding
        .accept_settings(OpenAttempt::Restore(run), &accepted)
        .expect("continue the held restore's exact setup");
    binding
        .activate(
            ActivationSetup::Restored(run),
            accepted.clone(),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("activate the original restore");
    let duplicate = binding
        .activate(
            ActivationSetup::Restored(run),
            accepted,
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect_err("the existing reader is already active");
    assert_eq!(
        duplicate,
        ActivationRefusal::AlreadyActive,
        "opening guidance must retain the call-specific duplicate-active fact"
    );
    assert_eq!(
        duplicate.valid_moves(),
        &[LifecycleMove::KeepActiveRun, LifecycleMove::Retire]
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), 1);
    binding.retire(run.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}

struct HoldReplay(Mutex<Option<oneshot::Sender<RunAttempt<LoadAttempt>>>>);

impl EventPort<SessionLifecycleFact> for HoldReplay {
    fn send<'a>(&'a self, fact: SessionLifecycleFact) -> EventSendFuture<'a>
    where
        SessionLifecycleFact: 'a,
    {
        Box::pin(async move {
            if let SessionLifecycleFact::Replay { run, .. } = fact {
                let reached = self.0.lock().expect("replay gate").take();
                if let Some(reached) = reached {
                    let _test_gone = reached.send(run);
                }
                std::future::pending().await
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn run_retirement_exits_the_real_replay_and_leaves_a_reopenable_slot() {
    let fixture = hub_runtime().await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let old_stop = CancellationToken::new();
    let (reached, replay) = oneshot::channel();
    let (reply, abandoned) = oneshot::channel();
    binding
        .open(
            restore_request(&fixture, owner, run),
            Box::new(Reply(reply)),
            Arc::new(HoldReplay(Mutex::new(Some(reached)))),
            Box::pin(old_stop.clone().cancelled_owned()),
        )
        .expect("open into a physically blocked replay");
    assert_eq!(replay.await.expect("actual load reached replay"), run);
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(LifecycleRefusal::OpeningInProgress)
    );
    old_stop.cancel();
    assert!(
        abandoned.await.is_err(),
        "the exact retired task exited without an answer"
    );
    let Held::NotHeld(missing) = binding.opening_is_held(OpenAttempt::Restore(run)) else {
        panic!("retired attempt retained a hold");
    };
    assert_eq!(
        missing,
        LifecycleRefusal::BoundSessionNotLoaded,
        "a retired task must not retain a pending opening"
    );
    assert_eq!(missing.valid_moves(), &[LifecycleMove::LoadBoundSession]);
    let replacement = RunAttempt {
        attempt: LoadAttempt(Uuid::now_v7()),
        ..run
    };
    assert_eq!(
        load_fixture_binding(
            &fixture,
            &mut binding,
            owner,
            replacement,
            CancellationToken::new()
        )
        .await
        .1,
        ReplayCompleteness::Complete,
        "a fresh attempt on the live binding actually reopens"
    );
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(replacement)),
        Held::Held
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), 2);
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);
    binding.retire(replacement.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}

#[tokio::test]
async fn a_retired_connection_overrides_a_still_held_restore() {
    let fixture = hub_runtime().await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let (readback, _) =
        load_fixture_binding(&fixture, &mut binding, owner, run, CancellationToken::new()).await;
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::Held
    );
    fixture.runtime.retirement().cancel();
    // Standalone binding has no owner processing its loss report or retiring the table.
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(LifecycleRefusal::ConnectionRetired),
        "the retired writer dominates the still-held local opening"
    );
    let Err(refused) = binding.accept_settings(
        OpenAttempt::Restore(run),
        &AcceptedRunSettings {
            catalogue: readback.catalogue,
            router_auto_requested: false,
        },
    ) else {
        panic!("the owner must stop with this dead binding");
    };
    assert_eq!(refused, LifecycleRefusal::ConnectionRetired);
    assert_eq!(refused.valid_moves(), &[LifecycleMove::StopOwner]);
    let (reply, _answer) = oneshot::channel();
    let fresh = RunAttempt {
        generation: SessionGeneration::from_number(2),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    assert_eq!(
        binding.open(
            restore_request(&fixture, owner, fresh),
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(std::future::pending())
        ),
        Err(LifecycleRefusal::ConnectionRetired)
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), 1);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}

#[tokio::test]
async fn a_settled_close_releases_the_bound_slot_before_host_retirement() {
    let fixture = hub_runtime_with(&["advertise-close"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let stop = CancellationToken::new();
    let (readback, _) =
        load_fixture_binding(&fixture, &mut binding, owner, run, stop.clone()).await;
    let settings = AcceptedRunSettings {
        catalogue: readback.catalogue,
        router_auto_requested: readback.router_auto_requested,
    };
    binding
        .accept_settings(OpenAttempt::Restore(run), &settings)
        .expect("prepare the exact restore");
    binding
        .activate(
            ActivationSetup::Restored(run),
            settings,
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("activate the original run");

    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let (reply, settled) = oneshot::channel();
    binding
        .close(close, Box::new(Reply(reply)))
        .expect("offer the exact close");
    assert_eq!(
        settled.await.expect("settled close"),
        CloseFact::Closed { run: close }
    );
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(LifecycleRefusal::BoundSessionNotLoaded),
        "the close result must release the terminal row before any Host retire"
    );

    let replacement = RunAttempt {
        attempt: LoadAttempt(Uuid::now_v7()),
        ..run
    };
    let (_, replay) =
        load_fixture_binding(&fixture, &mut binding, owner, replacement, stop.clone()).await;
    assert_eq!(replay, ReplayCompleteness::Complete);
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), 2);
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 1);
    binding.retire(replacement.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}

#[tokio::test]
async fn a_refused_live_close_reopens_only_for_its_exact_attempt() {
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
    let close_count = || {
        std::fs::read(&fixture.receipt)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|receipt| receipt["closes"].as_array().map(Vec::len))
    };
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let stop = CancellationToken::new();
    let (readback, _) =
        load_fixture_binding(&fixture, &mut binding, owner, run, stop.clone()).await;
    let settings = AcceptedRunSettings {
        catalogue: readback.catalogue,
        router_auto_requested: readback.router_auto_requested,
    };
    binding
        .accept_settings(OpenAttempt::Restore(run), &settings)
        .expect("prepare the exact restore");
    binding
        .activate(
            ActivationSetup::Restored(run),
            settings,
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("activate the live run");

    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let wire = fixture.runtime.client.owner_wire();
    let capacity = wire.hold_writer_capacity_for_test();
    let (reply, unused) = oneshot::channel();
    assert_eq!(
        binding.close(close, Box::new(Reply(reply))),
        Err(LifecycleRefusal::Writer(WriterState::Full))
    );
    assert!(unused.await.is_err());
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(LifecycleRefusal::OpeningActive),
        "failed close admission must preserve the active opening"
    );
    drop(capacity);

    let (reply, refusal) = oneshot::channel();
    binding
        .close(close, Box::new(Reply(reply)))
        .expect("queue real close");
    while close_count() != Some(1) {
        tokio::task::yield_now().await;
    }
    let next_close = RunAttempt {
        attempt: CloseAttempt(Uuid::now_v7()),
        ..close
    };
    let (reply, _unused) = oneshot::channel();
    assert_eq!(
        binding.close(next_close, Box::new(Reply(reply))),
        Err(LifecycleRefusal::RouteClosing)
    );
    node_test_fixture::release_fixture(&fixture.receipt, ".release-close")
        .expect("let the provider refuse the close");
    assert_eq!(
        refusal.await.expect("exact reply"),
        CloseFact::RefusedStillLive { run: close }
    );
    for wrong in [
        next_close,
        RunAttempt {
            generation: SessionGeneration::from_number(2),
            ..close
        },
    ] {
        binding.admit_again(wrong);
        let (reply, _unused) = oneshot::channel();
        assert_eq!(
            binding.close(next_close, Box::new(Reply(reply))),
            Err(LifecycleRefusal::CloseAlreadyRefused),
            "a wrong close identity cleared the original route marker"
        );
    }
    binding.admit_again(close);
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(LifecycleRefusal::OpeningActive)
    );
    let (reply, refusal) = oneshot::channel();
    binding
        .close(next_close, Box::new(Reply(reply)))
        .expect("exact admit-again releases the real writer route");
    assert_eq!(
        refusal.await.expect("next real close reached the provider"),
        CloseFact::RefusedStillLive { run: next_close }
    );
    assert_eq!(close_count(), Some(2));
    binding.admit_again(next_close);
    fixture
        .runtime
        .retire_provider_route_for_test(owner.incarnation, run.generation);
    let (reply, _unused) = oneshot::channel();
    let no_route = binding
        .close(close, Box::new(Reply(reply)))
        .expect_err("the exact wire route is gone");
    assert_eq!(no_route, LifecycleRefusal::BoundRouteUnavailable);
    assert_eq!(no_route.valid_moves(), &[LifecycleMove::Retire]);
    binding.retire(run.generation);
    assert_eq!(close_count(), Some(2));
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
