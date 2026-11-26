//! Run-only cleanup and held-restore guidance on the actual ACP binding.

use super::*;
use provider_session_machines::harness_lifecycle::{HeldOpeningKind, LifecycleRefusal};
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
        LifecycleRefusal::OpeningNotHeld,
        "a retired task must not retain a pending opening"
    );
    assert_eq!(missing.valid_moves(), &[LifecycleMove::Reopen]);
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
