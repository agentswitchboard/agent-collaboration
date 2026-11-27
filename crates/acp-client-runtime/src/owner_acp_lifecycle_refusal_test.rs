//! Reachable opening refusals at ACP's ordered writer.

use super::*;
use crate::{
    OwnerApprovalBindings,
    fixture_policy::{FixturePayloads, FixturePolicy},
    owner_session_setting::tests::ConnectionCatalogueBehavior,
};
use provider_session_machines::harness_lifecycle::{
    CallbackAdmission, EventPort, EventSendFuture, HarnessOwnerEvents, HarnessProvider,
    SessionLifecycleFact,
};
use session_event_model::ProviderAuthStatus;
use std::num::NonZeroUsize;
use switchboard_harness_contracts::{
    ConnectionEpoch, LifecycleMove, LoadAttempt, OwnerIncarnation, ReservedReply, WriterState,
};
use tokio::sync::oneshot;
use uuid::Uuid;

#[path = "owner_acp_opening_reply_stop_test.rs"]
mod reply_stop;

struct IgnoreObservations;

impl HarnessOwnerEvents<ProviderAuthStatus> for IgnoreObservations {
    fn report_harness_gone(&self) -> EventSendFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn report_connection_auth(&self, _: ProviderAuthStatus) -> CallbackAdmission {
        CallbackAdmission::Admitted
    }
}

impl EventPort<SessionLifecycleFact> for IgnoreObservations {
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
            "opening receiver must still be present"
        );
    }
}

struct NoInteractions;

impl crate::InteractionPort for NoInteractions {
    type Context = CancellationToken;
    type OperationId = ();

    fn operation_id(_: &Self::Context) -> Self::OperationId {}

    fn binding_retirement(context: &Self::Context) -> CancellationToken {
        context.clone()
    }
}

#[tokio::test]
async fn an_empty_bound_opening_on_a_stopped_writer_offers_only_stop_owner() {
    let owner = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    let (wire, output) =
        OwnerAcpWire::new(NonZeroUsize::MIN, Arc::new(ConnectionCatalogueBehavior));
    let connection_retired = CancellationToken::new();
    let tasks = TaskTracker::new();
    let approvals = OwnerApprovalBindings::<FixturePayloads<NoInteractions>>::new(
        Arc::default(),
        Arc::new(FixturePolicy(Arc::new(NoInteractions))),
        connection_retired.clone(),
    );
    let provider = super::super::OwnerAcpProvider::new(
        Arc::new(wire),
        approvals,
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        tasks.clone(),
        connection_retired.clone(),
    );
    let mut binding = provider.bind(owner, Arc::new(IgnoreObservations));
    drop(output);
    assert!(
        !connection_retired.is_cancelled(),
        "only the real ordered writer has stopped"
    );
    let absent = CreateAttempt(Uuid::now_v7());
    let Held::NotHeld(refusal) = binding.opening_is_held(OpenAttempt::Create(absent)) else {
        panic!("empty binding unexpectedly holds a create");
    };
    assert_eq!(
        refusal,
        LifecycleRefusal::Writer(WriterState::Stopped),
        "an empty bound opening on a stopped writer must not advertise reopen"
    );
    assert_eq!(refusal.valid_moves(), &[LifecycleMove::StopOwner]);
    let (reply, _answer) = oneshot::channel();
    assert_eq!(
        binding.open(
            OpenRequest::Create {
                owner,
                attempt: CreateAttempt(Uuid::now_v7()),
                setup: OpenSetup {
                    cwd: std::path::PathBuf::from("/"),
                    work_roots: Default::default(),
                    scratch_folder: None
                },
                requested: Default::default(),
            },
            Box::new(Reply(reply)),
            Arc::new(IgnoreObservations),
            Box::pin(std::future::pending()),
        ),
        Err(refusal)
    );
    drop(binding);
    tasks.close();
    tasks.wait().await;
}

#[tokio::test]
async fn a_lost_receipt_and_a_stopped_writer_do_not_offer_same_binding_reopen() {
    let owner = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    let (wire, output) =
        OwnerAcpWire::new(NonZeroUsize::MIN, Arc::new(ConnectionCatalogueBehavior));
    let tasks = TaskTracker::new();
    let openings = OwnerOpenings::new(
        owner,
        Arc::new(wire),
        Arc::new(OnceLock::new()),
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        tasks.clone(),
        CancellationToken::new(),
    );
    let open = |generation, reply| {
        let run = RunAttempt {
            generation: SessionGeneration::from_number(generation),
            attempt: LoadAttempt(Uuid::now_v7()),
        };
        let result = openings.open(
            OpenRequest::Restore {
                owner,
                run,
                session: HarnessSessionId("saved-session".into()),
                via: RestoreVia::Load,
                setup: OpenSetup {
                    cwd: std::path::PathBuf::from("/"),
                    work_roots: Default::default(),
                    scratch_folder: None,
                },
                requested_mode: None,
            },
            Box::new(Reply(reply)),
            Arc::new(IgnoreObservations),
            Box::pin(std::future::pending()),
        );
        (run, result)
    };
    let (close_reply, _close) = oneshot::channel();
    let close = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: provider_session_machines::session_lifecycle_machine::CloseAttempt(Uuid::now_v7()),
    };
    let no_route = openings
        .wire
        .enqueue_lifecycle_close(
            owner.incarnation,
            close,
            "saved-session".into(),
            Box::new(Reply(close_reply)),
        )
        .map_err(super::super::lifecycle_wire_refusal);
    let Err(no_route) = no_route else {
        panic!("close unexpectedly found a route")
    };
    assert_eq!(no_route, LifecycleRefusal::RouteUnavailable);
    assert_eq!(no_route.valid_moves(), &[LifecycleMove::Reopen]);

    let (reply, received) = oneshot::channel();
    let (first, offered) = open(1, reply);
    assert_eq!(offered, Ok(()), "live writer admits the new opening");
    drop(output);
    let OpeningFact::RestoreFailed { run, failure } =
        received.await.expect("lost receipt settles opening")
    else {
        panic!("the lost receipt did not produce an opening failure");
    };
    assert_eq!(run, first);
    assert_eq!(failure, OpeningFailure::OutcomeUnknown);
    assert_eq!(failure.valid_moves(), &[LifecycleMove::KeepOutcomeUnknown]);
    assert!(
        lock(&openings.attempts).runs_is_empty(),
        "a failed receipt leaves no opening that could still answer"
    );
    assert_eq!(
        openings.opening_is_held(OpenAttempt::Restore(first)),
        Held::NotHeld(LifecycleRefusal::Writer(WriterState::Stopped)),
        "an empty table on a closed writer must direct the owner to stop"
    );
    for generation in [2, 3] {
        let (reply, _received) = oneshot::channel();
        let (_, offered) = open(generation, reply);
        let refusal = offered.expect_err("new attempt on the stopped writer");
        assert_eq!(refusal, LifecycleRefusal::Writer(WriterState::Stopped));
        assert_eq!(refusal.valid_moves(), &[LifecycleMove::StopOwner]);
    }
    drop(openings);
    tasks.close();
    tasks.wait().await;
}

#[tokio::test]
async fn the_retired_task_cannot_clean_up_a_reused_opening_stamp() {
    let owner = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    let (wire, _output) = OwnerAcpWire::new(
        NonZeroUsize::new(2).expect("two possible wire frames"),
        Arc::new(ConnectionCatalogueBehavior),
    );
    let tasks = TaskTracker::new();
    let openings = OwnerOpenings::new(
        owner,
        Arc::new(wire),
        Arc::new(OnceLock::new()),
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        tasks.clone(),
        CancellationToken::new(),
    );
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let open = || {
        let (reply, answer) = oneshot::channel();
        openings
            .open(
                OpenRequest::Restore {
                    owner,
                    run,
                    session: HarnessSessionId("saved-session".into()),
                    via: RestoreVia::Load,
                    setup: OpenSetup {
                        cwd: std::path::PathBuf::from("/"),
                        work_roots: Default::default(),
                        scratch_folder: None,
                    },
                    requested_mode: None,
                },
                Box::new(Reply(reply)),
                Arc::new(IgnoreObservations),
                Box::pin(std::future::pending()),
            )
            .expect("enqueue the exact restore");
        answer
    };
    let old = open();
    openings.retire(run.generation);
    let replacement = open();
    // The retired task could not run until the current-thread caller yielded.
    assert!(old.await.is_err());
    assert_eq!(
        openings.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(LifecycleRefusal::OpeningInProgress),
        "the stale task must not remove the replacement with the same generation and attempt"
    );
    let (close_reply, _close) = oneshot::channel();
    let close = RunAttempt {
        generation: run.generation,
        attempt: provider_session_machines::session_lifecycle_machine::CloseAttempt(Uuid::now_v7()),
    };
    assert_eq!(
        openings
            .wire
            .enqueue_lifecycle_close(
                owner.incarnation,
                close,
                "saved-session".into(),
                Box::new(Reply(close_reply)),
            )
            .map_err(super::super::lifecycle_wire_refusal),
        Err(LifecycleRefusal::Writer(WriterState::Full)),
        "the replacement route still exists, even though its writer has no spare slot"
    );
    openings.retire(run.generation);
    assert!(replacement.await.is_err());
    drop(openings);
    tasks.close();
    tasks.wait().await;
}

fn tracked_test_opening(
    capacity: NonZeroUsize,
) -> (
    OwnerOpenings,
    crate::owner_wire::OwnerAcpOutput,
    TaskTracker,
    CancellationToken,
) {
    let owner = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    let (wire, output) = OwnerAcpWire::new(capacity, Arc::new(ConnectionCatalogueBehavior));
    let tasks = TaskTracker::new();
    let retirement = CancellationToken::new();
    let openings = OwnerOpenings::new(
        owner,
        Arc::new(wire),
        Arc::new(OnceLock::new()),
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        tasks.clone(),
        retirement.clone(),
    );
    (openings, output, tasks, retirement)
}

fn pending_load(
    openings: &OwnerOpenings,
    run: RunAttempt<LoadAttempt>,
) -> oneshot::Receiver<OpeningFact> {
    let (reply, result) = oneshot::channel();
    openings
        .open(
            OpenRequest::Restore {
                owner: openings.owner,
                run,
                session: HarnessSessionId("saved-session".into()),
                via: RestoreVia::Load,
                setup: OpenSetup {
                    cwd: "/".into(),
                    work_roots: Default::default(),
                    scratch_folder: None,
                },
                requested_mode: None,
            },
            Box::new(Reply(reply)),
            Arc::new(IgnoreObservations),
            Box::pin(std::future::pending()),
        )
        .expect("the exact opening entered the real writer");
    result
}

async fn wait_for_actual_row(
    openings: &OwnerOpenings,
    generation: SessionGeneration,
    expected: Option<OpeningPhase>,
) {
    loop {
        let observed = lock(&openings.attempts).phase_of(generation);
        if observed == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_writer_close_reaches_the_pending_row_without_a_followup_call() {
    let (openings, mut output, tasks, _retirement) = tracked_test_opening(NonZeroUsize::MIN);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let answer = pending_load(&openings, run);
    let original_write = output.hold_next_frame_for_test();
    let delivered = openings
        .wire
        .pause_opening_for_test(crate::owner_wire::OpeningPausePoint::WriterStopDelivered);
    drop(output);
    delivered
        .reached
        .await
        .expect("the real stopped writer observation completed");
    let phase = lock(&openings.attempts).phase_of(run.generation);
    assert_eq!(
        phase,
        Some(OpeningPhase::StoppedAwaitingReply),
        "skipped-writer-event: the pending row must change before any caller samples it"
    );
    delivered
        .release
        .send(())
        .expect("the writer observer can continue");
    // The row transition came from the writer closing. Its physically
    // retained receipt can still resolve independently.
    drop(original_write);
    let fact = answer.await.expect("original receipt must be settled");
    assert!(
        matches!(fact, OpeningFact::RestoreFailed { run: exact, failure: OpeningFailure::OutcomeUnknown } if exact == run)
    );
    assert!(lock(&openings.attempts).runs_is_empty());
    drop(openings);
    tasks.close();
    tasks.wait().await;
}

#[tokio::test]
async fn physical_route_loss_and_connection_retirement_reach_the_row_without_a_call() {
    let (openings, _output, tasks, retirement) = tracked_test_opening(NonZeroUsize::MIN);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let answer = pending_load(&openings, run);
    openings.wire.apply_membership(
        openings.owner.incarnation,
        provider_session_machines::session_owner::TurnMembership::Retire(run.generation),
    );
    wait_for_actual_row(&openings, run.generation, Some(OpeningPhase::RouteLost)).await;
    retirement.cancel();
    wait_for_actual_row(&openings, run.generation, None).await;
    assert!(matches!(
        answer.await.expect("physical route loss must settle the admitted restore"),
        OpeningFact::RestoreFailed { run: exact, failure: OpeningFailure::OutcomeUnknown } if exact == run
    ));
    assert_eq!(
        lock(&openings.attempts).port_refusal(),
        Some(LifecycleRefusal::ConnectionRetired)
    );
    drop(openings);
    tasks.close();
    tasks.wait().await;
}

#[tokio::test]
async fn delayed_retirement_cleanup_cannot_take_a_replacement_route_at_the_same_generation() {
    let (openings, mut output, tasks, _retirement) = tracked_test_opening(NonZeroUsize::MIN);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let old_answer = pending_load(&openings, run);
    let old_frame = output.hold_next_frame_for_test();
    let (_old_instance, old_row) = openings
        .take_retired(run.generation)
        .expect("release only the exact old row");
    let new_answer = pending_load(&openings, run);
    old_row.finish(openings.wire.as_ref());
    assert!(old_answer.await.is_err());
    drop(old_frame);
    assert_eq!(
        openings.opening_is_held(OpenAttempt::Restore(run)),
        Held::NotHeld(LifecycleRefusal::OpeningInProgress)
    );
    let close = RunAttempt {
        generation: run.generation,
        attempt: provider_session_machines::session_lifecycle_machine::CloseAttempt(Uuid::now_v7()),
    };
    let (reply, _settled) = oneshot::channel();
    assert_eq!(
        openings.close(close, Box::new(Reply(reply))),
        Err(LifecycleRefusal::Writer(WriterState::Full)),
        "the replacement's real route still exists and reaches writer admission"
    );
    openings.retire(run.generation);
    assert!(new_answer.await.is_err());
    drop(openings);
    tasks.close();
    tasks.wait().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_buffered_close_reply_survives_the_real_writer_stop() {
    use crate::owner_wire::OpeningPausePoint;
    use provider_session_machines::{
        harness_lifecycle::CloseFact, session_lifecycle_machine::CloseAttempt,
    };

    let (openings, mut output, tasks, _retirement) = tracked_test_opening(NonZeroUsize::MIN);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let opening_reply = pending_load(&openings, run);
    let unreturned_open = output.hold_next_frame_for_test();
    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let (reply, mut closed) = oneshot::channel();
    let before_commit = openings
        .wire
        .pause_opening_for_test(OpeningPausePoint::BeforeCloseCommit);
    let writer_stopped = openings
        .wire
        .pause_opening_for_test(OpeningPausePoint::WriterStopDelivered);
    let openings = Arc::new(openings);
    let caller = tokio::task::spawn_blocking({
        let openings = Arc::clone(&openings);
        move || openings.close(close, Box::new(Reply(reply)))
    });

    before_commit
        .reached
        .await
        .expect("the close reached the real writer before its row result");
    output.buffer_next_close_reply_for_test(&ConnectionCatalogueBehavior);
    assert!(matches!(
        closed.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(output);
    writer_stopped
        .reached
        .await
        .expect("the real writer receiver is stopped");
    assert_eq!(
        lock(&openings.attempts).phase_of(run.generation),
        Some(OpeningPhase::StoppedAwaitingReply),
        "writer stop lost a buffered close receipt"
    );
    writer_stopped
        .release
        .send(())
        .expect("release stopped observer");
    before_commit
        .release
        .send(())
        .expect("continue the accepted close path");
    assert_eq!(caller.await.expect("close caller"), Ok(()));
    assert_eq!(
        closed.await.expect("the buffered result remains owed"),
        CloseFact::Closed { run: close }
    );
    drop(unreturned_open);
    assert!(opening_reply.await.is_err());
    assert!(lock(&openings.attempts).runs_is_empty());
    drop(openings);
    tasks.close();
    tasks.wait().await;
}

#[tokio::test]
async fn a_vacant_restore_offers_only_the_row_decided_by_the_step() {
    let (openings, mut output, tasks, _retirement) = tracked_test_opening(NonZeroUsize::MIN);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let _reply = pending_load(&openings, run);
    assert_eq!(
        lock(&openings.attempts).phase_of(run.generation),
        Some(OpeningPhase::AwaitingRestore),
        "the declared vacant restore must commit its restore row",
    );
    let offered = output.hold_next_frame_for_test();
    openings.retire(run.generation);
    drop(offered);
    drop(openings);
    tasks.close();
    tasks.wait().await;
}
