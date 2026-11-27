//! Stop the real ordered carrier while older lifecycle receipts are still owed.

use super::*;
use crate::owner_wire::OpeningPausePoint;
use codex_router_test_support::reply_ledger::{ReplyLedger as ExactReplyLedger, ReplySite};
use provider_session_machines::{
    harness_lifecycle::CloseFact, session_lifecycle_machine::CloseAttempt,
};
use switchboard_harness_contracts::CloseUncertainty;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ReplyId {
    Restore(Uuid),
    Close(Uuid),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiveOwner {
    RestoreWorker,
    CloseWorker,
}

type Site = ReplySite<LiveOwner>;
type ReplyLedger = ExactReplyLedger<ReplyId, LiveOwner>;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn writer_stop_does_not_lose_the_older_restore_under_an_untaken_close() {
    let (openings, mut output, tasks, _connection) = tracked_test_opening(NonZeroUsize::MIN);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let open_id = ReplyId::Restore(run.attempt.0);
    let close_id = ReplyId::Close(close.attempt.0);
    let mut ledger = ReplyLedger::default();
    ledger.reserve(open_id);
    let mut answer = pending_load(&openings, run);
    let original_frame = output.hold_next_frame_for_test();
    ledger.move_to(
        open_id,
        Site::Reserved,
        Site::Admitted(LiveOwner::RestoreWorker),
    );
    let opening = Arc::new(openings);
    let before_offer = opening
        .wire
        .pause_opening_for_test(OpeningPausePoint::BeforeCloseOffer);
    let writer_stop = opening
        .wire
        .pause_opening_for_test(OpeningPausePoint::WriterStopDelivered);
    let (reply, close_answer) = oneshot::channel();
    ledger.reserve(close_id);
    let caller = tokio::task::spawn_blocking({
        let opening = Arc::clone(&opening);
        move || opening.close(close, Box::new(Reply(reply)))
    });
    before_offer
        .reached
        .await
        .expect("the new close is still in its pre-offer owner");
    let owed_before = ledger.owed();
    drop(output);
    writer_stop
        .reached
        .await
        .expect("observe actual writer stop");
    assert_eq!(
        lock(&opening.attempts)
            .get(OpeningSlot::Run(run.generation))
            .map(|row| (row.attempt(), row.phase())),
        Some((
            OpenAttempt::Restore(run),
            OpeningPhase::StoppedAwaitingReply
        )),
        "writer stop lost the exact older restore beneath an untaken close"
    );
    assert!(matches!(
        answer.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    ledger.assert_conserved(
        owed_before.clone(),
        &[],
        &[(open_id, LiveOwner::RestoreWorker)],
        &[],
    );
    opening.connection_retirement_observer()();
    assert_eq!(
        lock(&opening.attempts)
            .get(OpeningSlot::Run(run.generation))
            .map(|row| (row.attempt(), row.phase())),
        Some((
            OpenAttempt::Restore(run),
            OpeningPhase::StoppedAwaitingReply
        )),
        "connection retirement could not discard the already-owed restore",
    );
    ledger.assert_conserved(
        owed_before.clone(),
        &[],
        &[(open_id, LiveOwner::RestoreWorker)],
        &[],
    );
    writer_stop.release.send(()).expect("finish stop observer");
    before_offer
        .release
        .send(())
        .expect("finish refused close offer");
    assert!(caller.await.expect("close caller completes").is_err());
    assert!(close_answer.await.is_err());
    ledger.move_to(close_id, Site::Reserved, Site::Unoffered);
    drop(original_frame);
    let fact = answer.await.expect("force the original carrier to settle");
    assert!(
        matches!(fact, OpeningFact::RestoreFailed { run: exact, failure: OpeningFailure::OutcomeUnknown } if exact == run)
    );
    ledger.move_to(
        open_id,
        Site::Admitted(LiveOwner::RestoreWorker),
        Site::Delivered,
    );
    ledger.assert_conserved(owed_before, &[], &[], &[open_id]);
    assert!(lock(&opening.attempts).runs.is_empty());
    drop(opening);
    tasks.close();
    tasks.wait().await;
}

#[derive(Clone, Copy)]
enum PeerClose {
    Confirmed,
    Refused,
    Missing,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_admitted_close_settles_each_exact_writer_stop_outcome() {
    for peer in [PeerClose::Confirmed, PeerClose::Refused, PeerClose::Missing] {
        assert_exact_close_after_stop(peer).await;
    }
}

async fn assert_exact_close_after_stop(peer: PeerClose) {
    let (openings, mut output, tasks, _connection) = tracked_test_opening(NonZeroUsize::MIN);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let old = pending_load(&openings, run);
    let old_frame = output.hold_next_frame_for_test();
    let (reply, mut answer) = oneshot::channel();
    let opening = Arc::new(openings);
    let commit = opening
        .wire
        .pause_opening_for_test(OpeningPausePoint::BeforeCloseCommit);
    let stop = opening
        .wire
        .pause_opening_for_test(OpeningPausePoint::WriterStopDelivered);
    let id = ReplyId::Close(close.attempt.0);
    let mut ledger = ReplyLedger::default();
    ledger.reserve(id);
    let caller = tokio::task::spawn_blocking({
        let opening = Arc::clone(&opening);
        move || opening.close(close, Box::new(Reply(reply)))
    });
    commit
        .reached
        .await
        .expect("the actual carrier accepted the close claim");
    ledger.move_to(id, Site::Reserved, Site::Admitted(LiveOwner::CloseWorker));
    let owed_before = ledger.owed();
    let (held_frame, expected, custody) = match peer {
        PeerClose::Confirmed => {
            output.buffer_next_close_reply_for_test(&ConnectionCatalogueBehavior);
            (
                None,
                CloseFact::Closed { run: close },
                Site::BufferedFromPeer(LiveOwner::CloseWorker),
            )
        }
        PeerClose::Refused => {
            output.buffer_close_outcome_for_test(
                Err(agent_client_protocol::Error::method_not_found()),
                &ConnectionCatalogueBehavior,
            );
            (
                None,
                CloseFact::RefusedStillLive { run: close },
                Site::BufferedFromPeer(LiveOwner::CloseWorker),
            )
        }
        PeerClose::Missing => (
            Some(output.hold_next_frame_for_test()),
            CloseFact::Unknown {
                run: close,
                cause: CloseUncertainty::ReplyLost,
            },
            Site::Admitted(LiveOwner::CloseWorker),
        ),
    };
    ledger.move_to(id, Site::Admitted(LiveOwner::CloseWorker), custody);
    drop(output);
    stop.reached
        .await
        .expect("actual writer stopped with the original close outstanding");
    assert_eq!(
        lock(&opening.attempts)
            .get(OpeningSlot::Run(run.generation))
            .map(OpeningRow::phase),
        Some(OpeningPhase::StoppedAwaitingReply)
    );
    assert!(matches!(
        answer.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    ledger.assert_conserved(
        owed_before.clone(),
        &[],
        &[(id, LiveOwner::CloseWorker)],
        &[],
    );
    stop.release.send(()).expect("finish the stop observer");
    commit
        .release
        .send(())
        .expect("admit the original close receipt");
    assert_eq!(caller.await.expect("original close caller"), Ok(()));
    drop(held_frame);
    assert_eq!(
        answer.await.expect("forced close drain settles once"),
        expected
    );
    ledger.move_to(id, custody, Site::Delivered);
    ledger.assert_conserved(owed_before, &[], &[], &[id]);
    assert!(lock(&opening.attempts).runs.is_empty());
    drop(old_frame);
    assert!(
        old.await.is_err(),
        "the admitted close superseded its older opening"
    );
    drop(opening);
    tasks.close();
    tasks.wait().await;
}
