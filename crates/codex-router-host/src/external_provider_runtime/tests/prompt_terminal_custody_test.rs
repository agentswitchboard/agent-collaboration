//! A real peer's completed prompt keeps its original reserved reply on reader exit.

use super::{
    calls::{ExpectedReply, TakenKind, TakenReply, take_work_with_reply},
    *,
};
use acp_client_runtime::owner_wire::{OpeningReaderEnd, TerminalAdmission};
use provider_session_machines::harness_lifecycle::{ReservedReply, WorkReply};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum ReaderExit {
    ReaderRetirement,
    ParentRetirement,
    ParentRetirementAfterDequeue,
    ParentRetirementBeforePublication,
}

struct CountedPromptReply {
    reply: oneshot::Sender<WorkReply>,
    settled: Arc<AtomicUsize>,
}

impl ReservedReply<WorkReply> for CountedPromptReply {
    fn send(self: Box<Self>, fact: WorkReply) {
        self.settled.fetch_add(1, Ordering::Relaxed);
        self.reply
            .send(fact)
            .expect("the caller still owns its receipt");
    }
}

#[tokio::test]
async fn queued_prompt_terminal_survives_the_readers_own_retirement() {
    assert_terminal_custody(ReaderExit::ReaderRetirement).await;
}

#[tokio::test]
async fn queued_prompt_terminal_survives_a_parent_dropping_the_reader() {
    assert_terminal_custody(ReaderExit::ParentRetirement).await;
}

#[tokio::test]
async fn dequeued_prompt_terminal_survives_a_parent_dropping_the_reader() {
    assert_terminal_custody(ReaderExit::ParentRetirementAfterDequeue).await;
}

#[tokio::test]
async fn a_late_prompt_terminal_uses_the_closed_readers_fallback() {
    assert_terminal_custody(ReaderExit::ParentRetirementBeforePublication).await;
}

async fn assert_terminal_custody(exit: ReaderExit) {
    let kind = TakenKind::Prompt;
    let (fixture, listener) = taken_reply_fixture(kind).await;
    let wire = fixture.runtime.client.owner_wire();
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let generation = SessionGeneration::from_number(1);
    let transport_run = TransportRun {
        incarnation: owner.incarnation,
        generation,
    };
    let parent_retired = CancellationToken::new();
    let reader_owned_elsewhere = matches!(exit, ReaderExit::ReaderRetirement);
    let (setup, accepted) = if reader_owned_elsewhere {
        let attempt = CreateAttempt(Uuid::now_v7());
        let accepted = super::super::reader_endings::prepare_created_reader(
            &fixture,
            owner,
            attempt,
            &mut binding,
        )
        .await;
        (
            ActivationSetup::Created {
                attempt,
                generation,
            },
            accepted,
        )
    } else {
        let run = RunAttempt {
            generation,
            attempt: LoadAttempt(Uuid::now_v7()),
        };
        let (readback, _) =
            load_fixture_binding(&fixture, &mut binding, owner, run, CancellationToken::new())
                .await;
        let accepted = AcceptedRunSettings {
            catalogue: readback.catalogue,
            router_auto_requested: false,
        };
        binding
            .accept_settings(OpenAttempt::Restore(run), &accepted)
            .expect("prepare the actual restored reader");
        (ActivationSetup::Restored(run), accepted)
    };
    let after_dequeue = matches!(exit, ReaderExit::ParentRetirementAfterDequeue);
    let late = matches!(exit, ReaderExit::ParentRetirementBeforePublication);
    let reader_pause = if after_dequeue {
        wire.hold_reader_with_terminal_in_hand_for_test()
    } else {
        wire.hold_reader_before_terminal_receive_for_test()
    };
    let mut reader_reached = reader_pause.reached;
    let reader_ending =
        (!reader_owned_elsewhere).then(|| wire.observe_next_reader_ending_for_test());
    let admission = wire.observe_terminal_admission_for_test();
    binding
        .activate(
            setup,
            accepted.clone(),
            Arc::new(AcceptEvents),
            Box::pin(parent_retired.clone().cancelled_owned()),
        )
        .expect("activate the real reader");
    binding.use_settings(generation, accepted);
    if !after_dequeue {
        (&mut reader_reached)
            .await
            .expect("the actual reader is parked before receive");
    }
    let writer_pause = late.then(|| wire.hold_taken_reply_for_test());
    let turn = TurnId(Uuid::now_v7());
    let (reply, mut receipt) = oneshot::channel();
    let settled = Arc::new(AtomicUsize::new(0));
    let authorization = take_work_with_reply(
        &binding,
        owner,
        generation,
        kind,
        turn,
        Box::new(CountedPromptReply {
            reply,
            settled: Arc::clone(&settled),
        }),
    );
    let (peer_notice, _) = notice(&listener).await;
    let exact_request: serde_json::Value =
        serde_json::from_str(&peer_notice).expect("exact peer request");
    assert_eq!(exact_request["method"], kind.method());
    assert!(
        !exact_request["id"].is_null(),
        "the peer acknowledged the exact RPC id"
    );
    assert_eq!(wire.current_turn(transport_run), Some(turn));
    node_test_fixture::release_fixture(&fixture.receipt, ".confirm")
        .expect("publish the actual provider success");
    assert_eq!(notice(&listener).await.0, "reply-buffered");

    let late_publication = if let Some(pause) = writer_pause {
        pause
            .reached
            .await
            .expect("the real writer holds the reply and original reader sender");
        Some((pause.release, admission))
    } else {
        assert_eq!(
            admission.await.expect("observe the actual queue write"),
            (turn, TerminalAdmission::Queued)
        );
        if after_dequeue {
            (&mut reader_reached)
                .await
                .expect("the reader dequeued the actual terminal");
        }
        None
    };
    assert!(matches!(
        receipt.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    if let Some(reader_ending) = reader_ending {
        parent_retired.cancel();
        assert_eq!(
            reader_ending
                .await
                .expect("the parent ended the exact reader"),
            (transport_run, OpeningReaderEnd::Retired),
        );
        assert!(
            reader_pause.release.send(()).is_err(),
            "the parent dropped the still-parked reader"
        );
    } else {
        binding.retire(generation);
        assert!(matches!(
            receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        reader_pause
            .release
            .send(())
            .expect("the reader is still alive to take its own retirement");
    }
    assert!(
        !fixture.runtime.retirement().is_cancelled(),
        "the writer stayed live"
    );
    let mut binding = Some(binding);
    if let Some((release, admission)) = late_publication {
        drop(binding.take());
        tasks.close();
        tasks.wait().await;
        assert!(!wire.opening_route_is_registered_for_test(transport_run));
        assert!(matches!(
            receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        release
            .send(())
            .expect("resume the writer holding the old reader sender");
        assert_eq!(
            admission
                .await
                .expect("observe the attempt on the closed reader"),
            (turn, TerminalAdmission::Closed)
        );
    }
    TakenReply::Work(authorization, receipt)
        .assert(kind, ExpectedReply::BufferedSuccess)
        .await;
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
    assert_eq!(
        settled.load(Ordering::Relaxed),
        1,
        "the original reservation settled once"
    );
}
