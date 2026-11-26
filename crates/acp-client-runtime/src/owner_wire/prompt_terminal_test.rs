//! Force the real bounded-channel publication to overlap the reader ending.

use super::*;
use crate::owner_wire::output_stop_tests::ReplyRecorder;
use provider_session_machines::{
    SessionGeneration,
    session_owner::{AuthorizationTarget, OwnerIncarnation},
};
use std::{sync::mpsc as blocking, thread};
use switchboard_harness_contracts::InputId;
use tokio::sync::oneshot;
use uuid::Uuid;

#[tokio::test]
async fn reader_close_collects_publication_from_an_already_granted_terminal_permit() {
    let (sender, mut receiver) = prompt_terminal_channel();
    let surviving_sender = sender.clone();
    let authorization = DispatchAuthorization {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        run: SessionGeneration::from_number(1),
        input_id: InputId::generate(),
        authorization_seq: 1,
        target: AuthorizationTarget::NewTurn,
    };
    let (reply, mut receipt) = oneshot::channel();
    let terminal = PromptTerminal::new(
        TurnId(Uuid::now_v7()),
        false,
        authorization.clone(),
        Some(Ok(serde_json::json!({"stopReason": "end_turn"}))),
        Box::new(ReplyRecorder(reply)),
    );
    let (permit_taken, permit_held) = oneshot::channel();
    let (publish, allow) = blocking::channel();
    let producer = thread::spawn(move || {
        let _publication = sender.publication.lock().expect("publication gate");
        let permit = sender
            .sender
            .try_reserve()
            .expect("the real queue grants capacity");
        permit_taken.send(()).expect("witness the held permit");
        allow.recv().expect("release the real publication");
        permit.send(terminal);
    });
    permit_held
        .await
        .expect("the producer owns capacity but has not published");

    let (waiting, mut closing) = oneshot::channel();
    receiver.close_waiting = Some(waiting);
    let (ended, mut reader_ended) = oneshot::channel();
    let reader = thread::spawn(move || {
        drop(receiver);
        ended.send(()).expect("witness the reader's completion");
    });
    // With the gate, the real close can prove it is waiting on publication.
    // With no gate, the receiver instead finishes before the permit is used.
    tokio::select! {
        _ = &mut closing => {}
        _ = &mut reader_ended => {}
    }
    publish
        .send(())
        .expect("allow publication at the decided close state");
    producer.join().expect("producer finished");
    reader.join().expect("reader finished");
    let settled_while_sender_lived = receipt.try_recv();
    drop(surviving_sender);
    assert_eq!(
        settled_while_sender_lived,
        Ok(WorkReply::Dispatch(DispatchResult::PromptFinished {
            authorization,
            outcome: WireOutcome::Confirmed(PromptCompletion {
                stop_reason: StopReason::EndTurn,
                local_cause: None,
                response: String::new(),
            }),
        })),
        "the reader must settle a terminal published across close while a sender clone still lives",
    );
}
