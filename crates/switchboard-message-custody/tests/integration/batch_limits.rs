#![expect(
    clippy::unwrap_used,
    reason = "A bad fixture or missing peer batch must fail its test at the point of observation."
)]

use central_link_client::LINK_LANE_FRAME_BYTES;
use central_link_protocol::{DataFrame, FrameDirection, LinkFrame, encode_text};
use router_clock::{ClockInstant, ManualClock};
use std::{sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxStore, MachineLocalStore, OutboxInsert, OutboxQueuedWrite, OutboxStorage,
    PendingWriteState,
};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_submission::{Submission, SubmissionPayload, SubmitRefusal},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
};
use workspace_ids::MachineId;
use workspace_model::{Addressee, PostText, WorkspaceOperation, WorkspaceRequest};

use super::{
    batch_replay::{KnownRecipient, NoFlushJitter, ScriptedBatchLane, new_admission},
    outbox_timing::{manual_clock, open_outbox, thread_post},
};

const MARK_MS: i64 = 1_763_380_800_000;

pub(super) async fn limits_fixture() -> (
    tempfile::TempDir,
    CustodyLink,
    Arc<ScriptedBatchLane>,
    Arc<ManualClock>,
) {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("machine_local.sqlite");
    let clock = manual_clock(MARK_MS - 20_000);
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let lane = Arc::new(ScriptedBatchLane::new(
        new_admission(machine.clone(), clock.clone()),
        Default::default(),
    ));
    let outbox = OutboxWork::new(Box::new(open_outbox(&database).await), clock.clone());
    let inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(&database).await.unwrap()),
        machine,
        Arc::new(KnownRecipient),
        clock.clone(),
    );
    let custody = CustodyLink::new(
        lane.clone(),
        clock.clone(),
        Arc::new(NoFlushJitter),
        outbox,
        inbox,
    );
    (root, custody, lane, clock)
}

async fn queue_post(
    custody: &mut CustodyLink,
    seed: u16,
    update: impl FnOnce(&mut WorkspaceRequest),
) -> Submission {
    let item = thread_post(seed);
    let OutboxQueuedWrite::Workspace(mut request) = item.payload else {
        return Submission::Refused(SubmitRefusal::OutboxUnavailable);
    };
    update(&mut request);
    custody
        .submit(item.caller, SubmissionPayload::Workspace(request))
        .await
}

#[tokio::test]
async fn a_single_overbudget_write_is_refused_before_entering_the_store() {
    let (_root, mut custody, lane, _) = limits_fixture().await;
    let submission = queue_post(&mut custody, 1, |request| {
        let WorkspaceOperation::ThreadPost { addressees, .. } = &mut request.operation else {
            return;
        };
        *addressees = vec![
            Addressee::Actor {
                actor_identity_id: "019f0000-0000-7000-8000-000000000071".parse().unwrap(),
            };
            20_000
        ];
    })
    .await;

    assert!(matches!(
        submission,
        Submission::Refused(SubmitRefusal::TooLargeToSend)
    ));
    assert!(
        custody
            .pending_writes(&thread_post(1).caller)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(lane.batch_commands().is_empty());
}

#[tokio::test]
async fn a_full_byte_budget_leaves_the_oldest_unfitted_writes_for_the_next_mark() {
    let (_root, mut custody, lane, clock) = limits_fixture().await;
    for seed in (1..=20).rev() {
        let submitted = queue_post(&mut custody, seed, |request| {
            let WorkspaceOperation::ThreadPost { body, .. } = &mut request.operation else {
                return;
            };
            *body = PostText::try_from("x".repeat(64 * 1024)).unwrap();
        })
        .await;
        assert!(matches!(submitted, Submission::InOutbox(_)));
    }
    clock.advance(Duration::from_secs(20)).unwrap();
    let first = custody.flush_writes().await.unwrap();
    assert_eq!(
        first
            .answered
            .iter()
            .map(|write| write.request.clone())
            .collect::<Vec<_>>(),
        (1..=15)
            .map(|seed| thread_post(seed).request)
            .collect::<Vec<_>>()
    );
    let waiting = custody
        .pending_writes(&thread_post(1).caller)
        .await
        .unwrap();
    assert_eq!(
        waiting
            .iter()
            .filter(|write| matches!(write.state, PendingWriteState::Unsent { .. }))
            .count(),
        5
    );

    clock.advance(Duration::from_secs(60)).unwrap();
    let second = custody.flush_writes().await.unwrap();
    assert_eq!(
        second
            .answered
            .iter()
            .map(|write| write.request.clone())
            .collect::<Vec<_>>(),
        (16..=20)
            .map(|seed| thread_post(seed).request)
            .collect::<Vec<_>>()
    );
    for command in lane.batch_commands() {
        let frame = LinkFrame::Data(DataFrame::Batch {
            requests: command.requests,
        });
        assert!(
            encode_text(&frame, FrameDirection::ToCentral)
                .unwrap()
                .len()
                <= LINK_LANE_FRAME_BYTES
        );
    }
}

#[tokio::test]
async fn a_batchs_send_fence_never_outlives_its_earliest_original_deadline() {
    let (root, mut custody, lane, clock) = limits_fixture().await;
    let mut fixture_store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    let aged_write = thread_post(1);
    assert_eq!(
        fixture_store
            .insert(&aged_write, MARK_MS - 899_000)
            .await
            .unwrap(),
        OutboxInsert::Stored
    );
    let submitted = queue_post(&mut custody, 2, |_| {}).await;
    assert!(matches!(submitted, Submission::InOutbox(_)));

    clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(custody.flush_writes().await.unwrap().answered.len(), 2);
    let commands = lane.batch_commands();
    let [batch] = commands.as_slice() else {
        panic!("fixture needs one batch");
    };
    assert_eq!(
        batch.send_before,
        ClockInstant::ZERO.after(Duration::from_secs(21))
    );
}
