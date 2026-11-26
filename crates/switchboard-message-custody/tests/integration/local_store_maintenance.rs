use central_link_protocol::{RequestScope, RequestStatusQuery};
use router_clock::ClockInstant;
use std::{path::Path, process::Command, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxDecision, InboxOfferExpiry, InboxRefusalReason, InboxStore, MachineLocalStore,
    OutboxAccepted, OutboxStorage, OutboxWriteOutcome,
};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_operator::CustodyOperatorEvent,
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
    custody_task::{CustodyTaskEvent, custody_task},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
};
use tokio::sync::mpsc;
use workspace_model::{
    Caller, ChangePosition, Timestamp,
    central_mail::{MessageBody, MessageRecord},
};

use super::{
    batch_limits::limits_fixture,
    batch_replay::{KnownRecipient, NoFlushJitter, ScriptedBatchLane, new_admission},
    custody_task::ObserveTask,
    inbox_custody::recipient_session,
    outbox_timing::{manual_clock, open_outbox, thread_post},
};

const NOW_MS: i64 = 1_763_380_780_000;
const RETENTION_MS: i64 = 60 * 24 * 60 * 60 * 1_000;

#[expect(
    clippy::unwrap_used,
    reason = "The test must actually corrupt the disposable SQLite row."
)]
fn mutate_fixture_row(database: &Path, sql: &str) {
    let output = Command::new("sqlite3")
        .arg("-batch")
        .arg(database)
        .arg(sql)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture SQLite failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn offline_inbox_retention_prunes_only_healthy_confirmed_rows() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("machine_local.sqlite");
    let clock = manual_clock(NOW_MS);
    let session = recipient_session();
    let lane = Arc::new(ScriptedBatchLane::new(
        new_admission(session.machine_id.clone(), clock.clone()),
        clock.clone(),
        Default::default(),
    ));
    lane.set_connected(None);
    let mut store = InboxStore::new(MachineLocalStore::open(&database).await.unwrap());
    let arrived_ms = NOW_MS - RETENTION_MS + 100;
    let make_record = |id: &str| {
        MessageRecord::queued_central(
            id.parse().unwrap(),
            session.clone(),
            MessageBody::Notice { items: Vec::new() },
            Timestamp::from_timestamp_millis(arrived_ms).unwrap(),
        )
    };
    let refused = make_record("019f0000-0000-7000-8000-000000000f01");
    let kept = make_record("019f0000-0000-7000-8000-000000000f02");
    let damaged = make_record("019f0000-0000-7000-8000-000000000f03");
    for message in [&refused, &damaged] {
        store
            .record_delivery(
                message,
                InboxDecision::Refused {
                    reason: InboxRefusalReason::UnknownSession,
                },
                arrived_ms,
                InboxOfferExpiry::SystemWithoutExpiry,
            )
            .await
            .unwrap();
        store
            .mark_receipt_confirmed(&message.message_id, arrived_ms)
            .await
            .unwrap();
    }
    store
        .record_delivery(
            &kept,
            InboxDecision::Accepted,
            arrived_ms,
            InboxOfferExpiry::SystemWithoutExpiry,
        )
        .await
        .unwrap();
    mutate_fixture_row(
        &database,
        &format!(
            "UPDATE inbox_messages SET delivery_state_json = '{{' WHERE message_id = '{}';",
            damaged.message_id
        ),
    );

    let owned_inbox = MachineLocalStore::open(&database).await.unwrap();
    let mut inbox_commits = owned_inbox.subscribe_commits();
    let custody = CustodyLink::new(
        lane.clone(),
        clock.clone(),
        Arc::new(NoFlushJitter),
        OutboxWork::new(Box::new(open_outbox(&database).await), clock.clone()),
        RecipientInbox::new(
            InboxStore::new(owned_inbox),
            session.machine_id.clone(),
            Arc::new(KnownRecipient),
            clock.clone(),
        ),
    );
    let (sender, mut observations) = mpsc::unbounded_channel();
    let (_calls, group) = custody_task(custody, Arc::new(ObserveTask(sender)));
    let eligible = ClockInstant::ZERO.after(Duration::from_millis(100));
    clock.not_due_check_now(eligible).await;
    let diagnostic = observations
        .recv()
        .await
        .expect("custody stopped before reporting the corrupt inbox row");
    assert!(matches!(diagnostic,
        CustodyTaskEvent::Operator(CustodyOperatorEvent::CorruptInboxRow(row))
            if row.key == damaged.message_id.as_str()));

    clock.advance(Duration::from_millis(99)).unwrap();
    assert!(
        store
            .receipt_for(&refused.message_id)
            .await
            .unwrap()
            .healthy
            .is_some()
    );
    clock.advance(Duration::from_millis(1)).unwrap();
    inbox_commits
        .changed()
        .await
        .expect("custody stopped before committing inbox retention");
    assert!(
        store
            .receipt_for(&refused.message_id)
            .await
            .unwrap()
            .healthy
            .is_none()
    );
    assert_eq!(store.pending_handoffs().await.unwrap().healthy, [kept]);
    assert_eq!(
        store
            .receipt_for(&damaged.message_id)
            .await
            .unwrap()
            .corrupt_rows
            .len(),
        1
    );
    assert!(lane.batch_commands().is_empty());
    group.request_stop();
    group.join().await.unwrap();
}

#[tokio::test]
async fn a_corrupt_owned_point_is_reported_without_leaking_it_to_other_callers() {
    use switchboard_machine_core_store::OutboxQueuedWrite;

    let (root, custody, lane, clock) = limits_fixture().await;
    let database = root.path().join("machine_local.sqlite");
    let mut store = open_outbox(&database).await;
    let damaged = thread_post(184);
    let OutboxQueuedWrite::Workspace(original) = &damaged.payload else {
        panic!("fixture must have a thread post");
    };
    store.insert(&damaged, NOW_MS).await.unwrap();
    assert_eq!(
        store
            .claim_next_batch(
                damaged.flush_at_ms,
                1,
                NOW_MS,
                NOW_MS,
                &std::collections::BTreeSet::new()
            )
            .await
            .unwrap()
            .healthy
            .writes
            .len(),
        1
    );
    let answered = OutboxWriteOutcome::from_accepted(
        &damaged.payload,
        OutboxAccepted::ThreadPost {
            post_id: "019f0000-0000-7000-8000-000000000f04".parse().unwrap(),
            position: ChangePosition::try_from(1_u64).unwrap(),
            actors: Vec::new(),
        },
    )
    .unwrap();
    store
        .record_outcome(&damaged.request, &answered, NOW_MS)
        .await
        .unwrap();
    mutate_fixture_row(
        &database,
        &format!(
            "UPDATE outbox_items SET outcome_json = '{{' WHERE request_id = '{}';",
            damaged.request
        ),
    );

    let (sender, mut observations) = mpsc::unbounded_channel();
    let (calls, group) = custody_task(custody, Arc::new(ObserveTask(sender)));
    let query = RequestStatusQuery {
        request_id: damaged.request.clone(),
        scope: RequestScope::Workspace(original.workspace_id.clone().unwrap()),
    };
    let foreign = Caller::Session(recipient_session());
    assert_ne!(foreign, damaged.caller);
    assert_eq!(
        calls.request_status(foreign.clone(), query.clone()).await,
        CustodyRequestStatus::NotFound
    );
    assert!(
        observations.try_recv().is_err(),
        "outbox corruption remains opaque to other callers"
    );
    assert_eq!(
        calls.request_status(damaged.caller, query).await,
        CustodyRequestStatus::StatusUnavailable
    );
    assert!(
        matches!(
            observations.try_recv(),
            Ok(CustodyTaskEvent::Operator(CustodyOperatorEvent::CorruptOutboxPoint { request_id, field: "outcome_json" }))
                if request_id == damaged.request.as_str()
        ),
        "the operator must receive the typed point diagnostic before the tool reply"
    );
    assert_eq!(
        lane.take_calls().len(),
        1,
        "only the foreign miss may ask Central"
    );

    let healthy = thread_post(185);
    let OutboxQueuedWrite::Workspace(request) = healthy.payload else {
        panic!("fixture must have a thread post");
    };
    assert!(matches!(
        calls
            .submit(foreign, SubmissionPayload::Workspace(request))
            .await,
        Submission::InOutbox(_)
    ));
    let mark = ClockInstant::ZERO.after(Duration::from_secs(20));
    clock.not_due_check_now(mark).await;
    clock.advance(Duration::from_secs(20)).unwrap();
    let settlement = observations
        .recv()
        .await
        .expect("custody stopped before reporting the healthy settlement");
    assert!(matches!(settlement,
        CustodyTaskEvent::Flush(report)
            if matches!(report.answered.as_slice(), [answer] if answer.request == healthy.request)));
    group.request_stop();
    group.join().await.unwrap();
}
