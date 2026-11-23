use central_link_protocol::{RequestScope, RequestStatusQuery};
use router_clock::ClockInstant;
use std::{future::Future, path::Path, process::Command, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxDecision, InboxRefusalReason, InboxStore, MachineLocalStore, OutboxAccepted,
    OutboxStorage, OutboxWriteOutcome,
};
use switchboard_message_custody::{
    custody_operator::CustodyOperatorEvent,
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
    custody_task::{CustodyTaskEvent, custody_task},
};
use tokio::sync::{mpsc, watch};
use workspace_model::{
    Caller, ChangePosition, Timestamp,
    central_mail::{MessageBody, MessageRecord},
};

use super::{
    batch_limits::limits_fixture,
    custody_task::ObserveTask,
    inbox_custody::recipient_session,
    outbox_timing::{open_outbox, thread_post},
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

#[expect(
    clippy::panic,
    reason = "The fixture fails at the missing custody progress rather than hanging."
)]
async fn maintenance_progress<Output>(
    operation: impl Future<Output = Output>,
    expected: &str,
) -> Output {
    tokio::select! {
        biased;
        output = operation => output,
        () = async { for _ in 0..50_000 { tokio::task::yield_now().await; } } => {
            panic!("custody did not reach {expected}");
        }
    }
}

#[tokio::test]
async fn offline_inbox_retention_prunes_only_healthy_confirmed_rows() {
    let (root, custody, lane, clock) = limits_fixture().await;
    lane.admitted.send_replace(None);
    let database = root.path().join("machine_local.sqlite");
    let mut store = InboxStore::new(MachineLocalStore::open(&database).await.unwrap());
    let session = recipient_session();
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
                None,
            )
            .await
            .unwrap();
        store
            .mark_receipt_confirmed(&message.message_id, arrived_ms)
            .await
            .unwrap();
    }
    store
        .record_delivery(&kept, InboxDecision::Accepted, arrived_ms, None)
        .await
        .unwrap();
    mutate_fixture_row(
        &database,
        &format!(
            "UPDATE inbox_messages SET delivery_state_json = '{{' WHERE message_id = '{}';",
            damaged.message_id
        ),
    );

    let (stop, stopped) = watch::channel(false);
    let (sender, mut observations) = mpsc::unbounded_channel();
    let (_calls, worker) = custody_task(custody, stopped, Arc::new(ObserveTask(sender)));
    let running = tokio::spawn(worker);
    let eligible = ClockInstant::ZERO.after(Duration::from_millis(100));
    maintenance_progress(
        clock.not_due_check_now(eligible),
        "the offline inbox retention wake",
    )
    .await;
    assert!(
        matches!(maintenance_progress(observations.recv(), "the corrupt inbox diagnostic").await,
        Some(CustodyTaskEvent::Operator(CustodyOperatorEvent::CorruptInboxRow(row)))
            if row.key == damaged.message_id.as_str())
    );

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
    maintenance_progress(
        async {
            loop {
                if store
                    .receipt_for(&refused.message_id)
                    .await
                    .unwrap()
                    .healthy
                    .is_none()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        },
        "healthy completed row pruning",
    )
    .await;
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
    stop.send_replace(true);
    running.await.unwrap();
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

    let (stop, stopped) = watch::channel(false);
    let (sender, mut observations) = mpsc::unbounded_channel();
    let (calls, worker) = custody_task(custody, stopped, Arc::new(ObserveTask(sender)));
    let running = tokio::spawn(worker);
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
    maintenance_progress(
        clock.not_due_check_now(mark),
        "the healthy writer's flush wake",
    )
    .await;
    clock.advance(Duration::from_secs(20)).unwrap();
    assert!(
        matches!(maintenance_progress(observations.recv(), "a healthy settlement").await,
        Some(CustodyTaskEvent::Flush(report))
            if matches!(report.answered.as_slice(), [answer] if answer.request == healthy.request))
    );
    stop.send_replace(true);
    running.await.unwrap();
}
