use router_clock::ClockInstant;
use std::{future::Future, path::Path, process::Command, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxDecision, InboxRefusalReason, InboxStore, MachineLocalStore,
};
use switchboard_message_custody::{
    custody_operator::CustodyOperatorEvent,
    custody_task::{CustodyTaskEvent, custody_task},
};
use tokio::sync::{mpsc, watch};
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageRecord},
};

use super::{
    batch_limits::limits_fixture, custody_task::ObserveTask, inbox_custody::recipient_session,
};

const NOW_MS: i64 = 1_763_380_780_000;
const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

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
async fn offline_inbox_retention_prunes_only_healthy_completed_rows() {
    let (root, custody, lane, clock) = limits_fixture().await;
    lane.admitted.send_replace(None);
    let database = root.path().join("machine_local.sqlite");
    let mut store = InboxStore::new(MachineLocalStore::open(&database).await.unwrap());
    let session = recipient_session();
    let ended_ms = NOW_MS - DAY_MS + 99;
    let make_record = |id: &str| {
        MessageRecord::queued_central(
            id.parse().unwrap(),
            session.clone(),
            MessageBody::Notice { items: Vec::new() },
            Timestamp::from_timestamp_millis(ended_ms).unwrap(),
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
                ended_ms,
            )
            .await
            .unwrap();
        store
            .mark_receipt_confirmed(&message.message_id, ended_ms)
            .await
            .unwrap();
    }
    store
        .record_delivery(&kept, InboxDecision::Accepted, ended_ms)
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
