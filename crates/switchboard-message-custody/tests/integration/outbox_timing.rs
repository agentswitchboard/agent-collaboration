#![expect(
    clippy::unwrap_used,
    reason = "The outbox fixture fails at the unexpected store or clock result."
)]

use central_link_client::WelcomeClockAnchor;
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{path::Path, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    MachineLocalOutbox, MachineLocalStore, NewOutboxItem, OutboxCancellation, OutboxInsert,
    OutboxQueuedWrite, OutboxStorage,
};
use switchboard_message_custody::outbox_work::{FlushSelection, OutboxWork};
use workspace_ids::{RequestId, SessionRef};
use workspace_model::{
    Caller, PostText, Timestamp, WorkspaceOperation, WorkspaceRequest,
    central_mail::{MessageDelivery, SendMessageRequest},
};

const SHARED_MARK_MS: i64 = 1_763_380_800_000;
const WRITE_WINDOW: Duration = Duration::from_secs(15 * 60);

fn local_session(session_id: &str) -> SessionRef {
    SessionRef {
        machine_id: "019f0000-0000-7000-8000-000000000011".parse().unwrap(),
        session_id: session_id.parse().unwrap(),
    }
}

fn write_id(seed: u16) -> RequestId {
    format!("019f0000-0000-7000-8000-{seed:012x}")
        .parse()
        .unwrap()
}

pub(super) fn thread_post(seed: u16) -> NewOutboxItem {
    let request = write_id(seed);
    NewOutboxItem {
        caller: Caller::Session(local_session("sender-session")),
        payload: OutboxQueuedWrite::Workspace(WorkspaceRequest {
            request_id: Some(request.clone()),
            workspace_id: Some("019f0000-0000-7000-8000-000000000021".parse().unwrap()),
            operation: WorkspaceOperation::ThreadPost {
                thread_id: "019f0000-0000-7000-8000-000000000022".parse().unwrap(),
                body: PostText::try_from("thread post".to_owned()).unwrap(),
                addressees: Vec::new(),
            },
        }),
        request,
        flush_at_ms: SHARED_MARK_MS,
    }
}

fn direct_message(seed: u16) -> NewOutboxItem {
    let request = write_id(seed);
    NewOutboxItem {
        caller: Caller::Session(local_session("sender-session")),
        payload: OutboxQueuedWrite::SendMessage {
            request_id: request.clone(),
            message: SendMessageRequest {
                to: local_session("recipient-session"),
                body: PostText::try_from("direct message".to_owned()).unwrap(),
                delivery: MessageDelivery::Auto,
                reply_to: None,
            },
        },
        request,
        flush_at_ms: SHARED_MARK_MS,
    }
}

pub(super) async fn open_outbox(path: &Path) -> MachineLocalOutbox {
    MachineLocalOutbox::new(MachineLocalStore::open(path).await.unwrap())
}

pub(super) fn manual_clock(start_ms: i64) -> Arc<ManualClock> {
    Arc::new(ManualClock::starting_at(SyncTime::from_unix_millis(start_ms)).unwrap())
}

pub(super) fn welcome_anchor(clock: &dyn RuntimeClock, central_ms: i64) -> WelcomeClockAnchor {
    WelcomeClockAnchor {
        central_time: Timestamp::from_timestamp_millis(central_ms).unwrap(),
        observed_at: clock.monotonic_now(),
    }
}

#[tokio::test]
async fn offline_writes_end_at_the_original_deadline_despite_wall_clock_changes() {
    let root = tempfile::tempdir().unwrap();
    let opened_at = SHARED_MARK_MS - 18_765;
    let clock = manual_clock(opened_at);
    let mut store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    let post = thread_post(44);
    let message = direct_message(45);
    for item in [&post, &message] {
        assert_eq!(
            store.insert(item, opened_at).await.unwrap(),
            OutboxInsert::Stored
        );
    }
    let mut work = OutboxWork::new(Box::new(store), clock.clone());
    let deadline = ClockInstant::ZERO.after(WRITE_WINDOW);

    clock.set_utc(Timestamp::from_timestamp_millis(opened_at + 3_600_000).unwrap());
    clock
        .advance(WRITE_WINDOW - Duration::from_millis(1))
        .unwrap();
    assert_eq!(work.earliest_deadline().await.unwrap(), Some(deadline));
    assert!(
        work.end_due_writes().await.unwrap().is_empty(),
        "a wall-clock correction cannot end either write early"
    );

    clock.set_utc(Timestamp::from_timestamp_millis(opened_at - 3_600_000).unwrap());
    clock.advance(Duration::from_millis(1)).unwrap();
    let outcomes = work.end_due_writes().await.unwrap();
    assert_eq!(
        outcomes
            .into_iter()
            .map(|outcome| (outcome.request_id, outcome.caller, outcome.reason))
            .collect::<Vec<_>>(),
        vec![
            (
                post.request,
                post.caller,
                OutboxCancellation::NotSentWithin15Minutes
            ),
            (
                message.request,
                message.caller,
                OutboxCancellation::NotSentWithin15Minutes
            )
        ]
    );
    assert!(work.end_due_writes().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_shared_mark_claims_only_sixty_four_ordered_writes_inside_its_window() {
    let root = tempfile::tempdir().unwrap();
    let local_ms = SHARED_MARK_MS + 25_000;
    let clock = manual_clock(local_ms);
    let mut store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    for seed in (1..=65).rev() {
        store.insert(&thread_post(seed), local_ms).await.unwrap();
    }
    let anchor = welcome_anchor(clock.as_ref(), SHARED_MARK_MS - 3_001);
    let mark = SyncTime::from_unix_millis(SHARED_MARK_MS);
    let mut work = OutboxWork::new(Box::new(store), clock.clone());

    assert!(matches!(
        work.claim_at_mark(anchor, mark).await.unwrap(),
        FlushSelection::OutsideWindow
    ));
    clock.advance(Duration::from_millis(1)).unwrap();
    let selected = work.claim_at_mark(anchor, mark).await.unwrap();
    assert!(
        matches!(&selected, FlushSelection::Claimed(batch) if batch.ended.is_empty()
        && batch.writes.iter().map(|item| item.request.clone()).eq((1..=64).map(write_id))
        && batch.writes.iter().all(|item| item.sent_at.timestamp_millis() == local_ms))
    );
    assert_eq!(
        work.earliest_waiting_mark().await.unwrap(),
        Some(mark),
        "the 65th write remains waiting"
    );
}

#[tokio::test]
async fn reopening_after_the_original_deadline_ends_a_claim_with_no_answer() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let opened_at = SHARED_MARK_MS - 1_000;
    let clock = manual_clock(opened_at);
    let mark = SyncTime::from_unix_millis(SHARED_MARK_MS);
    let anchor = welcome_anchor(clock.as_ref(), opened_at);
    let mut store = open_outbox(&path).await;
    let message = direct_message(51);
    store.insert(&message, opened_at).await.unwrap();
    let mut first_run = OutboxWork::new(Box::new(store), clock.clone());
    clock.advance(Duration::from_secs(1)).unwrap();
    assert!(
        matches!(first_run.claim_at_mark(anchor, mark).await.unwrap(), FlushSelection::Claimed(batch)
        if batch.writes.iter().any(|write| write.request == message.request))
    );
    drop(first_run);

    clock
        .advance(WRITE_WINDOW - Duration::from_secs(1))
        .unwrap();
    let mut reopened = OutboxWork::new(Box::new(open_outbox(&path).await), clock);
    let ended = reopened.recover_orphaned_claims().await.unwrap();
    assert!(
        matches!(ended.as_slice(), [outcome] if outcome.request_id == message.request
        && outcome.caller == message.caller && outcome.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
    );
    assert!(reopened.earliest_waiting_mark().await.unwrap().is_none());
    assert!(reopened.recover_orphaned_claims().await.unwrap().is_empty());
}
