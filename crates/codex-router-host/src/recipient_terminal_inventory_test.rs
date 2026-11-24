use super::*;
use crate::recipient_session_inventory::tests::{IgnoreCustodyEvents, admission::Admission};
use router_clock::ManualClock;
use std::{future::pending, sync::Mutex};
use switchboard_message_custody::recipient_sessions::RecipientSessionSources;
use tokio::sync::oneshot;

struct EmptyInventory;

impl SessionInventory for EmptyInventory {
    fn lookup_session<'lookup>(&'lookup self, _: &'lookup SessionId) -> InventoryRead<'lookup> {
        Box::pin(std::future::ready(SessionInventoryAnswer::Complete(0)))
    }
}

fn fixture() -> (
    tempfile::TempDir,
    Arc<ManualClock>,
    Arc<ClaudeTerminalSessionInventory>,
) {
    let root = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let inventory = Arc::new(ClaudeTerminalSessionInventory::new(
        Arc::new(ClaudeCodeSessionRegistry::new(root.path().to_owned())),
        clock.clone(),
        Arc::new(IgnoreCustodyEvents),
    ));
    (root, clock, inventory)
}

#[tokio::test]
async fn new_offer_sees_record_created_after_predecessor_enumeration() {
    let (root, clock, inventory) = fixture();
    let (enumerated, after_enumeration) = oneshot::channel();
    let (release, held_probe) = oneshot::channel();
    let prior_target = PeerSessionId::try_from("predecessor".to_owned()).unwrap();
    let pid = std::process::id();
    std::fs::write(
        root.path().join(format!("{pid}.json")),
        serde_json::json!({"pid":pid,"sessionId":"predecessor","startedAt":4_102_444_800_000_i64})
            .to_string(),
    )
    .unwrap();
    let registry = inventory.registry.clone();
    let previous = tokio::spawn({
        let inventory = inventory.clone();
        async move {
            inventory
                .lookup_with(None, |permit, observation| async move {
                    let census = tokio::task::spawn_blocking(move || {
                        PeerSessionCensus::scan(&registry, &prior_target, || true)
                    })
                    .await
                    .unwrap();
                    enumerated.send(()).unwrap();
                    held_probe.await.unwrap();
                    let answer = census
                        .answer(
                            || observation.is_active(),
                            || observation.stopped(),
                            pending,
                            |_| {},
                        )
                        .await;
                    drop(permit);
                    answer
                })
                .await
        }
    });
    after_enumeration.await.unwrap();

    let target: SessionId = "new-terminal".parse().unwrap();
    let mut new_terminal = tokio::process::Command::new("/bin/cat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = new_terminal.id().unwrap();
    std::fs::write(
        root.path().join(format!("{pid}.json")),
        serde_json::json!({"pid":pid,"sessionId":target,"startedAt":4_102_444_800_000_i64})
            .to_string(),
    )
    .unwrap();
    clock.advance(Duration::from_secs(1)).unwrap();
    let next_deadline = clock.monotonic_now().after(RECIPIENT_LOOKUP_BUDGET);
    let mut admission = Admission::new(
        root.path(),
        clock.clone(),
        RecipientSessionSources {
            codex: Arc::new(EmptyInventory),
            managed_providers: Arc::new(EmptyInventory),
            claude_terminal: inventory.clone(),
        },
    )
    .await;
    let waits_for_permit = async {
        clock.not_due_check_now(next_deadline).await;
        assert_eq!(inventory.permit.available_permits(), 0);
        release.send(()).unwrap();
    };
    tokio::join!(admission.expect_accepted(&target), waits_for_permit);
    assert_eq!(previous.await.unwrap(), SessionInventoryAnswer::Complete(1));
    drop(new_terminal.stdin.take());
    assert!(new_terminal.wait().await.unwrap().success());
}

#[tokio::test]
async fn blocked_directory_uses_one_worker_and_each_lookup_expires_on_its_own_deadline() {
    let (_root, clock, inventory) = fixture();
    let (entered, blocking_read_entered) = oneshot::channel();
    let (unblock, blocking_read) = std::sync::mpsc::channel();
    let (drained, blocking_read_drained) = oneshot::channel();
    let first = tokio::spawn({
        let inventory = inventory.clone();
        async move {
            inventory
                .lookup_with(None, |permit, _observation| async move {
                    tokio::task::spawn_blocking(move || {
                        entered.send(()).unwrap();
                        blocking_read.recv().unwrap();
                        drop(permit);
                        drained.send(()).unwrap();
                        incomplete_census()
                    })
                    .await
                    .unwrap()
                })
                .await
        }
    });
    blocking_read_entered.await.unwrap();
    clock.advance(RECIPIENT_LOOKUP_BUDGET).unwrap();
    assert_eq!(first.await.unwrap(), UNAVAILABLE);

    let deadline = clock.monotonic_now().after(Duration::from_secs(2));
    let later = inventory.lookup_with(Some(deadline), |_, _| async {
        panic!("a blocked read must keep the only worker permit");
    });
    let advance = async {
        clock.not_due_check_now(deadline).await;
        assert_eq!(inventory.permit.available_permits(), 0);
        clock.advance(Duration::from_secs(2)).unwrap();
    };
    let (answer, ()) = tokio::join!(later, advance);
    assert_eq!(answer, UNAVAILABLE);
    unblock.send(()).unwrap();
    blocking_read_drained.await.unwrap();
    assert_eq!(
        inventory.lookup_session(&"fresh".parse().unwrap()).await,
        SessionInventoryAnswer::Complete(0)
    );
}

#[tokio::test]
async fn expiry_publishes_a_finished_claim_before_the_worker_finishes_cleanup() {
    let (_root, clock, inventory) = fixture();
    let (entered, observed) = oneshot::channel();
    let (reaping, reap_started) = oneshot::channel();
    let (release, held_reap) = oneshot::channel();
    let (finished, reap_finished) = oneshot::channel();
    let lookup = tokio::spawn({
        let inventory = inventory.clone();
        async move {
            inventory
                .lookup_with(None, |permit, observation| async move {
                    observation.progress.store(1, Ordering::Release);
                    entered.send(()).unwrap();
                    observation.stopped().await;
                    reaping.send(()).unwrap();
                    held_reap.await.unwrap();
                    drop(permit);
                    finished.send(()).unwrap();
                    incomplete_census()
                })
                .await
        }
    });
    observed.await.unwrap();
    clock.advance(RECIPIENT_LOOKUP_BUDGET).unwrap();
    assert_eq!(lookup.await.unwrap(), SessionInventoryAnswer::AtLeast(1));
    reap_started.await.unwrap();
    assert_eq!(inventory.permit.available_permits(), 0);
    release.send(()).unwrap();
    reap_finished.await.unwrap();
    assert_eq!(inventory.permit.available_permits(), 1);
}

#[derive(Default)]
struct RecordingEvents(Mutex<Vec<CustodyTaskEvent>>);

impl CustodyEventSink for RecordingEvents {
    fn record_event(&self, event: CustodyTaskEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn reap_handoff_emits_one_typed_event_and_releases_the_permit() {
    let root = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let events = Arc::new(RecordingEvents::default());
    let inventory = ClaudeTerminalSessionInventory::new(
        Arc::new(ClaudeCodeSessionRegistry::new(root.path().to_owned())),
        clock,
        events.clone(),
    );
    let outcome = inventory
        .lookup_with(None, |permit, _observation| async move {
            drop(permit);
            PeerCensusOutcome::ProbeReapDeferred {
                process_id: 42,
                answer: PeerCensusAnswer {
                    at_least: 1,
                    complete: false,
                    managed_claude_alias: false,
                },
            }
        })
        .await;

    assert_eq!(outcome, SessionInventoryAnswer::AtLeast(1));
    assert!(matches!(
        events.0.lock().unwrap().as_slice(),
        [CustodyTaskEvent::Operator(
            CustodyOperatorEvent::TerminalProbeReapDeferred { process_id: 42 }
        )]
    ));
    assert_eq!(
        inventory.lookup_session(&"fresh".parse().unwrap()).await,
        SessionInventoryAnswer::Complete(0)
    );
}
