use super::{ExternalProviderSupervisor, Mutex, ProviderOperationStore};
use collaboration_contracts::{HubEvent, SessionEventHub};
use collaboration_protocol::{ProviderIdentity, ProviderRequestedPolicy, RouterAccess};
use router_clock::ManualClock;
use session_event_model::{SessionEvent, SessionState};
use std::sync::Arc;
use switchboard_session_store::ProviderSessionRecord;

#[tokio::test]
async fn supervisor_with_no_bindings_starts_at_the_injected_time() {
    let directory = tempfile::tempdir().expect("store directory");
    let store = ProviderOperationStore::open(&directory.path().join("operations.sqlite"))
        .await
        .expect("open store");
    let started_at = 1_700_012_345_678;
    let clock = Arc::new(ManualClock::new(
        chrono::DateTime::from_timestamp_millis(started_at).unwrap(),
    ));

    let supervisor = ExternalProviderSupervisor::new_with_hub(
        Vec::new(),
        Arc::new(Mutex::new(store)),
        None,
        None,
        clock,
    )
    .expect("empty supervisor");

    assert_eq!(supervisor.inner.started_at_ms, started_at);
}

#[tokio::test]
async fn fallback_hub_uses_the_injected_time_for_activity() {
    let directory = tempfile::tempdir().expect("store directory");
    let mut store = ProviderOperationStore::open(&directory.path().join("operations.sqlite"))
        .await
        .expect("open store");
    let target = super::tests::requester();
    let requester = ProviderIdentity::from(target.clone());
    store
        .record_session(&ProviderSessionRecord {
            target: target.clone(),
            working_directory: directory
                .path()
                .display()
                .to_string()
                .try_into()
                .expect("cwd"),
            requested_policy: ProviderRequestedPolicy {
                access: RouterAccess::WriteRestricted,
            },
            created_by: requester.clone(),
            approver: requester,
            updated_at_ms: 10,
            resume: Default::default(),
        })
        .await
        .expect("save fixture session");
    let board = ProviderIdentity::session_to_board(&target).expect("board session");
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let supervisor = ExternalProviderSupervisor::new_with_hub(
        Vec::new(),
        Arc::new(Mutex::new(store)),
        None,
        None,
        clock.clone(),
    )
    .expect("empty supervisor");
    let hub = &supervisor.inner.owner_hub;
    let observed_at = 1_700_012_346;
    clock.set_utc(chrono::DateTime::from_timestamp(observed_at, 0).unwrap());
    let event = SessionEvent::StateChanged {
        state: SessionState::Idle,
    };

    let receipt = hub
        .publish(board.clone(), event.clone())
        .await
        .expect("publish");
    let sessions = hub
        .sessions(board.endpoint.clone())
        .await
        .expect("sessions");

    assert_eq!(receipt, HubEvent { sequence: 1, event });
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session, board);
    assert_eq!(sessions[0].state, SessionState::Idle);
    assert_eq!(sessions[0].updated_at_seconds, observed_at);
}
