//! Read run-only retirement on the same live ACP connection and on the wire.

use super::*;
use provider_session_machines::{
    session_lifecycle_machine::SessionLifecycle, session_owner::OwnerEvent,
};

enum RetirementTrigger {
    ReaderProjectionError,
    RunLost,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acp_reader_projection_failure_retires_only_its_run() {
    check_run_retirement(RetirementTrigger::ReaderProjectionError).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_run_retirement_publishes_unloaded_and_sends_zero_close_frames() {
    check_run_retirement(RetirementTrigger::RunLost).await;
}

async fn check_run_retirement(trigger: RetirementTrigger) {
    let behaviours: &[&str] = match trigger {
        RetirementTrigger::ReaderProjectionError => &["advertise-close", "reader-projection-error"],
        RetirementTrigger::RunLost => &["advertise-close"],
    };
    let fixture = hub_runtime_with(behaviours).await;
    let runtime = &fixture.runtime;
    let session_id = "reactive-session-1";
    let cwd = fixture._root.path().to_owned();
    runtime
        .load_session(session_id.into(), cwd.clone())
        .await
        .expect("real initial load");
    wait_for_hub_state(&fixture, session_id, SessionState::Idle)
        .await
        .expect("published initial activation");
    let door = runtime.owner_door_slot.get().expect("real ACP owner door");
    let owner = door
        .existing_provider_session(session_id)
        .expect("registered owner");
    let mut snapshots = owner.snapshots();
    let live = snapshots.borrow().clone();
    let SessionLifecycle::Live { generation } = live.lifecycle else {
        panic!("test needs an activated run");
    };
    let mut history = fixture
        .hub
        .attach(fixture.hub_target.clone())
        .await
        .expect("attached live history");
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), 1);

    match trigger {
        RetirementTrigger::ReaderProjectionError => {
            node_test_fixture::release_fixture(&fixture.receipt, ".release-reader-error")
                .expect("release one live projection error on the wire");
        }
        RetirementTrigger::RunLost => {
            owner
                .report_event(OwnerEvent::RunLost(generation))
                .expect("admit standalone retirement");
        }
    }
    let ending = loop {
        let observed = history
            .receiver
            .recv()
            .await
            .expect("same run's hub publication");
        if let SessionEvent::StateChanged {
            state: state @ (SessionState::Unloaded | SessionState::Closed),
        } = observed.event
        {
            break state;
        }
    };
    assert_eq!(
        ending,
        SessionState::Unloaded,
        "run retirement must not publish a confirmed close"
    );
    let retired = snapshots.wait_for(|snapshot| matches!(snapshot.lifecycle, SessionLifecycle::NotLoaded { last } if last == generation))
        .await.expect("only the original run retired");
    assert_eq!(retired.owner, live.owner);
    drop(retired);
    let retained = fixture
        .hub
        .attach(fixture.hub_target.clone())
        .await
        .expect("retained retirement");
    assert_eq!(
        retained
            .snapshot
            .iter()
            .filter(|event| matches!(
                event.event,
                SessionEvent::StateChanged {
                    state: SessionState::Unloaded
                }
            ))
            .count(),
        1
    );
    assert!(
        !runtime.retirement().is_cancelled(),
        "the provider connection must remain usable"
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);

    runtime
        .load_session(session_id.into(), cwd)
        .await
        .expect("same connection admits a fresh generation");
    let new = door
        .existing_provider_session(session_id)
        .expect("same owner can reopen");
    assert!(Arc::ptr_eq(&owner, &new));
    let reopened = new.snapshots().borrow().clone();
    assert_eq!(reopened.owner, live.owner);
    assert!(
        matches!(reopened.lifecycle, SessionLifecycle::Live { generation: fresh } if fresh > generation)
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), 2);
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), 0);
    runtime.shutdown().await;
}
