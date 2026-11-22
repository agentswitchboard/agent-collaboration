//! Observe the worker that the real Host starts, with its real wake sources.

use super::{Operations, at};
use crate::{CollaborationRuntime, CollaborationRuntimeInputs, ExternalProviderStartup};
use collaboration_protocol::{
    OperationId, ProviderKind, ProviderOperationEffect, ProviderOperationStage,
    ProviderReconciliationState,
};
use collaboration_runtime::{SourceWaitProbe, WorkerObservation};
use router_clock::{ManualClock, RuntimeClock as _};
use std::{num::NonZeroU32, os::unix::fs::PermissionsExt as _, sync::Arc};
use switchboard_session_store::ProviderOperationRecord;

async fn retained_row(
    operations: &Operations,
    id: &OperationId,
) -> Option<ProviderOperationRecord> {
    operations
        .store
        .lock()
        .await
        .inspect(id)
        .await
        .expect("read saved operation")
}

async fn require_idle_wait(
    observation: &WorkerObservation,
    mut probe: SourceWaitProbe,
    worker: &mut tokio::task::JoinHandle<()>,
    sleeps: u64,
) -> SourceWaitProbe {
    let parked = tokio::select! {
        polled = probe.parked() => polled.ok(),
        _completed = worker => None,
    };
    if parked != Some(true) {
        drop(probe);
        assert_eq!(
            parked,
            Some(true),
            "spawned worker must poll a pending idle source wait"
        );
        unreachable!();
    }
    let snapshot = observation.snapshot();
    assert_eq!(snapshot.sleeps, sleeps);
    assert!(snapshot.asleep);
    assert_eq!(snapshot.armed_deadline, None);
    assert!(!probe.was_woken(), "the idle worker has received no signal");
    probe
}

#[tokio::test(flavor = "current_thread")]
async fn started_host_retention_wakes_on_its_supervisor_and_store() {
    let seeded = Operations::open().await;
    let root = seeded._directory.path().to_path_buf();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("private fixture root");
    let now = at("2026-06-15T12:00:00.000Z");
    // Forty-five days old. With the default keep interval, this row survives.
    let old = at("2026-05-01T00:00:00.000Z");
    let clock = Arc::new(ManualClock::new(now));
    let held = seeded.settled(old).await;
    let kept = retained_row(&seeded, &held)
        .await
        .expect("held terminal row");
    let free = seeded.settled(old).await;
    let committed_later = seeded.dispatched().await;
    let directory = seeded._directory;
    drop(seeded.supervisor);
    Arc::try_unwrap(seeded.store)
        .unwrap_or_else(|_| panic!("seeded store still shared"))
        .into_inner()
        .close()
        .await
        .expect("close seeded store before real Host startup");
    let protected = held.clone();
    let configured = ExternalProviderStartup::unavailable(
        ProviderKind::Cursor,
        "fixture has no external provider process".to_owned(),
        "fixture uses saved operations".to_owned(),
    )
    .expect("configured endpoint");
    let mut runtime = Box::pin(
        CollaborationRuntime::start_for_host_with_router_proxy_endpoint(
            crate::HostCollaborationInputs {
                collaboration_runtime: CollaborationRuntimeInputs {
                    directory: root.clone(),
                    codex_home: root.join("fixture-codex"),
                    backend_socket: root.join("missing-backend.sock"),
                    mcp_bind: "127.0.0.1:0".parse().expect("loopback"),
                    native_schema: None,
                    peer_registry_directory: Some(root.join("peers")),
                    remote_control_server_name: None,
                    owner_human_id: Some(
                        message_board::HumanId::try_from("fixture-owner".to_owned())
                            .expect("owner"),
                    ),
                },
                router_proxy_endpoint: "127.0.0.1:1".parse().expect("unused Router endpoint"),
                remote_access: None,
                provider_operation_retention_days: NonZeroU32::new(30),
            },
            vec![configured],
            Arc::new(collaboration_mcp::FixedRouterExecutableRelation(
                collaboration_protocol::RouterExecutableRelation::Match,
            )),
            clock.clone(),
            Some(Box::new(move |runtime| {
                runtime
                    .external_provider_supervisor
                    .as_ref()
                    .expect("composed supervisor")
                    .hold_live_operation(protected);
            })),
        ),
    )
    .await
    .expect("start real Host runtime");
    let operations = Operations {
        _directory: directory,
        path: root.join("provider-operations.sqlite"),
        store: runtime
            .provider_store
            .as_ref()
            .expect("composed store")
            .clone(),
        supervisor: runtime
            .external_provider_supervisor
            .as_ref()
            .expect("composed supervisor")
            .clone(),
    };

    let worker = runtime
        .provider_retention
        .as_mut()
        .expect("Host startup must spawn the retention worker");
    let observation = runtime
        .provider_retention_observation
        .clone()
        .expect("started worker");
    let first_wait = observation.probe_next_source_wait();
    let free_after_startup = retained_row(&operations, &free).await;
    if free_after_startup.is_some() {
        drop(first_wait);
        assert_eq!(
            free_after_startup, None,
            "startup retention must apply the configured 30-day setting"
        );
        unreachable!();
    }
    let first_wait = require_idle_wait(&observation, first_wait, worker, 1).await;
    assert_eq!(retained_row(&operations, &held).await, Some(kept));
    let mut commits = operations.store.lock().await.subscribe_commits();
    let mut completed = operations.supervisor.subscribe_completions();

    assert!(operations.supervisor.publish_held_result(&held).await);
    let completion_woke = first_wait.was_woken();
    let second_wait = observation.probe_next_source_wait();
    first_wait.release();
    assert!(
        completion_woke,
        "the supervisor's completion must wake the actual worker wait"
    );
    assert!(
        completed
            .has_changed()
            .expect("supervisor signal stays open")
    );
    assert!(!commits.has_changed().expect("store signal stays open"));
    let second_wait = require_idle_wait(&observation, second_wait, worker, 2).await;
    assert_eq!(retained_row(&operations, &held).await, None);
    let _completed_revision = *completed.borrow_and_update();
    let _commit_revision = *commits.borrow_and_update();

    let terminal = operations
        .store
        .lock()
        .await
        .record_terminal(
            &committed_later,
            ProviderOperationEffect::Applied,
            ProviderReconciliationState::Confirmed,
            None,
            old.timestamp_millis(),
        )
        .await
        .expect("commit a third eligible row");
    let commit_woke = second_wait.was_woken();
    let third_wait = observation.probe_next_source_wait();
    second_wait.release();
    assert!(
        commit_woke,
        "a commit on the Host store must wake the actual worker wait"
    );
    assert_eq!(terminal.operation_id, committed_later);
    assert_eq!(terminal.stage, ProviderOperationStage::Terminal);
    assert_eq!(terminal.terminal_at_ms, Some(old.timestamp_millis()));
    assert!(commits.has_changed().expect("store signal stays open"));
    assert!(
        !completed
            .has_changed()
            .expect("supervisor signal stays open")
    );
    let third_wait = require_idle_wait(&observation, third_wait, worker, 3).await;
    assert_eq!(retained_row(&operations, &committed_later).await, None);
    assert_eq!(clock.utc_now(), now);
    third_wait.release();
    drop(operations.store);
    drop(operations.supervisor);
    runtime.shutdown().await.expect("stop real Host runtime");
}
