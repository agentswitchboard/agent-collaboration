use super::*;
use router_clock::{ManualClock, RuntimeClock as _};
use std::{os::unix::fs::PermissionsExt as _, sync::Arc};

#[tokio::test]
async fn host_startup_keeps_journal_history_recent_on_the_injected_clock() {
    let root = tempfile::tempdir().expect("isolated Host root");
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private Host root");
    let clock = Arc::new(ManualClock::new(
        "2020-02-01T01:00:00Z".parse().expect("fixed Host UTC"),
    ));
    let observation: collaboration_protocol::LifecycleObservation =
        serde_json::from_value(serde_json::json!({
            "observedAt": "2020-02-01T00:00:00Z",
            "source": "observerLifecycle",
            "scope": {
                "endpoint": {"serviceId": new_service_uuid().expect("service ID"), "endpointId": "codex-local"},
                "generation": null,
                "observerId": new_service_uuid().expect("observer ID"),
            },
            "subject": {"kind": "backend"},
            "change": {"kind": "coverageLost"},
        }))
        .expect("fixture lifecycle observation");
    let mut journal = lifecycle_observation::ObservationJournal::open(
        &root.path().join("session-registry.sqlite"),
        new_service_uuid().expect("journal ID"),
    )
    .await
    .expect("open fixture lifecycle journal");
    let saved = journal
        .append(&observation, clock.utc_now().timestamp() - 3_600)
        .await
        .expect("save observation one hour before startup");
    journal.close().await;

    let runtime = CollaborationRuntime::start_for_host_with_router_proxy_endpoint(
        HostCollaborationInputs {
            collaboration_runtime: CollaborationRuntimeInputs {
                directory: root.path().to_path_buf(),
                codex_home: root.path().join("codex-fixture"),
                backend_socket: root.path().join("backend.sock"),
                mcp_bind: SocketAddr::from(([127, 0, 0, 1], 0)),
                native_schema: None,
                peer_registry_directory: Some(root.path().join("peers")),
                remote_control_server_name: None,
                owner_human_id: None,
            },
            router_proxy_endpoint: SocketAddr::from(([127, 0, 0, 1], 1)),
            remote_access: None,
            provider_operation_retention_days: None,
        },
        Vec::new(),
        Arc::new(collaboration_mcp::FixedRouterExecutableRelation(
            RouterExecutableRelation::Match,
        )),
        clock,
        None,
    )
    .await
    .expect("start through the shipping Host entry");
    let bounds = runtime
        .journal
        .as_ref()
        .expect("prepared journal")
        .bounds()
        .await;
    runtime.shutdown().await.expect("stop owned Host fixture");

    let bounds = bounds.expect("read prepared journal bounds");
    assert_eq!(
        bounds.earliest_sequence, saved.sequence,
        "Host startup must retain the observation one hour old on its injected clock"
    );
}
