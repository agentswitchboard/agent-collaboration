use std::{os::unix::fs::PermissionsExt as _, path::Path, sync::Arc};

use router_clock::ManualClock;
use switchboard_machine_core_store::MACHINE_LOCAL_DATABASE;
use switchboard_message_system::{FrontRefusal, MessageRoots, MessageSystemStatus};

use super::super::{
    CollaborationRuntime, CollaborationRuntimeInputs, HostCollaborationInputs,
    RouterExecutableRelation, SocketAddr,
};

fn private_root() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("isolated Host root");
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private Host root");
    root
}

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(
        "2030-03-17T17:46:40Z".parse().expect("fixed Host time"),
    ))
}

async fn start_host(
    root: &Path,
    roots: MessageRoots,
    clock: Arc<ManualClock>,
) -> CollaborationRuntime {
    CollaborationRuntime::start_for_host_with_router_proxy_endpoint(
        HostCollaborationInputs {
            collaboration_runtime: CollaborationRuntimeInputs {
                directory: root.to_owned(),
                codex_home: root.join("codex-fixture"),
                backend_socket: root.join("backend.sock"),
                mcp_bind: SocketAddr::from(([127, 0, 0, 1], 0)),
                native_schema: None,
                peer_registry_directory: Some(root.join("peers")),
                remote_control_server_name: None,
                owner_human_id: None,
            },
            message_roots: Some(roots),
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
    .expect("shipping Host composition")
}

#[tokio::test]
async fn unenrolled_host_refuses_message_custody_and_serves_collaboration() {
    let root = private_root();
    let roots = MessageRoots::new(root.path().to_owned(), root.path().join("secrets"));
    let runtime = start_host(root.path(), roots, clock()).await;
    let mut status = runtime
        .message_system
        .as_ref()
        .expect("Router roots configured")
        .status();
    status
        .wait_for(|status| *status == MessageSystemStatus::NotEnrolled)
        .await
        .expect("initial metadata observed");
    assert!(
        root.path().join(MACHINE_LOCAL_DATABASE).is_file(),
        "Host read the machine store"
    );
    assert_eq!(
        runtime.message_status(),
        Some(MessageSystemStatus::NotEnrolled)
    );
    let front = runtime.message_front().expect("Router roots configured");
    assert!(matches!(front.custody(), Err(FrontRefusal::NotEnrolled)));

    let client = collaboration_client::CollaborationClient::connect(
        root.path(),
        "message-startup-proof",
        "1",
    )
    .await
    .expect("collaboration control surface still serves");
    assert_eq!(
        &client
            .installation_show()
            .await
            .expect("installation identity")
            .service_id,
        runtime.service_id()
    );
    assert!(client.journal_status().await.is_ok());
    drop(client);
    runtime.shutdown().await.expect("shut down unenrolled Host");
    assert!(matches!(front.custody(), Err(FrontRefusal::ShuttingDown)));
}
