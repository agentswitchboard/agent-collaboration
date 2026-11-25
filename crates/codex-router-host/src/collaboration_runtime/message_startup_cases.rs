use std::{os::unix::fs::PermissionsExt as _, path::Path, sync::Arc};

use collaboration_protocol::message_tools::{
    MessageFrontRefusal, MessageSendOutcome, MessageToolRefusal, MessageToolResult,
};
use router_clock::ManualClock;
use serde_json::{Value, json};
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
    roots: Option<MessageRoots>,
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
            message_roots: roots.map(Into::into),
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
    let runtime = start_host(root.path(), Some(roots), clock()).await;
    let mut status = runtime
        .message_system
        .as_ref()
        .expect("Router roots configured")
        .status();
    status
        .wait_for(|status| status.status == MessageSystemStatus::NotEnrolled)
        .await
        .expect("initial metadata observed");
    assert!(
        root.path().join(MACHINE_LOCAL_DATABASE).is_file(),
        "Host read the machine store"
    );
    assert_eq!(
        runtime.message_status().map(|observation| observation.status),
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
    drop(client);
    runtime.shutdown().await.expect("shut down unenrolled Host");
    assert!(matches!(front.custody(), Err(FrontRefusal::ShuttingDown)));
}

#[tokio::test]
async fn served_message_send_distinguishes_unenrolled_front_from_missing_link() {
    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("MCP client");
    for (configured, expected, words) in [
        (
            true,
            MessageToolRefusal::Front(MessageFrontRefusal::NotEnrolled),
            "This Mac is not enrolled.",
        ),
        (
            false,
            MessageToolRefusal::NoMessageLink,
            "This Router has no message link.",
        ),
    ] {
        let root = private_root();
        let roots = configured
            .then(|| MessageRoots::new(root.path().to_owned(), root.path().join("secrets")));
        let runtime = start_host(root.path(), roots, clock()).await;
        if let Some(system) = &runtime.message_system {
            drop(
                system
                    .status()
                    .wait_for(|status| status.status == MessageSystemStatus::NotEnrolled)
                    .await
                    .expect("observe initial enrollment metadata"),
            );
        }
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "message_send",
                "arguments": {
                    "callerSessionId": "message-startup-proof",
                    "to": {
                        "machineId": "019f0000-0000-7000-8000-00000000cd11",
                        "sessionId": "recipient"
                    },
                    "body": "the front must refuse this message"
                }
            }
        });
        let response = client
            .post(format!("http://{}/mcp", runtime.mcp_listen_address()))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", "2025-11-25")
            .body(request.to_string())
            .send()
            .await
            .expect("served MCP response")
            .error_for_status()
            .expect("MCP HTTP success")
            .bytes()
            .await
            .expect("MCP response body");
        let answer: Value = serde_json::from_slice(&response).expect("MCP JSON-RPC result");
        let result = &answer["result"];
        assert_eq!(result["isError"], true, "{answer}");
        assert_eq!(result["content"], json!([{"type": "text", "text": words}]));
        let structured: MessageToolResult<MessageSendOutcome> =
            serde_json::from_value(result["structuredContent"].clone())
                .expect("typed message refusal");
        assert_eq!(structured, MessageToolResult::Refused(expected));
        runtime.shutdown().await.expect("shut down Host fixture");
    }
}
