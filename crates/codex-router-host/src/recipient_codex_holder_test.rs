use super::{ROUTER, SAVED, create_codex_catalog, seed_codex_thread, session_id};
use crate::recipient_session_inventory::CodexSessionInventory;
use codex_acp_adapter::{
    AcpSchemaCatalog, AcpSessionBinding, ConversationOperationRecorder, ConversationRecordFuture,
    SessionSetupInputs, UnmaterializedBindingStore,
};
use codex_native_integration::{
    NativePayloadSchemas, NativeProtocolConnection, NativeSchemaBundle,
};
use collaboration_protocol::{CodexGeneration, CodexPermissionModeReport, OperationId, SessionId};
use collaboration_runtime::{OpenScratchScopes, UnmaterializedThreadHolder};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sqlx::Connection as _;
use std::{
    collections::BTreeMap, future::ready, io, os::unix::fs::PermissionsExt, path::Path, sync::Arc,
};
use switchboard_message_custody::recipient_sessions::{SessionInventory, SessionInventoryAnswer};
use tokio::net::UnixStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role},
};

struct UnusedRecorder;

fn unexpected_recording() -> ConversationRecordFuture<'static> {
    Box::pin(ready(Err(io::Error::other(
        "inventory fixture has no recorded operation",
    ))))
}

impl ConversationOperationRecorder for UnusedRecorder {
    fn admit_create<'a>(
        &'a self,
        _: &'a OperationId,
        _: &'a CodexGeneration,
    ) -> ConversationRecordFuture<'a> {
        unexpected_recording()
    }

    fn before_native_dispatch<'a>(&'a self, _: &'a OperationId) -> ConversationRecordFuture<'a> {
        unexpected_recording()
    }

    fn record_created<'a>(
        &'a self,
        _: &'a OperationId,
        _: &'a SessionId,
        _: &'a CodexPermissionModeReport,
    ) -> ConversationRecordFuture<'a> {
        unexpected_recording()
    }

    fn record_failure<'a>(&'a self, _: &'a OperationId, _: bool) -> ConversationRecordFuture<'a> {
        unexpected_recording()
    }
}

fn native_schemas() -> Arc<NativePayloadSchemas> {
    let mut definitions = serde_json::Map::new();
    for name in [
        "ThreadRead",
        "ThreadFork",
        "ThreadResume",
        "ThreadStart",
        "ThreadLoadedList",
        "TurnInterrupt",
        "TurnStart",
        "TurnSteer",
    ] {
        definitions.insert(format!("{name}Params"), json!({"type": "object"}));
        definitions.insert(format!("{name}Response"), json!({"type": "object"}));
    }
    let schemas = json!({"definitions": {"v2": definitions}});
    let bundle = NativeSchemaBundle::from_documents(BTreeMap::from([(
        "codex_app_server_protocol.schemas.json".to_owned(),
        serde_json::to_vec(&schemas).unwrap(),
    )]))
    .unwrap();
    Arc::new(NativePayloadSchemas::from_bundle(&bundle).unwrap())
}

async fn held_thread(root: &Path) -> AcpSessionBinding {
    let scope = format!("session-{ROUTER}");
    let scratch = root.join("scratch").join(&scope);
    std::fs::create_dir_all(&scratch).unwrap();
    std::fs::set_permissions(&scratch, std::fs::Permissions::from_mode(0o700)).unwrap();
    let actor = json!({
        "endpoint": {"serviceId": ROUTER, "endpointId": "codex-local"},
        "sessionId": ROUTER,
    });
    let params = json!({"cwd": root, "mcpServers": [], "_meta": {"codexRouter": {
        "model": "fixture-model", "effort": "medium", "access": "write-restricted",
        "scratchScope": scope, "scratchPath": scratch, "createdBy": actor, "approver": actor,
    }}});
    let generation =
        serde_json::from_value(json!({"serviceEpoch": ROUTER, "generation": 1})).unwrap();
    let (client, server) = UnixStream::pair().unwrap();
    let connection = NativeProtocolConnection::from_websocket(
        WebSocketStream::from_raw_socket(client, Role::Client, None).await,
    );
    let mut catalog = AcpSchemaCatalog::load().unwrap();
    let native = async {
        let mut wire = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let frame = wire.next().await.unwrap().unwrap();
        let request: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(request["method"], "thread/start");
        wire.send(Message::text(
            json!({"id": request["id"], "result": {
                "cwd": root, "model": "fixture-model", "thread": {"id": SAVED, "cwd": root}
            }})
            .to_string(),
        ))
        .await
        .unwrap();
    };
    let (binding, ()) = tokio::join!(
        AcpSessionBinding::create(
            &mut catalog,
            SessionSetupInputs {
                connection,
                schemas: native_schemas(),
                generation,
                params,
                operation_id: None,
                recorder: Arc::new(UnusedRecorder),
                scratch_scopes: Arc::new(OpenScratchScopes::new()),
            }
        ),
        native,
    );
    binding.unwrap()
}

#[tokio::test]
async fn held_codex_thread_is_authoritative_and_counts_once_with_its_stored_row() {
    let root = tempfile::tempdir().unwrap();
    let holder = Arc::new(UnmaterializedThreadHolder::new());
    holder.hold(held_thread(root.path()).await);
    assert!(holder.contains(SAVED));
    let inventory = CodexSessionInventory::new(root.path().to_owned(), holder);
    let target = session_id(SAVED);
    let present = SessionInventoryAnswer {
        at_least: 1,
        complete: true,
    };
    assert_eq!(
        inventory.lookup_session(&target).await,
        present,
        "no state file"
    );

    let file = root.path().join("state_5.sqlite");
    tokio::fs::write(&file, b"not a SQLite database")
        .await
        .unwrap();
    assert_eq!(
        inventory.lookup_session(&target).await,
        present,
        "unreadable state"
    );
    tokio::fs::remove_file(&file).await.unwrap();

    let mut connection = create_codex_catalog(root.path()).await;
    seed_codex_thread(&mut connection, SAVED, 1, false)
        .await
        .unwrap();
    assert_eq!(
        inventory.lookup_session(&target).await,
        present,
        "one live thread and its stored view"
    );
    sqlx::query("UPDATE threads SET archived = 1 WHERE id = ?")
        .bind(SAVED)
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    assert_eq!(
        inventory.lookup_session(&target).await,
        present,
        "live claim still present"
    );
}
