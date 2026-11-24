use super::test_support::ScriptedSessionBackend;
use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{UnixListener, UnixStream};
use tokio_tungstenite::{client_async, tungstenite::Message};

struct AddressedLiteralCatalog {
    models: std::collections::HashMap<SessionRef, ProviderModelEntry>,
}
impl crate::SessionModelCatalog for AddressedLiteralCatalog {
    fn read_models(
        &self,
        target: SessionRef,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<ProviderModelEntry>, crate::SessionModelCatalogError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.models
                .get(&target)
                .cloned()
                .map(|model| vec![model])
                .ok_or(crate::SessionModelCatalogError::SessionNotFound)
        })
    }
}

async fn model_rpc(
    client: &mut tokio_tungstenite::WebSocketStream<UnixStream>,
    id: u64,
    method: &str,
    params: Value,
) -> Result<Value, Box<dyn std::error::Error>> {
    client
        .send(Message::Text(
            json!({"id":id,"method":method,"params":params})
                .to_string()
                .into(),
        ))
        .await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let frame = client.next().await.ok_or("native reply")??;
            let value: Value = serde_json::from_str(frame.to_text()?)?;
            if value.get("id") == Some(&json!(id)) {
                return Ok(value);
            }
        }
    })
    .await?
}

/// Oracle: Codex 0.165 config_update.rs:87-104,155-172 and the reviewed
/// M1 no-op contract. The extra default save must not repeat session settings.
#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Literal native transport contract assertions; setup failures propagate."
)]
async fn model_default_save_receipt_preserves_the_applied_session_settings()
-> Result<(), Box<dyn std::error::Error>> {
    // Arrange: apply a real native settings request through the owning socket.
    let directory = tempfile::tempdir()?;
    let socket_path = directory.path().join("defaults.sock");
    let listener = UnixListener::bind(&socket_path)?;
    let backend = ScriptedSessionBackend::new()?;
    let model = super::test_support::qualified_model("haiku", "Haiku", &["low", "medium"], "low")?;
    let (_catalog_sender, catalog) = tokio::sync::watch::channel(vec![model]);
    let context = Arc::new(RouterSessionAppServerContext::new(
        backend.endpoint.clone(),
        ScriptedSessionBackend::actor()?,
        backend.clone(),
        backend.clone(),
        super::test_support::model_catalog(catalog),
    ));
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        serve_router_session_app_server_connection(stream, context)
            .await
            .map_err(std::io::Error::other)
    });
    let (mut client, _) = client_async(
        "ws://localhost/rpc",
        UnixStream::connect(&socket_path).await?,
    )
    .await?;
    let created = model_rpc(
        &mut client,
        1,
        "thread/start",
        json!({"cwd":directory.path()}),
    )
    .await?;
    let applied = model_rpc(
        &mut client,
        2,
        "thread/settings/update",
        json!({"threadId":created["result"]["thread"]["id"],
            "model":"haiku","effort":"medium"}),
    )
    .await?;
    assert_eq!(applied, json!({"id":2,"result":{}}));
    assert_eq!(backend.setting_commands.lock().expect("settings").len(), 2);

    // Act: the same connection sends the picker's separate housekeeping batch.
    let response = model_rpc(
        &mut client,
        3,
        "config/batchWrite",
        json!({"edits":[
            {"keyPath":"model","value":"haiku","mergeStrategy":"replace"},
            {"keyPath":"model_reasoning_effort","value":"medium","mergeStrategy":"replace"}
        ],"filePath":null,"expectedVersion":null,"reloadUserConfig":true}),
    )
    .await?;

    // Assert: a correlated native receipt, with no extra provider command.
    assert_eq!(
        response,
        json!({"id":3,"result":{
            "status":"ok","version":"acp-compat-noop-v1",
            "filePath":"/dev/null","overriddenMetadata":null
        }})
    );
    assert_eq!(backend.setting_commands.lock().expect("settings").len(), 2);
    assert_eq!(
        backend.effective_model.lock().expect("model").as_deref(),
        Some("haiku")
    );
    assert_eq!(
        backend.effective_effort.lock().expect("effort").as_deref(),
        Some("medium")
    );
    assert!(backend.prompt_commands.lock().expect("prompts").is_empty());
    assert!(backend.steer_commands.lock().expect("steers").is_empty());
    assert!(
        backend
            .compact_commands
            .lock()
            .expect("compacts")
            .is_empty()
    );
    assert!(backend.cancel_commands.lock().expect("cancels").is_empty());
    client.close(None).await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), server).await???;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Literal consumer contract assertions; setup errors propagate."
)]
async fn native_consumer_reads_differing_injected_outputs_only_for_its_bound_session()
-> Result<(), Box<dyn std::error::Error>> {
    for literal in ["catalogue-a", "catalogue-b"] {
        let root = tempfile::tempdir()?;
        let path = root.path().join("catalog.sock");
        let listener = UnixListener::bind(&path)?;
        let backend = ScriptedSessionBackend::new()?;
        let first = backend.session.clone();
        let mut second = first.clone();
        second.session_id = message_board::SessionId::try_from("second-session".to_owned())?;
        backend
            .create_sessions
            .lock()
            .expect("sessions")
            .extend([first.clone(), second.clone()]);
        let models = Arc::new(AddressedLiteralCatalog {
            models: std::collections::HashMap::from([
                (
                    first,
                    ProviderModelEntry::try_new(literal.into(), literal.into(), String::new())?,
                ),
                (
                    second,
                    ProviderModelEntry::try_new("other".into(), "Other".into(), String::new())?,
                ),
            ]),
        });
        let context = Arc::new(RouterSessionAppServerContext::new(
            backend.endpoint.clone(),
            ScriptedSessionBackend::actor()?,
            backend.clone(),
            backend.clone(),
            models,
        ));
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            serve_router_session_app_server_connection(stream, context)
                .await
                .map_err(std::io::Error::other)
        });
        let (mut client, _) =
            client_async("ws://localhost/rpc", UnixStream::connect(&path).await?).await?;
        let initial = model_rpc(&mut client, 1, "model/list", json!({})).await?;
        assert_eq!(initial["result"]["data"][0]["id"], "provider-default");
        let created = model_rpc(&mut client, 2, "thread/start", json!({"cwd":root.path()})).await?;
        assert!(created.get("result").is_some());
        let bound = model_rpc(&mut client, 3, "model/list", json!({})).await?;
        assert_eq!(bound["result"]["data"][0]["id"], literal);
        let failed = model_rpc(
            &mut client,
            4,
            "thread/resume",
            json!({"threadId":"unknown"}),
        )
        .await?;
        assert!(failed.get("error").is_some());
        let retained = model_rpc(&mut client, 5, "model/list", json!({})).await?;
        assert_eq!(retained["result"]["data"][0]["id"], literal);
        model_rpc(&mut client, 6, "thread/start", json!({"cwd":root.path()})).await?;
        let second_models = model_rpc(&mut client, 7, "model/list", json!({})).await?;
        assert_eq!(second_models["result"]["data"][0]["id"], "other");
        model_rpc(
            &mut client,
            8,
            "thread/read",
            json!({"threadId":created["result"]["thread"]["id"]}),
        )
        .await?;
        let after_read = model_rpc(&mut client, 9, "model/list", json!({})).await?;
        assert_eq!(
            after_read["result"]["data"][0]["id"], "other",
            "history reads never bind model scope"
        );
        client.close(None).await?;
        server.await??;
    }
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Contract asserts; fixture failures propagate."
)]
async fn unbound_model_list_never_borrows_another_sessions_catalogue()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("models.sock");
    let listener = UnixListener::bind(&path)?;
    let backend = ScriptedSessionBackend::new()?;
    let foreign = vec![ProviderModelEntry::try_new(
        "foreign".into(),
        "Foreign".into(),
        String::new(),
    )?];
    let context = Arc::new(RouterSessionAppServerContext::new(
        backend.endpoint.clone(),
        ScriptedSessionBackend::actor()?,
        backend.clone(),
        backend.clone(),
        super::test_support::model_catalog(tokio::sync::watch::channel(foreign).1),
    ));
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        serve_router_session_app_server_connection(stream, context)
            .await
            .map_err(std::io::Error::other)
    });
    let (mut client, _) =
        client_async("ws://localhost/rpc", UnixStream::connect(&path).await?).await?;
    client
        .send(Message::Text(
            json!({"id":1,"method":"model/list","params":{}})
                .to_string()
                .into(),
        ))
        .await?;
    let reply: Value =
        serde_json::from_str(client.next().await.ok_or("model response")??.to_text()?)?;
    client.close(None).await?;
    server.await??;
    assert_eq!(reply["result"]["data"][0]["id"], "provider-default");
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Contract asserts; setup may fail."
)]
async fn malformed_model_refuses_before_creating_a_provider_session()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let socket_path = directory.path().join("claude.sock");
    let listener = UnixListener::bind(&socket_path)?;
    let backend = ScriptedSessionBackend::new()?;
    *backend.effective_model.lock().expect("test lock") = Some("default".into());
    let catalog = vec![
        ProviderModelEntry::try_new("default".into(), "Default".into(), String::new())?,
        ProviderModelEntry::try_new("sonnet".into(), "Sonnet".into(), String::new())?,
    ];
    let context = Arc::new(RouterSessionAppServerContext::new(
        backend.endpoint.clone(),
        ScriptedSessionBackend::actor()?,
        Arc::clone(&backend) as Arc<dyn SessionCommandPort>,
        Arc::clone(&backend) as Arc<dyn SessionEventHub>,
        super::test_support::model_catalog(tokio::sync::watch::channel(catalog).1),
    ));
    let serving = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept client");
        serve_router_session_app_server_connection(stream, context)
            .await
            .expect("serve request");
    });

    let stream = UnixStream::connect(&socket_path).await?;
    let (mut client, _) = client_async("ws://localhost/rpc", stream).await?;
    client
        .send(Message::Text(
            json!({"id":1,"method":"thread/start","params":{
                "cwd":"/tmp","model":""
            }})
            .to_string()
            .into(),
        ))
        .await?;
    let response: Value = serde_json::from_str(client.next().await.ok_or("response")??.to_text()?)?;
    assert_eq!(response["error"]["code"], -32602);
    assert!(
        backend
            .create_commands
            .lock()
            .expect("test lock")
            .is_empty()
    );

    client.close(None).await?;
    serving.await?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Contract asserts; setup may fail."
)]
async fn turn_model_uses_only_provider_advertised_choices() -> Result<(), Box<dyn std::error::Error>>
{
    let catalog = vec![
        ProviderModelEntry::try_new("default".into(), "Default".into(), String::new())?,
        ProviderModelEntry::try_new("sonnet".into(), "Sonnet".into(), String::new())?,
    ];
    for requested_model in ["gpt-6-luna", "provider-default", "sonnet"] {
        let expected_override = (requested_model == "sonnet").then_some("sonnet");
        let backend = ScriptedSessionBackend::new()?;
        let actor = ScriptedSessionBackend::actor()?;
        let started = handle_app_server_thread_request(
            "thread/start",
            json!({"cwd":"/tmp","model":"provider-default"}),
            Arc::clone(&backend) as Arc<dyn SessionCommandPort>,
            Arc::clone(&backend) as Arc<dyn SessionEventHub>,
            backend.endpoint.clone(),
            actor.clone(),
            &catalog,
        )
        .await?;
        let turn = handle_app_server_turn_request(
            "turn/start",
            session_event_model::InputId::generate(),
            json!({"threadId":started["thread"]["id"],
                "model":requested_model,
                "input":[{"type":"text","text":"hello"}]}),
            Arc::clone(&backend) as Arc<dyn SessionCommandPort>,
            Arc::clone(&backend) as Arc<dyn SessionEventHub>,
            backend.endpoint.clone(),
            actor,
            &catalog,
        )
        .await;
        if requested_model == "gpt-6-luna" {
            assert!(matches!(turn, Err(ThreadMethodError::InvalidParams)));
            assert!(
                backend
                    .setting_commands
                    .lock()
                    .expect("test lock")
                    .is_empty()
            );
            assert!(
                backend
                    .prompt_commands
                    .lock()
                    .expect("test lock")
                    .is_empty()
            );
            continue;
        }
        let turn = turn?;
        assert_eq!(turn["turn"]["status"], "inProgress");
        let settings = backend.setting_commands.lock().expect("test lock");
        assert_eq!(settings.len(), usize::from(expected_override.is_some()));
        if let Some(expected) = expected_override {
            assert_eq!(settings[0].setting_id, "model");
            assert_eq!(settings[0].value, expected);
        }
        assert_eq!(backend.prompt_commands.lock().expect("test lock").len(), 1);
    }
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Contract assertions reject wrong effects; fixture errors propagate."
)]
async fn unmapped_native_plan_has_zero_effect_while_default_metadata_keeps_ordinary_chat()
-> Result<(), Box<dyn std::error::Error>> {
    let catalog = vec![ProviderModelEntry::try_new(
        "sonnet".into(),
        "Sonnet".into(),
        String::new(),
    )?];
    for mode in ["plan", "default"] {
        let backend = ScriptedSessionBackend::new()?;
        let actor = ScriptedSessionBackend::actor()?;
        let started = handle_app_server_thread_request(
            "thread/start",
            json!({"cwd":"/tmp"}),
            backend.clone(),
            backend.clone(),
            backend.endpoint.clone(),
            actor.clone(),
            &catalog,
        )
        .await?;
        let result = handle_app_server_turn_request("turn/start",
        session_event_model::InputId::generate(), json!({"threadId":started["thread"]["id"],"model":"sonnet","collaborationMode":{"mode":mode,"settings":{"model":"sonnet","reasoning_effort":"low","developer_instructions":null}},"input":[{"type":"text","text":"ordinary text"}]}), backend.clone(), backend.clone(), backend.endpoint.clone(), actor, &catalog).await;
        if mode == "plan" {
            assert!(matches!(result, Err(ThreadMethodError::Unsupported)));
            assert!(
                backend
                    .setting_commands
                    .lock()
                    .expect("settings")
                    .is_empty()
            );
            assert!(backend.prompt_commands.lock().expect("prompts").is_empty());
        } else {
            assert!(result.is_ok());
            let settings = backend.setting_commands.lock().expect("settings");
            assert_eq!(settings.len(), 2);
            assert_eq!(settings[0].setting_id, "model");
            assert_eq!(settings[0].value, "sonnet");
            assert_eq!(settings[1].setting_id, "effort");
            assert_eq!(settings[1].value, "low");
            assert_eq!(backend.prompt_commands.lock().expect("prompts").len(), 1);
        }
    }
    Ok(())
}

struct RefusingModelCatalog(crate::SessionModelCatalogError);

impl crate::SessionModelCatalog for RefusingModelCatalog {
    fn read_models(
        &self,
        _target: SessionRef,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<ProviderModelEntry>, crate::SessionModelCatalogError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(std::future::ready(Err(self.0)))
    }
}

#[tokio::test]
async fn native_model_list_distinguishes_missing_from_unavailable() {
    for (failure, expected) in [
        (
            crate::SessionModelCatalogError::SessionNotFound,
            json!({"id": 2, "error": {"code": -32002, "message": "session model catalogue not found"}}),
        ),
        (
            crate::SessionModelCatalogError::Unavailable,
            json!({"id": 2, "error": {"code": -32000, "message": "session model catalogue unavailable"}}),
        ),
    ] {
        let root = tempfile::tempdir().expect("fixture root");
        let path = root.path().join("catalog.sock");
        let listener = UnixListener::bind(&path).expect("native socket");
        let backend = ScriptedSessionBackend::new().expect("scripted session backend");
        let catalog = Arc::new(RefusingModelCatalog(failure));
        let (sender, mut replies) = mpsc::channel(1);
        let response = super::send_model_list_response(
            json!(2),
            Some(backend.session.clone()),
            catalog.clone(),
            sender,
        );
        assert!(
            futures_util::FutureExt::now_or_never(response).is_some(),
            "the literal catalogue and an empty response lane must settle in one poll"
        );
        let response = replies
            .try_recv()
            .expect("model/list must send its correlated response before completing")
            .expect("handled request");
        assert_eq!(response.response, expected);

        let context = Arc::new(RouterSessionAppServerContext::new(
            backend.endpoint.clone(),
            ScriptedSessionBackend::actor().expect("actor"),
            backend.clone(),
            backend,
            catalog,
        ));
        let (dispatched, mut dispatches) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("native peer");
            super::serve_app_server_connection(stream, context, Some(dispatched)).await
        });
        let (mut client, _) = client_async(
            "ws://localhost/rpc",
            UnixStream::connect(&path).await.expect("native connection"),
        )
        .await
        .expect("native channel");
        let start = json!({"id":1,"method":"thread/start","params":{"cwd":root.path()}});
        client
            .send(Message::Text(start.to_string().into()))
            .await
            .expect("thread/start sent");
        loop {
            let frame = client
                .next()
                .await
                .expect("thread/start reply")
                .expect("frame");
            let value: Value =
                serde_json::from_str(frame.to_text().expect("text frame")).expect("RPC");
            if value["id"] == 1 {
                assert!(value["result"]["thread"]["id"].is_string(), "{value}");
                break;
            }
        }

        for request in [
            json!({"id":2,"method":"model/list","params":{}}),
            json!({"id":3,"method":"skills/list","params":{}}),
        ] {
            client
                .send(Message::Text(request.to_string().into()))
                .await
                .expect("RPC sent");
        }
        let mut catalogue_reply = None;
        loop {
            let frame = client
                .next()
                .await
                .expect("skills/list fence")
                .expect("frame");
            let value: Value =
                serde_json::from_str(frame.to_text().expect("text frame")).expect("RPC");
            match value["id"].as_u64() {
                Some(2) => catalogue_reply = Some(value),
                Some(3) => break,
                _ => {}
            }
        }
        let dispatched = dispatches.try_recv().ok();
        if dispatched != Some(json!(2)) {
            client.close(None).await.expect("close native fixture");
            server.await.expect("server task").expect("server stopped");
            panic!(
                "model/list id 2 must dispatch before the real request loop reaches skills/list"
            );
        }
        let catalogue_reply = match catalogue_reply {
            Some(reply) => reply,
            None => loop {
                let frame = client
                    .next()
                    .await
                    .expect("dispatched native reply")
                    .expect("frame");
                let value: Value =
                    serde_json::from_str(frame.to_text().expect("text frame")).expect("RPC");
                if value["id"] == 2 {
                    break value;
                }
            },
        };
        assert_eq!(catalogue_reply, expected);
        client.close(None).await.expect("native close");
        server.await.expect("server task").expect("server stopped");
    }
}
