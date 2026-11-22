//! A bounded observation streams each event to its caller while the call is still open, on
//! both listeners, then ends at its bound with every event in its result and the cursor a
//! later call resumes from without losing an event.
use crate::api_test_harness::{
    ServedApi, TEST_PROTOCOL_VERSION, TEST_SERVICE_ID, api_config, test_identity,
};
use collaboration_protocol::{
    EndpointDescription, OBSERVATION_EVENT_NOTIFICATION, ProviderRequestedPolicy,
    ProviderWorkingDirectory, RouterAccess, SessionRef,
};
use collaboration_runtime::ProviderOperationStore;
use collaboration_runtime::ProviderSessionEventHub;
use collaboration_runtime::ProviderSessionRecord;
use collaboration_service::ApplicationFixture;
use rmcp::{
    ClientHandler, RoleClient,
    model::{CallToolRequestParams, CustomNotification},
    service::NotificationContext,
};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};

/// An MCP client that hands the test each observation event notification it receives.
struct ObservationEvents(mpsc::UnboundedSender<Value>);

impl ClientHandler for ObservationEvents {
    async fn on_custom_notification(
        &self,
        notification: CustomNotification,
        _context: NotificationContext<RoleClient>,
    ) {
        if notification.method == OBSERVATION_EVENT_NOTIFICATION {
            let _received = self.0.send(notification.params.unwrap_or(Value::Null));
        }
    }
}

fn private_directory() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt as _;
    // Unix socket paths are short; the system temporary directory can exceed the limit.
    let directory = tempfile::tempdir_in("/tmp").expect("private test directory");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private directory permissions");
    directory
}

fn provider_target(session_id: &str) -> SessionRef {
    serde_json::from_value(json!({
        "endpoint": {"serviceId": TEST_SERVICE_ID, "endpointId": "claude-local"},
        "sessionId": session_id
    }))
    .expect("provider target")
}

/// A Router with one provider endpoint whose Session hub records `sessions`.
async fn provider_router(
    directory: &Path,
    sessions: &[&SessionRef],
) -> (ApplicationFixture, Arc<ProviderSessionEventHub>) {
    let owner: SessionRef = serde_json::from_value(json!({
        "endpoint": {"serviceId": TEST_SERVICE_ID, "endpointId": "codex-local"},
        "sessionId": "owner"
    }))
    .expect("owner");
    let store = Arc::new(Mutex::new(
        ProviderOperationStore::open(&directory.join("operations.sqlite"))
            .await
            .expect("provider operation store"),
    ));
    for &session in sessions {
        store
            .lock()
            .await
            .record_session(&ProviderSessionRecord {
                target: session.clone(),
                working_directory: ProviderWorkingDirectory::try_from(
                    directory.display().to_string(),
                )
                .expect("working directory"),
                requested_policy: ProviderRequestedPolicy {
                    access: RouterAccess::WriteRestricted,
                },
                created_by: owner.clone().into(),
                approver: owner.clone().into(),
                updated_at_ms: 1,
                resume: Default::default(),
            })
            .await
            .expect("session record");
    }
    let hub = Arc::new(ProviderSessionEventHub::with_capacity(store, 16));
    let identity = test_identity().with_provider_session_hub(hub.clone());
    let description: EndpointDescription = serde_json::from_value(json!({
        "endpoint": {"serviceId": TEST_SERVICE_ID, "endpointId": "claude-local"},
        "label": "Claude fixture",
        "availability": {"state": "available", "observedAt": "2026-09-26T00:00:00Z"},
        "channels": [{"kind": "externalProvider", "transport": "stdioAcp",
            "bindingId": "fixture-binding", "bindingGeneration": 7,
            "runtime": {"provider": "claudeCode", "runtimeName": "fixture"},
            "capabilities": [{"name": "create", "status": "supported", "evidence": "advertised"}]}]
    }))
    .expect("provider endpoint");
    identity
        .endpoint_directory()
        .publish(description)
        .expect("endpoint publication");
    (identity, hub)
}

async fn publish(hub: &ProviderSessionEventHub, target: &SessionRef, item_id: &str) {
    publish_text(hub, target, item_id, item_id).await;
}

async fn publish_text(
    hub: &ProviderSessionEventHub,
    target: &SessionRef,
    item_id: &str,
    text: &str,
) {
    let session = serde_json::from_value(serde_json::to_value(target).expect("target JSON"))
        .expect("hub target");
    let event = serde_json::from_value(json!({
        "kind": "itemStarted",
        "item": {"itemId": item_id, "kind": {"kind": "agentMessage"}, "text": text}
    }))
    .expect("session event");
    hub.publish(session, event).await.expect("hub event");
}

async fn next_streamed(streamed: &mut mpsc::UnboundedReceiver<Value>) -> Value {
    streamed.recv().await.expect("notification channel open")
}

// Bound silence by cooperative scheduler turns. Advancing the same clock on exhaustion
// prevents a missing notification or missing clock registration from owning the test.
async fn observation_progress<Output>(
    clock: &ManualClock,
    stage: &str,
    signal: impl std::future::Future<Output = Output>,
) -> Output {
    tokio::select! {
        biased;
        value = signal => value,
        () = async {
            for _ in 0..100_000 {
                tokio::task::yield_now().await;
            }
            clock.advance(Duration::from_secs(31)).expect("advance the observation bound");
        } => panic!("observation made no progress: {stage}"),
    }
}

fn observe(
    target: &SessionRef,
    timeout_seconds: u64,
    cursor: Option<&Value>,
) -> CallToolRequestParams {
    let mut arguments = json!({
        "target": target, "timeoutSeconds": timeout_seconds,
        "maxEvents": 2, "maxBytes": 262_144
    });
    if let Some(cursor) = cursor {
        arguments["afterSequence"] = cursor["afterSequence"].clone();
        arguments["epoch"] = cursor["epoch"].clone();
    }
    CallToolRequestParams::new("events_observe").with_arguments(
        arguments
            .as_object()
            .expect("observation arguments")
            .clone(),
    )
}

#[tokio::test]
async fn a_bounded_observation_streams_each_event_while_the_call_is_open_on_both_listeners() {
    // Arrange: one provider Session per listener, each with one event already retained.
    let directory = private_directory();
    let over_tcp = provider_target("observed-over-tcp");
    let over_unix = provider_target("observed-over-unix");
    let (identity, hub) = provider_router(directory.path(), &[&over_tcp, &over_unix]).await;
    let clock = Arc::new(ManualClock::new(Default::default()));
    let config = api_config(identity.with_clock(clock.clone()).build(), directory.path());
    let tcp = ServedApi::tcp(&config).await;
    let unix = ServedApi::unix(&config, &directory.path().join("api.sock"));

    for (api, target) in [(&tcp, &over_tcp), (&unix, &over_unix)] {
        publish(&hub, target, "retained").await;
        let (events, mut streamed) = mpsc::unbounded_channel();
        let client = api.client_with(ObservationEvents(events)).await;
        let peer = client.peer().clone();

        // Act: a call bounded at two events and thirty seconds.
        let mut first_call = {
            let peer = peer.clone();
            let request = observe(target, 30, None);
            tokio::spawn(async move { peer.call_tool(request).await })
        };
        let (retained, cut) = observation_progress(&clock, "kept record and cut", async {
            tokio::select! {
                outcome = &mut first_call => panic!("call answered before kept record and cut: {outcome:?}"),
                records = async {
                    let retained = next_streamed(&mut streamed).await;
                    let cut = next_streamed(&mut streamed).await;
                    (retained, cut)
                } => records,
            }
        }).await;
        let still_open = !first_call.is_finished();
        publish(&hub, target, "live").await;
        let first = observation_progress(&clock, "first result", first_call)
            .await
            .expect("first call joins")
            .expect("first call answers");
        let live =
            observation_progress(&clock, "live notification", next_streamed(&mut streamed)).await;
        // Published between the calls; the second call resumes from the first's cursor.
        publish(&hub, target, "between-calls").await;
        let next_request = observe(target, 1, Some(&live["cursor"]));
        let mut next_call = tokio::spawn(async move { peer.call_tool(next_request).await });
        let deadline = clock.monotonic_now().after(Duration::from_secs(1));
        let (resumed, resumed_cut) = observation_progress(&clock, "resume notifications and live deadline", async {
            tokio::select! {
                outcome = &mut next_call => panic!("continued call answered before the clock advance: {outcome:?}"),
                records = async {
                    let resumed = next_streamed(&mut streamed).await;
                    let resumed_cut = next_streamed(&mut streamed).await;
                    clock.not_due_check_now(deadline).await;
                    (resumed, resumed_cut)
                } => records,
            }
        }).await;
        clock
            .advance(Duration::from_secs(1))
            .expect("advance past wait");
        let second = observation_progress(&clock, "continued result", next_call)
            .await
            .expect("second call joins")
            .expect("second call answers");

        // Assert: the retained event arrived while the call was open, with its cursor.
        let epoch = first
            .structured_content
            .as_ref()
            .map(|result| result["epoch"].clone())
            .expect("first result");
        assert!(
            still_open,
            "the call ended before its first event was streamed"
        );
        assert_eq!(retained["event"]["sequence"], 1);
        assert_eq!(
            retained["cursor"],
            json!({"epoch": epoch, "afterSequence": 1})
        );
        assert_eq!(
            cut["event"],
            json!({"kind":"snapshotEnd","cursor":retained["cursor"]})
        );
        assert_eq!(cut["cursor"], retained["cursor"]);
        assert_eq!(live["cursor"], json!({"epoch": epoch, "afterSequence": 2}));
        // The result preserves the notification order.
        let first = first.structured_content.expect("first result");
        assert_eq!(first["endReason"], "resultLimitReached");
        assert_eq!(
            first["events"],
            json!([retained["event"], cut["event"], live["event"]])
        );
        // The second call lost nothing between calls and ended at its deadline.
        let second = second.structured_content.expect("second result");
        assert_eq!(resumed["event"]["sequence"], 3);
        assert_eq!(
            resumed_cut["event"],
            json!({"kind":"snapshotEnd","cursor":resumed["cursor"]})
        );
        assert_eq!(
            second["events"],
            json!([resumed["event"], resumed_cut["event"]])
        );
        assert_eq!(second["endReason"], "deadlineReached");
        client.cancel().await.expect("close the client");
    }
    tcp.stop().await;
    unix.stop().await;
}

#[tokio::test]
async fn a_reader_that_stops_reading_neither_keeps_the_call_past_its_bound_nor_holds_its_slot() {
    // Arrange: one request slot on the Unix listener, and far more retained event bytes than
    // the notification channel, the HTTP writer and the socket buffers can hold.
    use futures_util::StreamExt as _;
    use rmcp::{
        model::{
            CallToolRequest, ClientJsonRpcMessage, ClientRequest, NumberOrString,
            ServerJsonRpcMessage, ServerResult,
        },
        transport::{
            UnixSocketHttpClient,
            streamable_http_client::{StreamableHttpClient, StreamableHttpPostResponse},
        },
    };
    let directory = private_directory();
    let target = provider_target("observed-by-a-stalled-reader");
    let (identity, hub) = provider_router(directory.path(), &[&target]).await;
    for index in 0..130 {
        publish_text(&hub, &target, &format!("item-{index}"), &"x".repeat(7_000)).await;
    }
    let clock = Arc::new(ManualClock::new(Default::default()));
    let mut config = api_config(identity.with_clock(clock.clone()).build(), directory.path());
    config.concurrent_requests = 1;
    let socket = directory.path().join("api.sock");
    let api = ServedApi::unix(&config, &socket);
    let observation = json!({
        "target": target, "timeoutSeconds": 1, "maxEvents": 130, "maxBytes": 1_048_576
    });

    // Act: the reader sends the call and then reads nothing.
    let client = UnixSocketHttpClient::new(socket.to_str().expect("socket path"), &api.url());
    let request = ClientJsonRpcMessage::request(
        ClientRequest::CallToolRequest(CallToolRequest::new(
            CallToolRequestParams::new("events_observe")
                .with_arguments(observation.as_object().expect("observe input").clone()),
        )),
        NumberOrString::Number(1),
    );
    let StreamableHttpPostResponse::Sse(mut stalled_reader, _) = observation_progress(
        &clock,
        "initial SSE headers",
        client.post_message(
            Arc::from(api.url()),
            request,
            None,
            None,
            [(
                http::HeaderName::from_static("mcp-protocol-version"),
                http::HeaderValue::from_static(TEST_PROTOCOL_VERSION),
            )]
            .into(),
        ),
    )
    .await
    .expect("send the observation") else {
        panic!("the observation must stream");
    };
    observation_progress(&clock, "drain checked the injected clock", async {
        tokio::select! {
            () = api.join_active_calls() => panic!("the stalled call completed before its drain deadline"),
            () = clock.not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(1))) => {}
        }
    }).await;
    assert_eq!(api.active_calls(), 1);
    clock
        .advance(Duration::from_secs(2))
        .expect("advance past drain");

    // Assert: the call ends at its bound although the reader still has not read.
    observation_progress(
        &clock,
        "stalled handler released its slot",
        api.join_active_calls(),
    )
    .await;
    assert_eq!(api.active_calls(), 0);
    // Assert: the listener's only slot is free for the next caller meanwhile.
    let next = observation_progress(
        &clock,
        "next caller took the slot",
        api.call_tool("endpoints_list", json!({})),
    )
    .await;
    assert_ne!(next.is_error, Some(true), "{next:?}");

    let mut prefix = Vec::new();
    let result = loop {
        let data = observation_progress(
            &clock,
            "stalled reader received the result or its prefix",
            stalled_reader.next(),
        )
        .await
        .expect("SSE frame")
        .expect("SSE")
        .data;
        let Some(data) = data.filter(|data| !data.is_empty()) else {
            continue;
        };
        let message: ServerJsonRpcMessage = serde_json::from_str(&data).expect("server message");
        match message {
            ServerJsonRpcMessage::Response(response) => {
                let ServerResult::CallToolResult(result) = response.result else {
                    panic!("expected observation result")
                };
                break result.structured_content.expect("whole observation");
            }
            ServerJsonRpcMessage::Notification(_) => {
                let params: Value = serde_json::from_str(&data).expect("notification JSON");
                assert_eq!(params["method"], OBSERVATION_EVENT_NOTIFICATION);
                prefix.push(params["params"]["event"].clone());
            }
            ServerJsonRpcMessage::Error(error) => panic!("{error:?}"),
            ServerJsonRpcMessage::Request(request) => panic!("unexpected request: {request:?}"),
        }
    };
    let all = result["events"].as_array().expect("result events");
    assert_eq!(all.len(), 131);
    assert_eq!(all[130]["kind"], "snapshotEnd");
    assert!(!prefix.is_empty() && prefix.len() < all.len());
    assert_eq!(prefix, all[..prefix.len()]);
    api.stop().await;
}

// Rejects another positional record kind or a cursor being added to a native Codex watch.
#[tokio::test]
async fn native_codex_watch_preserves_its_result_and_notification_shape() {
    use futures_util::{SinkExt as _, StreamExt as _};
    use tokio_tungstenite::tungstenite::Message;
    let directory = private_directory();
    let identity = test_identity().with_endpoints(vec![serde_json::from_value(json!({
        "endpoint":{"serviceId":TEST_SERVICE_ID,"endpointId":"codex-local"},
        "label":"Codex fixture",
        "availability":{"state":"available","observedAt":"2026-11-21T00:00:00Z"},
        "channels":[{"kind":"nativeCodex","transport":"unixWebSocket","path":"native.sock",
            "generation":{"serviceEpoch":crate::api_test_harness::TEST_SERVICE_EPOCH,"generation":1}}]
    })).unwrap()]).unwrap();
    let native = tokio::net::UnixListener::bind(directory.path().join("native.sock")).unwrap();
    let event = json!({"method":"item/agentMessage/delta","params":{
        "threadId":"native-thread","turnId":"native-turn","delta":"fixture delta"
    }});
    let sent = event.clone();
    let peer = tokio::spawn(async move {
        let (stream, _) = native.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let initialize: Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(initialize["method"], "initialize");
        socket
            .send(Message::Text(
                json!({"id":initialize["id"],"result":{"userAgent":"fixture"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let initialized: Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(initialized["method"], "initialized");
        let resume: Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(resume["method"], "thread/resume");
        socket
            .send(Message::Text(sent.to_string().into()))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"id":resume["id"],"result":{"thread":{"id":"native-thread"}}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        while let Some(Ok(frame)) = socket.next().await {
            assert!(
                !frame.is_text(),
                "watch wrote an unexpected frame: {frame:?}"
            );
        }
    });
    let config = api_config(identity.build(), directory.path());
    let api = ServedApi::unix(&config, &directory.path().join("api.sock"));
    let (events, mut streamed) = mpsc::unbounded_channel();
    let client = api.client_with(ObservationEvents(events)).await;
    let args = json!({"target":{"endpoint":{"serviceId":TEST_SERVICE_ID,"endpointId":"codex-local"},
        "sessionId":"native-thread"},"timeoutSeconds":30,"maxEvents":1,"maxBytes":4096});

    let result = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("events_observe")
                .with_arguments(args.as_object().unwrap().clone()),
        )
        .await
        .unwrap()
        .structured_content
        .unwrap();
    let notification = next_streamed(&mut streamed).await;

    assert_eq!(result["events"], json!([event]));
    assert!(result.get("epoch").is_none());
    assert_eq!(notification, json!({"event":event,"cursor":null}));
    let shared: Value = serde_json::from_str(include_str!(
        "mcp_server/snapshots/main_success_schemas.json"
    ))
    .expect("shared tool result shapes");
    jsonschema::validator_for(&shared["events_observe"])
        .expect("shared observation validator")
        .validate(&result)
        .expect("native result still fits the published shape");
    let native_notifications =
        schemars::schema_for!(collaboration_protocol::ObservationEventNotification);
    let native_notifications =
        serde_json::to_value(native_notifications).expect("notification shape");
    jsonschema::validator_for(&native_notifications)
        .expect("shared notification validator")
        .validate(&notification)
        .expect("native notification still fits the shared shape");
    assert!(streamed.try_recv().is_err());
    peer.await.unwrap();
    client.cancel().await.unwrap();
    api.stop().await;
}

// A continuation can have the current Host identity and a valid sequence while naming an old stream.
#[tokio::test]
async fn an_earlier_stream_is_refused_on_the_current_host() {
    let directory = private_directory();
    let target = provider_target("reset");
    let (identity, hub) = provider_router(directory.path(), &[&target]).await;
    publish(&hub, &target, "old-record").await;
    let clock = Arc::new(ManualClock::new(Default::default()));
    let config = api_config(identity.with_clock(clock.clone()).build(), directory.path());
    let api = ServedApi::unix(&config, &directory.path().join("api.sock"));
    let (events, mut streamed) = mpsc::unbounded_channel();
    let client = api.client_with(ObservationEvents(events)).await;
    let args = json!({"target":target,"timeoutSeconds":1,"maxEvents":1,"maxBytes":4096});
    let first = observation_progress(
        &clock,
        "obtain the old position",
        client.peer().call_tool(
            CallToolRequestParams::new("events_observe")
                .with_arguments(args.as_object().unwrap().clone()),
        ),
    )
    .await
    .unwrap()
    .structured_content
    .unwrap();
    let old_cut = observation_progress(&clock, "old record and cut", async {
        next_streamed(&mut streamed).await;
        next_streamed(&mut streamed).await
    })
    .await;
    assert_eq!(
        old_cut["cursor"],
        json!({"epoch":first["epoch"],"afterSequence":1})
    );

    let session = serde_json::from_value(serde_json::to_value(&target).unwrap()).unwrap();
    let current_epoch = hub.begin_history_replay(session).await.unwrap();
    publish(&hub, &target, "fresh-record-one").await;
    publish(&hub, &target, "fresh-record-two").await;
    let mut guarded = args;
    guarded["epoch"] = old_cut["cursor"]["epoch"].clone();
    guarded["afterSequence"] = old_cut["cursor"]["afterSequence"].clone();
    guarded["serviceEpoch"] = first["generation"]["serviceEpoch"].clone();

    let refused = observation_progress(
        &clock,
        "refuse an older stream",
        client.peer().call_tool(
            CallToolRequestParams::new("events_observe")
                .with_arguments(guarded.as_object().unwrap().clone()),
        ),
    )
    .await
    .unwrap()
    .structured_content
    .unwrap();
    let note = observation_progress(
        &clock,
        "bare gap notification",
        next_streamed(&mut streamed),
    )
    .await;

    assert_eq!(
        refused["generation"]["serviceEpoch"],
        first["generation"]["serviceEpoch"]
    );
    assert_eq!(refused["epoch"], current_epoch);
    assert_eq!(refused["endReason"], "resyncRequired");
    assert_eq!(refused["events"], json!([{"kind":"resyncRequired"}]));
    assert_eq!(
        note,
        json!({"event":{"kind":"resyncRequired"},"cursor":null})
    );
    assert!(streamed.try_recv().is_err());
    client.cancel().await.unwrap();
    api.stop().await;
}

// A Host identity requires both position fields, even when the other position would be valid.
#[tokio::test]
async fn partial_guarded_positions_are_invalid_parameters() {
    let directory = private_directory();
    let target = provider_target("partial");
    let (identity, hub) = provider_router(directory.path(), &[&target]).await;
    publish(&hub, &target, "kept").await;
    let config = api_config(identity.build(), directory.path());
    let api = ServedApi::unix(&config, &directory.path().join("api.sock"));
    for position in [json!({}), json!({"epoch":0}), json!({"afterSequence":0})] {
        let mut args = json!({"target":target,"timeoutSeconds":1,"maxEvents":1,"maxBytes":4096,
            "serviceEpoch":crate::api_test_harness::TEST_SERVICE_EPOCH});
        args.as_object_mut()
            .unwrap()
            .extend(position.as_object().unwrap().clone());
        let result = api.call_tool("events_observe", args).await;
        assert_eq!(result.is_error, Some(true));
        let refusal = result.structured_content.unwrap();
        assert_eq!(refusal["kind"], "rejected");
        assert_eq!(refusal["serviceKind"], "invalidField");
        assert_eq!(refusal["code"], -32602);
        assert_eq!(refusal["effect"], "none");
    }
    api.stop().await;
}
