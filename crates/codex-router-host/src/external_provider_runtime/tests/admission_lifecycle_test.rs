use super::*;

#[tokio::test]
async fn missing_executable_fails_before_runtime_admission() {
    let error = ExternalProviderRuntime::initialize(
        ExternalProviderLaunch {
            persistence_target: acp_client_runtime::ProviderPersistenceTarget::Unspecified,
            executable: PathBuf::from("/definitely/missing/codex-router-acp-provider"),
            arguments: vec![],
            environment: vec![],
        },
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect_err("missing executable must fail");

    assert!(matches!(error, ExternalProviderRuntimeError::Launch(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn stable_v1_initialize_admits_runtime_and_capabilities() {
    let runtime = ExternalProviderRuntime::initialize(
        initialization_fixture(1, None),
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("fixture initializes");

    assert_eq!(
        runtime.admission(),
        &ExternalProviderAdmission {
            runtime_name: Some("fixture-agent".to_owned()),
            runtime_version: Some("1.2.3".to_owned()),
            supports_load: true,
            supports_mcp_http: false,
            supports_steering: false,
        }
    );

    runtime.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn initialize_advertises_exact_supported_client_capabilities() {
    // ACP v1 initialization.mdx:28-54 requires protocolVersion and the
    // supported capabilities; omitted capabilities are unsupported (lines
    // 100-115). The Router supports form elicitation through its Question broker.
    let root = tempfile::tempdir().expect("fixture root");
    let fixture = acp_scripted_fixture::AcpFixtureScript::new()
        .expect_exact_request(
            "initialize",
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "clientCapabilities": {"auth": {"terminal": false}, "elicitation": {"form": {}}},
                "clientInfo": {"name": "codex-router", "version": env!("CARGO_PKG_VERSION")}
            }),
        )
        .respond("initialize", serde_json::json!({"protocolVersion": 1, "agentCapabilities": {}, "agentInfo": {"name": "initialize-fixture", "version": "1"}}))
        .record_diagnostics(root.path().join("fixture-diagnostics.txt"))
        .launch();
    let runtime = ExternalProviderRuntime::initialize_with_timeout(
        fixture,
        Duration::from_secs(2),
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        acp_scripted_fixture::manual_clock(),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "initialize failed: {error}; wire diff: {}",
            std::fs::read_to_string(root.path().join("fixture-diagnostics.txt"))
                .unwrap_or_default()
        )
    });
    runtime.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn capability_report_uses_initialize_and_session_advertisements() {
    // ACP v1 initialization.mdx:100-115 and session-setup.mdx:52-85:
    // omitted features are unsupported; Session responses add modes/options.
    let fixture = acp_scripted_fixture::AcpFixtureScript::new()
        .expect_request(
            "initialize",
            "initialize",
            serde_json::json!({"protocolVersion": 1}),
        )
        .respond(
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "agentCapabilities": {
                    "loadSession": true,
                    "sessionCapabilities": {"list": {}, "resume": {}, "close": {}},
                    "promptCapabilities": {"image": true, "audio": false, "embeddedContext": true}
                },
                "agentInfo": {"name": "capability-fixture", "version": "1"}
            }),
        )
        .expect_request("create", "session/new", serde_json::json!({}))
        .respond(
            "create",
            serde_json::json!({
                "sessionId": "fixture-session",
                "modes": {"currentModeId": "ask", "availableModes": [{"id": "ask", "name": "Ask"}]},
                "configOptions": [],
                "_meta": {"steering": {"supported": true}}
            }),
        )
        .launch();
    let runtime = ExternalProviderRuntime::initialize(
        fixture,
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("fixture initializes");
    let before = runtime.capability_report("fixture-session").await;
    assert!(before.supports_load);
    assert!(before.supports_resume);
    assert!(before.supports_close);
    assert!(before.supports_list);
    assert!(before.router_queue);
    assert!(before.supports_cancel_queued);
    assert!(before.accepts_image);
    assert!(!before.accepts_audio);
    assert!(before.accepts_embedded_resource);
    assert!(!before.supports_steering);
    assert!(!before.supports_modes);
    assert!(!before.supports_config_options);
    assert!(before.supports_elicitation);
    runtime
        .create_session(PathBuf::from("/tmp"))
        .await
        .expect("session created");
    let after = runtime.capability_report("fixture-session").await;
    assert!(after.supports_steering);
    assert!(after.supports_modes);
    assert!(after.supports_config_options);
    runtime.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn text_prompt_reaches_agent_without_optional_prompt_capabilities() {
    // ACP v1 initialization.mdx:202-217 requires text and resource links as
    // baseline prompt content even when promptCapabilities is omitted.
    let fixture = acp_scripted_fixture::AcpFixtureScript::new()
        .expect_request(
            "initialize",
            "initialize",
            serde_json::json!({"protocolVersion": 1}),
        )
        .respond(
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "agentCapabilities": {},
                "agentInfo": {"name": "text-only-fixture", "version": "1"}
            }),
        )
        .expect_request("create", "session/new", serde_json::json!({}))
        .respond(
            "create",
            serde_json::json!({"sessionId": "text-only-session"}),
        )
        .expect_request(
            "prompt",
            "session/prompt",
            serde_json::json!({
                "sessionId": "text-only-session",
                "prompt": [{"type": "text", "text": "baseline text"}]
            }),
        )
        .respond("prompt", serde_json::json!({"stopReason": "end_turn"}))
        .launch();
    let runtime = ExternalProviderRuntime::initialize(
        fixture,
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("fixture initializes");
    let session_id = runtime
        .create_session(PathBuf::from("/tmp"))
        .await
        .expect("session created");
    let outcome = runtime
        .prompt(session_id, "baseline text".to_owned())
        .await
        .expect("baseline text prompt completes");
    assert_eq!(outcome.stop_reason, ProviderPromptStopReason::EndTurn);
    runtime.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn configured_router_mcp_is_injected_into_create_and_load_requests() {
    let create_runtime = ExternalProviderRuntime::initialize_with_mcp_http(
        mcp_session_setup_fixture("session/new"),
        acp_client_runtime::ProviderModelPicker::Standard,
        "router-collaboration",
        "http://127.0.0.1:19090/mcp",
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("create fixture initializes");
    assert!(create_runtime.admission().supports_mcp_http);
    assert_eq!(
        create_runtime
            .create_session(PathBuf::from("/tmp"))
            .await
            .expect("create with MCP binding"),
        "fixture-session"
    );
    create_runtime.shutdown().await;

    let load_runtime = ExternalProviderRuntime::initialize_with_mcp_http(
        mcp_session_setup_fixture("session/load"),
        acp_client_runtime::ProviderModelPicker::Standard,
        "router-collaboration",
        "http://127.0.0.1:19090/mcp",
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("load fixture initializes");
    assert!(load_runtime.admission().supports_mcp_http);
    load_runtime
        .load_session("fixture-session".to_owned(), PathBuf::from("/tmp"))
        .await
        .expect("load with MCP binding");
    load_runtime.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn unsupported_protocol_version_is_rejected() {
    let fixture_root = tempfile::tempdir().expect("fixture root");
    let process_id_path = fixture_root.path().join("provider.pid");
    let error = ExternalProviderRuntime::initialize(
        initialization_fixture(0, Some(&process_id_path)),
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect_err("v0 must not be admitted");

    assert_eq!(
        error.to_string(),
        "provider selected unsupported ACP protocol version ProtocolVersion(0)"
    );
    assert!(matches!(
        &error,
        ExternalProviderRuntimeError::UnsupportedProtocol {
            actual
        } if *actual == acp_client_runtime::AcpProtocolVersion::new(0)
    ));
    assert_process_reaped(&process_id_path);
}

#[cfg(unix)]
#[tokio::test]
async fn clean_eof_before_initialize_is_a_typed_failure() {
    let error = ExternalProviderRuntime::initialize(
        ExternalProviderLaunch {
            persistence_target: acp_client_runtime::ProviderPersistenceTarget::Unspecified,
            executable: PathBuf::from("/bin/sh"),
            arguments: vec!["-c".to_owned(), "exit 0".to_owned()],
            environment: vec![],
        },
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect_err("EOF must fail initialization");

    assert!(matches!(error, ExternalProviderRuntimeError::Initialize(_)));
}

#[cfg(unix)]
async fn assert_owned_provider_reaped(
    process_id_path: &std::path::Path,
    reaping: CancellationToken,
    settled: CancellationToken,
) {
    let completed = tokio::select! {
        biased;
        () = reaping.cancelled() => true,
        () = settled.cancelled() => false,
    };
    let process_id = fixture_process_id(process_id_path);
    let reaped = matches!(
        rustix::process::test_kill_process(process_id),
        Err(rustix::io::Errno::SRCH)
    );
    if !reaped {
        let _cleanup = rustix::process::kill_process(process_id, rustix::process::Signal::KILL);
    }
    settled.cancelled().await;
    assert!(
        completed && reaped,
        "the owned provider must be reaped before draining stderr"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn stdout_eof_after_initialize_retires_live_provider() {
    let fixture_root = tempfile::tempdir().expect("fixture root");
    let process_id_path = fixture_root.path().join("provider.pid");
    use std::io::Write as _;
    let control_path = fixture_root.path().join("close-stdout.sock");
    let control =
        std::os::unix::net::UnixListener::bind(&control_path).expect("stdout control socket");
    control
        .set_nonblocking(true)
        .expect("nonblocking stdout control");
    let fixture = acp_scripted_fixture::launch_fixture_agent(
        "host-failing-initialization-agent.ts",
        vec![
            "eof".to_owned(),
            process_id_path.to_string_lossy().into_owned(),
            control_path.to_string_lossy().into_owned(),
        ],
    );
    let runtime = ExternalProviderRuntime::initialize(
        fixture,
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("fixture initializes before stdout closes");
    let accepted = control.accept();
    let mut fixture = match accepted {
        Ok((fixture, _)) => fixture,
        Err(error) => {
            runtime.shutdown().await;
            panic!("fixture must connect stdout control before its initialize reply: {error}");
        }
    };
    fixture
        .write_all(b"c")
        .expect("command fixture to close stdout");

    drop(fixture);
    let (_, settled) = runtime.client.connection_fixture_signals();
    let reaping = runtime.client.provider_reaping_finished_for_test();
    assert_owned_provider_reaped(&process_id_path, reaping, settled).await;
    assert!(
        runtime.retirement().is_cancelled(),
        "stdout EOF must retire the connection"
    );
    assert_process_reaped(&process_id_path);
}

#[cfg(unix)]
async fn initialize_admission_with_clock(
    root: &std::path::Path,
    fixture: ExternalProviderLaunch,
    clock: Arc<router_clock::ManualClock>,
) -> Result<ExternalProviderRuntime, ExternalProviderRuntimeError> {
    let store = Arc::new(tokio::sync::Mutex::new(
        switchboard_session_store::ProviderOperationStore::open(&root.join("operations.sqlite"))
            .await
            .expect("isolated operation store"),
    ));
    let endpoint = serde_json::from_value(serde_json::json!({
        "serviceId":"019f0000-0000-7000-8000-000000000001",
        "endpointId":"cursor-local",
    }))
    .expect("fixture endpoint");
    ExternalProviderRuntime::initialize_with_fixture_clock(
        fixture,
        Arc::new(acp_provider_adapters::StandardAcpBehavior),
        Arc::new(switchboard_session_events::ProviderSessionEventHub::new(
            store,
            clock.clone(),
        )),
        endpoint,
        clock,
    )
    .await
}

#[cfg(unix)]
#[tokio::test]
async fn held_initialize_times_out_and_reaps_owned_process() {
    let fixture_root = tempfile::tempdir().expect("fixture root");
    let process_id_path = fixture_root.path().join("provider.pid");
    let observed_path = fixture_root.path().join("init.sock");
    let observed = tokio::net::UnixListener::bind(&observed_path).expect("initialize witness");
    let fixture = acp_scripted_fixture::launch_fixture_agent(
        "host-failing-initialization-agent.ts",
        vec![
            "held".to_owned(),
            process_id_path.to_string_lossy().into_owned(),
            observed_path.to_string_lossy().into_owned(),
        ],
    );
    let clock = Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let mut initialization = Box::pin(initialize_admission_with_clock(
        fixture_root.path(),
        fixture,
        clock.clone(),
    ));
    // The child observes both connection fences before withholding initialize.
    let held = tokio::select! {
        biased;
        observed = observed.accept() => observed.ok(),
        result = &mut initialization => {
            drop(initialization);
            if let Ok(runtime) = result {
                runtime.shutdown().await;
            }
            assert_process_reaped(&process_id_path);
            panic!("the provider never observed initialize before its dispatch fences");
        }
    };
    let budget = Duration::from_secs(15);
    let deadline_armed =
        clock.has_not_due_check_now(router_clock::ClockInstant::ZERO.after(budget));
    if held.is_none() || !deadline_armed {
        drop(held);
        let process_id = fixture_process_id(&process_id_path);
        rustix::process::kill_process(process_id, rustix::process::Signal::KILL)
            .expect("terminate the held fixture");
        if let Ok(runtime) = initialization.await {
            runtime.shutdown().await;
        }
        assert_process_reaped(&process_id_path);
        panic!("held initialize must arm the 15-second Host deadline");
    }
    drop(held);
    clock
        .advance(budget)
        .expect("expire the initialization deadline");
    let error = initialization
        .await
        .expect_err("held initialize must time out");

    assert!(matches!(
        error,
        ExternalProviderRuntimeError::InitializeTimeout
    ));
    assert_process_reaped(&process_id_path);
}

#[cfg(unix)]
#[tokio::test]
async fn oversized_initialize_frame_is_rejected_and_reaps_owned_process() {
    let fixture_root = tempfile::tempdir().expect("fixture root");
    let process_id_path = fixture_root.path().join("provider.pid");
    let observed_path = fixture_root.path().join("init.sock");
    let observed = tokio::net::UnixListener::bind(&observed_path).expect("oversize witness");
    let fixture = acp_scripted_fixture::launch_fixture_agent(
        "host-failing-initialization-agent.ts",
        vec![
            "oversized".to_owned(),
            process_id_path.to_string_lossy().into_owned(),
            observed_path.to_string_lossy().into_owned(),
            MAX_ACP_FRAME_BYTES.to_string(),
        ],
    );
    let clock = Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let before = acp_client_runtime::provider_frame_limit_exceeded_count_for_test();
    let initialization = initialize_admission_with_clock(fixture_root.path(), fixture, clock);
    tokio::pin!(initialization);
    tokio::select! {
        biased;
        result = observed.accept() => { result.expect("provider read initialize before writing the frame"); }
        result = &mut initialization => panic!("initialization ended before the frame witness: {result:?}"),
    }
    let error = initialization
        .await
        .expect_err("oversized frame must fail initialization");
    assert!(
        matches!(error, ExternalProviderRuntimeError::Initialize(_)),
        "expected oversized frame rejection, got {error:?}"
    );
    assert_eq!(
        acp_client_runtime::provider_frame_limit_exceeded_count_for_test(),
        before + 1,
        "the incoming codec rejected exactly one oversized frame"
    );
    assert_process_reaped(&process_id_path);
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_admitted_runtime_reaps_owned_process() {
    let fixture_root = tempfile::tempdir().expect("fixture root");
    let process_id_path = fixture_root.path().join("provider.pid");
    let runtime = ExternalProviderRuntime::initialize(
        initialization_fixture(1, Some(&process_id_path)),
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("fixture initializes");

    let (shutdown_requested, settled) = runtime.client.connection_fixture_signals();
    let reaping = runtime.client.provider_reaping_finished_for_test();
    drop(runtime);
    assert!(
        shutdown_requested.is_cancelled(),
        "dropping the runtime must request shutdown"
    );
    assert_owned_provider_reaped(&process_id_path, reaping, settled).await;
}

#[cfg(unix)]
#[tokio::test]
async fn host_owned_connection_creates_and_prompts_through_official_sdk() {
    let runtime = ExternalProviderRuntime::initialize(
        conversation_fixture(),
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("fixture initializes");

    let provider_session_id = runtime
        .create_session(PathBuf::from("/tmp"))
        .await
        .expect("session created");
    assert_eq!(provider_session_id, "fixture-session");
    let output = runtime
        .prompt(provider_session_id, "hello".to_owned())
        .await
        .expect("prompt settled");
    assert!(output.output.is_empty());
    assert_eq!(output.stop_reason, ProviderPromptStopReason::EndTurn);

    runtime.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_cancel_remains_responsive_while_prompt_is_active() {
    let runtime = ExternalProviderRuntime::initialize(
        cancellation_fixture(),
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("fixture initializes");
    runtime
        .create_session(PathBuf::from("/tmp"))
        .await
        .expect("session created");

    {
        let (dispatch, taken) = tokio::sync::oneshot::channel();
        let prompt = runtime.prompt_for_operation(
            "fixture-session".to_owned(),
            None,
            "hold".to_owned(),
            Some(dispatch),
        );
        tokio::pin!(prompt);
        tokio::select! {
            biased;
            result = &mut prompt => panic!("prompt settled before cancellation: {result:?}"),
            observed = taken => assert_eq!(observed.expect("prompt dispatch observation"), ProviderPromptDispatchObservation::Submitted),
        }
        runtime
            .cancel_active_prompt("fixture-session".to_owned())
            .await
            .expect("cancel notification accepted");
        let outcome = prompt.await.expect("cancelled prompt settles");
        assert!(outcome.output.is_empty());
        assert_eq!(outcome.stop_reason, ProviderPromptStopReason::Cancelled);
    }

    runtime.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn managed_permission_refuses_once_without_payload_retention() {
    let runtime = ExternalProviderRuntime::initialize(
        permission_request_fixture(),
        std::sync::Arc::new(acp_provider_adapters::StandardAcpBehavior),
        crate::external_provider_runtime::acp_scripted_fixture::manual_clock(),
    )
    .await
    .expect("fixture initializes");
    runtime.set_endpoint_id("cursor-local".to_owned()).await;
    runtime
        .create_session(PathBuf::from("/tmp"))
        .await
        .expect("session created");

    let outcome = runtime
        .prompt(
            "fixture-session".to_owned(),
            "request permission".to_owned(),
        )
        .await
        .expect("prompt settles after permission cancellation");
    assert_eq!(outcome.stop_reason, ProviderPromptStopReason::EndTurn);
    assert_eq!(
        outcome.permission_refusal_reason,
        Some(ExternalProviderApprovalRefusalReason::Managed(
            session_event_model::managed_interaction_refusal::ManagedInteractionRefusal::ApprovalsUnavailableOnBuild,
        ))
    );
    assert_eq!(
        runtime.approval_refusal_warnings(),
        vec![ExternalProviderApprovalRefusalWarning {
            endpoint: "cursor-local".to_owned(),
            provider_session_id: "fixture-session".to_owned(),
            method: "session/request_permission",
            reason_code: ExternalProviderApprovalRefusalReason::Managed(
            session_event_model::managed_interaction_refusal::ManagedInteractionRefusal::ApprovalsUnavailableOnBuild,
        ),
        }]
    );
    assert_eq!(
        runtime.permission_observation(),
        ExternalProviderPermissionObservation {
            method: "session/request_permission",
            request_count: 1,
            last_outcome: Some(ExternalProviderPermissionOutcome::Cancelled),
            execute_tool_call_count: 0,
        }
    );

    runtime.shutdown().await;
}
