use super::*;

#[test]
fn endpoint_startup_reason_preserves_sanitized_failure_metadata() {
    let account_sentinel = "synthetic-account-sentinel@example.invalid";
    let token_sentinel = "synthetic-token-sentinel-7f4e";
    let source_error =
        agent_client_protocol::Error::new(-32001, format!("denied {token_sentinel}"))
            .data(serde_json::json!({"account": account_sentinel, "token": token_sentinel}));
    let runtime_error = crate::ExternalProviderRuntimeError::Initialize(
        acp_client_runtime::sanitized_initialization_error_for_test(&source_error),
    );

    let reason = provider_startup_failure_reason("/fixture/acp-provider", &runtime_error);

    assert!(reason.contains("initialize"));
    assert!(reason.contains("-32001"));
    assert!(reason.contains("error_data_bytes="));
    assert!(!reason.contains(account_sentinel));
    assert!(!reason.contains(token_sentinel));
}

#[tokio::test]
async fn unavailable_provider_start_uses_host_timestamp() {
    let clock = crate::external_provider_runtime::acp_scripted_fixture::manual_clock();
    let identity = new_service_uuid().expect("service ID");
    let disabled = ExternalProviderStartup::unavailable(
        ProviderKind::Cursor,
        "fixture disabled".into(),
        "enable fixture".into(),
    )
    .expect("unavailable startup");
    let startup = compose_provider_startup(
        vec![disabled],
        None,
        &identity,
        &identity,
        "http://127.0.0.1:1/mcp",
        None,
        None,
        clock,
    )
    .await
    .expect("unavailable provider startup");

    let [endpoint] = startup.endpoints.as_slice() else {
        panic!("one unavailable endpoint");
    };
    assert_eq!(
        endpoint.availability,
        EndpointAvailability::Unavailable {
            observed_at: ObservationTimestamp::try_from("2023-11-15T01:39:05.000Z".to_owned())
                .expect("time"),
            reason: NonEmptyText::try_from("fixture disabled".to_owned()).expect("reason"),
            fix: Some(NonEmptyText::try_from("enable fixture".to_owned()).expect("fix")),
        },
        "unavailable inventory uses the Host clock",
    );
}

#[tokio::test]
async fn available_provider_start_and_retirement_use_distinct_host_timestamps() {
    use router_clock::RuntimeClock as _;
    use std::future::Future as _;
    use std::task::{Context, Poll, Waker};

    let root = tempfile::tempdir().expect("fixture root");
    let launch = crate::provider_test_support::AcpFixtureScript::new()
        .expect_request(
            "initialize",
            "initialize",
            serde_json::json!({"protocolVersion": 1}),
        )
        .respond(
            "initialize",
            serde_json::json!({"protocolVersion": 1, "agentCapabilities": {}}),
        )
        .launch();
    let mut binding =
        ExternalProviderLaunchBinding::claude(launch.executable.clone(), launch.arguments.clone())
            .expect("Claude binding");
    binding.launch = launch;
    codex_router_test_support::global_claude_fixture::declare_global_claude_fixture(
        root.path(),
        &mut binding.launch.environment,
    )
    .expect("test-owned Claude name");
    let store = ProviderOperationStore::open(&root.path().join("operations.sqlite"))
        .await
        .expect("store");
    let clock = crate::external_provider_runtime::acp_scripted_fixture::manual_clock();
    let start_at =
        chrono::DateTime::from_timestamp_millis(1_700_012_345_123).expect("start instant");
    clock.set_utc(start_at);
    let identity = new_service_uuid().expect("service ID");
    let startup = compose_provider_startup(
        vec![ExternalProviderStartup::Launch(binding)],
        Some(Arc::new(Mutex::new(store))),
        &identity,
        &identity,
        "http://127.0.0.1:1/mcp",
        None,
        None,
        clock.clone(),
    )
    .await
    .expect("provider startup");
    let [endpoint] = startup.endpoints.as_slice() else {
        panic!("one admitted endpoint");
    };
    let directory = collaboration_runtime::EndpointDirectory::new(identity);
    directory
        .publish(endpoint.clone())
        .expect("publish initial availability");
    let mut retirements = startup.retirements;
    let (retired, description) = retirements.pop().expect("retirement registration");
    let mut publication = Box::pin(publish_provider_retirement(
        retired.clone(),
        description,
        directory.clone(),
        clock.clone(),
    ));
    let first = publication
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    clock
        .advance(std::time::Duration::from_millis(1_234))
        .expect("retirement instant");
    retired.cancel();
    let result = publication
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    drop(publication);
    startup
        .supervisor
        .expect("supervisor")
        .shutdown()
        .await
        .expect("stop provider");

    assert_eq!(
        endpoint.availability,
        EndpointAvailability::Available {
            observed_at: ObservationTimestamp::try_from("2023-11-15T01:39:05.123Z".to_owned())
                .expect("started time")
        },
        "available inventory uses the Host clock",
    );
    assert!(
        first.is_pending(),
        "retirement must await the process retirement signal"
    );
    assert!(
        matches!(result, Poll::Ready(Ok(()))),
        "retirement must publish after the signal"
    );
    let expected_at = ObservationTimestamp::try_from(
        clock
            .utc_now()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    )
    .expect("retired time");
    let actual = directory
        .read_endpoint(&endpoint.endpoint)
        .expect("inventory")
        .expect("retired endpoint");
    assert_eq!(actual.channels, endpoint.channels);
    assert_eq!(
        actual.availability,
        EndpointAvailability::Unavailable {
            observed_at: expected_at,
            reason: NonEmptyText::try_from("provider process retired".to_owned()).expect("reason"),
            fix: Some(
                NonEmptyText::try_from("restart the Host to relaunch the provider".to_owned())
                    .expect("fix")
            ),
        },
        "retirement inventory uses the advanced Host clock",
    );
}
