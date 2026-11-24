use std::process::Command;
#[test]
fn missing_service_is_a_machine_readable_unavailable_result() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-collaboration"))
        .args([
            "endpoints",
            "list",
            "--json",
            "--service-directory",
            "/tmp/nonexistent-agent-service-proof/missing",
        ])
        .output()
        .unwrap_or_else(|e| panic!("command: {e}"));
    assert_eq!(output.status.code(), Some(3));
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).unwrap_or_else(|e| panic!("JSON: {e}"));
    assert_eq!(value["kind"], "error");
    assert_eq!(value["error"]["kind"], "unavailable");
    assert!(output.stderr.is_empty());
}

#[tokio::test]
async fn executable_discovers_an_isolated_published_service() {
    use collaboration_service::ApplicationFixture;
    use std::os::unix::fs::DirBuilderExt;
    let root = std::path::PathBuf::from(format!("/tmp/endpoint-cli-{}", std::process::id()));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .unwrap_or_else(|e| panic!("directory: {e}"));
    let identity = ApplicationFixture::new(
        "00000000-0000-4000-8000-000000000001",
        "00000000-0000-4000-8000-000000000002",
    )
    .unwrap_or_else(|e| panic!("identity: {e}"));
    let endpoints = identity.endpoint_directory();
    let served =
        collaboration_mcp::test_support::ServedCollaborationApi::start(&root, (identity).build())
            .unwrap_or_else(|e| panic!("serve: {e}"));
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-collaboration"))
        .args(["endpoints", "list", "--json", "--service-directory"])
        .arg(&root)
        .output()
        .await
        .unwrap_or_else(|e| panic!("command: {e}"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&output.stdout).unwrap_or_else(|e| panic!("JSON: {e}"));
    assert_eq!(result["kind"], "result");
    assert_eq!(result["result"]["page"]["records"], serde_json::json!([]));
    assert!(output.stderr.is_empty());
    let endpoint = serde_json::from_value(serde_json::json!({
        "endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"codex-local"},
        "label":"Local fixture", "availability":{"state":"unavailable","observedAt":"2026-09-05T12:00:00Z","reason":"No runtime"},
        "channels":[{"kind":"nativeCodex","transport":"unixWebSocket","path":"codex-native.sock","schemaDigest":null,"generation":null}]
    })).unwrap_or_else(|error| panic!("endpoint: {error}"));
    endpoints
        .publish(endpoint)
        .unwrap_or_else(|error| panic!("endpoint publication: {error}"));
    let updated = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-collaboration"))
        .args(["endpoints", "list", "--json", "--service-directory"])
        .arg(&root)
        .output()
        .await
        .expect("read published endpoint");
    assert!(updated.status.success());
    let updated: serde_json::Value =
        serde_json::from_slice(&updated.stdout).expect("endpoint JSON");
    assert_eq!(
        updated["result"]["page"]["records"][0]["endpoint"]["endpointId"],
        "codex-local"
    );
    served
        .stop()
        .await
        .unwrap_or_else(|e| panic!("listener: {e}"));
    std::fs::remove_dir(root).unwrap_or_else(|e| panic!("cleanup: {e}"));
}
