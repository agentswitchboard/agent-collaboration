use collaboration_client::CollaborationClient;
use collaboration_mcp::test_support::ServedCollaborationApi;
use collaboration_service::ApplicationFixture;
use serde_json::json;

#[tokio::test]
async fn client_discovers_over_the_real_api_socket() {
    // Arrange: no backend process; only the owned public service boundary.
    let root = private_directory().unwrap_or_else(|error| panic!("directory: {error}"));
    let identity = ApplicationFixture::new(
        "00000000-0000-4000-8000-000000000001",
        "00000000-0000-4000-8000-000000000002",
    )
    .unwrap_or_else(|error| panic!("identity: {error}"));
    let directory = identity.endpoint_directory();
    let served = ServedCollaborationApi::start(root.path(), (identity).build())
        .unwrap_or_else(|error| panic!("serve: {error}"));
    // Act
    let client = CollaborationClient::connect(root.path(), "client-proof", "1")
        .await
        .unwrap_or_else(|error| panic!("connect: {error}"));
    let inventory = client
        .list_endpoints()
        .await
        .unwrap_or_else(|error| panic!("inventory: {error}"));
    // Assert
    assert!(inventory.endpoints.is_empty());
    assert_eq!(inventory.service_epoch, client.identity().service_epoch);
    let endpoint = json!({"endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"codex-local"},"label":"Debug Codex","availability":{"state":"unprobed"},"channels":[{"kind":"nativeCodex","transport":"unixWebSocket","path":"codex-native.sock","schemaDigest":null,"generation":null}]});
    directory
        .publish(
            serde_json::from_value(endpoint.clone())
                .unwrap_or_else(|error| panic!("endpoint: {error}")),
        )
        .unwrap_or_else(|error| panic!("publish: {error}"));
    let current = client
        .list_endpoints()
        .await
        .unwrap_or_else(|error| panic!("updated inventory: {error}"));
    assert_eq!(current.sequence, 1);
    assert_eq!(current.endpoints.len(), 1);
    assert_eq!(
        serde_json::to_value(&current.endpoints[0]).unwrap_or_else(|error| panic!("{error}")),
        endpoint
    );
    served
        .stop()
        .await
        .unwrap_or_else(|error| panic!("server: {error}"));
}

#[tokio::test]
async fn real_service_snapshot_folds_prior_publications() {
    // Arrange: actual service publication and directory sequence assignment.
    let service_id = "00000000-0000-4000-8000-000000000001";
    let epoch = "00000000-0000-4000-8000-000000000002";
    let endpoint_description = |label: &str| -> collaboration_protocol::EndpointDescription {
        serde_json::from_value(json!({"endpoint":{"serviceId":service_id,"endpointId":"codex-local"},
            "label":label,"availability":{"state":"unprobed"},"channels":[{"kind":"nativeCodex",
                "transport":"unixWebSocket","path":"native.sock","schemaDigest":null,"generation":null}]})).unwrap()
    };
    let identity = ApplicationFixture::new(service_id, epoch)
        .unwrap()
        .with_endpoints(vec![endpoint_description("initial")])
        .unwrap();
    let directory = identity.endpoint_directory();
    let root = private_directory().unwrap_or_else(|error| panic!("directory: {error}"));
    let served = ServedCollaborationApi::start(root.path(), (identity).build()).unwrap();
    let client = CollaborationClient::connect(root.path(), "publication-proof", "1")
        .await
        .unwrap();
    directory.publish(endpoint_description("older")).unwrap();
    directory
        .publish(endpoint_description("snapshot-current"))
        .unwrap();

    // Act: capture the directory after both publications, then publish a later change.
    let snapshot = client.list_endpoints().await.unwrap();
    directory.publish(endpoint_description("later")).unwrap();
    let later = client.list_endpoints().await.unwrap();

    // Assert: the inventory is the directory's current state at the real public boundary, and
    // its sequence orders publications (the directory's own count, not a connection's).
    assert_eq!(later.sequence, snapshot.sequence + 1);
    assert_eq!(
        String::from(snapshot.endpoints[0].label.clone()),
        "snapshot-current"
    );
    assert_eq!(String::from(later.endpoints[0].label.clone()), "later");
    served.stop().await.unwrap();
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Fixture errors propagate and assertions identify a changed SDK response."
)]
async fn served_inventory_returns_both_endpoint_descriptions()
-> Result<(), Box<dyn std::error::Error>> {
    // Arrange: two logical endpoints on one service, with distinct native channels.
    let root = private_directory()?;
    let service_id = "019b011e-4000-7000-8000-000000000001";
    let service_epoch = "019b011e-4000-7000-8000-000000000002";
    let description = |endpoint_id, label, path| {
        serde_json::from_value::<collaboration_protocol::EndpointDescription>(json!({
            "endpoint": {"serviceId": service_id, "endpointId": endpoint_id},
            "label": label,
            "availability": {"state": "unprobed"},
            "channels": [{"kind": "nativeCodex", "transport": "unixWebSocket",
                "path": path, "schemaDigest": null, "generation": null}]
        }))
    };
    let first = description("first-codex", "First Codex", "first.sock")?;
    let second = description("second-codex", "Second Codex", "second.sock")?;
    let application = ApplicationFixture::new(service_id, service_epoch)?
        .with_endpoints(vec![first.clone(), second.clone()])?
        .build();
    let served = ServedCollaborationApi::start(root.path(), application)?;
    let client = CollaborationClient::connect(root.path(), "distinct-endpoints-proof", "1").await?;

    // Act
    let inventory = client.list_endpoints().await?;
    served.stop().await?;

    // Assert: compare both full descriptions without assuming a catalogue order.
    assert_eq!(inventory.endpoints.len(), 2);
    assert!(
        inventory.endpoints.contains(&first),
        "{:?}",
        inventory.endpoints
    );
    assert!(
        inventory.endpoints.contains(&second),
        "{:?}",
        inventory.endpoints
    );
    Ok(())
}

/// The owner-only service directory the API socket requires.
fn private_directory() -> std::io::Result<tempfile::TempDir> {
    use std::os::unix::fs::PermissionsExt;
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
}
