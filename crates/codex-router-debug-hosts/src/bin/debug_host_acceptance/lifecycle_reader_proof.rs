//! Public discovery and inspection of a real owned Codex thread.
use super::owned_thread_registry::OwnedThreadRegistry;
use codex_native_integration::NativeProtocolConnection;
use collaboration_client::CollaborationClient;
use collaboration_protocol::{
    ChannelDescription, EndpointAvailability, NativeSessionListParams, NativeSessionView,
};
use serde_json::json;
use std::{error::Error, path::Path};

pub async fn run_reader_proof(
    native: &mut NativeProtocolConnection,
    owned: &mut OwnedThreadRegistry,
    directory: &Path,
) -> Result<(), Box<dyn Error>> {
    let client =
        CollaborationClient::connect(directory, "lifecycle-proof", env!("CARGO_PKG_VERSION"))
            .await?;
    let cwd = std::env::current_dir()?;
    let target = owned.create(native, &cwd).await?;
    let receipt = owned
        .submit_text(
            native,
            &target,
            "Do not use tools. Reply exactly LIFECYCLE_READER_READY.",
        )
        .await?;
    if owned.observe_text(native, receipt).await?.trim() != "LIFECYCLE_READER_READY" {
        return Err("lifecycle preparation failed".into());
    }
    let endpoint = collaboration_protocol::EndpointRef {
        service_id: client.identity().service_id.clone(),
        endpoint_id: "codex-local".to_owned().try_into()?,
    };
    let stored = client
        .list_sessions(NativeSessionListParams {
            endpoint: endpoint.clone(),
            view: NativeSessionView::Stored,
            scope: collaboration_client::protocol::NativeSessionScope::Any,
            source: collaboration_client::protocol::NativeSessionSource::All,
            include_empty_sessions: false,
            query: None,
            page_size: 100,
            cursor: None,
        })
        .await?;
    if !stored
        .sessions
        .iter()
        .any(|entry| String::from(entry.target.session_id.clone()) == target)
    {
        return Err("fresh materialized thread absent from native stored catalog".into());
    }
    let published = client.list_endpoints().await?;
    let generation = published
        .endpoints
        .into_iter()
        .find_map(|entry| {
            if entry.endpoint != endpoint
                || !matches!(entry.availability, EndpointAvailability::Available { .. })
            {
                return None;
            }
            entry
                .channels
                .into_iter()
                .find_map(|channel| match channel {
                    ChannelDescription::NativeCodex { generation, .. } => generation,
                    _ => None,
                })
        })
        .ok_or("native backend readiness was not published")?;
    let loaded = client
        .list_sessions(NativeSessionListParams {
            endpoint: endpoint.clone(),
            view: NativeSessionView::Loaded,
            scope: collaboration_protocol::NativeSessionScope::Any,
            source: collaboration_protocol::NativeSessionSource::All,
            include_empty_sessions: false,
            query: None,
            page_size: 100,
            cursor: None,
        })
        .await?;
    let target = collaboration_protocol::SessionRef {
        endpoint,
        session_id: target.try_into()?,
    };
    if !loaded.sessions.iter().any(|entry| entry.target == target) {
        return Err("materialized thread absent from native loaded list".into());
    }
    let inspected = client.inspect_session(&target).await?;
    if inspected.generation != generation {
        return Err("native inspect did not use the published generation".into());
    }
    println!(
        "{}",
        json!({"kind":"ownedLifecycleReadersPassed","threadId":target.session_id,"generation":generation})
    );
    Ok(())
}
