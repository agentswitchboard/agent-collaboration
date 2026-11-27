//! The provider fixtures used by Host tests across the controller boundary.

pub use crate::external_provider_runtime::acp_scripted_fixture::{
    AcpFixtureScript, launch_fixture_agent,
};
pub use crate::external_provider_supervisor::HeldResult;
pub use crate::session_owner_preparation_fixture::{DeliveryAckControl, DeliveryAcknowledgement};

use crate::{
    ExternalProviderLaunch, ExternalProviderRuntime, ExternalProviderRuntimeError,
    ExternalProviderSupervisor,
};
use collaboration_protocol::ProviderBindingIdentity;
use router_clock::RuntimeClock;
use std::sync::Arc;
use switchboard_session_events::ProviderSessionEventHub;
use switchboard_session_store::ProviderOperationStore;
use tokio::sync::Mutex;

/// Starts one scripted binding with one clock, event hub and shared store.
pub async fn bind_scripted_provider(
    identity: ProviderBindingIdentity,
    launch: ExternalProviderLaunch,
    store: Arc<Mutex<ProviderOperationStore>>,
    behavior: Arc<dyn acp_client_runtime::ProviderBehavior>,
    clock: Arc<dyn RuntimeClock>,
) -> Result<(ExternalProviderSupervisor, Arc<ProviderSessionEventHub>), ExternalProviderRuntimeError>
{
    let endpoint = serde_json::to_value(&identity.endpoint)
        .and_then(serde_json::from_value)
        .map_err(|error| ExternalProviderRuntimeError::Operation(error.to_string()))?;
    let hub = Arc::new(ProviderSessionEventHub::new(store.clone(), clock.clone()));
    let runtime = ExternalProviderRuntime::initialize_with_mcp_http_and_hub(
        launch,
        acp_client_runtime::ProviderModelPicker::Standard,
        "fixture",
        "http://127.0.0.1:1/mcp",
        hub.clone(),
        endpoint,
        behavior,
        clock.clone(),
    )
    .await?;
    let supervisor = ExternalProviderSupervisor::with_fixture_hub(
        vec![runtime.into_binding(identity)],
        store,
        hub.clone(),
        clock,
    )
    .map_err(|message| ExternalProviderRuntimeError::Operation(message.to_owned()))?;
    Ok((supervisor, hub))
}
