//! The provider fixtures used by Host tests across the controller boundary.

#[path = "external_provider_runtime/owner_fixture_failures.rs"]
pub(crate) mod acp_failures;

pub use crate::external_provider_runtime::acp_scripted_fixture::{
    AcpFixtureScript, launch_fixture_agent,
};
pub use crate::external_provider_supervisor::HeldResult;
pub use crate::session_owner_preparation_fixture::{DeliveryAckControl, DeliveryAcknowledgement};

use crate::external_provider_supervisor::FixtureOwnerDoorSlot;
use crate::session_owner_door::SessionOwnerDoor;
use crate::{
    ExternalProviderLaunch, ExternalProviderRuntime, ExternalProviderRuntimeError,
    ExternalProviderSupervisor,
};
use collaboration_protocol::{EndpointRef, ProviderBindingIdentity};
use router_clock::RuntimeClock;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use switchboard_session_events::ProviderSessionEventHub;
use switchboard_session_store::ProviderOperationStore;
use tokio::sync::Mutex;

struct FixtureRuntime {
    owner_door_slot: Weak<OnceLock<Arc<SessionOwnerDoor>>>,
    runtime: Weak<ExternalProviderRuntime>,
}

static FIXTURE_RUNTIMES: StdMutex<Vec<FixtureRuntime>> = StdMutex::new(Vec::new());

pub(crate) fn register_fixture_runtime(
    owner_door_slot: &FixtureOwnerDoorSlot,
    runtime: &Arc<ExternalProviderRuntime>,
) {
    let mut registered = FIXTURE_RUNTIMES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registered.retain(|fixture| fixture.runtime.strong_count() != 0);
    registered.push(FixtureRuntime {
        owner_door_slot: Arc::downgrade(owner_door_slot),
        runtime: Arc::downgrade(runtime),
    });
}

/// Resolve a concrete Host fixture through the exact supervisor binding.
pub fn fixture_runtime_for(
    supervisor: &ExternalProviderSupervisor,
    endpoint: &EndpointRef,
) -> Option<Arc<ExternalProviderRuntime>> {
    let slot = Arc::downgrade(supervisor.fixture_owner_door_slot(endpoint)?);
    FIXTURE_RUNTIMES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .find(|fixture| fixture.owner_door_slot.ptr_eq(&slot))?
        .runtime
        .upgrade()
}

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
