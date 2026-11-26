//! Legacy ACP test doubles run through the same neutral connection as product code.

use crate::{
    ExternalProviderLaunch, ExternalProviderRuntimeError, InteractionPort, ProviderBehavior,
    ProviderModelPicker, SessionEventSink,
    acp_owner_fixture::PolicyFixtureClient,
    fixture_policy::{FixturePayloads, FixturePolicy},
};
use router_clock::RuntimeClock;
use std::{ops::Deref, sync::Arc};

pub struct FixtureClient<Port: InteractionPort>(PolicyFixtureClient<FixturePayloads<Port>>);

impl<Port: InteractionPort> Deref for FixtureClient<Port> {
    type Target = PolicyFixtureClient<FixturePayloads<Port>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Port: InteractionPort> std::fmt::Debug for FixtureClient<Port> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<Port: InteractionPort> FixtureClient<Port> {
    fn with_observer(client: PolicyFixtureClient<FixturePayloads<Port>>, port: Arc<Port>) -> Self {
        client.observe_malformed_approval_offers(Arc::new(move |context, refusal| {
            let port = Arc::clone(&port);
            Box::pin(async move { port.record_refusal(context, refusal).await })
        }));
        Self(client)
    }

    pub async fn initialize(
        launch: ExternalProviderLaunch,
        port: Arc<Port>,
        sink: Arc<dyn SessionEventSink>,
        behavior: Arc<dyn ProviderBehavior>,
    ) -> Result<Self, ExternalProviderRuntimeError> {
        let client = PolicyFixtureClient::initialize(
            launch,
            Arc::new(FixturePolicy(Arc::clone(&port))),
            sink,
            behavior,
        )
        .await?;
        Ok(Self::with_observer(client, port))
    }

    #[cfg(feature = "test-observation")]
    pub async fn initialize_with_model_picker(
        launch: ExternalProviderLaunch,
        picker: ProviderModelPicker,
        port: Arc<Port>,
        sink: Arc<dyn SessionEventSink>,
        behavior: Arc<dyn ProviderBehavior>,
    ) -> Result<Self, ExternalProviderRuntimeError> {
        let client = PolicyFixtureClient::initialize_with_model_picker(
            launch,
            picker,
            Arc::new(FixturePolicy(Arc::clone(&port))),
            sink,
            behavior,
        )
        .await?;
        Ok(Self::with_observer(client, port))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Matches the existing ACP fixture constructor."
    )]
    pub async fn initialize_with_mcp_http(
        launch: ExternalProviderLaunch,
        picker: ProviderModelPicker,
        name: impl Into<String>,
        url: impl Into<String>,
        port: Arc<Port>,
        sink: Arc<dyn SessionEventSink>,
        behavior: Arc<dyn ProviderBehavior>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Result<Self, ExternalProviderRuntimeError> {
        let client = PolicyFixtureClient::initialize_with_mcp_http(
            launch,
            picker,
            name,
            url,
            Arc::new(FixturePolicy(Arc::clone(&port))),
            sink,
            behavior,
            clock,
        )
        .await?;
        Ok(Self::with_observer(client, port))
    }

    pub async fn initialize_in_process(
        io: tokio::io::DuplexStream,
        agent_task: tokio::task::JoinHandle<()>,
        port: Arc<Port>,
        sink: Arc<dyn SessionEventSink>,
        behavior: Arc<dyn ProviderBehavior>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Result<Self, ExternalProviderRuntimeError> {
        let client = PolicyFixtureClient::initialize_in_process(
            io,
            agent_task,
            Arc::new(FixturePolicy(Arc::clone(&port))),
            sink,
            behavior,
            clock,
        )
        .await?;
        Ok(Self::with_observer(client, port))
    }
}
