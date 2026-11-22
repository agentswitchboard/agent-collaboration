//! The Host adapter for the native door’s session catalogue read.

use collaboration_contracts::{ProviderModelEntry, SessionModelCatalog, SessionModelCatalogError};

pub(crate) struct SupervisorModelCatalog {
    supervisor: std::sync::Arc<crate::ExternalProviderSupervisor>,
}
impl SupervisorModelCatalog {
    pub(crate) fn new(supervisor: std::sync::Arc<crate::ExternalProviderSupervisor>) -> Self {
        Self { supervisor }
    }
}
impl SessionModelCatalog for SupervisorModelCatalog {
    fn read_models(
        &self,
        session: message_board::SessionRef,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<ProviderModelEntry>, SessionModelCatalogError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let target = serde_json::to_value(session)
                .and_then(serde_json::from_value)
                .map_err(|_| SessionModelCatalogError::Unavailable)?;
            self.supervisor.session_model_catalog(&target).await
        })
    }
}
