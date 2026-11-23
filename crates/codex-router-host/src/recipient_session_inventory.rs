//! Existing local session sources viewed through the custody inventory port.

use crate::ExternalProviderSupervisor;
pub use crate::recipient_codex_inventory::CodexSessionInventory;
pub use crate::recipient_terminal_inventory::ClaudeTerminalSessionInventory;
use collaboration_protocol::{EndpointId, EndpointRef, SessionRef, UuidIdentity};
use provider_session_machines::session_lifecycle_machine::SessionLifecycle;
use std::{future::Future, pin::Pin, sync::Arc};
use switchboard_message_custody::recipient_sessions::{
    SessionId, SessionInventory, SessionInventoryAnswer, SessionInventoryPart,
};
use switchboard_session_store::ProviderOperationStore;
use tokio::sync::Mutex;

const UNAVAILABLE: SessionInventoryAnswer = SessionInventoryAnswer::AtLeast(0);

const fn complete(at_least: usize) -> SessionInventoryAnswer {
    SessionInventoryAnswer::Complete(at_least)
}

pub struct ManagedProviderSessionInventory {
    endpoints: [EndpointRef; 2],
    store: Option<Arc<Mutex<ProviderOperationStore>>>,
    supervisor: Option<Arc<ExternalProviderSupervisor>>,
}

impl ManagedProviderSessionInventory {
    pub fn new(
        service_id: UuidIdentity,
        store: Option<Arc<Mutex<ProviderOperationStore>>>,
        supervisor: Option<Arc<ExternalProviderSupervisor>>,
    ) -> Result<Self, collaboration_protocol::IdentityError> {
        let endpoints = [
            EndpointRef {
                service_id: service_id.clone(),
                endpoint_id: EndpointId::try_from("claude-local".to_owned())?,
            },
            EndpointRef {
                service_id,
                endpoint_id: EndpointId::try_from("cursor-local".to_owned())?,
            },
        ];
        Ok(Self {
            endpoints,
            store,
            supervisor,
        })
    }

    fn lookup_endpoint(&self, target: SessionRef) -> SessionInventoryPart<'_> {
        let owner_knows = self
            .supervisor
            .as_ref()
            .and_then(|supervisor| supervisor.owner_session_snapshot(&target))
            .is_some_and(|snapshot| {
                matches!(
                    snapshot.lifecycle,
                    SessionLifecycle::Live { .. } | SessionLifecycle::Loading { .. }
                )
            });
        let observed = SessionInventoryAnswer::AtLeast(usize::from(owner_knows));
        SessionInventoryPart {
            observed,
            read: Box::pin(async move {
                let Some(store) = &self.store else {
                    return observed;
                };
                match store.lock().await.session_record(&target).await {
                    Ok(record) => complete(usize::from(owner_knows || record.is_some())),
                    Err(_) => observed,
                }
            }),
        }
    }

    fn endpoint_parts(&self, session_id: &SessionId) -> Option<[SessionInventoryPart<'_>; 2]> {
        let session_id =
            collaboration_protocol::SessionId::try_from(session_id.as_str().to_owned()).ok()?;
        Some(self.endpoints.each_ref().map(|endpoint| {
            self.lookup_endpoint(SessionRef {
                endpoint: endpoint.clone(),
                session_id: session_id.clone(),
            })
        }))
    }
}

impl SessionInventory for ManagedProviderSessionInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            let Some([claude, cursor]) = self.endpoint_parts(session_id) else {
                return UNAVAILABLE;
            };
            let (claude, cursor) = tokio::join!(claude.read, cursor.read);
            claude.merge(cursor)
        })
    }

    fn lookup_parts<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Vec<SessionInventoryPart<'lookup>> {
        match self.endpoint_parts(session_id) {
            Some(parts) => Vec::from(parts),
            None => vec![SessionInventoryPart {
                observed: UNAVAILABLE,
                read: Box::pin(std::future::ready(UNAVAILABLE)),
            }],
        }
    }
}

#[cfg(test)]
#[path = "recipient_session_inventory_test.rs"]
mod tests;
