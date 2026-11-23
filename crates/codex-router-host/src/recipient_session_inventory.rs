//! Existing local session sources viewed through the custody inventory port.

use crate::ExternalProviderSupervisor;
pub use crate::recipient_terminal_inventory::ClaudeTerminalSessionInventory;
use codex_acp_adapter::CodexSessionPresence;
use codex_native_integration::{
    StoredThreadCatalog, StoredThreadCursor, StoredThreadProvider, StoredThreadQuery,
    StoredThreadRoot, StoredThreadSort, StoredThreadSource,
};
use collaboration_protocol::{EndpointId, EndpointRef, SessionRef, UuidIdentity};
use collaboration_runtime::UnmaterializedThreadHolder;
use provider_session_machines::session_lifecycle_machine::SessionLifecycle;
use sqlx::Row;
use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};
use switchboard_message_custody::recipient_sessions::{
    SessionId, SessionInventory, SessionInventoryAnswer, SessionInventoryPart,
};
use switchboard_session_store::ProviderOperationStore;
use tokio::sync::Mutex;

const UNAVAILABLE: SessionInventoryAnswer = SessionInventoryAnswer {
    at_least: 0,
    complete: false,
};

const fn complete(at_least: usize) -> SessionInventoryAnswer {
    SessionInventoryAnswer {
        at_least,
        complete: true,
    }
}

pub struct CodexSessionInventory {
    codex_home: PathBuf,
    session_presence: CodexSessionPresence,
}

impl CodexSessionInventory {
    #[must_use]
    pub fn new(
        codex_home: PathBuf,
        unmaterialized_threads: Arc<UnmaterializedThreadHolder>,
    ) -> Self {
        Self {
            codex_home,
            session_presence: unmaterialized_threads.session_presence(),
        }
    }

    async fn lookup_stored(&self, session_id: &SessionId) -> SessionInventoryAnswer {
        match tokio::fs::try_exists(self.codex_home.join("state_5.sqlite")).await {
            Ok(false) => return UNAVAILABLE,
            Ok(true) => {}
            Err(_) => return UNAVAILABLE,
        }
        let Ok(catalog) = StoredThreadCatalog::open(&self.codex_home).await else {
            return UNAVAILABLE;
        };
        let found = contains_stored_thread(&catalog, session_id.as_str()).await;
        catalog.close().await;
        match found {
            Ok(true) => complete(1),
            // The opaque native relay does not index its pre-persistence threads.
            Ok(false) | Err(_) => UNAVAILABLE,
        }
    }
}

impl SessionInventory for CodexSessionInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            if let Ok(native_id) =
                collaboration_protocol::SessionId::try_from(session_id.as_str().to_owned())
                && self.session_presence.contains(&native_id)
            {
                return complete(1);
            }
            self.lookup_stored(session_id).await
        })
    }
}

async fn contains_stored_thread(
    catalog: &StoredThreadCatalog,
    session_id: &str,
) -> Result<bool, sqlx::Error> {
    const PAGE_SIZE: usize = 100;
    let mut query = StoredThreadQuery {
        root: StoredThreadRoot::Any,
        provider: StoredThreadProvider::Any,
        source: StoredThreadSource::All,
        sort: StoredThreadSort::Created,
        page_size: PAGE_SIZE,
        cursor: None,
        query: None,
    };
    loop {
        let rows = catalog.read_page(&query).await?;
        for row in &rows {
            if row.try_get::<String, _>("id")? == session_id {
                return Ok(true);
            }
        }
        if rows.len() < PAGE_SIZE {
            return Ok(false);
        }
        if let Some(last) = rows.last() {
            query.cursor = Some(StoredThreadCursor {
                sort_value: last.try_get("created_at_ms")?,
                session_id: last.try_get("id")?,
            });
        }
    }
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
        let observed = SessionInventoryAnswer {
            at_least: usize::from(owner_knows),
            complete: false,
        };
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
