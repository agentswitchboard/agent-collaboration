//! Existing local session sources viewed through the custody inventory port.

use crate::ExternalProviderSupervisor;
use claude_code_peer_messaging::{ClaudeCodeSessionRegistry, PeerSessionLookup};
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
    SessionId, SessionInventory, SessionInventoryAnswer,
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
    unmaterialized_threads: Arc<UnmaterializedThreadHolder>,
}

impl CodexSessionInventory {
    #[must_use]
    pub fn new(
        codex_home: PathBuf,
        unmaterialized_threads: Arc<UnmaterializedThreadHolder>,
    ) -> Self {
        Self {
            codex_home,
            unmaterialized_threads,
        }
    }

    async fn lookup_stored(&self, session_id: &SessionId) -> SessionInventoryAnswer {
        match tokio::fs::try_exists(self.codex_home.join("state_5.sqlite")).await {
            Ok(false) => return complete(0),
            Ok(true) => {}
            Err(_) => return UNAVAILABLE,
        }
        let Ok(catalog) = StoredThreadCatalog::open(&self.codex_home).await else {
            return UNAVAILABLE;
        };
        let found = contains_stored_thread(&catalog, session_id.as_str()).await;
        catalog.close().await;
        match found {
            Ok(present) => complete(usize::from(present)),
            Err(_) => UNAVAILABLE,
        }
    }
}

impl SessionInventory for CodexSessionInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            if self.unmaterialized_threads.contains(session_id.as_str()) {
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

    async fn lookup_endpoint(&self, target: &SessionRef) -> SessionInventoryAnswer {
        let owner_knows = self
            .supervisor
            .as_ref()
            .and_then(|supervisor| supervisor.owner_session_snapshot(target))
            .is_some_and(|snapshot| {
                matches!(
                    snapshot.lifecycle,
                    SessionLifecycle::Live { .. } | SessionLifecycle::Loading { .. }
                )
            });
        let Some(store) = &self.store else {
            return SessionInventoryAnswer {
                at_least: usize::from(owner_knows),
                complete: false,
            };
        };
        match store.lock().await.session_record(target).await {
            Ok(record) => complete(usize::from(owner_knows || record.is_some())),
            Err(_) => SessionInventoryAnswer {
                at_least: usize::from(owner_knows),
                complete: false,
            },
        }
    }
}

impl SessionInventory for ManagedProviderSessionInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            let Ok(session_id) =
                collaboration_protocol::SessionId::try_from(session_id.as_str().to_owned())
            else {
                return UNAVAILABLE;
            };
            let [claude, cursor] = &self.endpoints;
            let claude = self
                .lookup_endpoint(&SessionRef {
                    endpoint: claude.clone(),
                    session_id: session_id.clone(),
                })
                .await;
            let cursor = self
                .lookup_endpoint(&SessionRef {
                    endpoint: cursor.clone(),
                    session_id,
                })
                .await;
            claude.merge(cursor)
        })
    }
}

pub struct ClaudeTerminalSessionInventory {
    registry: Arc<ClaudeCodeSessionRegistry>,
}

impl ClaudeTerminalSessionInventory {
    #[must_use]
    pub fn new(registry: Arc<ClaudeCodeSessionRegistry>) -> Self {
        Self { registry }
    }
}

impl SessionInventory for ClaudeTerminalSessionInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            let Ok(session_id) =
                collaboration_protocol::SessionId::try_from(session_id.as_str().to_owned())
            else {
                return UNAVAILABLE;
            };
            let registry = Arc::clone(&self.registry);
            match tokio::task::spawn_blocking(move || registry.lookup(&session_id)).await {
                Ok(Ok(PeerSessionLookup::Absent)) => complete(0),
                Ok(Ok(
                    PeerSessionLookup::Writable(_) | PeerSessionLookup::LiveUnsupported { .. },
                )) => complete(1),
                Ok(Ok(PeerSessionLookup::Ambiguous { claims })) => complete(claims.len()),
                Ok(Err(_)) | Err(_) => UNAVAILABLE,
            }
        })
    }
}

#[cfg(test)]
#[path = "recipient_session_inventory_test.rs"]
mod tests;
