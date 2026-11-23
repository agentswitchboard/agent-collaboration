//! Read-only custody evidence for exact Codex thread IDs.

use codex_acp_adapter::CodexSessionPresence;
use codex_native_integration::{
    NativeOperation, NativeProtocolConnection, StoredThreadCatalog, StoredThreadCursor,
    StoredThreadProvider, StoredThreadQuery, StoredThreadRoot, StoredThreadSort,
    StoredThreadSource,
};
use collaboration_runtime::{NativeAdmission, NativeGenerationGate, UnmaterializedThreadHolder};
use serde::Deserialize;
use serde_json::json;
use sqlx::Row;
use std::{path::PathBuf, sync::Arc};
use switchboard_message_custody::recipient_sessions::{
    InventoryRead, SessionId, SessionInventory, SessionInventoryAnswer,
};

#[derive(Clone, Copy)]
enum ThreadObservation {
    Found,
    Absent,
    Incomplete,
}

impl ThreadObservation {
    const fn answer(self) -> SessionInventoryAnswer {
        match self {
            Self::Found => SessionInventoryAnswer::Complete(1),
            Self::Absent => SessionInventoryAnswer::Complete(0),
            Self::Incomplete => SessionInventoryAnswer::AtLeast(0),
        }
    }
}

/// The list is process-wide for this Mac's one managed Codex backend (relay,
/// direct same-process clients and Remote Control on that process). It excludes
/// internal threads and private samples, and it cannot see a separate Codex
/// process, which is not a session of this Router.
pub struct CodexSessionInventory {
    codex_home: PathBuf,
    session_presence: CodexSessionPresence,
    native_gate: NativeGenerationGate,
}

impl CodexSessionInventory {
    #[must_use]
    pub fn new(
        codex_home: PathBuf,
        unmaterialized_threads: Arc<UnmaterializedThreadHolder>,
        native_gate: NativeGenerationGate,
    ) -> Self {
        Self {
            codex_home,
            session_presence: unmaterialized_threads.session_presence(),
            native_gate,
        }
    }

    fn has_binding(&self, session_id: &SessionId) -> bool {
        collaboration_protocol::SessionId::try_from(session_id.as_str().to_owned())
            .is_ok_and(|native_id| self.session_presence.contains(&native_id))
    }

    async fn lookup(&self, session_id: &SessionId) -> ThreadObservation {
        if self.has_binding(session_id) {
            return ThreadObservation::Found;
        }
        let stored = self.lookup_stored(session_id).await;
        if matches!(stored, ThreadObservation::Found) || self.has_binding(session_id) {
            return ThreadObservation::Found;
        }
        // Codex's ThreadId::from_string uses Uuid::parse_str for native IDs.
        if uuid::Uuid::parse_str(session_id.as_str()).is_err() {
            return stored;
        }

        let Ok(admission) = self.native_gate.acquire() else {
            return ThreadObservation::Incomplete;
        };
        let retired = admission.retirement();
        let observation = tokio::select! {
            biased;
            () = retired.cancelled() => ThreadObservation::Incomplete,
            observation = self.lookup_under_admission(session_id, &admission) => observation,
        };
        if retired.is_cancelled() {
            ThreadObservation::Incomplete
        } else {
            observation
        }
    }

    async fn lookup_under_admission(
        &self,
        session_id: &SessionId,
        admission: &NativeAdmission,
    ) -> ThreadObservation {
        match lookup_loaded(session_id, admission).await {
            ThreadObservation::Absent => {
                // The thread may have materialized and unloaded after the
                // first catalogue read. Recheck both views after the list.
                let stored_after_list = self.lookup_stored(session_id).await;
                if self.has_binding(session_id) {
                    ThreadObservation::Found
                } else {
                    stored_after_list
                }
            }
            observed => observed,
        }
    }

    async fn lookup_stored(&self, session_id: &SessionId) -> ThreadObservation {
        match tokio::fs::try_exists(self.codex_home.join("state_5.sqlite")).await {
            Ok(false) => return ThreadObservation::Absent,
            Ok(true) => {}
            Err(_) => return ThreadObservation::Incomplete,
        }
        let Ok(catalog) = StoredThreadCatalog::open(&self.codex_home).await else {
            return ThreadObservation::Incomplete;
        };
        let found = contains_stored_thread(&catalog, session_id.as_str()).await;
        catalog.close().await;
        match found {
            Ok(true) => ThreadObservation::Found,
            Ok(false) => ThreadObservation::Absent,
            Err(_) => ThreadObservation::Incomplete,
        }
    }
}

impl SessionInventory for CodexSessionInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> InventoryRead<'lookup> {
        Box::pin(async move { self.lookup(session_id).await.answer() })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoadedThreads {
    data: Vec<String>,
    next_cursor: Option<String>,
}

async fn lookup_loaded(session_id: &SessionId, admission: &NativeAdmission) -> ThreadObservation {
    let Some(schemas) = admission.schemas() else {
        return ThreadObservation::Incomplete;
    };
    let Ok(mut native) = NativeProtocolConnection::connect(admission.backend_path()).await else {
        return ThreadObservation::Incomplete;
    };
    let Ok(page) = native
        .request_validated(&schemas, NativeOperation::ListLoadedThreads, json!({}))
        .await
    else {
        return ThreadObservation::Incomplete;
    };
    let Ok(page) = serde_json::from_value::<LoadedThreads>(page) else {
        return ThreadObservation::Incomplete;
    };
    if page.next_cursor.is_some()
        || page
            .data
            .iter()
            .any(|id| uuid::Uuid::parse_str(id).is_err())
    {
        return ThreadObservation::Incomplete;
    }
    if page.data.iter().any(|id| id == session_id.as_str()) {
        ThreadObservation::Found
    } else {
        ThreadObservation::Absent
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
