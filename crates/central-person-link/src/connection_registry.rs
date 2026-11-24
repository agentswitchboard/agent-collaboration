//! Track live native connections and signal their transport after invalidation.

use crate::{PersonConnectionState, PersonRevision, PersonSocketAttachment};
use std::sync::{Arc, Mutex, PoisonError, Weak};

#[derive(Clone, Default)]
pub struct PersonConnectionRegistry(Arc<Mutex<Vec<Weak<RegisteredPersonConnection>>>>);

pub struct RegisteredPersonConnection {
    state: Mutex<PersonConnectionState>,
    request_reconnect: Box<dyn Fn() + Send + Sync>,
}

impl PersonConnectionRegistry {
    pub fn register(
        &self,
        attachment: PersonSocketAttachment,
        request_reconnect: impl Fn() + Send + Sync + 'static,
    ) -> Arc<RegisteredPersonConnection> {
        let connection = Arc::new(RegisteredPersonConnection {
            state: Mutex::new(PersonConnectionState::Admitted(attachment)),
            request_reconnect: Box::new(request_reconnect),
        });
        let mut connections = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        connections.retain(|existing| existing.strong_count() > 0);
        connections.push(Arc::downgrade(&connection));
        connection
    }

    pub fn close_affected(&self, revision: &PersonRevision) {
        let mut ended = Vec::new();
        let mut connections = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        connections.retain(|existing| {
            let Some(connection) = existing.upgrade() else {
                return false;
            };
            let superseded = connection
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .observe_revision(revision);
            if superseded {
                ended.push(connection);
            }
            true
        });
        drop(connections);
        for connection in ended {
            (connection.request_reconnect)();
        }
    }
}

impl RegisteredPersonConnection {
    pub fn current(&self, now_ms: i64) -> Option<PersonSocketAttachment> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .check_time(now_ms)
            .cloned()
    }
}
