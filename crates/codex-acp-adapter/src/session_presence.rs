//! Read-only projection of the thread IDs owned by admitted ACP bindings.

use collaboration_protocol::SessionId;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio_util::sync::CancellationToken;

type ThreadClaims = HashMap<SessionId, Vec<Weak<CancellationToken>>>;

/// A Host-lifetime view of bindings that still belong to a live backend generation.
#[derive(Clone, Default)]
pub struct CodexSessionPresence(Arc<Mutex<ThreadClaims>>);

impl CodexSessionPresence {
    #[must_use]
    pub fn contains(&self, session_id: &SessionId) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .is_some_and(|claims| {
                claims
                    .iter()
                    .filter_map(Weak::upgrade)
                    .any(|retirement| !retirement.is_cancelled())
            })
    }

    pub(crate) fn claim(
        &self,
        session_id: SessionId,
        retirement: CancellationToken,
    ) -> SessionPresenceLease {
        // The allocation identifies this binding's claim independently of the
        // shared lifetime token that fences the whole backend generation.
        let claim = Arc::new(retirement);
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(session_id.clone())
            .or_default()
            .push(Arc::downgrade(&claim));
        SessionPresenceLease {
            state: Arc::downgrade(&self.0),
            session_id,
            claim,
        }
    }
}

pub(crate) struct SessionPresenceLease {
    state: Weak<Mutex<ThreadClaims>>,
    session_id: SessionId,
    claim: Arc<CancellationToken>,
}

impl Drop for SessionPresenceLease {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let std::collections::hash_map::Entry::Occupied(mut thread) =
            state.entry(self.session_id.clone())
        {
            let own_claim = Arc::downgrade(&self.claim);
            thread.get_mut().retain(|claim| !claim.ptr_eq(&own_claim));
            if thread.get().is_empty() {
                thread.remove();
            }
        }
    }
}

#[cfg(test)]
#[path = "session_presence_test.rs"]
mod tests;
