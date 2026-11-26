//! Admission, reply commitment and drainage for one ACP approval scope.

use provider_session_machines::session_owner::PendingTurnResponse;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct PermissionResponseBarrier {
    scope: Mutex<ResponseScope>,
    ended: CancellationToken,
    drained: tokio::sync::Notify,
}

#[derive(Default)]
struct ResponseScope {
    pending: BTreeMap<PendingTurnResponse, usize>,
    closed: bool,
}

impl PermissionResponseBarrier {
    fn scope(&self) -> MutexGuard<'_, ResponseScope> {
        self.scope.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn track(
        self: &Arc<Self>,
        method: &'static str,
        id: &impl serde::Serialize,
        peer: agent_client_protocol::RequestCancellation,
    ) -> Option<PermissionResponseGuard> {
        let key = PendingTurnResponse {
            method,
            request_id: serde_json::to_string(id).unwrap_or_default(),
        };
        let mut scope = self.scope();
        if scope.closed {
            return None;
        }
        *scope.pending.entry(key.clone()).or_default() += 1;
        Some(PermissionResponseGuard {
            barrier: Arc::clone(self),
            key,
            peer,
        })
    }

    /// Close admission and answer commitment before signaling pending requests.
    pub(super) fn close(&self) -> bool {
        let mut scope = self.scope();
        scope.closed = true;
        !scope.pending.is_empty()
    }

    pub(super) fn signal_end(&self) {
        self.ended.cancel();
    }

    pub(super) async fn ended(&self) {
        self.ended.cancelled().await;
    }

    pub(super) fn is_open(&self) -> bool {
        !self.scope().closed
    }

    pub(super) fn reserve_stop<T>(
        &self,
        had_pending_at_close: bool,
        reserve: impl FnOnce() -> T,
    ) -> Option<T> {
        let scope = self.scope();
        (had_pending_at_close || !scope.closed).then(reserve)
    }

    #[cfg(feature = "test-observation")]
    pub(super) fn lock_available_for_test(&self) -> bool {
        self.scope.try_lock().is_ok()
    }

    pub(super) fn pending_responses(&self) -> Vec<PendingTurnResponse> {
        self.scope().pending.keys().cloned().collect()
    }

    pub(super) async fn wait_drained(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.scope().pending.is_empty() {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct PermissionResponseGuard {
    barrier: Arc<PermissionResponseBarrier>,
    key: PendingTurnResponse,
    peer: agent_client_protocol::RequestCancellation,
}

impl PermissionResponseGuard {
    /// The response enqueue and scope closure are serialized for this request.
    pub(crate) fn commit<T>(&self, reply: impl FnOnce(bool) -> T) -> T {
        let scope = self.barrier.scope();
        // Keep the SDK's non-blocking enqueue under this lock so scope closure
        // cannot interleave. A peer cancel observed here wins; a later cancel
        // can cross the committed reply. No policy code runs in this call.
        reply(!scope.closed && !self.peer.is_cancelled())
    }
}

impl Drop for PermissionResponseGuard {
    fn drop(&mut self) {
        let drained = {
            let mut scope = self.barrier.scope();
            if let Some(count) = scope.pending.get_mut(&self.key) {
                *count -= 1;
                if *count == 0 {
                    scope.pending.remove(&self.key);
                }
            }
            scope.pending.is_empty()
        };
        if drained {
            self.barrier.drained.notify_waiters();
        }
    }
}
