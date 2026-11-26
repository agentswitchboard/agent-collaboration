//! Admission, reply commitment and drainage for one ACP approval scope.

use provider_session_machines::session_owner::PendingTurnResponse;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct PermissionResponseBarrier {
    pending: Mutex<BTreeMap<PendingTurnResponse, usize>>,
    ended: CancellationToken,
    drained: tokio::sync::Notify,
}

impl PermissionResponseBarrier {
    fn pending(&self) -> MutexGuard<'_, BTreeMap<PendingTurnResponse, usize>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn track(
        self: &Arc<Self>,
        method: &'static str,
        id: &impl serde::Serialize,
    ) -> Option<PermissionResponseGuard> {
        let key = PendingTurnResponse {
            method,
            request_id: serde_json::to_string(id).unwrap_or_default(),
        };
        let mut pending = self.pending();
        if self.ended.is_cancelled() {
            return None;
        }
        *pending.entry(key.clone()).or_default() += 1;
        Some(PermissionResponseGuard {
            barrier: Arc::clone(self),
            key,
        })
    }

    /// Close admission and answer commitment before signaling pending requests.
    pub(super) fn end(&self, cancelling: &CancellationToken) -> bool {
        let pending = self.pending();
        self.ended.cancel();
        cancelling.cancel();
        !pending.is_empty()
    }

    pub(super) async fn ended(&self) {
        self.ended.cancelled().await;
    }

    pub(super) fn while_open<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
        let _pending = self.pending();
        (!self.ended.is_cancelled()).then(operation)
    }

    pub(super) fn schedule_stop(&self, had_pending_at_close: bool, schedule: impl FnOnce()) {
        let pending = self.pending();
        if had_pending_at_close || !self.ended.is_cancelled() || !pending.is_empty() {
            schedule();
        }
    }

    pub(super) fn pending_responses(&self) -> Vec<PendingTurnResponse> {
        self.pending().keys().cloned().collect()
    }

    pub(super) async fn wait_drained(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.pending().is_empty() {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct PermissionResponseGuard {
    barrier: Arc<PermissionResponseBarrier>,
    key: PendingTurnResponse,
}

impl PermissionResponseGuard {
    /// The response enqueue and scope closure are serialized for this request.
    pub(crate) fn commit<T>(&self, reply: impl FnOnce(bool) -> T) -> T {
        let _pending = self.barrier.pending();
        reply(!self.barrier.ended.is_cancelled())
    }
}

impl Drop for PermissionResponseGuard {
    fn drop(&mut self) {
        let mut pending = self.barrier.pending();
        if let Some(count) = pending.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                pending.remove(&self.key);
            }
        }
        if pending.is_empty() {
            self.barrier.drained.notify_waiters();
        }
    }
}
