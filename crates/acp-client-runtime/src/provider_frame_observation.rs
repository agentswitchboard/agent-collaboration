//! Bounded ACP frame observation and test-only reply probes.

use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "test-observation")]
use agent_client_protocol::{RequestCancellation, schema::v1::RequestId};
#[cfg(feature = "test-observation")]
use tokio::sync::oneshot;

#[cfg(feature = "test-observation")]
pub type PermissionReplyProbe = (
    oneshot::Receiver<RequestCancellation>,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
);

#[cfg(feature = "test-observation")]
#[derive(Debug)]
struct HeldPermissionReply {
    id: RequestId,
    reached: oneshot::Sender<RequestCancellation>,
    release: oneshot::Receiver<()>,
    queued: oneshot::Sender<()>,
}

#[cfg(any(test, feature = "test-observation"))]
static FRAME_LIMIT_EXCEEDED_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(any(test, feature = "test-observation"))]
pub(crate) fn frame_limit_exceeded_count_for_test() -> u64 {
    FRAME_LIMIT_EXCEEDED_COUNT.load(Ordering::Relaxed)
}

#[derive(Debug, Default)]
pub(crate) struct ProviderFrameObservation {
    limit_exceeded: AtomicBool,
    #[cfg(feature = "test-observation")]
    managed_permission_enqueued:
        std::sync::Mutex<Option<agent_client_protocol::schema::v1::RequestId>>,
    #[cfg(feature = "test-observation")]
    held_permission_reply: std::sync::Mutex<Option<HeldPermissionReply>>,
}

impl ProviderFrameObservation {
    pub(crate) fn record_limit_exceeded(&self) {
        #[cfg(any(test, feature = "test-observation"))]
        FRAME_LIMIT_EXCEEDED_COUNT.fetch_add(1, Ordering::Relaxed);
        self.limit_exceeded.store(true, Ordering::Relaxed);
    }

    pub(crate) fn limit_was_exceeded(&self) -> bool {
        self.limit_exceeded.load(Ordering::Relaxed)
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn record_managed_permission_enqueue(
        &self,
        id: agent_client_protocol::schema::v1::RequestId,
    ) {
        *self
            .managed_permission_enqueued
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(id);
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn managed_permission_enqueued(
        &self,
    ) -> Option<agent_client_protocol::schema::v1::RequestId> {
        self.managed_permission_enqueued.lock().ok()?.clone()
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn hold_permission_reply(&self, id: RequestId) -> PermissionReplyProbe {
        let (reached, observe) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let (queued, observe_queue) = oneshot::channel();
        let old = self
            .held_permission_reply
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(HeldPermissionReply {
                id,
                reached,
                release: wait,
                queued,
            });
        drop(old);
        (observe, release, observe_queue)
    }

    #[cfg(feature = "test-observation")]
    pub(crate) async fn before_permission_reply(
        &self,
        id: &RequestId,
        peer: RequestCancellation,
    ) -> Option<oneshot::Sender<()>> {
        let held = self
            .held_permission_reply
            .lock()
            .ok()?
            .take_if(|held| held.id == *id)?;
        let _observer = held.reached.send(peer);
        let _observer = held.release.await;
        Some(held.queued)
    }
}
