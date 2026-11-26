//! Bounded ACP frame-limit observation shared by connection and Session actors.

use std::sync::atomic::{AtomicBool, Ordering};

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
}
