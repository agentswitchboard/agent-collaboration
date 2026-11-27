//! Exact route lifetime and its independent removal and notification.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use switchboard_harness_contracts::TransportRun;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct LifecycleRoute {
    pub run: TransportRun,
    lifetime: Arc<RouteLifetime>,
}

struct RouteLifetime {
    removed: AtomicBool,
    notify_removed: CancellationToken,
}

impl LifecycleRoute {
    pub fn new(run: TransportRun) -> Self {
        Self {
            run,
            lifetime: Arc::new(RouteLifetime {
                removed: AtomicBool::new(false),
                notify_removed: CancellationToken::new(),
            }),
        }
    }

    pub fn is_removed(&self) -> bool {
        self.lifetime.removed.load(Ordering::Acquire)
    }

    pub fn mark_removed(&self) {
        self.lifetime.removed.store(true, Ordering::Release);
    }

    pub async fn removed(&self) {
        self.lifetime.notify_removed.cancelled().await;
    }

    pub fn notify_removed(&self) {
        self.lifetime.notify_removed.cancel();
    }

    pub fn is_same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.lifetime, &other.lifetime)
    }
}
