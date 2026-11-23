//! Observes an outbox wake alongside the receipt wake on the same injected clock.

use chrono::{DateTime, Utc};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use runtime_primitives::{Clock, SyncTime};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::watch;

/// These tests observe second-mark sleepers while delegating time to their one manual clock.
pub(super) struct NextMarkClock {
    pub(super) source: Arc<ManualClock>,
    watched_mark: ClockInstant,
    pending_wakes: watch::Sender<usize>,
}

struct MarkWait<'clock>(&'clock watch::Sender<usize>);

impl Drop for MarkWait<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|wakes| *wakes -= 1);
    }
}

impl Clock for NextMarkClock {
    fn now(&self) -> SyncTime {
        self.source.now()
    }
}

impl RuntimeClock for NextMarkClock {
    fn utc_now(&self) -> DateTime<Utc> {
        self.source.utc_now()
    }

    fn monotonic_now(&self) -> ClockInstant {
        self.source.monotonic_now()
    }

    fn sleep_until(&self, deadline: ClockInstant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let sleeping = self.source.sleep_until(deadline);
        if deadline != self.watched_mark {
            return sleeping;
        }
        Box::pin(async move {
            self.pending_wakes.send_modify(|wakes| *wakes += 1);
            let _wait = MarkWait(&self.pending_wakes);
            sleeping.await;
        })
    }
}

impl NextMarkClock {
    pub(super) fn watching(source: Arc<ManualClock>, watched_mark: ClockInstant) -> Arc<Self> {
        Arc::new(Self {
            source,
            watched_mark,
            pending_wakes: watch::channel(0).0,
        })
    }

    pub(super) async fn suffix_is_deferred(&self) {
        // The receipt wait is always armed for this minute. A second live wait
        // proves the worker scheduled another outbox frame on that later mark.
        let mut wakes = self.pending_wakes.subscribe();
        assert!(wakes.wait_for(|wakes| *wakes == 2).await.is_ok());
    }
}
