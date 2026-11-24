//! The Worker's one clock: the platform's time, read in one place.

use fleet_runtime_ports::{Clock, SyncTime};
use std::time::Duration;

#[derive(Clone, Copy)]
pub(crate) struct CloudflareClock;

impl Clock for CloudflareClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(chrono::Utc::now().timestamp_millis())
    }
}

/// The modern object's single clock, shared with its id source.
#[derive(Clone, Default)]
pub(crate) struct WorkspaceClock {
    #[cfg(feature = "test-support")]
    held: std::sync::Arc<std::sync::Mutex<HeldClock>>,
}
#[cfg(feature = "test-support")]
#[derive(Default)]
struct HeldClock {
    time: Option<SyncTime>,
    mac_call_waits: u64,
    after_receipt_enrollment_read: Option<i64>,
    changes: Vec<futures_channel::oneshot::Sender<()>>,
}

impl WorkspaceClock {
    #[cfg(feature = "test-support")]
    fn held(&self) -> std::sync::MutexGuard<'_, HeldClock> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn hold(&self, milliseconds: i64) {
        let mut held = self.held();
        held.time = Some(SyncTime::from_unix_millis(milliseconds));
        for changed in std::mem::take(&mut held.changes) {
            let _ = changed.send(());
        }
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn move_after_receipt_enrollment_read(&self, milliseconds: i64) {
        self.held().after_receipt_enrollment_read = Some(milliseconds);
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn observe_receipt_enrollment_read(&self) {
        let milliseconds = self.held().after_receipt_enrollment_read.take();
        if let Some(milliseconds) = milliseconds {
            self.hold(milliseconds);
        }
    }

    pub(crate) async fn wait_for_mac_call(&self, deadline: SyncTime) {
        #[cfg(feature = "test-support")]
        {
            self.held().mac_call_waits += 1;
        }
        self.wait_until(deadline).await;
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn mac_call_waits(&self) -> u64 {
        self.held().mac_call_waits
    }

    pub(crate) async fn wait_until(&self, deadline: SyncTime) {
        loop {
            let now = self.now();
            if deadline <= now {
                return;
            }
            let remaining = deadline.unix_millis().saturating_sub(now.unix_millis());
            let delay = worker::Delay::from(Duration::from_millis(remaining.unsigned_abs()));
            #[cfg(not(feature = "test-support"))]
            delay.await;
            #[cfg(feature = "test-support")]
            {
                let (changed, is_held) = {
                    let mut held = self.held();
                    let (wake, changed) = futures_channel::oneshot::channel();
                    held.changes.retain(|wake| !wake.is_canceled());
                    held.changes.push(wake);
                    (changed, held.time.is_some())
                };
                if is_held {
                    // A held fixture's only source of time is the next injected sample.
                    let _ = changed.await;
                } else {
                    let _ = futures_util::future::select(changed, delay).await;
                }
            }
        }
    }
}
impl Clock for WorkspaceClock {
    fn now(&self) -> SyncTime {
        #[cfg(feature = "test-support")]
        if let Some(time) = self.held().time {
            return time;
        }
        CloudflareClock.now()
    }
}
