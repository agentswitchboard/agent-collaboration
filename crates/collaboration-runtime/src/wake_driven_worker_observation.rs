//! What a [`crate::WakeDrivenWorkerLoop`] reports about itself. Production keeps
//! nothing; under test-support a test sees each store read at its read site, every pass,
//! the deadline armed before each sleep, whether the loop is asleep now, a barrier for
//! "asleep" and a gate that holds the loop after it decided to sleep and before it waits.
use runtime_primitives::SyncTime;

#[derive(Clone, Default)]
pub struct WorkerObservation {
    #[cfg(any(test, feature = "test-support"))]
    observed: std::sync::Arc<observed::ObservedLoop>,
}

impl WorkerObservation {
    /// Called by a worker pass at each store read it makes.
    pub fn record_read(&self, read: &'static str) {
        #[cfg(any(test, feature = "test-support"))]
        self.observed.record_read(read);
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = read;
    }

    pub(crate) fn pass_started(&self) {
        #[cfg(any(test, feature = "test-support"))]
        self.observed.pass_started();
    }

    /// The loop woke from its sleep and is about to run a pass.
    pub(crate) fn woke(&self) {
        #[cfg(any(test, feature = "test-support"))]
        self.observed.update(|snapshot| snapshot.asleep = false);
    }

    /// The loop stopped waiting on a source that can never change again.
    pub(crate) fn source_closed(&self) {
        #[cfg(any(test, feature = "test-support"))]
        self.observed
            .update(|snapshot| snapshot.closed_sources += 1);
    }

    /// The loop is about to sleep with this deadline.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn armed(&self, deadline: Option<SyncTime>) {
        self.observed.armed(deadline).await;
    }

    /// The loop is about to sleep with this deadline.
    #[cfg(not(any(test, feature = "test-support")))]
    pub(crate) fn armed(&self, deadline: Option<SyncTime>) -> std::future::Ready<()> {
        let _ = deadline;
        std::future::ready(())
    }
}

#[cfg(any(test, feature = "test-support"))]
pub use observed::{LoopSnapshot, SleepGate, SourceWaitProbe};

#[cfg(any(test, feature = "test-support"))]
mod observed {
    use super::WorkerObservation;
    use runtime_primitives::SyncTime;
    use std::{
        future::Future,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };
    use tokio::sync::{oneshot, watch};

    /// The loop's counters at one moment.
    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub struct LoopSnapshot {
        /// Each store read, in order, named by its read site.
        pub reads: Vec<&'static str>,
        pub passes: u64,
        pub sleeps: u64,
        /// Closed sources the loop removed from its wait set.
        pub closed_sources: u64,
        /// The deadline armed before the latest sleep; `None` means no timer.
        pub armed_deadline: Option<SyncTime>,
        /// The loop is waiting now, not running a pass.
        pub asleep: bool,
    }

    struct HeldSleep {
        reached: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    }

    pub(super) struct ObservedLoop {
        snapshot: Mutex<LoopSnapshot>,
        sleeps: watch::Sender<u64>,
        held: Mutex<Option<HeldSleep>>,
        next_source_wait: Mutex<Option<HeldSourceWait>>,
    }

    impl Default for ObservedLoop {
        fn default() -> Self {
            Self {
                snapshot: Mutex::default(),
                sleeps: watch::channel(0).0,
                held: Mutex::default(),
                next_source_wait: Mutex::default(),
            }
        }
    }

    impl ObservedLoop {
        pub(super) fn update(&self, change: impl FnOnce(&mut LoopSnapshot)) {
            if let Ok(mut snapshot) = self.snapshot.lock() {
                change(&mut snapshot);
            }
        }

        pub(super) fn record_read(&self, read: &'static str) {
            self.update(|snapshot| {
                snapshot.reads.push(read);
                // A loop reads only during a pass.
                snapshot.asleep = false;
            });
        }

        pub(super) fn pass_started(&self) {
            self.update(|snapshot| {
                snapshot.passes += 1;
                snapshot.asleep = false;
            });
        }

        /// Holds at the gate if one is set, then counts the sleep with the deadline the loop
        /// armed.
        pub(super) async fn armed(&self, deadline: Option<SyncTime>) {
            let held = self.held.lock().ok().and_then(|mut held| held.take());
            if let Some(held) = held {
                let _ = held.reached.send(());
                let _ = held.release.await;
            }
            let mut sleeps = 0;
            self.update(|snapshot| {
                snapshot.sleeps += 1;
                snapshot.armed_deadline = deadline;
                snapshot.asleep = true;
                sleeps = snapshot.sleeps;
            });
            self.sleeps.send_replace(sleeps);
        }
    }

    #[derive(Default)]
    struct WaitWake(AtomicBool);

    impl Wake for WaitWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    struct HeldSourceWait {
        wake: Arc<WaitWake>,
        polled: oneshot::Sender<bool>,
        release: oneshot::Receiver<()>,
    }

    /// Holds a real worker wait after its first poll with a recording waker.
    pub struct SourceWaitProbe {
        wake: Arc<WaitWake>,
        polled: oneshot::Receiver<bool>,
        release: oneshot::Sender<()>,
    }

    impl SourceWaitProbe {
        /// Reports whether the held first poll registered a pending wait.
        pub async fn parked(&mut self) -> Result<bool, oneshot::error::RecvError> {
            (&mut self.polled).await
        }

        #[must_use]
        pub fn was_woken(&self) -> bool {
            self.wake.0.load(Ordering::Acquire)
        }

        pub fn release(self) {
            let _test_released_probe = self.release.send(());
        }
    }

    /// Holds the loop after it decided to sleep and before it waits.
    pub struct SleepGate {
        reached: oneshot::Receiver<()>,
        release: oneshot::Sender<()>,
    }

    impl SleepGate {
        /// Resolves once the loop is held at the gate.
        pub async fn reached(&mut self) {
            let _ = (&mut self.reached).await;
        }

        /// Lets the held loop go to sleep.
        pub fn release(self) {
            let _ = self.release.send(());
        }
    }

    impl WorkerObservation {
        /// Probes the next real wait while it is paused after polling the sources.
        #[must_use]
        pub fn probe_next_source_wait(&self) -> SourceWaitProbe {
            let (polled, first_poll) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let wake = Arc::new(WaitWake::default());
            *self
                .observed
                .next_source_wait
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(HeldSourceWait {
                wake: wake.clone(),
                polled,
                release: released,
            });
            SourceWaitProbe {
                wake,
                polled: first_poll,
                release,
            }
        }

        pub(crate) async fn poll_source_wait(
            &self,
            wait: impl Future<Output = bool> + Send,
        ) -> bool {
            let held = self
                .observed
                .next_source_wait
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            let Some(held) = held else {
                return wait.await;
            };
            let mut wait = std::pin::pin!(wait);
            let waker = Waker::from(held.wake);
            let first = wait.as_mut().poll(&mut Context::from_waker(&waker));
            let _reader_closed = held.polled.send(first.is_pending());
            let _test_released_probe = held.release.await;
            match first {
                Poll::Pending => wait.await,
                Poll::Ready(ready) => ready,
            }
        }

        #[must_use]
        pub fn snapshot(&self) -> LoopSnapshot {
            self.observed
                .snapshot
                .lock()
                .map(|snapshot| snapshot.clone())
                .unwrap_or_default()
        }

        /// Resolves once the loop has gone to sleep at least `count` times; a loop that
        /// never started never resolves it.
        pub async fn asleep_at_least(&self, count: u64) {
            let mut sleeps = self.observed.sleeps.subscribe();
            let _ = sleeps.wait_for(|sleeps| *sleeps >= count).await;
        }

        /// Holds the loop the next time it is about to sleep.
        #[must_use]
        pub fn hold_before_next_sleep(&self) -> SleepGate {
            let (reached_sender, reached) = oneshot::channel();
            let (release, release_receiver) = oneshot::channel();
            if let Ok(mut held) = self.observed.held.lock() {
                *held = Some(HeldSleep {
                    reached: reached_sender,
                    release: release_receiver,
                });
            }
            SleepGate { reached, release }
        }
    }
}
