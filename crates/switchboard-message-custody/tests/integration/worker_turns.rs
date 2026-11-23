//! Drive the real worker with an injected clock and observe its parking boundary.

#![expect(
    clippy::unwrap_used,
    reason = "A failed clock step or task makes the test invalid."
)]

use chrono::{DateTime, Utc};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use runtime_primitives::{Clock, SyncTime};
use std::{
    future::{Future, poll_fn},
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use switchboard_message_custody::{
    custody_link::{CustodyLink, CustodyLinkError},
    custody_task::{CustodyTaskEvent, CustodyTaskHandle, custody_task},
    outbox_dispatch::FlushReport,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use super::custody_task::ObserveTask;

/// Tests use this clock decorator to observe a complete poll of the worker's select.
pub(super) struct WorkerClock {
    pub(super) manual: Arc<ManualClock>,
    timers: AtomicUsize,
    parked: watch::Sender<(usize, ClockInstant)>,
}

struct PendingTimer<'clock>(&'clock AtomicUsize);

impl Drop for PendingTimer<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Clock for WorkerClock {
    fn now(&self) -> SyncTime {
        self.manual.now()
    }
}

impl RuntimeClock for WorkerClock {
    fn utc_now(&self) -> DateTime<Utc> {
        self.manual.utc_now()
    }

    fn monotonic_now(&self) -> ClockInstant {
        self.manual.monotonic_now()
    }

    fn sleep_until(&self, deadline: ClockInstant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.timers.fetch_add(1, Ordering::Relaxed);
            let _timer = PendingTimer(&self.timers);
            self.manual.sleep_until(deadline).await;
        })
    }
}

impl WorkerClock {
    pub(super) fn observing(manual: Arc<ManualClock>) -> Arc<Self> {
        Arc::new(Self {
            manual,
            timers: AtomicUsize::new(0),
            parked: watch::channel((0, ClockInstant::ZERO)).0,
        })
    }

    pub(super) fn checkpoint(&self) -> usize {
        self.parked.borrow().0
    }

    pub(super) async fn wait_after(&self, before: usize) {
        let now = self.monotonic_now();
        let mut parked = self.parked.subscribe();
        assert!(
            parked
                .wait_for(|(polls, elapsed)| *polls > before && *elapsed >= now)
                .await
                .is_ok()
        );
    }

    async fn observe(&self, worker: impl Future<Output = ()>) {
        // Observe a complete select poll even when this test has already
        // consumed Tokio's cooperative budget. Real SQLite awaits still park.
        let mut worker = pin!(tokio::task::unconstrained(worker));
        poll_fn(|context| {
            let result = worker.as_mut().poll(context);
            // The select drops its timer futures before entering any selected
            // branch, including branches that then wait for SQLite.
            if result.is_pending() && self.timers.load(Ordering::Relaxed) != 0 {
                self.parked.send_modify(|(polls, elapsed)| {
                    *polls += 1;
                    *elapsed = self.monotonic_now();
                });
            }
            result
        })
        .await;
    }
}

/// Tests hold this running custody task and its report stream.
pub(super) struct ObservedWorker {
    pub(super) calls: CustodyTaskHandle,
    pub(super) events: mpsc::UnboundedReceiver<CustodyTaskEvent>,
    pub(super) clock: Arc<WorkerClock>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ObservedWorker {
    pub(super) fn start(custody: CustodyLink, clock: Arc<WorkerClock>) -> Self {
        let (stop, stopped) = watch::channel(false);
        let (events_sink, events) = mpsc::unbounded_channel();
        let (calls, worker) = custody_task(custody, stopped, Arc::new(ObserveTask(events_sink)));
        let worker_clock = clock.clone();
        let task = tokio::spawn(async move { worker_clock.observe(worker).await });
        Self {
            calls,
            events,
            clock,
            stop,
            task,
        }
    }

    pub(super) async fn advance(
        &mut self,
        elapsed: Duration,
    ) -> Result<FlushReport, CustodyLinkError> {
        let before = self.clock.checkpoint();
        self.clock.manual.advance(elapsed).unwrap();
        // A clock move can make no timer due. A real tool read wakes the
        // worker in that case and asks it to reproject from the new instant.
        let caller = super::outbox_timing::thread_post(1).caller;
        self.calls.pending_writes(caller).await?;
        self.clock.wait_after(before).await;
        self.take_reports()
    }

    pub(super) fn take_reports(&mut self) -> Result<FlushReport, CustodyLinkError> {
        let mut collected = FlushReport::default();
        let mut fault = None;
        while let Ok(event) = self.events.try_recv() {
            match event {
                CustodyTaskEvent::Flush(report) => {
                    collected.answered.extend(report.answered);
                    collected.sender_notices.extend(report.sender_notices);
                    collected.ended.extend(report.ended);
                    collected.issues.extend(report.issues);
                }
                CustodyTaskEvent::Failed(error) => {
                    fault.get_or_insert(error);
                }
                CustodyTaskEvent::Receipt(_) | CustodyTaskEvent::Operator(_) => {}
            }
        }
        fault.map_or(Ok(collected), Err)
    }

    pub(super) async fn stop(self) {
        self.stop.send_replace(true);
        self.task.await.unwrap();
    }
}
