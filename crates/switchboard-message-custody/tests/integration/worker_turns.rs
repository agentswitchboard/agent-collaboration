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
    custody_task::{CustodyRunGroup, CustodyTaskEvent, CustodyTaskHandle, custody_task},
    outbox_dispatch::FlushReport,
};
use tokio::sync::{mpsc, watch};

use super::custody_task::ObserveTask;

/// Tests use this clock decorator to observe the worker parked with a timer armed.
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
            let mut timer = pin!(self.manual.sleep_until(deadline));
            poll_fn(|context| {
                let result = timer.as_mut().poll(context);
                if result.is_pending() {
                    self.parked.send_modify(|(polls, elapsed)| {
                        *polls += 1;
                        *elapsed = self.monotonic_now();
                    });
                }
                result
            })
            .await;
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

    async fn wait_for_armed_timer(&self, before: usize) {
        let now = self.monotonic_now();
        let mut parked = self.parked.subscribe();
        let mut after = before;
        loop {
            let (sequence, _) = *parked
                .wait_for(|(polls, elapsed)| *polls > after && *elapsed >= now)
                .await
                .unwrap();
            // These fixtures use a current-thread runtime. The test can only
            // observe this signal after the worker yields. A selected branch
            // drops its timer before SQLite or any other operation can park.
            if self.timers.load(Ordering::Relaxed) != 0 {
                return;
            }
            after = sequence;
        }
    }

    #[expect(
        clippy::panic,
        reason = "The worker must park with a registered clock timer."
    )]
    pub(super) async fn wait_after(&self, before: usize) {
        tokio::select! {
            biased;
            () = self.wait_for_armed_timer(before) => {}
            () = async { for _ in 0..50_000 { tokio::task::yield_now().await; } } => {
                panic!("custody worker never parked with an armed timer");
            }
        }
    }
}

/// Tests hold this running custody task and its report stream.
pub(super) struct ObservedWorker {
    pub(super) calls: CustodyTaskHandle,
    pub(super) events: mpsc::UnboundedReceiver<CustodyTaskEvent>,
    pub(super) clock: Arc<WorkerClock>,
    group: CustodyRunGroup,
}

impl ObservedWorker {
    pub(super) fn start(custody: CustodyLink, clock: Arc<WorkerClock>) -> Self {
        let (events_sink, events) = mpsc::unbounded_channel();
        let (calls, group) = custody_task(custody, Arc::new(ObserveTask(events_sink)));
        Self {
            calls,
            events,
            clock,
            group,
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
        self.group.request_stop();
        self.group.join().await.unwrap();
    }
}
