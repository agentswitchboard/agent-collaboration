//! Drive the real worker with an injected clock and observe its parking boundary.

#![expect(
    clippy::unwrap_used,
    reason = "A failed clock step or task makes the test invalid."
)]

use chrono::{DateTime, Utc};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use runtime_primitives::{Clock, SyncTime};
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    pin::{Pin, pin},
    sync::{Arc, Mutex},
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
    timers: Mutex<BTreeMap<ClockInstant, ArmedTimer>>,
    parked: watch::Sender<usize>,
}

#[derive(Default)]
struct ArmedTimer {
    count: usize,
    last_poll: Option<(usize, ClockInstant)>,
}

struct PendingTimer<'clock> {
    clock: &'clock WorkerClock,
    deadline: ClockInstant,
}

impl Drop for PendingTimer<'_> {
    fn drop(&mut self) {
        let mut timers = self.clock.timers.lock().unwrap();
        if let Some(timer) = timers.get_mut(&self.deadline) {
            timer.count -= 1;
            if timer.count == 0 {
                timers.remove(&self.deadline);
            }
        }
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
            let mut timer = pin!(self.manual.sleep_until(deadline));
            let mut armed = None;
            poll_fn(|context| {
                let result = timer.as_mut().poll(context);
                if result.is_pending() {
                    self.parked.send_modify(|polls| {
                        *polls += 1;
                        let mut timers = self.timers.lock().unwrap();
                        let current = timers.entry(deadline).or_default();
                        current.last_poll = Some((*polls, self.monotonic_now()));
                        if armed.is_none() {
                            current.count += 1;
                            armed = Some(PendingTimer {
                                clock: self,
                                deadline,
                            });
                        }
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
            timers: Mutex::new(BTreeMap::new()),
            parked: watch::channel(0).0,
        })
    }

    pub(super) fn checkpoint(&self) -> usize {
        *self.parked.borrow()
    }

    pub(super) async fn wait_for_armed_timer(&self, before: usize, deadline: Option<ClockInstant>) {
        let now = self.monotonic_now();
        let mut parked = self.parked.subscribe();
        let mut after = before;
        loop {
            let sequence = *parked.wait_for(|polls| *polls > after).await.unwrap();
            // These fixtures use a current-thread runtime. The test can only
            // observe this signal after the worker yields. A selected branch
            // drops its timer before SQLite or any other operation can park.
            if self.timers.lock().unwrap().iter().any(|(armed, timer)| {
                deadline.is_none_or(|deadline| *armed == deadline)
                    && timer
                        .last_poll
                        .is_some_and(|(poll, elapsed)| poll > before && elapsed >= now)
            }) {
                return;
            }
            after = sequence;
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

    pub(super) async fn wait_after(&self, before: usize) {
        self.wait_for_progress(self.clock.wait_for_armed_timer(before, None))
            .await;
    }

    pub(super) async fn wait_for_timer(&self, before: usize, deadline: ClockInstant) {
        self.wait_for_progress(self.clock.wait_for_armed_timer(before, Some(deadline)))
            .await;
    }

    async fn wait_for_progress<T>(&self, witness: impl Future<Output = T>) -> T {
        tokio::select! {
            biased;
            observation = witness => observation,
            exit = self.group.join() => panic!("custody ended before the expected progress: {exit:?}"),
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
        self.wait_after(before).await;
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
