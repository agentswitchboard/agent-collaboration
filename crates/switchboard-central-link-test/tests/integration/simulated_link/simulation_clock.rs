use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use runtime_primitives::{Clock, SyncTime};
use tokio::sync::watch;
use workspace_model::Timestamp;

#[derive(Clone, Copy)]
pub(super) struct TimerPoll {
    pub(super) deadline: ClockInstant,
    pub(super) sampled_at: ClockInstant,
    pub(super) elapsed: bool,
}

/// Observes only the timers of the task that receives this view of the shared clock.
pub(super) struct ObservedClock {
    clock: Arc<ManualClock>,
    polls: watch::Sender<Vec<TimerPoll>>,
    active_timers: watch::Sender<BTreeMap<usize, ClockInstant>>,
    next_timer: AtomicUsize,
    boundary_step: Mutex<Option<ClockInstant>>,
}

impl ObservedClock {
    pub(super) fn new(clock: Arc<ManualClock>) -> Self {
        Self {
            clock,
            polls: watch::channel(Vec::new()).0,
            active_timers: watch::channel(BTreeMap::new()).0,
            next_timer: AtomicUsize::new(0),
            boundary_step: Mutex::new(None),
        }
    }

    pub(super) fn checkpoint(&self) -> usize {
        self.polls.borrow().len()
    }

    pub(super) async fn next_active_deadline(&self) -> ClockInstant {
        let now = self.monotonic_now();
        let mut active = self.active_timers.subscribe();
        let active = active
            .wait_for(|active| active.values().any(|deadline| *deadline > now))
            .await
            .unwrap();
        active
            .values()
            .copied()
            .filter(|deadline| *deadline > now)
            .min()
            .unwrap()
    }

    pub(super) async fn wait_until_pending(&self, deadline: ClockInstant) {
        let mut active = self.active_timers.subscribe();
        drop(
            active
                .wait_for(|active| active.values().any(|candidate| *candidate == deadline))
                .await
                .unwrap(),
        );
    }

    pub(super) async fn observe(
        &self,
        after: usize,
        is_expected: impl Fn(TimerPoll) -> bool,
    ) -> TimerPoll {
        let matches_timer = |polls: &[TimerPoll]| {
            polls
                .iter()
                .skip(after)
                .copied()
                .find(|poll| is_expected(*poll))
        };
        let mut polls = self.polls.subscribe();
        let polls = polls
            .wait_for(|polls| matches_timer(polls).is_some())
            .await
            .unwrap();
        matches_timer(&polls).unwrap()
    }

    pub(super) fn cross_after_pending_poll(&self, deadline: ClockInstant) {
        assert!(deadline > self.monotonic_now());
        assert!(
            self.boundary_step
                .lock()
                .unwrap()
                .replace(deadline)
                .is_none()
        );
    }
}

impl Clock for ObservedClock {
    fn now(&self) -> SyncTime {
        self.clock.now()
    }
}

impl RuntimeClock for ObservedClock {
    fn utc_now(&self) -> Timestamp {
        self.clock.utc_now()
    }

    fn monotonic_now(&self) -> ClockInstant {
        self.clock.monotonic_now()
    }

    fn wall_deadline(&self, deadline: SyncTime) -> ClockInstant {
        self.clock.wall_deadline(deadline)
    }

    fn sleep_until(&self, deadline: ClockInstant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let mut sleep = self.clock.sleep_until(deadline);
        let mut last_poll = None;
        let active = ActiveTimer {
            id: self.next_timer.fetch_add(1, Ordering::Relaxed),
            timers: &self.active_timers,
        };
        Box::pin(poll_fn(move |context| {
            let sampled_at = self.monotonic_now();
            let outcome = sleep.as_mut().poll(context);
            let elapsed = outcome.is_ready();
            if last_poll != Some((sampled_at, elapsed)) {
                if elapsed {
                    active.remove();
                } else {
                    active.timers.send_modify(|timers| {
                        timers.insert(active.id, deadline);
                    });
                }
                self.polls.send_modify(|polls| {
                    polls.push(TimerPoll {
                        deadline,
                        sampled_at,
                        elapsed,
                    })
                });
                last_poll = Some((sampled_at, elapsed));
            }
            // Model the boundary passing after the timer returned Pending but
            // before the writer's select polls its next branch in this turn.
            let mut boundary_step = self.boundary_step.lock().unwrap();
            if outcome == Poll::Pending && *boundary_step == Some(deadline) {
                *boundary_step = None;
                self.clock
                    .advance(deadline.saturating_duration_since(sampled_at))
                    .unwrap();
            }
            outcome
        }))
    }
}

struct ActiveTimer<'clock> {
    id: usize,
    timers: &'clock watch::Sender<BTreeMap<usize, ClockInstant>>,
}

impl ActiveTimer<'_> {
    fn remove(&self) {
        self.timers
            .send_if_modified(|timers| timers.remove(&self.id).is_some());
    }
}

impl Drop for ActiveTimer<'_> {
    fn drop(&mut self) {
        self.remove();
    }
}
