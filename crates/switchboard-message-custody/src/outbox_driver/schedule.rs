//! The link, wake and single batch-slot lifecycle for outgoing minute marks.

use crate::flush_marks::{earliest_open_mark, mark_in_window, next_mark_after};
use runtime_primitives::SyncTime;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use std::{collections::BTreeSet, sync::Arc};

/// Minute-flush state advanced by custody from link and store observations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OutboxFlushState {
    link: LinkAvailability,
    consumed_marks: Arc<BTreeSet<SyncTime>>,
    /// The mark of the batch on the wire, if any.
    in_flight: Option<SyncTime>,
    draining_mark: Option<SyncTime>,
    armed_wake: Option<SyncTime>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum LinkAvailability {
    #[default]
    Disconnected,
    Connected,
}

/// An observation the outbox driver supplies to the minute scheduler.
/// `earliest_due` is the store's earliest receipt mark among waiting writes,
/// read by the Host just before the event; None when nothing waits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FlushEvent {
    /// The Host stored a write for this mark.
    Stored {
        flush_at: SyncTime,
    },
    /// The Host's wake fired, normally on a mark plus its jitter.
    Tick {
        now: SyncTime,
        earliest_due: Option<SyncTime>,
    },
    LinkConnected {
        now: SyncTime,
        earliest_due: Option<SyncTime>,
    },
    LinkLost,
    /// A replacement connection ends the previous connection's open drain.
    DrainFenced,
    /// The batch for this flush ended: answered, returned to the store, or
    /// never sent because the claim found nothing or the store failed.
    BatchEnded {
        earliest_due: Option<SyncTime>,
        continue_mark: bool,
    },
}

/// A request from the scheduler for custody to arm a wake or claim a batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FlushEffect {
    /// Claim the next due writes (up to the link frame limit) from the store and
    /// send them as one batch frame.
    SendBatch { mark: SyncTime },
    /// Wake at this mark plus the Host's ±3 s jitter and send a Tick.
    WakeAt { mark: SyncTime },
}

/// A scheduler refusal for an event that cannot apply to the current batch.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FlushTransitionError {
    #[error("no batch is in flight")]
    NoBatchInFlight,
}

impl OutboxFlushState {
    /// The first eligible mark for a write stored at `now`.
    #[must_use]
    pub fn receipt_mark(&self, now: SyncTime) -> SyncTime {
        self.first_unused_mark(earliest_open_mark(now, None))
    }

    #[must_use]
    pub const fn batch_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    #[must_use]
    pub fn last_flushed(&self) -> Option<SyncTime> {
        self.consumed_marks.last().copied()
    }

    #[must_use]
    pub(crate) const fn armed_mark(&self) -> Option<SyncTime> {
        self.armed_wake
    }

    pub(crate) const fn draining_mark(&self) -> Option<SyncTime> {
        self.draining_mark
    }

    fn stop_draining_mark(&mut self) {
        self.draining_mark = None;
    }

    /// A backward Welcome can select older marks the process never used. A mark
    /// already consumed by this worker is skipped even after clock turnover.
    /// If all remaining windows were consumed, the independent deadline still ends the write.
    fn first_unused_mark(&self, mut candidate: SyncTime) -> SyncTime {
        while self.consumed_marks.contains(&candidate) {
            let next = next_mark_after(candidate);
            if next <= candidate {
                break;
            }
            candidate = next;
        }
        candidate
    }
}

pub fn transition(
    state: OutboxFlushState,
    event: FlushEvent,
) -> Result<(OutboxFlushState, Vec<FlushEffect>), FlushTransitionError> {
    OutboxSchedule::transition(&state, event).map_err(|_| FlushTransitionError::NoBatchInFlight)
}

pub(super) fn advance_schedule(
    state: &mut OutboxFlushState,
    event: FlushEvent,
) -> Result<Vec<FlushEffect>, FlushTransitionError> {
    let (next, effects) = OutboxSchedule::transition(state, event)
        .map_err(|_| FlushTransitionError::NoBatchInFlight)?;
    *state = next;
    Ok(effects)
}

struct OutboxSchedule;

#[derive(Clone, Copy)]
enum SchedulePhase {
    Disconnected,
    Idle,
    WakeArmed,
    DrainOpen,
    DrainArmed,
    SlotReserved,
}

impl SchedulePhase {
    const fn tag(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Idle => "idle",
            Self::WakeArmed => "wakeArmed",
            Self::DrainOpen => "drainOpen",
            Self::DrainArmed => "drainArmed",
            Self::SlotReserved => "slotReserved",
        }
    }
}

impl StateTag for OutboxFlushState {
    fn state_tag(&self) -> &'static str {
        let phase = match (
            self.link,
            self.in_flight,
            self.draining_mark,
            self.armed_wake,
        ) {
            (LinkAvailability::Disconnected, _, _, _) => SchedulePhase::Disconnected,
            (LinkAvailability::Connected, Some(_), _, _) => SchedulePhase::SlotReserved,
            (LinkAvailability::Connected, None, None, None) => SchedulePhase::Idle,
            (LinkAvailability::Connected, None, None, Some(_)) => SchedulePhase::WakeArmed,
            (LinkAvailability::Connected, None, Some(_), None) => SchedulePhase::DrainOpen,
            (LinkAvailability::Connected, None, Some(_), Some(_)) => SchedulePhase::DrainArmed,
        };
        phase.tag()
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

const STORED: &str = "stored";
const TICK: &str = "tick";
const LINK_CONNECTED: &str = "linkConnected";
const LINK_LOST: &str = "linkLost";
const DRAIN_FENCED: &str = "drainFenced";
const BATCH_ENDED: &str = "batchEnded";

impl EventTag for FlushEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Stored { .. } => STORED,
            Self::Tick { .. } => TICK,
            Self::LinkConnected { .. } => LINK_CONNECTED,
            Self::LinkLost => LINK_LOST,
            Self::DrainFenced => DRAIN_FENCED,
            Self::BatchEnded { .. } => BATCH_ENDED,
        }
    }
}

impl RefusalTag for FlushTransitionError {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::NoBatchInFlight => "noBatchInFlight",
        }
    }
}

const SCHEDULE_TRANSITIONS: &[Transition] = {
    use SchedulePhase::{Disconnected, DrainArmed, DrainOpen, Idle, SlotReserved, WakeArmed};
    &[
        edge(Disconnected, STORED, Disconnected),
        edge(Disconnected, TICK, Disconnected),
        edge(Disconnected, LINK_CONNECTED, Idle),
        edge(Disconnected, LINK_CONNECTED, WakeArmed),
        edge(Disconnected, LINK_LOST, Disconnected),
        edge(Disconnected, DRAIN_FENCED, Disconnected),
        edge(Idle, STORED, WakeArmed),
        edge(Idle, TICK, Idle),
        edge(Idle, TICK, WakeArmed),
        edge(Idle, TICK, SlotReserved),
        edge(Idle, LINK_CONNECTED, Idle),
        edge(Idle, LINK_CONNECTED, WakeArmed),
        edge(Idle, LINK_LOST, Disconnected),
        edge(Idle, DRAIN_FENCED, Idle),
        edge(WakeArmed, STORED, WakeArmed),
        edge(WakeArmed, TICK, Idle),
        edge(WakeArmed, TICK, WakeArmed),
        edge(WakeArmed, TICK, SlotReserved),
        edge(WakeArmed, LINK_CONNECTED, Idle),
        edge(WakeArmed, LINK_CONNECTED, WakeArmed),
        edge(WakeArmed, LINK_LOST, Disconnected),
        edge(WakeArmed, DRAIN_FENCED, WakeArmed),
        edge(DrainOpen, STORED, DrainArmed),
        edge(DrainOpen, TICK, Idle),
        edge(DrainOpen, TICK, WakeArmed),
        edge(DrainOpen, TICK, DrainOpen),
        edge(DrainOpen, TICK, DrainArmed),
        edge(DrainOpen, TICK, SlotReserved),
        edge(DrainOpen, LINK_CONNECTED, Idle),
        edge(DrainOpen, LINK_CONNECTED, WakeArmed),
        edge(DrainOpen, LINK_CONNECTED, DrainOpen),
        edge(DrainOpen, LINK_CONNECTED, DrainArmed),
        edge(DrainOpen, LINK_LOST, Disconnected),
        edge(DrainOpen, DRAIN_FENCED, Idle),
        edge(DrainArmed, STORED, DrainArmed),
        edge(DrainArmed, TICK, Idle),
        edge(DrainArmed, TICK, WakeArmed),
        edge(DrainArmed, TICK, DrainOpen),
        edge(DrainArmed, TICK, DrainArmed),
        edge(DrainArmed, TICK, SlotReserved),
        edge(DrainArmed, LINK_CONNECTED, Idle),
        edge(DrainArmed, LINK_CONNECTED, WakeArmed),
        edge(DrainArmed, LINK_CONNECTED, DrainOpen),
        edge(DrainArmed, LINK_CONNECTED, DrainArmed),
        edge(DrainArmed, LINK_LOST, Disconnected),
        edge(DrainArmed, DRAIN_FENCED, WakeArmed),
        edge(SlotReserved, STORED, SlotReserved),
        edge(SlotReserved, TICK, SlotReserved),
        edge(SlotReserved, LINK_CONNECTED, SlotReserved),
        edge(SlotReserved, LINK_LOST, Disconnected),
        edge(SlotReserved, DRAIN_FENCED, SlotReserved),
        edge(SlotReserved, BATCH_ENDED, Idle),
        edge(SlotReserved, BATCH_ENDED, WakeArmed),
        edge(SlotReserved, BATCH_ENDED, DrainOpen),
        edge(SlotReserved, BATCH_ENDED, DrainArmed),
    ]
};

const fn edge(from: SchedulePhase, event: &'static str, to: SchedulePhase) -> Transition {
    Transition {
        from: from.tag(),
        event,
        to: to.tag(),
        terminal: false,
    }
}

impl StateMachine for OutboxSchedule {
    type State = OutboxFlushState;
    type Event = FlushEvent;
    type Effect = FlushEffect;
    type Error = FlushTransitionError;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        let mut next = state.clone();
        let mut effects = Vec::new();
        let mut tick_now = None;
        let earliest_due = match event {
            FlushEvent::Stored { flush_at } => {
                // Keep an earlier wake when a new write is queued.
                Some(
                    next.armed_wake
                        .map_or(flush_at, |armed| armed.min(flush_at)),
                )
            }
            FlushEvent::Tick { now, earliest_due } => {
                tick_now = Some(now);
                discard_closed_drain(&mut next, now);
                next.armed_wake = None;
                if let Some(mark) = flush_on_mark(&mut next, now, earliest_due) {
                    effects.push(FlushEffect::SendBatch { mark });
                }
                earliest_due
            }
            FlushEvent::LinkConnected { now, earliest_due } => {
                next.link = LinkAvailability::Connected;
                discard_closed_drain(&mut next, now);
                tick_now = Some(now);
                earliest_due
            }
            FlushEvent::LinkLost => {
                next.link = LinkAvailability::Disconnected;
                next.in_flight = None;
                next.stop_draining_mark();
                next.armed_wake = None;
                None
            }
            FlushEvent::DrainFenced => {
                next.stop_draining_mark();
                return Ok((next, effects));
            }
            FlushEvent::BatchEnded {
                earliest_due,
                continue_mark,
            } => {
                next.in_flight.take().ok_or(TransitionError::Domain(
                    FlushTransitionError::NoBatchInFlight,
                ))?;
                if !continue_mark {
                    next.stop_draining_mark();
                }
                earliest_due
            }
        };
        if let Some(mark) = rearm(&mut next, earliest_due, tick_now) {
            effects.push(FlushEffect::WakeAt { mark });
        }
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        SCHEDULE_TRANSITIONS
    }
}

fn flush_on_mark(
    state: &mut OutboxFlushState,
    now: SyncTime,
    earliest_due: Option<SyncTime>,
) -> Option<SyncTime> {
    let mark = mark_in_window(now)?;
    let open = state.draining_mark == Some(mark) || !state.consumed_marks.contains(&mark);
    let ready = state.link == LinkAvailability::Connected && state.in_flight.is_none();
    let has_due = earliest_due.is_some_and(|due| due <= mark);
    if !(open && ready && has_due) {
        return None;
    }
    if !state.consumed_marks.contains(&mark) {
        Arc::make_mut(&mut state.consumed_marks).insert(mark);
    }
    state.draining_mark = Some(mark);
    state.in_flight = Some(mark);
    Some(mark)
}

fn discard_closed_drain(state: &mut OutboxFlushState, now: SyncTime) {
    if state.draining_mark != mark_in_window(now) {
        state.stop_draining_mark();
    }
}

/// The next mark worth waking for, if it differs from the armed one. Nothing
/// is armed while the link is down, a batch is in flight or nothing waits;
/// the event that ends each of those re-arms.
fn rearm(
    state: &mut OutboxFlushState,
    earliest_due: Option<SyncTime>,
    tick_now: Option<SyncTime>,
) -> Option<SyncTime> {
    let target = match (state.link, state.in_flight, earliest_due) {
        (LinkAvailability::Connected, None, Some(earliest_due)) => {
            let candidate = tick_now.map_or(earliest_due, |now| {
                earliest_due.max(earliest_open_mark(now, None))
            });
            if let Some(mark) = state.draining_mark.filter(|mark| candidate <= *mark) {
                Some(mark)
            } else {
                Some(state.first_unused_mark(candidate))
            }
        }
        (LinkAvailability::Disconnected, _, _) | (_, Some(_), _) | (_, None, None) => None,
    };
    if target == state.armed_wake {
        return None;
    }
    state.armed_wake = target;
    target
}

#[cfg(test)]
#[path = "schedule_test.rs"]
mod tests;
