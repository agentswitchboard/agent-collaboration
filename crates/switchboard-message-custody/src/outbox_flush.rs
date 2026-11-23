//! When the outbox flushes, as a pure reducer: `transition(state, event) ->
//! (state, effects)`. The Mac outbox table is the only truth for what waits,
//! is sent and is answered (Lead A, 2026-11-18: one owner per truth); this
//! reducer holds no request ids. It decides only WHEN a batch may go: on a
//! 1-minute mark (±3 s, the caller picks the jitter), at most one batch in
//! flight, only on a connected link. On SendBatch the Host claims the next due
//! rows from the store; a store error is not a link event, and the next mark
//! retries from the store. A settled frame can continue the same open mark.
use crate::flush_marks::{earliest_open_mark, mark_in_window, next_mark_after};
use runtime_primitives::SyncTime;
use std::collections::{BTreeMap, BTreeSet};
use workspace_ids::RequestId;

/// Custody records whether the open mark still owes recovery of a local claim.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum ClaimRecovery {
    Clear,
    ScanOwed,
    ParkedForScan,
    ParkedForReturn,
}

impl ClaimRecovery {
    pub(crate) fn park_after_empty(self) -> Self {
        match self {
            Self::ScanOwed => Self::ParkedForScan,
            other => other,
        }
    }

    pub(crate) fn after_scan(self, owes_local_return: bool) -> Self {
        match (self, owes_local_return) {
            (Self::Clear | Self::ParkedForReturn, _) => self,
            (_, false) => Self::Clear,
            (_, true) => Self::ParkedForReturn,
        }
    }
}

/// Custody remembers which new receipts must be honored during a backward Welcome rebase.
pub(crate) struct RebasedMarks {
    pub(crate) due_through: SyncTime,
    fresh_receipts: BTreeMap<RequestId, SyncTime>,
}

impl RebasedMarks {
    pub(crate) fn new(due_through: SyncTime) -> Self {
        Self {
            due_through,
            fresh_receipts: BTreeMap::new(),
        }
    }

    pub(crate) fn remember_admission(&mut self, request: RequestId, mark: SyncTime) {
        if mark <= self.due_through {
            self.fresh_receipts.entry(request).or_insert(mark);
        }
    }

    pub(crate) fn defer_fresh_receipts(&mut self, mark: SyncTime) -> BTreeSet<RequestId> {
        self.fresh_receipts.retain(|_, receipt| *receipt > mark);
        self.fresh_receipts.keys().cloned().collect()
    }
}

/// Minute-flush state advanced by custody from link and store observations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OutboxFlushState {
    link: LinkAvailability,
    consumed_marks: BTreeSet<SyncTime>,
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

/// An observation custody supplies to the minute scheduler.
/// `earliest_due` is the store's earliest receipt mark among waiting writes,
/// read by the Host just before the event; None when nothing waits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FlushEvent {
    /// The store changed the next waiting write's absolute expiry.
    LocalDeadlineChanged {
        deadline: Option<SyncTime>,
    },
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
    /// Schedule local expiry on the exact clock time, regardless of the link.
    WakeAtLocalDeadline { deadline: Option<SyncTime> },
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

    pub(crate) fn stop_draining_mark(&mut self) {
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
    mut state: OutboxFlushState,
    event: FlushEvent,
) -> Result<(OutboxFlushState, Vec<FlushEffect>), FlushTransitionError> {
    let effects = advance_schedule(&mut state, event)?;
    Ok((state, effects))
}

pub(crate) fn advance_schedule(
    state: &mut OutboxFlushState,
    event: FlushEvent,
) -> Result<Vec<FlushEffect>, FlushTransitionError> {
    let mut effects = Vec::new();
    let mut tick_now = None;
    let earliest_due = match event {
        FlushEvent::LocalDeadlineChanged { deadline } => {
            effects.push(FlushEffect::WakeAtLocalDeadline { deadline });
            return Ok(effects);
        }
        FlushEvent::Stored { flush_at } => {
            // Something waits at least this early; keep an earlier armed wake.
            Some(
                state
                    .armed_wake
                    .map_or(flush_at, |armed| armed.min(flush_at)),
            )
        }
        FlushEvent::Tick { now, earliest_due } => {
            tick_now = Some(now);
            discard_closed_drain(state, now);
            state.armed_wake = None;
            if let Some(mark) = flush_on_mark(state, now, earliest_due) {
                effects.push(FlushEffect::SendBatch { mark });
            }
            earliest_due
        }
        FlushEvent::LinkConnected { now, earliest_due } => {
            state.link = LinkAvailability::Connected;
            discard_closed_drain(state, now);
            tick_now = Some(now);
            earliest_due
        }
        FlushEvent::LinkLost => {
            state.link = LinkAvailability::Disconnected;
            state.in_flight = None;
            state.stop_draining_mark();
            state.armed_wake = None;
            None
        }
        FlushEvent::BatchEnded {
            earliest_due,
            continue_mark,
        } => {
            state
                .in_flight
                .take()
                .ok_or(FlushTransitionError::NoBatchInFlight)?;
            if !continue_mark {
                state.stop_draining_mark();
            }
            earliest_due
        }
    };
    if let Some(mark) = rearm(state, earliest_due, tick_now) {
        effects.push(FlushEffect::WakeAt { mark });
    }
    Ok(effects)
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
    state.consumed_marks.insert(mark);
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
#[path = "outbox_flush_test.rs"]
mod tests;
