//! Owns the eight accepted work calls, their deadlines, and their exact socket identities.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use central_link_client::ConnectionAttempt;
use central_link_protocol::{
    CentralCall, CentralCallReply, CorrelationId, FreshCorrelationId, MacReplyExpectation,
    MacWorkCall, TypedCallResult,
};
use router_clock::{ClockInstant, RuntimeClock};
use state_machine_core::StateMachine;
use tokio::sync::{Notify, oneshot};

use crate::{
    central_call_policy::{
        CallEvent, CallGate, CallGateEvent, CallGatePolicy, CallPhase, CallPolicy, FrameExposure,
    },
    wire_write_fence::WireWriteFence,
};

pub use crate::central_call_policy::{CallEnding, CallNotReached, CallSettlement};

const CALL_CAPACITY: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAdmissionRefusal {
    Unavailable,
    EnrollmentEnded,
}

pub enum CentralCallSubmission {
    Accepted(AcceptedCentralCall),
    RefusedByLink(CallAdmissionRefusal),
    NotReached(CallNotReached),
}

/// May submit a work call to the socket that currently owns registration.
#[derive(Clone, Default)]
pub struct CentralCallHandle(Arc<CallQueue>);

/// A receipt for an accepted call. Its ending belongs to this connection.
pub struct AcceptedCentralCall {
    handle: CentralCallHandle,
    key: CallKey,
    reply: oneshot::Receiver<CallSettlement>,
}

impl AcceptedCentralCall {
    pub fn connection(&self) -> &ConnectionAttempt {
        &self.key.connection
    }

    pub async fn resolve(mut self) -> CallSettlement {
        (&mut self.reply)
            .await
            .unwrap_or(CallSettlement::AnswerLost(CallEnding::DriverDropped))
    }
}

impl Drop for AcceptedCentralCall {
    fn drop(&mut self) {
        self.reply.close();
        self.handle.cancel_waiter(&self.key);
    }
}

#[derive(Clone, Eq, PartialEq)]
struct CallKey {
    connection: ConnectionAttempt,
    correlation: CorrelationId,
}

struct CallEntry {
    key: CallKey,
    deadline: ClockInstant,
    phase: CallPhase,
    queued: Option<Box<CentralCall>>,
    expected: MacReplyExpectation,
    reply: oneshot::Sender<CallSettlement>,
}

#[derive(Default)]
struct Calls {
    gate: CallGate,
    clock: Option<Arc<dyn RuntimeClock>>,
    entries: VecDeque<CallEntry>,
}

#[derive(Default)]
struct CallQueue {
    state: Mutex<Calls>,
    writer_changed: Notify,
    deadlines_changed: Notify,
}

impl CallQueue {
    fn lock(&self) -> MutexGuard<'_, Calls> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Calls {
    fn apply_gate(&mut self, event: CallGateEvent) -> bool {
        let Ok((next, _)) = CallGatePolicy::transition(&self.gate, event) else {
            return false;
        };
        self.gate = next;
        true
    }

    fn apply_call(&mut self, index: usize, event: CallEvent) -> bool {
        let Some(entry) = self.entries.get_mut(index) else {
            return false;
        };
        let Ok((next, effects)) = CallPolicy::transition(&entry.phase, event) else {
            return false;
        };
        entry.phase = next;
        if let Some(ending) = effects.into_iter().next()
            && let Some(entry) = self.entries.remove(index)
        {
            // The entry is the only slot. Remove it before making the outcome visible.
            let _ = entry.reply.send(ending);
        }
        true
    }

    fn expire(&mut self, now: ClockInstant) -> Option<ClockInstant> {
        let mut next = None;
        for index in (0..self.entries.len()).rev() {
            let Some(entry) = self.entries.get(index) else {
                continue;
            };
            // The active writer owns its deadline and evidence until it relinquishes the frame.
            if entry.phase == CallPhase::Writing {
                continue;
            }
            if now >= entry.deadline {
                self.apply_call(index, CallEvent::End(CallEnding::Deadline));
            } else {
                next = Some(next.map_or(entry.deadline, |current: ClockInstant| {
                    current.min(entry.deadline)
                }));
            }
        }
        next
    }

    fn end_attempt(&mut self, connection: &ConnectionAttempt, cause: CallEnding) {
        for index in (0..self.entries.len()).rev() {
            if self
                .entries
                .get(index)
                .is_some_and(|entry| &entry.key.connection == connection)
            {
                self.apply_call(index, CallEvent::End(cause));
            }
        }
    }
}

impl CentralCallHandle {
    pub fn submit(&self, request: MacWorkCall, deadline: ClockInstant) -> CentralCallSubmission {
        let mut calls = self.0.lock();
        let connection = match &calls.gate {
            CallGate::Unavailable => {
                return CentralCallSubmission::RefusedByLink(CallAdmissionRefusal::Unavailable);
            }
            CallGate::EnrollmentEnded => {
                return CentralCallSubmission::RefusedByLink(CallAdmissionRefusal::EnrollmentEnded);
            }
            CallGate::Open(connection) => connection.clone(),
        };
        let Some(clock) = calls.clock.clone() else {
            return CentralCallSubmission::RefusedByLink(CallAdmissionRefusal::Unavailable);
        };
        let now = clock.monotonic_now();
        calls.expire(now);
        if now >= deadline {
            return CentralCallSubmission::NotReached(CallNotReached::Ended(CallEnding::Deadline));
        }
        if calls.entries.len() >= CALL_CAPACITY {
            return CentralCallSubmission::NotReached(CallNotReached::LaneFull);
        }
        let Ok((wire, expected)) = request.prepare() else {
            return CentralCallSubmission::NotReached(CallNotReached::InvalidFrame);
        };
        let key = CallKey {
            connection,
            correlation: FreshCorrelationId::mint_at(clock.utc_now().timestamp_millis())
                .wire_id()
                .clone(),
        };
        let (reply, received) = oneshot::channel();
        calls.entries.push_back(CallEntry {
            key: key.clone(),
            deadline,
            phase: CallPhase::Queued,
            queued: Some(Box::new(wire)),
            expected,
            reply,
        });
        self.0.writer_changed.notify_one();
        self.0.deadlines_changed.notify_one();
        CentralCallSubmission::Accepted(AcceptedCentralCall {
            handle: self.clone(),
            key,
            reply: received,
        })
    }

    fn cancel_waiter(&self, key: &CallKey) {
        let mut calls = self.0.lock();
        if let Some(index) = calls
            .entries
            .iter()
            .position(|entry| &entry.key == key && entry.phase != CallPhase::Writing)
        {
            calls.apply_call(index, CallEvent::End(CallEnding::CallerCancelled));
            self.0.deadlines_changed.notify_one();
        }
    }

    pub(super) fn attach_clock(&self, clock: Arc<dyn RuntimeClock>) {
        self.0.lock().clock = Some(clock);
    }

    pub(super) fn new_binding(&self) {
        self.0.lock().apply_gate(CallGateEvent::NewBinding);
    }

    pub(super) fn enrollment_ended(&self) {
        self.0.lock().apply_gate(CallGateEvent::EndEnrollment);
    }

    pub(super) fn registered(&self, connection: ConnectionAttempt) {
        self.0
            .lock()
            .apply_gate(CallGateEvent::Registered(connection));
    }

    pub(super) fn retire(&self, connection: &ConnectionAttempt, cause: CallEnding) {
        let mut calls = self.0.lock();
        if matches!(&calls.gate, CallGate::Open(current) if current == connection) {
            let event = if matches!(cause, CallEnding::EnrollmentEnded(_)) {
                CallGateEvent::EndEnrollment
            } else {
                CallGateEvent::Withdraw(connection.clone())
            };
            calls.apply_gate(event);
        }
        calls.end_attempt(connection, cause);
        self.0.deadlines_changed.notify_one();
    }

    pub(super) async fn watch_deadlines(&self, clock: &dyn RuntimeClock) {
        loop {
            let changed = self.0.deadlines_changed.notified();
            let next = self.0.lock().expire(clock.monotonic_now());
            if let Some(deadline) = next {
                tokio::select! {
                    () = changed => {},
                    () = clock.sleep_until(deadline) => {},
                }
            } else {
                changed.await;
            }
        }
    }

    pub(super) async fn ready_to_write(&self, attempt: &ConnectionAttempt) {
        loop {
            let changed = self.0.writer_changed.notified();
            if self
                .0
                .lock()
                .entries
                .iter()
                .any(|entry| entry.phase == CallPhase::Queued && &entry.key.connection == attempt)
            {
                return;
            }
            changed.await;
        }
    }

    pub(super) fn begin_write<'socket>(
        &self,
        connection: &ConnectionAttempt,
        now: ClockInstant,
        fence: &'socket WireWriteFence,
    ) -> Option<CallWrite<'socket>> {
        let mut calls = self.0.lock();
        calls.expire(now);
        if !matches!(&calls.gate, CallGate::Open(current) if current == connection) {
            return None;
        }
        let index = calls.entries.iter().position(|entry| {
            entry.phase == CallPhase::Queued && &entry.key.connection == connection
        })?;
        if !calls.apply_call(index, CallEvent::BeginWrite) {
            return None;
        }
        let entry = calls.entries.get_mut(index)?;
        let request = entry.queued.take()?;
        Some(CallWrite {
            handle: self.clone(),
            key: entry.key.clone(),
            deadline: entry.deadline,
            request: Some(request),
            fence,
            finished: false,
        })
    }

    pub(super) fn answer(
        &self,
        attempt: &ConnectionAttempt,
        correlation: &CorrelationId,
        result: TypedCallResult<CentralCallReply>,
        now: ClockInstant,
    ) {
        let mut calls = self.0.lock();
        calls.expire(now);
        let Some(index) = calls.entries.iter().position(|entry| {
            entry.phase == CallPhase::Written
                && &entry.key.connection == attempt
                && &entry.key.correlation == correlation
        }) else {
            log::debug!("ignored Central work reply without a waiter on its connection");
            return;
        };
        let Some(entry) = calls.entries.get(index) else {
            return;
        };
        let event = match result {
            TypedCallResult::Answered { reply } => match entry.expected.check(reply) {
                Ok(checked) => CallEvent::Answered(Box::new(checked)),
                Err(mismatch) => {
                    log::debug!("ignored mismatched Central work reply: {mismatch}");
                    return;
                }
            },
            TypedCallResult::Refused { reason } => CallEvent::RefusedByCentral(reason),
        };
        calls.apply_call(index, event);
        self.0.deadlines_changed.notify_one();
    }
}

/// The active writer alone can finish the call during framing or a partially written frame.
pub(super) struct CallWrite<'socket> {
    handle: CentralCallHandle,
    key: CallKey,
    pub(super) deadline: ClockInstant,
    pub(super) request: Option<Box<CentralCall>>,
    fence: &'socket WireWriteFence,
    finished: bool,
}

impl CallWrite<'_> {
    pub(super) fn correlation(&self) -> &CorrelationId {
        &self.key.correlation
    }

    pub(super) fn flushed(mut self) {
        self.settle(CallEvent::FlushCompleted);
    }

    pub(super) fn ended(mut self, cause: CallEnding) {
        self.finish_with_evidence(cause);
    }

    fn finish_with_evidence(&mut self, cause: CallEnding) {
        let exposure = if self.fence.crossed() {
            FrameExposure::PossiblyWritten
        } else {
            FrameExposure::Unwritten
        };
        self.settle(CallEvent::WriteEnded { exposure, cause });
    }

    fn settle(&mut self, event: CallEvent) {
        let mut calls = self.handle.0.lock();
        if let Some(index) = calls.entries.iter().position(|entry| entry.key == self.key) {
            calls.apply_call(index, event);
            if calls.entries.get(index).is_some_and(|entry| {
                entry.key == self.key
                    && entry.phase == CallPhase::Written
                    && entry.reply.is_closed()
            }) {
                calls.apply_call(index, CallEvent::End(CallEnding::CallerCancelled));
            }
        }
        self.finished = true;
        self.handle.0.deadlines_changed.notify_one();
    }
}

impl Drop for CallWrite<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_with_evidence(CallEnding::DriverDropped);
        }
    }
}

#[cfg(test)]
#[path = "central_call_lane_test.rs"]
mod tests;
