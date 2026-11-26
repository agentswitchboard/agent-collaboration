//! Offer, settle, and reopen exact ACP closing routes.

use super::{OpeningAttempts, OpeningEvent, OpeningInstance, OwnerOpenings, lock, transport_run};
use provider_session_machines::{
    harness_lifecycle::{CloseFact, LifecycleRefusal, LifecycleReply},
    session_lifecycle_machine::CloseAttempt,
    session_owner::RunAttempt,
};
use std::sync::{Arc, Mutex};
use switchboard_harness_contracts::ReservedReply;

impl OwnerOpenings {
    pub(in super::super) fn close(
        &self,
        close: RunAttempt<CloseAttempt>,
        reply: LifecycleReply<CloseFact>,
    ) -> Result<(), LifecycleRefusal> {
        if let Some(refusal) = self.writer_refusal() {
            return Err(refusal);
        }
        let mut attempts = lock(&self.attempts);
        let Some(entry) = attempts.runs.get_mut(&close.generation) else {
            return Err(if let Some(create) = attempts.creates.values().next() {
                create.phase.refusal()
            } else if attempts.runs.is_empty() {
                self.idle_phase().refusal()
            } else {
                self.mismatched_call()
            });
        };
        let mut next = entry.phase;
        if !next.advance(OpeningEvent::CloseAsked) {
            return Err(entry.phase.refusal());
        }
        let Some(session) = self.session.get() else {
            return Err(entry.phase.refusal());
        };
        self.wire
            .enqueue_lifecycle_close(
                self.owner.incarnation,
                close,
                session.clone(),
                Box::new(OpeningCloseReply {
                    close,
                    instance: entry.instance.clone(),
                    attempts: self.attempts.clone(),
                    reply,
                }),
            )
            .map_err(
                |refusal| match super::super::lifecycle_wire_refusal(refusal) {
                    LifecycleRefusal::RouteUnavailable
                        if entry.phase.accepts(OpeningEvent::Retire) =>
                    {
                        LifecycleRefusal::BoundRouteUnavailable
                    }
                    other => other,
                },
            )?;
        entry.phase = next;
        entry.close_attempt = Some(close.attempt);
        Ok(())
    }

    pub(in super::super) fn admit_again(&self, close: RunAttempt<CloseAttempt>) {
        let mut attempts = lock(&self.attempts);
        if let Some(entry) = attempts.runs.get_mut(&close.generation)
            && entry.close_attempt == Some(close.attempt)
            && entry.phase.advance(OpeningEvent::AdmitAgain)
        {
            self.wire
                .reopen_route(transport_run(self.owner, close.generation), close.attempt);
            entry.close_attempt = None;
        }
    }
}

struct OpeningCloseReply {
    close: RunAttempt<CloseAttempt>,
    instance: OpeningInstance,
    attempts: Arc<Mutex<OpeningAttempts>>,
    reply: LifecycleReply<CloseFact>,
}

impl ReservedReply<CloseFact> for OpeningCloseReply {
    fn send(self: Box<Self>, fact: CloseFact) {
        let (close, event) = match fact {
            CloseFact::Closed { run } => (run, OpeningEvent::CloseConfirmed),
            CloseFact::RefusedStillLive { run } => (run, OpeningEvent::CloseRefused),
            CloseFact::Unknown { run, .. } | CloseFact::NotOffered { run } => {
                (run, OpeningEvent::CloseUnknown)
            }
        };
        if close != self.close {
            return;
        }
        let accepted = {
            let mut attempts = lock(&self.attempts);
            attempts
                .runs
                .get_mut(&close.generation)
                .is_some_and(|entry| {
                    entry.instance.is_same_as(&self.instance)
                        && entry.close_attempt == Some(close.attempt)
                        && entry.phase.advance(event)
                })
        };
        if accepted {
            self.reply.send(fact);
        }
    }
}
