//! First Raise publication, oldest-first deferral and paced replay for one run.

use std::{collections::BTreeSet, marker::PhantomData, time::Duration};

use approval_request_protocol::{Raise, RaiseReply};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::{ApprovalRequestId, RouterRunId};

const DEFERRED_RETRY: Duration = Duration::from_secs(30);
const REPLAY_PERIOD_SECONDS: u64 = 5 * 60;
const REPLAY_ALLOWANCE: usize = 16;

/// Identifies the dispatcher's exact Raise attempt on its carrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaiseAttempt<Carrier> {
    pub carrier: Carrier,
    sequence: u64,
}

/// Why this publication is eligible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RaisePublication {
    First,
    Retry,
    AcknowledgedReplay,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SentStage {
    MayHaveArrived,
    Deferred { retry_at: Duration },
    Acknowledged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Publication<Carrier> {
    Queued,
    Sent {
        stage: SentStage,
        pending: Option<RaiseAttempt<Carrier>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Candidate<Carrier> {
    request: Raise,
    publication: Publication<Carrier>,
}

impl<Carrier> Candidate<Carrier> {
    fn is_deferred(&self) -> bool {
        matches!(
            self.publication,
            Publication::Sent {
                stage: SentStage::Deferred { .. },
                ..
            }
        )
    }

    fn has_active_deferred_attempt(&self) -> bool {
        matches!(
            self.publication,
            Publication::Sent {
                stage: SentStage::Deferred { .. },
                pending: Some(_)
            }
        )
    }

    fn can_replay(&self) -> bool {
        matches!(
            self.publication,
            Publication::Sent {
                stage: SentStage::Acknowledged,
                pending: None
            }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Admission<Carrier> {
    Unavailable,
    Holding(Carrier),
    Ready(Carrier),
    Fenced,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplayPeriod {
    starts: Duration,
    index: u64,
    due: bool,
    replayed: BTreeSet<ApprovalRequestId>,
    cursor: Option<ApprovalRequestId>,
}

impl ReplayPeriod {
    fn advance(&mut self, now: Duration) {
        let index = now.saturating_sub(self.starts).as_secs() / REPLAY_PERIOD_SECONDS;
        if index > self.index {
            self.index = index;
            self.due = true;
            self.replayed.clear();
        }
    }

    fn next<'a, Carrier>(
        &self,
        candidates: &'a [Candidate<Carrier>],
    ) -> Option<&'a ApprovalRequestId> {
        if !self.due || self.replayed.len() >= REPLAY_ALLOWANCE {
            return None;
        }
        candidates
            .iter()
            .filter(|candidate| candidate.can_replay())
            .map(|candidate| candidate.request.request_id())
            .filter(|request_id| !self.replayed.contains(*request_id))
            .min_by_key(|request_id| {
                (
                    self.cursor
                        .as_ref()
                        .is_some_and(|cursor| *request_id <= cursor),
                    *request_id,
                )
            })
    }
}

/// Stable enqueue order and publication state for the live waits of one Router run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatcherState<Carrier> {
    run: RouterRunId,
    admission: Admission<Carrier>,
    candidates: Vec<Candidate<Carrier>>,
    next_attempt: u64,
    expedite_head: bool,
    replay: ReplayPeriod,
}

impl<Carrier> DispatcherState<Carrier> {
    /// The supplied elapsed time anchors this run's replay periods.
    pub fn new(run: RouterRunId, start: Duration) -> Self {
        Self {
            run,
            admission: Admission::Unavailable,
            candidates: Vec::new(),
            next_attempt: 0,
            expedite_head: false,
            replay: ReplayPeriod {
                starts: start,
                index: 0,
                due: false,
                replayed: BTreeSet::new(),
                cursor: None,
            },
        }
    }

    fn candidate(&self, id: &ApprovalRequestId) -> Option<usize> {
        self.candidates
            .iter()
            .position(|candidate| candidate.request.request_id() == id)
    }

    /// Selects the next acknowledged replay using the currently available period allowance.
    pub fn next_acknowledged_replay(&self, now: Duration) -> Option<&ApprovalRequestId> {
        if !matches!(self.admission, Admission::Ready(_)) {
            return None;
        }
        let mut replay = self.replay.clone();
        replay.advance(now);
        replay.next(&self.candidates)
    }

    fn oldest_deferred(&self) -> Option<usize> {
        self.candidates.iter().position(Candidate::is_deferred)
    }

    fn clear_attempts(&mut self) {
        for candidate in &mut self.candidates {
            if let Publication::Sent { pending, .. } = &mut candidate.publication {
                *pending = None;
            }
        }
    }
}

impl<Carrier: Eq> DispatcherState<Carrier> {
    fn has_current_carrier(&self, carrier: &Carrier) -> bool {
        matches!(&self.admission, Admission::Ready(current) | Admission::Holding(current) if current == carrier)
    }
}

/// Commands and observations the run owner passes to the dispatcher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchEvent<Carrier> {
    Enqueue(Raise),
    /// A matched RegisterRun success from the carrier gate.
    Registered {
        carrier: Carrier,
    },
    CarrierLost {
        carrier: Carrier,
    },
    Publish {
        request_id: ApprovalRequestId,
        carrier: Carrier,
        now: Duration,
    },
    Reply {
        request_id: ApprovalRequestId,
        attempt: RaiseAttempt<Carrier>,
        reply: RaiseReply,
        now: Duration,
    },
    AttemptEnded {
        request_id: ApprovalRequestId,
        attempt: RaiseAttempt<Carrier>,
    },
    /// Local Stop or other closure of the wait that owns the Raise.
    RemoveWait {
        request_id: ApprovalRequestId,
    },
    /// A matched Stop or Report acknowledgement that frees a deferred retry opportunity.
    DebtAcknowledged,
    Fence,
}

/// A typed Raise publication or the reply successfully correlated to it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchEffect<Carrier> {
    PublishRaise {
        attempt: RaiseAttempt<Carrier>,
        publication: RaisePublication,
        request: Raise,
    },
    RaiseReply {
        request_id: ApprovalRequestId,
        reply: RaiseReply,
    },
}

/// An attempted dispatcher operation violates the request or scheduling contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchRefusal {
    WrongRun,
    RequestAlreadyQueued,
    AttemptSequenceExhausted,
    HeldByDeferredHead,
    DeferredRetryNotDue,
    AcknowledgedReplayNotDue,
    ReceiptForDifferentRequest,
}

/// Owns publication order independently of the individual request waits.
pub struct FirstRaiseDispatcher<Carrier>(PhantomData<Carrier>);

type DispatchError = TransitionError<DispatchRefusal>;

impl<Carrier> StateTag for DispatcherState<Carrier> {
    fn state_tag(&self) -> &'static str {
        if self.is_terminal() { "fenced" } else { "open" }
    }

    fn is_terminal(&self) -> bool {
        matches!(self.admission, Admission::Fenced)
    }
}

impl<Carrier> EventTag for DispatchEvent<Carrier> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Enqueue(_) => "enqueue",
            Self::Registered { .. } => "registered",
            Self::CarrierLost { .. } => "carrier_lost",
            Self::Publish { .. } => "publish",
            Self::Reply { .. } => "reply",
            Self::AttemptEnded { .. } => "attempt_ended",
            Self::RemoveWait { .. } => "remove_wait",
            Self::DebtAcknowledged => "debt_acknowledged",
            Self::Fence => "fence",
        }
    }
}

impl RefusalTag for DispatchRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::WrongRun => "wrong_run",
            Self::RequestAlreadyQueued => "request_already_queued",
            Self::AttemptSequenceExhausted => "attempt_sequence_exhausted",
            Self::HeldByDeferredHead => "held_by_deferred_head",
            Self::DeferredRetryNotDue => "deferred_retry_not_due",
            Self::AcknowledgedReplayNotDue => "acknowledged_replay_not_due",
            Self::ReceiptForDifferentRequest => "receipt_for_different_request",
        }
    }
}

fn raise_run(request: &Raise) -> &RouterRunId {
    match request {
        Raise::ProviderApproval { holder, .. } | Raise::ProviderQuestion { holder, .. } => {
            &holder.router_run
        }
        Raise::UnprovenCall { holder, .. } => &holder.router_run,
    }
}

impl<Carrier: Clone + Eq> DispatcherState<Carrier> {
    fn publish(
        &mut self,
        id: &ApprovalRequestId,
        carrier: Carrier,
        now: Duration,
    ) -> Result<DispatchEffect<Carrier>, DispatchError> {
        if !matches!(&self.admission, Admission::Ready(current) if current == &carrier) {
            return Err(TransitionError::Stale);
        }
        let index = self.candidate(id).ok_or(TransitionError::Stale)?;
        let candidate = self.candidates.get(index).ok_or(TransitionError::Stale)?;
        self.replay.advance(now);
        let (publication, stage) = match &candidate.publication {
            Publication::Queued => {
                if self.oldest_deferred().is_some() {
                    return Err(TransitionError::Domain(DispatchRefusal::HeldByDeferredHead));
                }
                (RaisePublication::First, SentStage::MayHaveArrived)
            }
            Publication::Sent {
                pending: Some(_), ..
            } => return Err(TransitionError::Stale),
            Publication::Sent {
                stage: SentStage::MayHaveArrived,
                pending: None,
            } => (RaisePublication::Retry, SentStage::MayHaveArrived),
            Publication::Sent {
                stage: SentStage::Deferred { retry_at },
                pending: None,
            } => {
                if self.oldest_deferred() != Some(index)
                    || self
                        .candidates
                        .iter()
                        .any(Candidate::has_active_deferred_attempt)
                {
                    return Err(TransitionError::Domain(DispatchRefusal::HeldByDeferredHead));
                }
                if now < *retry_at && !self.expedite_head {
                    return Err(TransitionError::Domain(
                        DispatchRefusal::DeferredRetryNotDue,
                    ));
                }
                self.expedite_head = false;
                // Only a new capacity refusal establishes a new retry deadline.
                (
                    RaisePublication::Retry,
                    SentStage::Deferred {
                        retry_at: Duration::ZERO,
                    },
                )
            }
            Publication::Sent {
                stage: SentStage::Acknowledged,
                pending: None,
            } => {
                if self.replay.next(&self.candidates) != Some(id) {
                    return Err(TransitionError::Domain(
                        DispatchRefusal::AcknowledgedReplayNotDue,
                    ));
                }
                self.replay.replayed.insert(id.clone());
                self.replay.cursor = Some(id.clone());
                (
                    RaisePublication::AcknowledgedReplay,
                    SentStage::Acknowledged,
                )
            }
        };
        let attempt = RaiseAttempt {
            carrier,
            sequence: self.next_attempt,
        };
        self.next_attempt = self
            .next_attempt
            .checked_add(1)
            .ok_or(TransitionError::Domain(
                DispatchRefusal::AttemptSequenceExhausted,
            ))?;
        let candidate = self
            .candidates
            .get_mut(index)
            .ok_or(TransitionError::Stale)?;
        candidate.publication = Publication::Sent {
            stage,
            pending: Some(attempt.clone()),
        };
        Ok(DispatchEffect::PublishRaise {
            attempt,
            publication,
            request: candidate.request.clone(),
        })
    }

    fn retire_attempt(
        &mut self,
        id: &ApprovalRequestId,
        attempt: &RaiseAttempt<Carrier>,
    ) -> Result<usize, DispatchError> {
        if !self.has_current_carrier(&attempt.carrier) {
            return Err(TransitionError::Stale);
        }
        let index = self.candidate(id).ok_or(TransitionError::Stale)?;
        let candidate = self
            .candidates
            .get_mut(index)
            .ok_or(TransitionError::Stale)?;
        if let Publication::Sent { pending, .. } = &mut candidate.publication
            && pending.as_ref() == Some(attempt)
        {
            *pending = None;
            return Ok(index);
        }
        Err(TransitionError::Stale)
    }

    fn observe_reply(
        &mut self,
        id: ApprovalRequestId,
        attempt: &RaiseAttempt<Carrier>,
        reply: RaiseReply,
        now: Duration,
    ) -> Result<DispatchEffect<Carrier>, DispatchError> {
        let index = self.retire_attempt(&id, attempt)?;
        let candidate = self
            .candidates
            .get_mut(index)
            .ok_or(TransitionError::Stale)?;
        let Publication::Sent { stage, .. } = &mut candidate.publication else {
            return Err(TransitionError::Stale);
        };
        match &reply {
            RaiseReply::Receipt { receipt } => {
                if receipt.request_id != id {
                    return Err(TransitionError::Domain(
                        DispatchRefusal::ReceiptForDifferentRequest,
                    ));
                }
                *stage = SentStage::Acknowledged;
            }
            RaiseReply::TooManyOutstanding {} => {
                if !matches!(stage, SentStage::Acknowledged) {
                    *stage = SentStage::Deferred {
                        retry_at: now.saturating_add(DEFERRED_RETRY),
                    };
                }
            }
            RaiseReply::AlreadyStopped {}
            | RaiseReply::AlreadyEnded {}
            | RaiseReply::SessionEnded {}
            | RaiseReply::ContentsDiffer {} => {
                self.candidates.remove(index);
            }
            RaiseReply::AuthorityRefused { .. } => {
                // The carrier gate owns recovery and fencing. Hold publication until it acts.
                self.admission = Admission::Holding(attempt.carrier.clone());
            }
        }
        Ok(DispatchEffect::RaiseReply {
            request_id: id,
            reply,
        })
    }
}

impl<Carrier: Clone + Eq> StateMachine for FirstRaiseDispatcher<Carrier> {
    type State = DispatcherState<Carrier>;
    type Event = DispatchEvent<Carrier>;
    type Effect = DispatchEffect<Carrier>;
    type Error = DispatchRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        if state.is_terminal() {
            return Err(TransitionError::Stale);
        }
        let mut next = state.clone();
        let mut effects = Vec::new();
        match event {
            DispatchEvent::Enqueue(request) => {
                if raise_run(&request) != &state.run {
                    return Err(TransitionError::Domain(DispatchRefusal::WrongRun));
                }
                if state.candidate(request.request_id()).is_some() {
                    return Err(TransitionError::Domain(
                        DispatchRefusal::RequestAlreadyQueued,
                    ));
                }
                next.candidates.push(Candidate {
                    request,
                    publication: Publication::Queued,
                });
            }
            DispatchEvent::Registered { carrier } => {
                if !state.has_current_carrier(&carrier) {
                    next.clear_attempts();
                }
                next.admission = Admission::Ready(carrier);
                next.replay.due = true;
            }
            DispatchEvent::CarrierLost { carrier } => {
                if !state.has_current_carrier(&carrier) {
                    return Err(TransitionError::Stale);
                }
                next.clear_attempts();
                next.admission = Admission::Unavailable;
            }
            DispatchEvent::Publish {
                request_id,
                carrier,
                now,
            } => {
                effects.push(next.publish(&request_id, carrier, now)?);
            }
            DispatchEvent::Reply {
                request_id,
                attempt,
                reply,
                now,
            } => {
                effects.push(next.observe_reply(request_id, &attempt, reply, now)?);
            }
            DispatchEvent::AttemptEnded {
                request_id,
                attempt,
            } => {
                next.retire_attempt(&request_id, &attempt)?;
            }
            DispatchEvent::RemoveWait { request_id } => {
                let index = state.candidate(&request_id).ok_or(TransitionError::Stale)?;
                next.candidates.remove(index);
            }
            DispatchEvent::DebtAcknowledged => {
                next.expedite_head = true;
            }
            DispatchEvent::Fence => {
                next.admission = Admission::Fenced;
                next.candidates.clear();
            }
        }
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        &DISPATCH_TRANSITIONS
    }
}

const fn dispatch(event: &'static str) -> Transition {
    Transition {
        from: "open",
        event,
        to: "open",
        terminal: false,
    }
}

const DISPATCH_TRANSITIONS: [Transition; 9] = [
    dispatch("enqueue"),
    dispatch("registered"),
    dispatch("carrier_lost"),
    dispatch("publish"),
    dispatch("reply"),
    dispatch("attempt_ended"),
    dispatch("remove_wait"),
    dispatch("debt_acknowledged"),
    Transition {
        from: "open",
        event: "fence",
        to: "fenced",
        terminal: true,
    },
];

#[cfg(test)]
#[path = "first_raise_dispatcher_test.rs"]
mod tests;
