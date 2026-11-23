//! One provider ask, its exact responder and its Central Raise or Stop duty.

use std::marker::PhantomData;

use approval_request_protocol::{
    ApprovalDigestError, DeliverEndingReply, DeliverEndingRequest, InvalidApprovalEnding,
    PrivateEnding, ProviderStopCause, Raise, RaiseReply, RequestContentsDigest, StopReply,
    StopRequest,
};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::run_gate::{
    CallAttempt, CallKey, EndingIngress, LinkAttempt, MatchedRunRefusal, ReadyRun,
};

#[path = "waiting_request/central_calls.rs"]
mod central_calls;

#[derive(Clone, Debug, Eq, PartialEq)]
struct FrozenRaise {
    body: Raise,
    digest: RequestContentsDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call<Connection> {
    Due,
    InFlight(CallKey<Connection>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CentralDuty<Connection> {
    Unsent,
    Raising(Call<Connection>),
    WaitingForEnding,
    Stopping {
        request: StopRequest,
        call: Call<Connection>,
    },
    Refusing,
    Done,
}

/// Why the Mac must refuse a provider ask through its existing responder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalNegative {
    SessionNotBound,
    HolderAtCapacity,
    Stopped(ProviderStopCause),
    RaiseRefused(RaiseRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RaiseRefusal {
    AlreadyStopped,
    SessionEnded,
    ContentsDiffer,
    TooManyOutstanding,
}

/// The reply command owned by the one-shot provider responder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderReply {
    CentralEnding(PrivateEnding),
    LocalNegative(LocalNegative),
}

/// What the writer observed about its one submission opportunity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterOutcome {
    NotSubmitted,
    WriteReturned,
    OutcomeUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Custody<Responder> {
    Available(Responder),
    Claimed {
        responder: Responder,
        reply: ProviderReply,
    },
    Permitted {
        responder: Responder,
        reply: ProviderReply,
    },
    Started(Responder),
    FencedAfterStart(Responder),
    Released,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Authority {
    Live,
    Ended,
}

/// Central duty and responder custody for a single provider ask.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitState<Responder, Connection> {
    request_id: Option<ApprovalRequestId>,
    router_run: Option<RouterRunId>,
    displaced_raise: Option<CallKey<Connection>>,
    frozen: Option<FrozenRaise>,
    central: CentralDuty<Connection>,
    custody: Custody<Responder>,
    claim: Option<DeliverEndingRequest>,
    authority: Authority,
}

impl<Responder, Connection> WaitState<Responder, Connection> {
    pub fn bound(raise: Raise, responder: Responder) -> Result<Self, ApprovalDigestError> {
        let digest = raise.contents_digest()?;
        Ok(Self {
            request_id: Some(raise.request_id().clone()),
            router_run: Some(raise.router_run().clone()),
            displaced_raise: None,
            frozen: Some(FrozenRaise {
                body: raise,
                digest,
            }),
            central: CentralDuty::Unsent,
            custody: Custody::Available(responder),
            claim: None,
            authority: Authority::Live,
        })
    }

    pub const fn unbound(responder: Responder) -> Self {
        Self::local_refusal(responder, LocalNegative::SessionNotBound)
    }

    pub const fn at_capacity(responder: Responder) -> Self {
        Self::local_refusal(responder, LocalNegative::HolderAtCapacity)
    }

    const fn local_refusal(responder: Responder, reason: LocalNegative) -> Self {
        Self {
            request_id: None,
            router_run: None,
            displaced_raise: None,
            frozen: None,
            central: CentralDuty::Refusing,
            custody: Custody::Claimed {
                responder,
                reply: ProviderReply::LocalNegative(reason),
            },
            claim: None,
            authority: Authority::Live,
        }
    }

    /// Both the local responder and any separate Stop duty have closed.
    pub const fn is_closed(&self) -> bool {
        matches!(self.central, CentralDuty::Done) && matches!(self.custody, Custody::Released)
    }

    /// A completed local wait can still have a matched Raise response to drain.
    pub const fn has_pending_calls(&self) -> bool {
        self.displaced_raise.is_some()
            || matches!(
                self.central,
                CentralDuty::Raising(Call::InFlight(_))
                    | CentralDuty::Stopping {
                        call: Call::InFlight(_),
                        ..
                    }
            )
    }

    /// The exact response the writer may take through its final permit and start.
    pub fn claimed_reply(&self) -> Option<(&Responder, &ProviderReply)> {
        match &self.custody {
            Custody::Claimed { responder, reply } | Custody::Permitted { responder, reply } => {
                Some((responder, reply))
            }
            Custody::Available(_)
            | Custody::Started(_)
            | Custody::FencedAfterStart(_)
            | Custody::Released => None,
        }
    }

    fn close_if_done(&mut self) {
        if matches!(self.custody, Custody::Released)
            && matches!(self.central, CentralDuty::Refusing)
        {
            self.central = CentralDuty::Done;
        }
        if self.is_closed() {
            self.frozen = None;
            self.claim = None;
        }
    }
}

/// Inputs from the authenticated link owner or the exact responder owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WaitEvent<'gate, Responder, Connection> {
    /// The run gate has admitted this connection for publishing a pending call.
    Publish {
        ready: ReadyRun<'gate, Connection>,
        attempt: CallAttempt,
    },
    RaiseReply {
        call: CallKey<Connection>,
        reply: RaiseReply,
    },
    StopReply {
        call: CallKey<Connection>,
        reply: StopReply,
    },
    CallEndedWithoutReply {
        call: CallKey<Connection>,
    },
    ConnectionRetired {
        link: LinkAttempt<Connection>,
    },
    DeliverEnding {
        request: DeliverEndingRequest,
        ingress: EndingIngress<'gate, Connection>,
    },
    LocalStop {
        cause: ProviderStopCause,
    },
    TakeFinalPermit {
        responder: Responder,
        reply: ProviderReply,
    },
    StartWrite {
        responder: Responder,
        reply: ProviderReply,
    },
    /// The provider took back this exact ask. The executor proves the match.
    ProviderWithdrew {
        responder: Responder,
    },
    /// The claimed responder was withdrawn before the writer started.
    CapabilityWithdrawn {
        responder: Responder,
    },
    WriteFinished {
        responder: Responder,
        outcome: WriterOutcome,
    },
    AuthorityEnded,
}

/// A typed call, an ending disposition, or a local provider action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WaitEffect<Responder, Connection> {
    Raise {
        call: CallKey<Connection>,
        request: Raise,
    },
    Stop {
        call: CallKey<Connection>,
        request: StopRequest,
    },
    EndingReply(DeliverEndingReply),
    EndingConflict {
        request_id: ApprovalRequestId,
        reason: InvalidApprovalEnding,
    },
    StopReconciled(approval_request_protocol::StopReceiptEnding),
    StopNotVisible {
        request_id: ApprovalRequestId,
        contents_digest: RequestContentsDigest,
    },
    RunRefused(MatchedRunRefusal<Connection>),
    Submit {
        responder: Responder,
        reply: ProviderReply,
    },
    ReleaseBinding {
        responder: Responder,
    },
    WriterResolved {
        responder: Responder,
        outcome: WriterOutcome,
    },
}

/// A current Raise reply names a different request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitRefusal {
    ReceiptForDifferentRequest,
}

/// Owns a provider ask through its one-shot handover and any exact owed Stop.
pub struct WaitingRequest<'gate, Responder, Connection>(
    PhantomData<(&'gate (), Responder, Connection)>,
);

type WaitError = TransitionError<WaitRefusal>;
type Effects<Responder, Connection> = Vec<WaitEffect<Responder, Connection>>;

impl<Responder, Connection> StateTag for WaitState<Responder, Connection> {
    fn state_tag(&self) -> &'static str {
        if self.is_closed() {
            "closed"
        } else if self.authority == Authority::Ended {
            "fenced_write"
        } else {
            "live"
        }
    }

    fn is_terminal(&self) -> bool {
        self.is_closed()
    }
}

impl<Responder, Connection> EventTag for WaitEvent<'_, Responder, Connection> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Publish { .. } => "publish",
            Self::RaiseReply { .. } => "raise_reply",
            Self::StopReply { .. } => "stop_reply",
            Self::CallEndedWithoutReply { .. } => "call_ended",
            Self::ConnectionRetired { .. } => "connection_retired",
            Self::DeliverEnding { .. } => "deliver_ending",
            Self::LocalStop { .. } => "local_stop",
            Self::TakeFinalPermit { .. } => "take_permit",
            Self::StartWrite { .. } => "start_write",
            Self::ProviderWithdrew { .. } => "provider_withdrew",
            Self::CapabilityWithdrawn { .. } => "capability_withdrawn",
            Self::WriteFinished { .. } => "write_finished",
            Self::AuthorityEnded => "authority_ended",
        }
    }
}

impl RefusalTag for WaitRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::ReceiptForDifferentRequest => "receipt_for_different_request",
        }
    }
}

impl<Responder: Clone + Eq, Connection: Clone + Eq> WaitState<Responder, Connection> {
    fn claim_available(&mut self, reply: ProviderReply) -> Result<(), WaitError> {
        let Custody::Available(responder) = &self.custody else {
            return Err(TransitionError::Stale);
        };
        self.custody = Custody::Claimed {
            responder: responder.clone(),
            reply,
        };
        Ok(())
    }

    fn deliver(
        &mut self,
        request: DeliverEndingRequest,
        ingress: EndingIngress<'_, Connection>,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<(), WaitError> {
        let reply = match ingress {
            EndingIngress::Discard => return Ok(()),
            EndingIngress::NotReady => DeliverEndingReply::HolderNotReady {},
            EndingIngress::Admitted(ready) => {
                if self.router_run.as_ref() != Some(ready.router_run()) {
                    return Ok(());
                }
                self.accept_ending(request, effects)?
            }
        };
        effects.push(WaitEffect::EndingReply(reply));
        Ok(())
    }

    fn accept_ending(
        &mut self,
        request: DeliverEndingRequest,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<DeliverEndingReply, WaitError> {
        if self.request_id.as_ref() != Some(&request.request_id) {
            return Err(TransitionError::Stale);
        }
        let Some(frozen) = self.frozen.as_ref() else {
            return Ok(DeliverEndingReply::NotHeld {});
        };
        let validation = if request.contents_digest != frozen.digest {
            Err(InvalidApprovalEnding::BindingMismatch)
        } else {
            request.ending.check_bound_raise(&frozen.body)
        };
        if let Err(reason) = validation {
            effects.push(WaitEffect::EndingConflict {
                request_id: request.request_id,
                reason,
            });
            return Ok(DeliverEndingReply::EndingDiffers {});
        }
        if let Some(claimed) = &self.claim {
            if claimed == &request {
                return Ok(DeliverEndingReply::Received {});
            }
            effects.push(WaitEffect::EndingConflict {
                request_id: request.request_id,
                reason: InvalidApprovalEnding::BindingMismatch,
            });
            return Ok(DeliverEndingReply::EndingDiffers {});
        }
        Ok(match self.central {
            CentralDuty::Unsent | CentralDuty::Refusing => DeliverEndingReply::HolderNotReady {},
            CentralDuty::Raising(_) | CentralDuty::WaitingForEnding => {
                self.claim_available(ProviderReply::CentralEnding(request.ending.clone()))?;
                self.preserve_raising_call();
                self.central = CentralDuty::Done;
                self.claim = Some(request);
                DeliverEndingReply::Received {}
            }
            CentralDuty::Stopping { .. } => DeliverEndingReply::StopOwed {},
            CentralDuty::Done => DeliverEndingReply::NotHeld {},
        })
    }

    fn local_stop(&mut self, cause: ProviderStopCause) -> Result<(), WaitError> {
        let reply = ProviderReply::LocalNegative(LocalNegative::Stopped(cause));
        match &self.central {
            CentralDuty::Unsent => {
                self.claim_available(reply)?;
                self.central = CentralDuty::Refusing;
            }
            CentralDuty::Raising(_) | CentralDuty::WaitingForEnding => {
                self.claim_available(reply)?;
                self.begin_stop(cause)?;
            }
            CentralDuty::Done if self.claim.is_some() => match &self.custody {
                Custody::Claimed {
                    responder,
                    reply: ProviderReply::CentralEnding(_),
                }
                | Custody::Permitted {
                    responder,
                    reply: ProviderReply::CentralEnding(_),
                } => {
                    self.custody = Custody::Claimed {
                        responder: responder.clone(),
                        reply,
                    };
                }
                Custody::Started(_) => {}
                Custody::Available(_)
                | Custody::Claimed { .. }
                | Custody::Permitted { .. }
                | Custody::Released
                | Custody::FencedAfterStart(_) => return Err(TransitionError::Stale),
            },
            CentralDuty::Stopping { .. } | CentralDuty::Refusing | CentralDuty::Done => {
                return Err(TransitionError::Stale);
            }
        }
        Ok(())
    }

    fn provider_withdrew(
        &mut self,
        exact: &Responder,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<(), WaitError> {
        match &self.custody {
            Custody::Available(responder)
            | Custody::Claimed { responder, .. }
            | Custody::Permitted { responder, .. }
                if responder == exact => {}
            Custody::Started(responder) if responder == exact => return Ok(()),
            Custody::Available(_)
            | Custody::Claimed { .. }
            | Custody::Permitted { .. }
            | Custody::Started(_)
            | Custody::FencedAfterStart(_)
            | Custody::Released => {
                return Err(TransitionError::Stale);
            }
        }
        self.abandon_provider_reply(ProviderStopCause::AskWithdrawn, effects)
    }

    fn abandon_provider_reply(
        &mut self,
        cause: ProviderStopCause,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<(), WaitError> {
        match self.central {
            CentralDuty::Unsent => self.central = CentralDuty::Refusing,
            CentralDuty::Raising(_) | CentralDuty::WaitingForEnding => {
                self.begin_stop(cause)?;
            }
            CentralDuty::Stopping { .. } | CentralDuty::Refusing | CentralDuty::Done => {}
        }
        match &self.custody {
            Custody::Available(responder)
            | Custody::Claimed { responder, .. }
            | Custody::Permitted { responder, .. } => {
                effects.push(WaitEffect::ReleaseBinding {
                    responder: responder.clone(),
                });
                self.custody = Custody::Released;
            }
            Custody::Started(_) | Custody::FencedAfterStart(_) | Custody::Released => {}
        }
        Ok(())
    }

    fn permit(
        &mut self,
        exact: &Responder,
        expected_reply: &ProviderReply,
    ) -> Result<(), WaitError> {
        let Custody::Claimed { responder, reply } = &self.custody else {
            return Err(TransitionError::Stale);
        };
        if responder != exact || reply != expected_reply {
            return Err(TransitionError::Stale);
        }
        self.custody = Custody::Permitted {
            responder: responder.clone(),
            reply: reply.clone(),
        };
        Ok(())
    }

    fn start(
        &mut self,
        exact: Responder,
        expected_reply: ProviderReply,
    ) -> Result<WaitEffect<Responder, Connection>, WaitError> {
        let Custody::Permitted { responder, reply } = &self.custody else {
            return Err(TransitionError::Stale);
        };
        if responder != &exact || reply != &expected_reply {
            return Err(TransitionError::Stale);
        }
        let reply = reply.clone();
        self.custody = Custody::Started(exact.clone());
        Ok(WaitEffect::Submit {
            responder: exact,
            reply,
        })
    }

    fn withdraw(
        &mut self,
        exact: Responder,
    ) -> Result<WaitEffect<Responder, Connection>, WaitError> {
        if !matches!(&self.custody, Custody::Claimed { responder, .. }
            | Custody::Permitted { responder, .. } if responder == &exact)
        {
            return Err(TransitionError::Stale);
        }
        self.custody = Custody::Released;
        Ok(WaitEffect::ReleaseBinding { responder: exact })
    }

    fn complete(
        &mut self,
        exact: Responder,
        outcome: WriterOutcome,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<(), WaitError> {
        match &self.custody {
            Custody::Started(responder) if responder == &exact => {
                effects.push(WaitEffect::ReleaseBinding {
                    responder: exact.clone(),
                });
            }
            Custody::FencedAfterStart(responder) if responder == &exact => {}
            Custody::Available(_)
            | Custody::Claimed { .. }
            | Custody::Permitted { .. }
            | Custody::Started(_)
            | Custody::FencedAfterStart(_)
            | Custody::Released => return Err(TransitionError::Stale),
        }
        self.custody = Custody::Released;
        effects.push(WaitEffect::WriterResolved {
            responder: exact,
            outcome,
        });
        Ok(())
    }

    fn fence(&mut self, effects: &mut Effects<Responder, Connection>) {
        self.authority = Authority::Ended;
        self.displaced_raise = None;
        self.central = CentralDuty::Done;
        self.claim = None;
        self.frozen = None;
        self.custody = match std::mem::replace(&mut self.custody, Custody::Released) {
            Custody::Available(responder)
            | Custody::Claimed { responder, .. }
            | Custody::Permitted { responder, .. } => {
                effects.push(WaitEffect::ReleaseBinding { responder });
                Custody::Released
            }
            Custody::Started(responder) => {
                effects.push(WaitEffect::ReleaseBinding {
                    responder: responder.clone(),
                });
                Custody::FencedAfterStart(responder)
            }
            kept @ (Custody::FencedAfterStart(_) | Custody::Released) => kept,
        };
    }
}

impl<'gate, Responder: Clone + Eq, Connection: Clone + Eq + 'gate> StateMachine
    for WaitingRequest<'gate, Responder, Connection>
{
    type State = WaitState<Responder, Connection>;
    type Event = WaitEvent<'gate, Responder, Connection>;
    type Effect = WaitEffect<Responder, Connection>;
    type Error = WaitRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        let mut next = state.clone();
        let mut effects = Vec::new();
        match event {
            WaitEvent::DeliverEnding { request, ingress } => {
                if state.authority == Authority::Live {
                    next.deliver(request, ingress, &mut effects)?;
                }
            }
            WaitEvent::WriteFinished { responder, outcome } => {
                next.complete(responder, outcome, &mut effects)?
            }
            WaitEvent::AuthorityEnded => next.fence(&mut effects),
            WaitEvent::RaiseReply { call, reply } => {
                next.receive_raise(call, reply, &mut effects)?
            }
            WaitEvent::StopReply { call, reply } => next.receive_stop(call, reply, &mut effects)?,
            WaitEvent::CallEndedWithoutReply { call } => next.end_call(&call)?,
            WaitEvent::ConnectionRetired { link } => next.retire_connection(&link)?,
            _ if state.is_closed() || state.authority == Authority::Ended => {
                return Err(TransitionError::Stale);
            }
            WaitEvent::Publish { ready, attempt } => effects.push(next.publish(ready, attempt)?),
            WaitEvent::LocalStop {
                cause: ProviderStopCause::ProviderConnectionLost,
            } => next
                .abandon_provider_reply(ProviderStopCause::ProviderConnectionLost, &mut effects)?,
            WaitEvent::LocalStop {
                cause:
                    cause @ (ProviderStopCause::TurnStopped
                    | ProviderStopCause::SessionClosed
                    | ProviderStopCause::AskReplaced),
            } => next.local_stop(cause)?,
            WaitEvent::LocalStop {
                cause: ProviderStopCause::AskWithdrawn,
            } => return Err(TransitionError::Stale),
            WaitEvent::TakeFinalPermit { responder, reply } => next.permit(&responder, &reply)?,
            WaitEvent::StartWrite { responder, reply } => {
                effects.push(next.start(responder, reply)?)
            }
            WaitEvent::ProviderWithdrew { responder } => {
                next.provider_withdrew(&responder, &mut effects)?
            }
            WaitEvent::CapabilityWithdrawn { responder } => effects.push(next.withdraw(responder)?),
        }
        next.close_if_done();
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        &WAIT_TRANSITIONS
    }
}

const fn step(
    from: &'static str,
    event: &'static str,
    to: &'static str,
    terminal: bool,
) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal,
    }
}

const WAIT_TRANSITIONS: [Transition; 27] = [
    step("live", "publish", "live", false),
    step("live", "raise_reply", "live", false),
    step("live", "stop_reply", "live", false),
    step("live", "stop_reply", "closed", true),
    step("live", "call_ended", "live", false),
    step("closed", "call_ended", "closed", true),
    step("live", "connection_retired", "live", false),
    step("closed", "connection_retired", "closed", true),
    step("closed", "raise_reply", "closed", true),
    step("live", "local_stop", "closed", true),
    step("live", "deliver_ending", "live", false),
    step("live", "local_stop", "live", false),
    step("live", "take_permit", "live", false),
    step("live", "start_write", "live", false),
    step("live", "provider_withdrew", "live", false),
    step("live", "provider_withdrew", "closed", true),
    step("live", "capability_withdrawn", "live", false),
    step("live", "capability_withdrawn", "closed", true),
    step("live", "write_finished", "live", false),
    step("live", "write_finished", "closed", true),
    step("live", "authority_ended", "closed", true),
    step("live", "authority_ended", "fenced_write", false),
    step("fenced_write", "deliver_ending", "fenced_write", false),
    step("fenced_write", "write_finished", "closed", true),
    step("closed", "deliver_ending", "closed", true),
    step("closed", "authority_ended", "closed", true),
    step("fenced_write", "authority_ended", "fenced_write", false),
];

#[cfg(test)]
#[path = "waiting_request_test.rs"]
mod tests;
