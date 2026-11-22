//! One provider ask, its exact responder and its Central Raise or Stop duty.

use std::marker::PhantomData;

use approval_request_protocol::{
    ApprovalDigestError, DeliverEndingReply, DeliverEndingRequest, InvalidApprovalEnding,
    PrivateEnding, ProviderStopCause, Raise, RaiseReply, RequestContentsDigest, RunRefusal,
    StopReply, StopRequest,
};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::run_gate::EndingIngress;

#[derive(Clone, Debug, Eq, PartialEq)]
struct FrozenRaise {
    body: Raise,
    digest: RequestContentsDigest,
    run: RouterRunId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call<Connection> {
    Due,
    InFlight(Connection),
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
    Stopped(ProviderStopCause),
    RaiseRefused(RaiseReply),
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
    frozen: Option<FrozenRaise>,
    central: CentralDuty<Connection>,
    custody: Custody<Responder>,
    claim: Option<DeliverEndingRequest>,
    authority: Authority,
}

/// Why a provider wait could not be constructed from its frozen input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenWaitRefusal {
    NotProviderAsk,
    InvalidContents(ApprovalDigestError),
}

impl<Responder, Connection> WaitState<Responder, Connection> {
    pub fn bound(raise: Raise, responder: Responder) -> Result<Self, OpenWaitRefusal> {
        let run = match &raise {
            Raise::ProviderApproval { holder, .. } | Raise::ProviderQuestion { holder, .. } => {
                holder.router_run.clone()
            }
            Raise::UnprovenCall { .. } => return Err(OpenWaitRefusal::NotProviderAsk),
        };
        let digest = raise
            .contents_digest()
            .map_err(OpenWaitRefusal::InvalidContents)?;
        Ok(Self {
            frozen: Some(FrozenRaise {
                body: raise,
                digest,
                run,
            }),
            central: CentralDuty::Unsent,
            custody: Custody::Available(responder),
            claim: None,
            authority: Authority::Live,
        })
    }

    pub const fn unbound(responder: Responder) -> Self {
        Self {
            frozen: None,
            central: CentralDuty::Refusing,
            custody: Custody::Claimed {
                responder,
                reply: ProviderReply::LocalNegative(LocalNegative::SessionNotBound),
            },
            claim: None,
            authority: Authority::Live,
        }
    }

    /// Both the local responder and any separate Stop duty have closed.
    pub const fn is_closed(&self) -> bool {
        matches!(self.central, CentralDuty::Done) && matches!(self.custody, Custody::Released)
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
pub enum WaitEvent<Responder, Connection> {
    /// The run gate has admitted this connection for publishing a pending call.
    Publish {
        connection: Connection,
    },
    RaiseReply {
        connection: Connection,
        reply: RaiseReply,
    },
    StopReply {
        connection: Connection,
        reply: StopReply,
    },
    CallEndedWithoutReply {
        connection: Connection,
    },
    DeliverEnding {
        request: DeliverEndingRequest,
        ingress: EndingIngress,
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
        connection: Connection,
        request: Raise,
    },
    Stop {
        connection: Connection,
        request: StopRequest,
    },
    EndingReply(DeliverEndingReply),
    EndingConflict {
        request_id: ApprovalRequestId,
        reason: InvalidApprovalEnding,
    },
    StopReconciled(StopReply),
    RunRefused {
        connection: Connection,
        refusal: RunRefusal,
    },
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
pub struct WaitingRequest<Responder, Connection>(PhantomData<(Responder, Connection)>);

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

impl<Responder, Connection> EventTag for WaitEvent<Responder, Connection> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Publish { .. } => "publish",
            Self::RaiseReply { .. } => "raise_reply",
            Self::StopReply { .. } => "stop_reply",
            Self::CallEndedWithoutReply { .. } => "call_ended",
            Self::DeliverEnding { .. } => "deliver_ending",
            Self::LocalStop { .. } => "local_stop",
            Self::TakeFinalPermit { .. } => "take_permit",
            Self::StartWrite { .. } => "start_write",
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
    fn publish(
        &mut self,
        connection: Connection,
    ) -> Result<WaitEffect<Responder, Connection>, WaitError> {
        match &mut self.central {
            CentralDuty::Unsent | CentralDuty::Raising(Call::Due) => {
                let frozen = self.frozen.as_ref().ok_or(TransitionError::Stale)?;
                self.central = CentralDuty::Raising(Call::InFlight(connection.clone()));
                Ok(WaitEffect::Raise {
                    connection,
                    request: frozen.body.clone(),
                })
            }
            CentralDuty::Stopping {
                request,
                call: call @ Call::Due,
            } => {
                *call = Call::InFlight(connection.clone());
                Ok(WaitEffect::Stop {
                    connection,
                    request: request.clone(),
                })
            }
            CentralDuty::Raising(Call::InFlight(_))
            | CentralDuty::WaitingForEnding
            | CentralDuty::Stopping {
                call: Call::InFlight(_),
                ..
            }
            | CentralDuty::Refusing
            | CentralDuty::Done => Err(TransitionError::Stale),
        }
    }

    fn reset_call(&mut self, connection: &Connection) -> Result<(), WaitError> {
        match &mut self.central {
            CentralDuty::Raising(call) | CentralDuty::Stopping { call, .. } if matches!(&*call, Call::InFlight(current) if current == connection) =>
            {
                *call = Call::Due;
                Ok(())
            }
            CentralDuty::Unsent
            | CentralDuty::Raising(_)
            | CentralDuty::Stopping { .. }
            | CentralDuty::WaitingForEnding
            | CentralDuty::Refusing
            | CentralDuty::Done => Err(TransitionError::Stale),
        }
    }

    fn receive_raise(
        &mut self,
        connection: Connection,
        reply: RaiseReply,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<(), WaitError> {
        if !matches!(&self.central, CentralDuty::Raising(Call::InFlight(current)) if current == &connection)
        {
            return Err(TransitionError::Stale);
        }
        match reply {
            RaiseReply::Receipt { receipt } => {
                let frozen = self.frozen.as_ref().ok_or(TransitionError::Stale)?;
                if frozen.body.request_id() != &receipt.request_id {
                    return Err(TransitionError::Domain(
                        WaitRefusal::ReceiptForDifferentRequest,
                    ));
                }
                self.central = CentralDuty::WaitingForEnding;
            }
            RaiseReply::AuthorityRefused { refusal } => {
                self.handle_run_refusal(connection, refusal, effects)?
            }
            refusal @ (RaiseReply::AlreadyStopped {}
            | RaiseReply::AlreadyEnded {}
            | RaiseReply::SessionEnded {}
            | RaiseReply::ContentsDiffer {}
            | RaiseReply::TooManyOutstanding {}) => {
                self.claim_available(ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
                    refusal,
                )))?;
                self.central = CentralDuty::Refusing;
            }
        }
        Ok(())
    }

    fn receive_stop(
        &mut self,
        connection: Connection,
        reply: StopReply,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<(), WaitError> {
        if !matches!(&self.central, CentralDuty::Stopping { call: Call::InFlight(current), .. } if current == &connection)
        {
            return Err(TransitionError::Stale);
        }
        match reply {
            StopReply::AuthorityRefused { refusal } => {
                self.handle_run_refusal(connection, refusal, effects)?
            }
            standing @ (StopReply::Stands { .. } | StopReply::NotVisible {}) => {
                self.central = CentralDuty::Done;
                effects.push(WaitEffect::StopReconciled(standing));
            }
        }
        Ok(())
    }

    fn handle_run_refusal(
        &mut self,
        connection: Connection,
        refusal: RunRefusal,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<(), WaitError> {
        effects.push(WaitEffect::RunRefused {
            connection: connection.clone(),
            refusal,
        });
        match refusal {
            RunRefusal::RunNotRegistered {} => self.reset_call(&connection)?,
            RunRefusal::RunRetired {} | RunRefusal::EnrollmentEnded {} => self.fence(effects),
        }
        Ok(())
    }

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
        ingress: EndingIngress,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<(), WaitError> {
        let reply = match ingress {
            EndingIngress::Discard => return Ok(()),
            EndingIngress::Reply(reply) => reply,
            EndingIngress::Authoritative => self.accept_ending(request, effects)?,
        };
        effects.push(WaitEffect::EndingReply(reply));
        Ok(())
    }

    fn accept_ending(
        &mut self,
        request: DeliverEndingRequest,
        effects: &mut Effects<Responder, Connection>,
    ) -> Result<DeliverEndingReply, WaitError> {
        let Some(frozen) = self
            .frozen
            .as_ref()
            .filter(|frozen| frozen.body.request_id() == &request.request_id)
        else {
            return Ok(DeliverEndingReply::NotHeld {});
        };
        let validation = if request.contents_digest != frozen.digest {
            Err(InvalidApprovalEnding::BindingMismatch)
        } else {
            request.check_bound_raise(&frozen.body)
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
                let frozen = self.frozen.as_ref().ok_or(TransitionError::Stale)?;
                let request = StopRequest::Provider {
                    request_id: frozen.body.request_id().clone(),
                    router_run: frozen.run.clone(),
                    contents_digest: frozen.digest.clone(),
                    cause,
                };
                self.claim_available(reply)?;
                self.central = CentralDuty::Stopping {
                    request,
                    call: Call::Due,
                };
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

impl<Responder: Clone + Eq, Connection: Clone + Eq> StateMachine
    for WaitingRequest<Responder, Connection>
{
    type State = WaitState<Responder, Connection>;
    type Event = WaitEvent<Responder, Connection>;
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
            _ if state.is_closed() || state.authority == Authority::Ended => {
                return Err(TransitionError::Stale);
            }
            WaitEvent::Publish { connection } => effects.push(next.publish(connection)?),
            WaitEvent::RaiseReply { connection, reply } => {
                next.receive_raise(connection, reply, &mut effects)?
            }
            WaitEvent::StopReply { connection, reply } => {
                next.receive_stop(connection, reply, &mut effects)?
            }
            WaitEvent::CallEndedWithoutReply { connection } => next.reset_call(&connection)?,
            WaitEvent::LocalStop { cause } => next.local_stop(cause)?,
            WaitEvent::TakeFinalPermit { responder, reply } => next.permit(&responder, &reply)?,
            WaitEvent::StartWrite { responder, reply } => {
                effects.push(next.start(responder, reply)?)
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

const WAIT_TRANSITIONS: [Transition; 23] = [
    step("live", "publish", "live", false),
    step("live", "raise_reply", "live", false),
    step("live", "raise_reply", "closed", true),
    step("live", "stop_reply", "live", false),
    step("live", "stop_reply", "closed", true),
    step("live", "stop_reply", "fenced_write", false),
    step("live", "call_ended", "live", false),
    step("live", "deliver_ending", "live", false),
    step("live", "local_stop", "live", false),
    step("live", "take_permit", "live", false),
    step("live", "start_write", "live", false),
    step("live", "capability_withdrawn", "live", false),
    step("live", "capability_withdrawn", "closed", true),
    step("live", "write_finished", "live", false),
    step("live", "write_finished", "closed", true),
    step("live", "authority_ended", "closed", true),
    step("live", "authority_ended", "fenced_write", false),
    step("fenced_write", "deliver_ending", "fenced_write", false),
    step("fenced_write", "write_finished", "closed", true),
    step("closed", "deliver_ending", "closed", true),
    step("live", "raise_reply", "fenced_write", false),
    step("closed", "authority_ended", "closed", true),
    step("fenced_write", "authority_ended", "fenced_write", false),
];

#[cfg(test)]
#[path = "waiting_request_test.rs"]
mod tests;
