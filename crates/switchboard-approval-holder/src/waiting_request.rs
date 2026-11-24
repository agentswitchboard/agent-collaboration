//! One approval wait, its exact responder and its Central Raise or Stop duty.

use std::marker::PhantomData;

use approval_request_protocol::{
    ApprovalDigestError, CallStopCause, DeliverEndingReply, DeliverEndingRequest, HolderStopCause,
    InvalidApprovalEnding, PrivateEnding, ProviderStopCause, Raise, RaiseReply,
    RequestContentsDigest, RequestKind, StopReceiptEnding, StopReply, StopRequest,
};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::run_gate::{CallKey, LinkAttempt, MatchedRunRefusal, RunAuthority};

#[path = "waiting_request/central_calls.rs"]
mod central_calls;
pub mod gate_access;

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
    // Local custody has ended or claimed a negative. Only evidence from the
    // exact pending Raise can make this candidate Stop eligible.
    AwaitingRaiseDisposition {
        raise: CallKey<Connection>,
        stop: StopRequest,
    },
    Stopping {
        request: StopRequest,
        call: Call<Connection>,
        published_attempts: Vec<CallKey<Connection>>,
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
    AlreadyEnded(StopReceiptEnding),
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

/// Which local responder this wait owns, including local capacity refusals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestFamily {
    ProviderAsk,
    HeldCall,
}

#[derive(Clone, Copy)]
enum FamilyStopCause {
    Provider(ProviderStopCause),
    Call(CallStopCause),
}

impl FamilyStopCause {
    const fn family(self) -> RequestFamily {
        match self {
            Self::Provider(_) => RequestFamily::ProviderAsk,
            Self::Call(_) => RequestFamily::HeldCall,
        }
    }
}

/// Central duty and responder custody for a single approval wait.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitState<Responder, Connection> {
    family: RequestFamily,
    request_id: Option<ApprovalRequestId>,
    router_run: Option<RouterRunId>,
    displaced_raise: Option<CallKey<Connection>>,
    // A later retry cannot disprove an admission concealed by a lost answer.
    earlier_raise_may_be_admitted: bool,
    frozen: Option<FrozenRaise>,
    central: CentralDuty<Connection>,
    custody: Custody<Responder>,
    claim: Option<DeliverEndingRequest>,
    authority: Authority,
}

impl<Responder, Connection> WaitState<Responder, Connection> {
    pub fn bound(raise: Raise, responder: Responder) -> Result<Self, ApprovalDigestError> {
        let digest = raise.contents_digest()?;
        let family = match raise.kind() {
            RequestKind::ProviderApproval | RequestKind::ProviderQuestion => {
                RequestFamily::ProviderAsk
            }
            RequestKind::UnprovenCall => RequestFamily::HeldCall,
        };
        Ok(Self {
            family,
            request_id: Some(raise.request_id().clone()),
            router_run: Some(raise.router_run().clone()),
            displaced_raise: None,
            earlier_raise_may_be_admitted: false,
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
        Self::local_refusal(
            RequestFamily::ProviderAsk,
            responder,
            LocalNegative::SessionNotBound,
        )
    }

    pub const fn at_capacity(family: RequestFamily, responder: Responder) -> Self {
        Self::local_refusal(family, responder, LocalNegative::HolderAtCapacity)
    }

    const fn local_refusal(
        family: RequestFamily,
        responder: Responder,
        reason: LocalNegative,
    ) -> Self {
        Self {
            family,
            request_id: None,
            router_run: None,
            displaced_raise: None,
            earlier_raise_may_be_admitted: false,
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

    /// Counts toward the local capacity limit until responder custody and Stop settle.
    pub const fn holds_place(&self) -> bool {
        !(matches!(self.central, CentralDuty::Done) && matches!(self.custody, Custody::Released))
    }

    /// Can be discarded once any matched Central calls have also drained.
    pub const fn may_be_forgotten(&self) -> bool {
        !self.holds_place() && !self.has_pending_calls()
    }

    /// A completed local wait can still have a matched Raise response to drain.
    pub const fn has_pending_calls(&self) -> bool {
        self.displaced_raise.is_some()
            || matches!(
                self.central,
                CentralDuty::Raising(Call::InFlight(_))
                    | CentralDuty::AwaitingRaiseDisposition { .. }
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
        if !self.holds_place() {
            self.earlier_raise_may_be_admitted = false;
            self.frozen = None;
            self.claim = None;
        }
    }
}

/// The link's observed disposition of one exact outgoing frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkAttemptDisposition {
    /// The writer proved that this frame never started.
    NotReached,
    /// The frame may have reached Central. The link has no answer for it.
    AnswerLost,
    Answered(LinkCallAnswer),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkCallAnswer {
    Raise(RaiseReply),
    Stop(StopReply),
}

/// Inputs from the authenticated link owner or the exact responder owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WaitEvent<Responder, Connection> {
    AttemptSettled {
        call: CallKey<Connection>,
        disposition: LinkAttemptDisposition,
    },
    /// Retires the carrier. An unresolved Raise still needs its exact disposition.
    ConnectionRetired {
        link: LinkAttempt<Connection>,
    },
    LocalStop {
        cause: HolderStopCause,
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
pub enum WaitEffect<Responder, Connection, Enrollment> {
    Raise {
        call: CallKey<Connection>,
        authority: RunAuthority<Enrollment>,
        request: Raise,
    },
    Stop {
        call: CallKey<Connection>,
        authority: RunAuthority<Enrollment>,
        request: StopRequest,
    },
    EndingReply {
        authority: RunAuthority<Enrollment>,
        link: LinkAttempt<Connection>,
        reply: DeliverEndingReply,
    },
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

/// An input conflicts with the approval wait's binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitRefusal {
    ReceiptForDifferentRequest,
    ErasureMarkerForDifferentRequestOrRun,
    StopCauseForDifferentKind,
    HolderRefusedLocallyRequiresRefusalSelection,
}

/// Owns an approval wait through its one-shot handover and any exact owed Stop.
pub struct WaitingRequest<Responder, Connection, Enrollment>(
    PhantomData<(Responder, Connection, Enrollment)>,
);

type WaitError = TransitionError<WaitRefusal>;
type Effects<Responder, Connection, Enrollment> =
    Vec<WaitEffect<Responder, Connection, Enrollment>>;

impl<Responder, Connection> StateTag for WaitState<Responder, Connection> {
    fn state_tag(&self) -> &'static str {
        if self.may_be_forgotten() {
            "closed"
        } else if !self.holds_place() {
            "draining"
        } else if self.authority == Authority::Ended {
            "fenced_write"
        } else {
            "live"
        }
    }

    fn is_terminal(&self) -> bool {
        self.may_be_forgotten()
    }
}

impl<Responder, Connection> EventTag for WaitEvent<Responder, Connection> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::AttemptSettled { disposition, .. } => match disposition {
                LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(_)) => "raise_reply",
                LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(_)) => "stop_reply",
                LinkAttemptDisposition::NotReached | LinkAttemptDisposition::AnswerLost => {
                    "call_ended"
                }
            },
            Self::ConnectionRetired { .. } => "connection_retired",
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
            Self::ErasureMarkerForDifferentRequestOrRun => {
                "erasure_marker_for_different_request_or_run"
            }
            Self::StopCauseForDifferentKind => "stop_cause_for_different_kind",
            Self::HolderRefusedLocallyRequiresRefusalSelection => {
                "holder_refused_locally_requires_refusal_selection"
            }
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

    fn accept_ending<Enrollment>(
        &mut self,
        request: DeliverEndingRequest,
        effects: &mut Effects<Responder, Connection, Enrollment>,
    ) -> Result<DeliverEndingReply, WaitError> {
        if self.request_id.as_ref() != Some(request.request_id()) {
            return Err(TransitionError::Stale);
        }
        let Some(frozen) = self.frozen.as_ref() else {
            return Ok(DeliverEndingReply::NotHeld {});
        };
        let validation = if request.contents_digest() != &frozen.digest {
            Err(InvalidApprovalEnding::BindingMismatch)
        } else {
            request.ending().check_bound_raise(&frozen.body)
        };
        if let Err(reason) = validation {
            effects.push(WaitEffect::EndingConflict {
                request_id: request.request_id().clone(),
                reason,
            });
            return Ok(DeliverEndingReply::EndingDiffers {});
        }
        if let Some(claimed) = &self.claim {
            if claimed == &request {
                return Ok(DeliverEndingReply::Received {});
            }
            effects.push(WaitEffect::EndingConflict {
                request_id: request.request_id().clone(),
                reason: InvalidApprovalEnding::BindingMismatch,
            });
            return Ok(DeliverEndingReply::EndingDiffers {});
        }
        Ok(match self.central {
            CentralDuty::Unsent | CentralDuty::Refusing => DeliverEndingReply::HolderNotReady {},
            CentralDuty::Raising(_) | CentralDuty::WaitingForEnding => {
                self.claim_available(ProviderReply::CentralEnding(request.ending().clone()))?;
                self.preserve_raising_call();
                self.central = CentralDuty::Done;
                self.claim = Some(request);
                DeliverEndingReply::Received {}
            }
            CentralDuty::Stopping { .. } => DeliverEndingReply::StopOwed {},
            CentralDuty::AwaitingRaiseDisposition { .. } => {
                self.stop_after_raise_proven_by_ending();
                DeliverEndingReply::StopOwed {}
            }
            CentralDuty::Done => DeliverEndingReply::NotHeld {},
        })
    }

    fn check_stop_family(&self, family: RequestFamily) -> Result<(), WaitError> {
        if self.family == family {
            Ok(())
        } else {
            Err(TransitionError::Domain(
                WaitRefusal::StopCauseForDifferentKind,
            ))
        }
    }

    fn stop_locally<Enrollment>(
        &mut self,
        cause: HolderStopCause,
        effects: &mut Effects<Responder, Connection, Enrollment>,
    ) -> Result<(), WaitError> {
        if cause != HolderStopCause::HolderRefusedLocally
            && (!self.holds_place() || self.authority == Authority::Ended)
        {
            return Err(TransitionError::Stale);
        }
        match cause {
            HolderStopCause::ProviderConnectionLost => self.abandon_reply(
                FamilyStopCause::Provider(ProviderStopCause::ProviderConnectionLost),
                effects,
            ),
            HolderStopCause::CallWithdrawn => {
                self.abandon_reply(FamilyStopCause::Call(CallStopCause::CallWithdrawn), effects)
            }
            HolderStopCause::TurnStopped => self.refuse_provider(ProviderStopCause::TurnStopped),
            HolderStopCause::SessionClosed => {
                self.refuse_provider(ProviderStopCause::SessionClosed)
            }
            HolderStopCause::AskReplaced => self.refuse_provider(ProviderStopCause::AskReplaced),
            HolderStopCause::AskWithdrawn => {
                self.check_stop_family(RequestFamily::ProviderAsk)?;
                Err(TransitionError::Stale)
            }
            HolderStopCause::HolderRefusedLocally => Err(TransitionError::Domain(
                WaitRefusal::HolderRefusedLocallyRequiresRefusalSelection,
            )),
        }
    }

    fn refuse_provider(&mut self, cause: ProviderStopCause) -> Result<(), WaitError> {
        self.check_stop_family(RequestFamily::ProviderAsk)?;
        let reply = ProviderReply::LocalNegative(LocalNegative::Stopped(cause));
        match &self.central {
            CentralDuty::Unsent | CentralDuty::Raising(_) | CentralDuty::WaitingForEnding => {
                self.claim_available(reply)?;
                self.begin_stop(FamilyStopCause::Provider(cause))?;
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
            CentralDuty::AwaitingRaiseDisposition { .. }
            | CentralDuty::Stopping { .. }
            | CentralDuty::Refusing
            | CentralDuty::Done => {
                return Err(TransitionError::Stale);
            }
        }
        Ok(())
    }

    fn provider_withdrew<Enrollment>(
        &mut self,
        exact: &Responder,
        effects: &mut Effects<Responder, Connection, Enrollment>,
    ) -> Result<(), WaitError> {
        self.check_stop_family(RequestFamily::ProviderAsk)?;
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
        self.abandon_reply(
            FamilyStopCause::Provider(ProviderStopCause::AskWithdrawn),
            effects,
        )
    }

    fn abandon_reply<Enrollment>(
        &mut self,
        cause: FamilyStopCause,
        effects: &mut Effects<Responder, Connection, Enrollment>,
    ) -> Result<(), WaitError> {
        self.check_stop_family(cause.family())?;
        match self.central {
            CentralDuty::Unsent | CentralDuty::Raising(_) | CentralDuty::WaitingForEnding => {
                self.begin_stop(cause)?;
            }
            CentralDuty::AwaitingRaiseDisposition { .. }
            | CentralDuty::Stopping { .. }
            | CentralDuty::Refusing
            | CentralDuty::Done => {}
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

    fn start<Enrollment>(
        &mut self,
        exact: Responder,
        expected_reply: ProviderReply,
    ) -> Result<WaitEffect<Responder, Connection, Enrollment>, WaitError> {
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

    fn withdraw<Enrollment>(
        &mut self,
        exact: Responder,
    ) -> Result<WaitEffect<Responder, Connection, Enrollment>, WaitError> {
        if !matches!(&self.custody, Custody::Claimed { responder, .. }
            | Custody::Permitted { responder, .. } if responder == &exact)
        {
            return Err(TransitionError::Stale);
        }
        self.custody = Custody::Released;
        Ok(WaitEffect::ReleaseBinding { responder: exact })
    }

    fn complete<Enrollment>(
        &mut self,
        exact: Responder,
        outcome: WriterOutcome,
        effects: &mut Effects<Responder, Connection, Enrollment>,
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

    fn fence<Enrollment>(&mut self, effects: &mut Effects<Responder, Connection, Enrollment>) {
        self.authority = Authority::Ended;
        self.displaced_raise = None;
        self.earlier_raise_may_be_admitted = false;
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

impl<Responder: Clone + Eq, Connection: Clone + Eq, Enrollment> StateMachine
    for WaitingRequest<Responder, Connection, Enrollment>
{
    type State = WaitState<Responder, Connection>;
    type Event = WaitEvent<Responder, Connection>;
    type Effect = WaitEffect<Responder, Connection, Enrollment>;
    type Error = WaitRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        let mut next = state.clone();
        let mut effects = Vec::new();
        match event {
            WaitEvent::WriteFinished { responder, outcome } => {
                next.complete(responder, outcome, &mut effects)?
            }
            WaitEvent::AuthorityEnded => next.fence(&mut effects),
            WaitEvent::AttemptSettled { call, disposition } => match disposition {
                LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(reply)) => {
                    next.receive_raise(call, reply, &mut effects)?
                }
                LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(reply)) => {
                    next.receive_stop(call, reply, &mut effects)?
                }
                LinkAttemptDisposition::NotReached => next.settle_unanswered(&call, false)?,
                LinkAttemptDisposition::AnswerLost => next.settle_unanswered(&call, true)?,
            },
            WaitEvent::ConnectionRetired { link } => next.retire_connection(&link)?,
            WaitEvent::LocalStop { cause } => next.stop_locally(cause, &mut effects)?,
            _ if !state.holds_place() || state.authority == Authority::Ended => {
                return Err(TransitionError::Stale);
            }
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
        WAIT_TRANSITIONS
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

const WAIT_TRANSITIONS: &[Transition] = &[
    step("live", "raise_reply", "live", false),
    step("live", "raise_reply", "closed", true),
    step("live", "stop_reply", "live", false),
    step("live", "stop_reply", "closed", true),
    step("live", "call_ended", "live", false),
    step("live", "call_ended", "closed", true),
    step("draining", "call_ended", "closed", true),
    step("live", "connection_retired", "live", false),
    step("draining", "connection_retired", "closed", true),
    step("draining", "raise_reply", "closed", true),
    step("live", "local_stop", "closed", true),
    step("live", "local_stop", "draining", false),
    step("live", "local_stop", "live", false),
    step("live", "take_permit", "live", false),
    step("live", "start_write", "live", false),
    step("live", "provider_withdrew", "live", false),
    step("live", "provider_withdrew", "closed", true),
    step("live", "capability_withdrawn", "live", false),
    step("live", "capability_withdrawn", "closed", true),
    step("live", "write_finished", "live", false),
    step("live", "write_finished", "closed", true),
    step("live", "stop_reply", "draining", false),
    step("live", "provider_withdrew", "draining", false),
    step("live", "capability_withdrawn", "draining", false),
    step("live", "write_finished", "draining", false),
    step("draining", "authority_ended", "closed", true),
    step("live", "authority_ended", "closed", true),
    step("live", "authority_ended", "fenced_write", false),
    step("fenced_write", "write_finished", "closed", true),
    step("closed", "authority_ended", "closed", true),
    step("fenced_write", "authority_ended", "fenced_write", false),
];

#[cfg(test)]
#[path = "waiting_request_test.rs"]
mod tests;
