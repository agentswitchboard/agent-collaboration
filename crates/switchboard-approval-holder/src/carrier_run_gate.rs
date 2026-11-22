//! One run's authority to publish approval calls and accept ending offers.

use std::marker::PhantomData;

use approval_request_protocol::{
    AuthorityProbeReply, AuthorityProbeRequest, DeliverEndingReply, RegisterRunReply,
    RegisterRunRequest, RunRefusal,
};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::RouterRunId;

/// The enrollment and Router run whose link authority the gate controls.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunAuthority<Enrollment> {
    pub enrollment: Enrollment,
    pub router_run: RouterRunId,
}

/// A correlation number allocated by the gate on an admitted carrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GateCallId(u64);

/// Identifies a failure episode as observed by the gate, across dial and carrier changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutageEpoch(u64);

/// The terminal authority loss observed by the Mac.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalAuthority {
    RunRetired,
    EnrollmentEnded,
}

/// The ending-offer disposition the gate supplies to its caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndingIngress {
    Authoritative,
    Reply(DeliverEndingReply),
    Discard,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Registration {
    Due,
    Pending(GateCallId),
    Ready,
    Probing(GateCallId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AdmittedCarrier<Carrier> {
    identity: Carrier,
    next_call: u64,
    registration: Registration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CarrierPhase<Dial, Carrier> {
    Disconnected,
    Dialing(Dial),
    Admitted(AdmittedCarrier<Carrier>),
    Fenced,
}

/// Link admission and outage custody for one local run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateState<Enrollment, Dial, Carrier> {
    authority: RunAuthority<Enrollment>,
    phase: CarrierPhase<Dial, Carrier>,
    outage: Option<OutageEpoch>,
    last_epoch: u64,
}

impl<Enrollment: Eq, Dial, Carrier: Eq> GateState<Enrollment, Dial, Carrier> {
    pub const fn new(authority: RunAuthority<Enrollment>) -> Self {
        Self {
            authority,
            phase: CarrierPhase::Disconnected,
            outage: None,
            last_epoch: 0,
        }
    }

    pub const fn outage(&self) -> Option<OutageEpoch> {
        self.outage
    }

    pub fn ending_ingress(
        &self,
        authority: &RunAuthority<Enrollment>,
        identity: &Carrier,
    ) -> EndingIngress {
        if authority == &self.authority {
            match &self.phase {
                CarrierPhase::Fenced => return EndingIngress::Discard,
                CarrierPhase::Admitted(carrier)
                    if &carrier.identity == identity
                        && matches!(
                            carrier.registration,
                            Registration::Ready | Registration::Probing(_)
                        ) =>
                {
                    return EndingIngress::Authoritative;
                }
                CarrierPhase::Disconnected
                | CarrierPhase::Dialing(_)
                | CarrierPhase::Admitted(_) => {}
            }
        }
        EndingIngress::Reply(DeliverEndingReply::HolderNotReady {})
    }
}

/// Observations and publication opportunities supplied by the link owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateEvent<Enrollment, Dial, Carrier> {
    DialStarted {
        dial: Dial,
    },
    Welcome {
        dial: Dial,
        authority: RunAuthority<Enrollment>,
        carrier: Carrier,
    },
    DialFailed {
        dial: Dial,
    },
    CarrierLost {
        carrier: Carrier,
    },
    PublishRegistration {
        carrier: Carrier,
    },
    RegistrationReply {
        carrier: Carrier,
        call: GateCallId,
        reply: RegisterRunReply,
    },
    PublishProbe {
        carrier: Carrier,
    },
    ProbeReply {
        carrier: Carrier,
        call: GateCallId,
        reply: AuthorityProbeReply,
    },
    AttemptFailed {
        carrier: Carrier,
        call: GateCallId,
    },
    /// The holder operation has matched a failure to its own live attempt.
    HolderAttemptFailed {
        carrier: Carrier,
    },
    /// The holder operation has matched the refusal to its own live attempt.
    HolderAuthorityRefused {
        carrier: Carrier,
        refusal: RunRefusal,
    },
    AuthenticatedEnrollmentEnded {
        enrollment: Enrollment,
    },
}

/// Instructions and status changes the gate returns to its owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateEffect<Enrollment, Carrier> {
    PublishRegistration {
        carrier: Carrier,
        call: GateCallId,
        request: RegisterRunRequest,
    },
    PublishProbe {
        carrier: Carrier,
        call: GateCallId,
        request: AuthorityProbeRequest,
    },
    OutageStarted {
        epoch: OutageEpoch,
    },
    OutageEnded {
        epoch: OutageEpoch,
    },
    AcknowledgedReplayDue,
    /// Fence the exact run and enrollment at the link, waits, and writers.
    Fence {
        authority: RunAuthority<Enrollment>,
        reason: TerminalAuthority,
    },
}

/// Local sequence allocation failures that leave the gate unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateRefusal {
    CallSequenceExhausted,
    OutageSequenceExhausted,
}

/// Reducer for the carrier gate belonging to one run's enrollment.
pub struct CarrierRunGate<Enrollment, Dial, Carrier>(PhantomData<(Enrollment, Dial, Carrier)>);

impl<Enrollment, Dial, Carrier> StateTag for GateState<Enrollment, Dial, Carrier> {
    fn state_tag(&self) -> &'static str {
        match &self.phase {
            CarrierPhase::Disconnected => "disconnected",
            CarrierPhase::Dialing(_) => "dialing",
            CarrierPhase::Admitted(carrier) => match carrier.registration {
                Registration::Due => "registration_due",
                Registration::Pending(_) => "registering",
                Registration::Ready => "ready",
                Registration::Probing(_) => "probing",
            },
            CarrierPhase::Fenced => "fenced",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self.phase, CarrierPhase::Fenced)
    }
}

impl<Enrollment, Dial, Carrier> EventTag for GateEvent<Enrollment, Dial, Carrier> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::DialStarted { .. } => "dial_started",
            Self::Welcome { .. } => "welcome",
            Self::DialFailed { .. } => "dial_failed",
            Self::CarrierLost { .. } => "carrier_lost",
            Self::PublishRegistration { .. } => "publish_registration",
            Self::RegistrationReply { .. } => "registration_reply",
            Self::PublishProbe { .. } => "publish_probe",
            Self::ProbeReply { .. } => "probe_reply",
            Self::AttemptFailed { .. } => "attempt_failed",
            Self::HolderAttemptFailed { .. } => "holder_attempt_failed",
            Self::HolderAuthorityRefused { .. } => "holder_authority_refused",
            Self::AuthenticatedEnrollmentEnded { .. } => "authenticated_enrollment_ended",
        }
    }
}

impl RefusalTag for GateRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::CallSequenceExhausted => "call_sequence_exhausted",
            Self::OutageSequenceExhausted => "outage_sequence_exhausted",
        }
    }
}

type GateError = TransitionError<GateRefusal>;

impl<Carrier> AdmittedCarrier<Carrier> {
    fn allocate_call(&mut self) -> Result<GateCallId, GateError> {
        let call = GateCallId(self.next_call);
        self.next_call = self
            .next_call
            .checked_add(1)
            .ok_or(TransitionError::Domain(GateRefusal::CallSequenceExhausted))?;
        Ok(call)
    }
}

impl<Enrollment: Clone, Dial, Carrier: Eq> GateState<Enrollment, Dial, Carrier> {
    fn current_carrier(
        &mut self,
        identity: &Carrier,
    ) -> Result<&mut AdmittedCarrier<Carrier>, GateError> {
        match &mut self.phase {
            CarrierPhase::Admitted(carrier) if &carrier.identity == identity => Ok(carrier),
            CarrierPhase::Disconnected
            | CarrierPhase::Dialing(_)
            | CarrierPhase::Admitted(_)
            | CarrierPhase::Fenced => Err(TransitionError::Stale),
        }
    }

    fn mark_outage(
        &mut self,
        effects: &mut Vec<GateEffect<Enrollment, Carrier>>,
    ) -> Result<(), GateError> {
        if self.outage.is_none() {
            self.last_epoch = self
                .last_epoch
                .checked_add(1)
                .ok_or(TransitionError::Domain(
                    GateRefusal::OutageSequenceExhausted,
                ))?;
            let epoch = OutageEpoch(self.last_epoch);
            self.outage = Some(epoch);
            effects.push(GateEffect::OutageStarted { epoch });
        }
        Ok(())
    }

    fn end_outage(&mut self, effects: &mut Vec<GateEffect<Enrollment, Carrier>>) {
        if let Some(epoch) = self.outage.take() {
            effects.push(GateEffect::OutageEnded { epoch });
        }
    }

    fn fence(
        &mut self,
        reason: TerminalAuthority,
        effects: &mut Vec<GateEffect<Enrollment, Carrier>>,
    ) {
        self.phase = CarrierPhase::Fenced;
        effects.push(GateEffect::Fence {
            authority: self.authority.clone(),
            reason,
        });
    }

    fn observe_refusal(
        &mut self,
        identity: &Carrier,
        refusal: RunRefusal,
        effects: &mut Vec<GateEffect<Enrollment, Carrier>>,
    ) -> Result<(), GateError> {
        match refusal {
            RunRefusal::RunNotRegistered {} => {
                self.current_carrier(identity)?.registration = Registration::Due
            }
            RunRefusal::RunRetired {} => self.fence(TerminalAuthority::RunRetired, effects),
            RunRefusal::EnrollmentEnded {} => {
                self.fence(TerminalAuthority::EnrollmentEnded, effects)
            }
        }
        Ok(())
    }
}

impl<Enrollment: Clone + Eq, Dial: Clone + Eq, Carrier: Clone + Eq> StateMachine
    for CarrierRunGate<Enrollment, Dial, Carrier>
{
    type State = GateState<Enrollment, Dial, Carrier>;
    type Event = GateEvent<Enrollment, Dial, Carrier>;
    type Effect = GateEffect<Enrollment, Carrier>;
    type Error = GateRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        if state.is_terminal() {
            return Err(TransitionError::Stale);
        }
        let mut next = state.clone();
        let mut effects = Vec::new();
        match event {
            GateEvent::DialStarted { dial } => {
                if !matches!(state.phase, CarrierPhase::Disconnected) {
                    return Err(TransitionError::Stale);
                }
                next.phase = CarrierPhase::Dialing(dial);
            }
            GateEvent::Welcome {
                dial,
                authority,
                carrier,
            } => {
                if !matches!(&state.phase, CarrierPhase::Dialing(pending) if pending == &dial)
                    || state.authority != authority
                {
                    return Err(TransitionError::Stale);
                }
                next.phase = CarrierPhase::Admitted(AdmittedCarrier {
                    identity: carrier,
                    next_call: 0,
                    registration: Registration::Due,
                });
            }
            GateEvent::DialFailed { dial } => {
                if !matches!(&state.phase, CarrierPhase::Dialing(pending) if pending == &dial) {
                    return Err(TransitionError::Stale);
                }
                next.phase = CarrierPhase::Disconnected;
                next.mark_outage(&mut effects)?;
            }
            GateEvent::CarrierLost { carrier } => {
                next.current_carrier(&carrier)?;
                next.phase = CarrierPhase::Disconnected;
                next.mark_outage(&mut effects)?;
            }
            GateEvent::PublishRegistration { carrier } => {
                let admitted = next.current_carrier(&carrier)?;
                if admitted.registration != Registration::Due {
                    return Err(TransitionError::Stale);
                }
                let call = admitted.allocate_call()?;
                admitted.registration = Registration::Pending(call);
                let request = RegisterRunRequest {
                    router_run: state.authority.router_run.clone(),
                };
                effects.push(GateEffect::PublishRegistration {
                    carrier,
                    call,
                    request,
                });
            }
            GateEvent::RegistrationReply {
                carrier,
                call,
                reply,
            } => {
                let admitted = next.current_carrier(&carrier)?;
                if admitted.registration != Registration::Pending(call) {
                    return Err(TransitionError::Stale);
                }
                match reply {
                    RegisterRunReply::Registered {} => {
                        admitted.registration = Registration::Ready;
                        next.end_outage(&mut effects);
                        effects.push(GateEffect::AcknowledgedReplayDue);
                    }
                    RegisterRunReply::RunNotRegistered {} => {
                        admitted.registration = Registration::Due
                    }
                    RegisterRunReply::RunRetired {} => {
                        next.fence(TerminalAuthority::RunRetired, &mut effects)
                    }
                    RegisterRunReply::EnrollmentEnded {} => {
                        next.fence(TerminalAuthority::EnrollmentEnded, &mut effects)
                    }
                }
            }
            GateEvent::PublishProbe { carrier } => {
                let outage = next.outage.is_some();
                let admitted = next.current_carrier(&carrier)?;
                if !outage || admitted.registration != Registration::Ready {
                    return Err(TransitionError::Stale);
                }
                let call = admitted.allocate_call()?;
                admitted.registration = Registration::Probing(call);
                let request = AuthorityProbeRequest {
                    router_run: state.authority.router_run.clone(),
                };
                effects.push(GateEffect::PublishProbe {
                    carrier,
                    call,
                    request,
                });
            }
            GateEvent::ProbeReply {
                carrier,
                call,
                reply,
            } => {
                let admitted = next.current_carrier(&carrier)?;
                if admitted.registration != Registration::Probing(call) {
                    return Err(TransitionError::Stale);
                }
                match reply {
                    AuthorityProbeReply::Admitted {} => {
                        admitted.registration = Registration::Ready;
                        next.end_outage(&mut effects);
                    }
                    AuthorityProbeReply::AuthorityRefused { refusal } => {
                        next.observe_refusal(&carrier, refusal, &mut effects)?
                    }
                }
            }
            GateEvent::AttemptFailed { carrier, call } => {
                let admitted = next.current_carrier(&carrier)?;
                admitted.registration = match admitted.registration {
                    Registration::Pending(current) if call == current => Registration::Due,
                    Registration::Probing(current) if call == current => Registration::Ready,
                    Registration::Due
                    | Registration::Pending(_)
                    | Registration::Ready
                    | Registration::Probing(_) => return Err(TransitionError::Stale),
                };
                next.mark_outage(&mut effects)?;
            }
            GateEvent::HolderAttemptFailed { carrier } => {
                next.current_carrier(&carrier)?;
                next.mark_outage(&mut effects)?;
            }
            GateEvent::HolderAuthorityRefused { carrier, refusal } => {
                next.current_carrier(&carrier)?;
                next.observe_refusal(&carrier, refusal, &mut effects)?;
            }
            GateEvent::AuthenticatedEnrollmentEnded { enrollment } => {
                if state.authority.enrollment != enrollment {
                    return Err(TransitionError::Stale);
                }
                next.fence(TerminalAuthority::EnrollmentEnded, &mut effects);
            }
        }
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        &GATE_TRANSITIONS
    }
}

const fn move_to(from: &'static str, event: &'static str, to: &'static str) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal: false,
    }
}

const fn fence_from(from: &'static str, event: &'static str) -> Transition {
    Transition {
        from,
        event,
        to: "fenced",
        terminal: true,
    }
}

const GATE_TRANSITIONS: [Transition; 35] = [
    move_to("disconnected", "dial_started", "dialing"),
    move_to("dialing", "welcome", "registration_due"),
    move_to("dialing", "dial_failed", "disconnected"),
    move_to("registration_due", "carrier_lost", "disconnected"),
    move_to("registering", "carrier_lost", "disconnected"),
    move_to("ready", "carrier_lost", "disconnected"),
    move_to("probing", "carrier_lost", "disconnected"),
    move_to("registration_due", "publish_registration", "registering"),
    move_to("registering", "registration_reply", "ready"),
    move_to("registering", "registration_reply", "registration_due"),
    fence_from("registering", "registration_reply"),
    move_to("ready", "publish_probe", "probing"),
    move_to("probing", "probe_reply", "ready"),
    move_to("probing", "probe_reply", "registration_due"),
    fence_from("probing", "probe_reply"),
    move_to("registering", "attempt_failed", "registration_due"),
    move_to("probing", "attempt_failed", "ready"),
    move_to(
        "registration_due",
        "holder_attempt_failed",
        "registration_due",
    ),
    move_to("registering", "holder_attempt_failed", "registering"),
    move_to("ready", "holder_attempt_failed", "ready"),
    move_to("probing", "holder_attempt_failed", "probing"),
    move_to(
        "registration_due",
        "holder_authority_refused",
        "registration_due",
    ),
    move_to(
        "registering",
        "holder_authority_refused",
        "registration_due",
    ),
    move_to("ready", "holder_authority_refused", "registration_due"),
    move_to("probing", "holder_authority_refused", "registration_due"),
    fence_from("registration_due", "holder_authority_refused"),
    fence_from("registering", "holder_authority_refused"),
    fence_from("ready", "holder_authority_refused"),
    fence_from("probing", "holder_authority_refused"),
    fence_from("disconnected", "authenticated_enrollment_ended"),
    fence_from("dialing", "authenticated_enrollment_ended"),
    fence_from("registration_due", "authenticated_enrollment_ended"),
    fence_from("registering", "authenticated_enrollment_ended"),
    fence_from("ready", "authenticated_enrollment_ended"),
    fence_from("probing", "authenticated_enrollment_ended"),
];

#[cfg(test)]
#[path = "carrier_run_gate_test.rs"]
mod tests;
