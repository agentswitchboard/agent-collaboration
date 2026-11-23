//! Lease and deadline transitions for a single Mac link.

use std::{marker::PhantomData, time::Duration};

use central_link_client::{ConnectionAttempt, EnrollmentEnd, LinkFault};
use central_link_protocol::{CorrelationId, MachineLinkCloseCode, TokenRenewalRefusal};
use router_clock::ClockInstant;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::MachineId;

const FRESH_CREDENTIAL_FAULT_DELAY: Duration = Duration::from_secs(60);
const RENEWAL_MARGIN: Duration = Duration::from_secs(60);

/// A lease source's monotonic revision of one enrollment binding.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LeaseVersion(pub u64);

/// Identity and usability of a lease confirmed by the lease source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseWindow<Binding> {
    pub binding: Binding,
    pub version: LeaseVersion,
    pub usable_until: ClockInstant,
}

/// Progress as seen by a reader of the driver status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkStatus {
    NotEnrolled,
    Connecting,
    Welcomed { machine_id: MachineId },
    Connected { machine_id: MachineId },
    Fault { kind: LinkFault },
    EnrollmentEnded { cause: EnrollmentEnd },
}

/// HTTP or transport disposition supplied by the dialer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DialFailure {
    Unauthorized,
    Busy,
    InvalidAddress,
    Transport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DialLease {
    Current,
    NewerThan(LeaseVersion),
    RetryUnauthorized(LeaseVersion),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Renewal {
    Scheduled(ClockInstant),
    Acquiring,
    Pending(CorrelationId),
    Refused,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LinkPhase<Binding> {
    NotEnrolled,
    Waiting {
        binding: Binding,
        lease: DialLease,
    },
    Dialing {
        attempt: ConnectionAttempt,
        lease: LeaseWindow<Binding>,
        refresh: DialLease,
    },
    Welcomed {
        attempt: ConnectionAttempt,
        lease: LeaseWindow<Binding>,
        deadline: ClockInstant,
        renewal: Renewal,
    },
    Fault {
        binding: Binding,
        kind: LinkFault,
        retry_at: Option<ClockInstant>,
    },
    EnrollmentEnded {
        binding: Binding,
        cause: EnrollmentEnd,
    },
}

/// The driver policy. Clock samples and observations arrive as inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkState<Binding> {
    machine_id: MachineId,
    phase: LinkPhase<Binding>,
}

impl<Binding> LinkState<Binding> {
    pub const fn new(machine_id: MachineId) -> Self {
        Self {
            machine_id,
            phase: LinkPhase::NotEnrolled,
        }
    }

    pub fn status(&self) -> LinkStatus {
        match self.phase {
            LinkPhase::NotEnrolled => LinkStatus::NotEnrolled,
            LinkPhase::Waiting { .. } | LinkPhase::Dialing { .. } => LinkStatus::Connecting,
            LinkPhase::Welcomed { .. } => LinkStatus::Welcomed {
                machine_id: self.machine_id.clone(),
            },
            LinkPhase::Fault { kind, .. } => LinkStatus::Fault { kind },
            LinkPhase::EnrollmentEnded { cause, .. } => LinkStatus::EnrollmentEnded { cause },
        }
    }

    pub fn next_deadline(&self) -> Option<ClockInstant> {
        match self.phase {
            LinkPhase::Welcomed {
                deadline,
                renewal: Renewal::Scheduled(at),
                ..
            } => Some(deadline.min(at)),
            LinkPhase::Welcomed { deadline, .. } => Some(deadline),
            LinkPhase::Fault { retry_at, .. } => retry_at,
            LinkPhase::NotEnrolled
            | LinkPhase::Waiting { .. }
            | LinkPhase::Dialing { .. }
            | LinkPhase::EnrollmentEnded { .. } => None,
        }
    }

    pub(super) fn dial_requirement(&self) -> Option<Option<LeaseVersion>> {
        let LinkPhase::Waiting { lease, .. } = &self.phase else {
            return None;
        };
        Some(match lease {
            DialLease::Current => None,
            DialLease::NewerThan(version) | DialLease::RetryUnauthorized(version) => Some(*version),
        })
    }

    pub(super) fn renewal_deadline(&self) -> Option<ClockInstant> {
        match self.phase {
            LinkPhase::Welcomed { deadline, .. } => Some(deadline),
            _ => None,
        }
    }

    pub(super) fn ending(&self) -> Option<EnrollmentEnd> {
        match self.phase {
            LinkPhase::EnrollmentEnded { cause, .. } => Some(cause),
            _ => None,
        }
    }

    fn binding(&self) -> Option<&Binding> {
        match &self.phase {
            LinkPhase::NotEnrolled => None,
            LinkPhase::Waiting { binding, .. }
            | LinkPhase::Fault { binding, .. }
            | LinkPhase::EnrollmentEnded { binding, .. } => Some(binding),
            LinkPhase::Dialing { lease, .. } | LinkPhase::Welcomed { lease, .. } => {
                Some(&lease.binding)
            }
        }
    }
}

/// Observations from the lease source, the socket, or the elapsed scheduler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkInput<Binding> {
    BindingAvailable(Binding),
    DialStarted {
        attempt: ConnectionAttempt,
        lease: LeaseWindow<Binding>,
        now: ClockInstant,
    },
    DialFailed {
        attempt: ConnectionAttempt,
        reason: DialFailure,
        now: ClockInstant,
    },
    Welcome {
        attempt: ConnectionAttempt,
        machine_id: MachineId,
        deadline: ClockInstant,
        now: ClockInstant,
    },
    Tick {
        now: ClockInstant,
    },
    RenewalLease {
        attempt: ConnectionAttempt,
        lease: LeaseWindow<Binding>,
        correlation_id: CorrelationId,
        now: ClockInstant,
    },
    RenewalAccepted {
        attempt: ConnectionAttempt,
        correlation_id: CorrelationId,
        deadline: ClockInstant,
        now: ClockInstant,
    },
    RenewalRefused {
        attempt: ConnectionAttempt,
        correlation_id: CorrelationId,
        reason: TokenRenewalRefusal,
        now: ClockInstant,
    },
    RenewalUnavailable {
        attempt: ConnectionAttempt,
    },
    SocketClosed {
        attempt: ConnectionAttempt,
        code: Option<MachineLinkCloseCode>,
    },
    LeaseEnded {
        binding: Binding,
    },
    LeaseFault {
        binding: Binding,
    },
    AddressCorrected,
    AddressChanged {
        attempt: ConnectionAttempt,
    },
}

/// Instructions for the driver to apply after publishing its next state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkEffect<Binding> {
    NeedLease {
        binding: Binding,
        newer_than: Option<LeaseVersion>,
    },
    RenewOnSocket {
        attempt: ConnectionAttempt,
        correlation_id: CorrelationId,
    },
    BackOff {
        attempt: ConnectionAttempt,
    },
    EndEnrollment {
        binding: Binding,
        cause: EnrollmentEnd,
    },
}

/// Domain refusal when a supplied lease cannot be used.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkRefusal {
    UnusableLease,
    WrongMachine,
}

type PolicyError = TransitionError<LinkRefusal>;

/// Pure state machine driven by the Mac socket and lease owners.
pub struct LinkPolicy<Binding>(PhantomData<fn() -> Binding>);

impl<Binding> StateTag for LinkState<Binding> {
    fn state_tag(&self) -> &'static str {
        match self.phase {
            LinkPhase::NotEnrolled => "not_enrolled",
            LinkPhase::Waiting { .. } => "waiting",
            LinkPhase::Dialing { .. } => "dialing",
            LinkPhase::Welcomed { .. } => "welcomed",
            LinkPhase::Fault { .. } => "fault",
            LinkPhase::EnrollmentEnded { .. } => "enrollment_ended",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

impl<Binding> EventTag for LinkInput<Binding> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::BindingAvailable(_) => "binding",
            Self::DialStarted { .. } => "dial",
            Self::DialFailed { .. } => "dial_failed",
            Self::Welcome { .. } => "welcome",
            Self::Tick { .. } => "tick",
            Self::RenewalLease { .. } => "renewal_lease",
            Self::RenewalAccepted { .. } => "renewal_accepted",
            Self::RenewalRefused { .. } => "renewal_refused",
            Self::RenewalUnavailable { .. } => "renewal_unavailable",
            Self::SocketClosed { .. } => "socket_closed",
            Self::LeaseEnded { .. } => "lease_ended",
            Self::LeaseFault { .. } => "lease_fault",
            Self::AddressCorrected => "address_corrected",
            Self::AddressChanged { .. } => "address_changed",
        }
    }
}

impl RefusalTag for LinkRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::UnusableLease => "unusable_lease",
            Self::WrongMachine => "wrong_machine",
        }
    }
}

fn renewal_schedule(now: ClockInstant, deadline: ClockInstant) -> Renewal {
    let left = deadline.saturating_duration_since(now);
    Renewal::Scheduled(now.after(left - RENEWAL_MARGIN.min(left / 2)))
}

fn lease_effect<Binding>(binding: Binding, lease: &DialLease) -> LinkEffect<Binding> {
    let newer_than = match lease {
        DialLease::Current => None,
        DialLease::NewerThan(version) | DialLease::RetryUnauthorized(version) => Some(*version),
    };
    LinkEffect::NeedLease {
        binding,
        newer_than,
    }
}

impl<Binding: Clone + Eq> LinkState<Binding> {
    fn wait_for_lease(
        &mut self,
        binding: Binding,
        lease: DialLease,
        effects: &mut Vec<LinkEffect<Binding>>,
    ) {
        effects.push(lease_effect(binding.clone(), &lease));
        self.phase = LinkPhase::Waiting { binding, lease };
    }

    fn end(
        &mut self,
        binding: Binding,
        cause: EnrollmentEnd,
        effects: &mut Vec<LinkEffect<Binding>>,
    ) {
        effects.push(LinkEffect::EndEnrollment {
            binding: binding.clone(),
            cause,
        });
        self.phase = LinkPhase::EnrollmentEnded { binding, cause };
    }

    fn check_renewal(
        &self,
        attempt: &ConnectionAttempt,
        correlation_id: &CorrelationId,
    ) -> Result<(), PolicyError> {
        match &self.phase {
            LinkPhase::Welcomed {
                attempt: current,
                renewal: Renewal::Pending(pending),
                ..
            } if current == attempt && pending == correlation_id => Ok(()),
            _ => Err(TransitionError::Stale),
        }
    }

    fn time_elapsed(&mut self, now: ClockInstant, effects: &mut Vec<LinkEffect<Binding>>) {
        match &mut self.phase {
            LinkPhase::Welcomed {
                attempt,
                lease,
                deadline,
                ..
            } if now >= *deadline => {
                effects.push(LinkEffect::BackOff {
                    attempt: attempt.clone(),
                });
                let binding = lease.binding.clone();
                let previous = lease.version;
                self.wait_for_lease(binding, DialLease::NewerThan(previous), effects);
            }
            LinkPhase::Welcomed {
                lease,
                renewal: renewal @ Renewal::Scheduled(_),
                ..
            } if matches!(&*renewal, Renewal::Scheduled(at) if now >= *at) => {
                effects.push(lease_effect(
                    lease.binding.clone(),
                    &DialLease::NewerThan(lease.version),
                ));
                *renewal = Renewal::Acquiring;
            }
            LinkPhase::Fault {
                binding,
                retry_at: Some(at),
                ..
            } if now >= *at => {
                let binding = binding.clone();
                self.wait_for_lease(binding, DialLease::Current, effects);
            }
            _ => {}
        }
    }
}

impl<Binding: Clone + Eq> StateMachine for LinkPolicy<Binding> {
    type State = LinkState<Binding>;
    type Event = LinkInput<Binding>;
    type Effect = LinkEffect<Binding>;
    type Error = LinkRefusal;

    fn transition(state: &Self::State, input: Self::Event) -> StateMachineResult<Self> {
        let mut next = state.clone();
        let mut effects = Vec::new();
        match input {
            LinkInput::BindingAvailable(binding) => {
                if state.binding() == Some(&binding) {
                    return Err(TransitionError::Stale);
                }
                if let LinkPhase::Dialing { attempt, .. } | LinkPhase::Welcomed { attempt, .. } =
                    &state.phase
                {
                    effects.push(LinkEffect::BackOff {
                        attempt: attempt.clone(),
                    });
                }
                next.wait_for_lease(binding, DialLease::Current, &mut effects);
            }
            LinkInput::DialStarted {
                attempt,
                lease,
                now,
            } => {
                let LinkPhase::Waiting {
                    binding,
                    lease: refresh,
                } = &state.phase
                else {
                    return Err(TransitionError::Stale);
                };
                if binding != &lease.binding {
                    return Err(TransitionError::Stale);
                }
                if now >= lease.usable_until
                    || matches!(refresh, DialLease::NewerThan(version) | DialLease::RetryUnauthorized(version) if lease.version <= *version)
                {
                    return Err(TransitionError::Domain(LinkRefusal::UnusableLease));
                }
                next.phase = LinkPhase::Dialing {
                    attempt,
                    lease,
                    refresh: refresh.clone(),
                };
            }
            LinkInput::DialFailed {
                attempt,
                reason,
                now,
            } => {
                let LinkPhase::Dialing {
                    attempt: current,
                    lease,
                    refresh,
                } = &state.phase
                else {
                    return Err(TransitionError::Stale);
                };
                if current != &attempt {
                    return Err(TransitionError::Stale);
                }
                let binding = lease.binding.clone();
                match reason {
                    DialFailure::Unauthorized
                        if !matches!(refresh, DialLease::RetryUnauthorized(_)) =>
                    {
                        effects.push(LinkEffect::BackOff { attempt });
                        next.wait_for_lease(
                            binding,
                            DialLease::RetryUnauthorized(lease.version),
                            &mut effects,
                        );
                    }
                    DialFailure::Unauthorized => {
                        next.phase = LinkPhase::Fault {
                            binding,
                            kind: LinkFault::FreshCredentialRefused,
                            retry_at: Some(now.after(FRESH_CREDENTIAL_FAULT_DELAY)),
                        };
                    }
                    DialFailure::InvalidAddress => {
                        next.phase = LinkPhase::Fault {
                            binding,
                            kind: LinkFault::MalformedAddress,
                            retry_at: None,
                        };
                    }
                    DialFailure::Busy | DialFailure::Transport => {
                        effects.push(LinkEffect::BackOff { attempt });
                        next.wait_for_lease(binding, DialLease::Current, &mut effects);
                    }
                }
            }
            LinkInput::Welcome {
                attempt,
                machine_id,
                deadline,
                now,
            } => {
                let LinkPhase::Dialing {
                    attempt: current,
                    lease,
                    ..
                } = &state.phase
                else {
                    return Err(TransitionError::Stale);
                };
                if current != &attempt {
                    return Err(TransitionError::Stale);
                }
                if state.machine_id != machine_id {
                    return Err(TransitionError::Domain(LinkRefusal::WrongMachine));
                }
                next.phase = LinkPhase::Welcomed {
                    attempt,
                    lease: lease.clone(),
                    deadline,
                    renewal: renewal_schedule(now, deadline),
                };
                next.time_elapsed(now, &mut effects);
            }
            LinkInput::Tick { now } => next.time_elapsed(now, &mut effects),
            LinkInput::RenewalLease {
                attempt,
                lease,
                correlation_id,
                now,
            } => {
                let LinkPhase::Welcomed {
                    attempt: current,
                    lease: previous,
                    renewal,
                    deadline,
                } = &mut next.phase
                else {
                    return Err(TransitionError::Stale);
                };
                if current != &attempt
                    || !matches!(renewal, Renewal::Acquiring)
                    || lease.binding != previous.binding
                {
                    return Err(TransitionError::Stale);
                }
                if now >= *deadline
                    || now >= lease.usable_until
                    || lease.version <= previous.version
                {
                    return Err(TransitionError::Domain(LinkRefusal::UnusableLease));
                }
                *previous = lease;
                *renewal = Renewal::Pending(correlation_id.clone());
                effects.push(LinkEffect::RenewOnSocket {
                    attempt,
                    correlation_id,
                });
            }
            LinkInput::RenewalAccepted {
                attempt,
                correlation_id,
                deadline,
                now,
            } => {
                next.check_renewal(&attempt, &correlation_id)?;
                let LinkPhase::Welcomed {
                    deadline: previous,
                    renewal,
                    ..
                } = &mut next.phase
                else {
                    return Err(TransitionError::Stale);
                };
                if now >= *previous {
                    return Err(TransitionError::Stale);
                }
                *previous = deadline;
                *renewal = renewal_schedule(now, deadline);
                next.time_elapsed(now, &mut effects);
            }
            LinkInput::RenewalRefused {
                attempt,
                correlation_id,
                reason,
                now,
            } => {
                next.check_renewal(&attempt, &correlation_id)?;
                let LinkPhase::Welcomed { renewal, .. } = &mut next.phase else {
                    return Err(TransitionError::Stale);
                };
                match reason {
                    TokenRenewalRefusal::PresentedTokenExpired => {
                        *renewal = Renewal::Scheduled(now);
                    }
                    TokenRenewalRefusal::TokenRejected
                    | TokenRenewalRefusal::ConnectionEnrollmentEnded
                    | TokenRenewalRefusal::DifferentMachine
                    | TokenRenewalRefusal::DifferentEnrollment
                    | TokenRenewalRefusal::CentralUnavailable => {
                        *renewal = Renewal::Refused;
                    }
                }
            }
            LinkInput::RenewalUnavailable { attempt } => {
                let LinkPhase::Welcomed {
                    attempt: current,
                    renewal,
                    ..
                } = &mut next.phase
                else {
                    return Err(TransitionError::Stale);
                };
                if current != &attempt || !matches!(renewal, Renewal::Acquiring) {
                    return Err(TransitionError::Stale);
                }
                *renewal = Renewal::Refused;
            }
            LinkInput::SocketClosed { attempt, code } => {
                let lease = match &state.phase {
                    LinkPhase::Dialing {
                        attempt: current,
                        lease,
                        ..
                    }
                    | LinkPhase::Welcomed {
                        attempt: current,
                        lease,
                        ..
                    } if current == &attempt => lease,
                    _ => return Err(TransitionError::Stale),
                };
                match code {
                    Some(MachineLinkCloseCode::Revoked) => {
                        next.end(lease.binding.clone(), EnrollmentEnd::Revoked, &mut effects)
                    }
                    Some(MachineLinkCloseCode::Inactive) => {
                        next.end(lease.binding.clone(), EnrollmentEnd::Inactive, &mut effects)
                    }
                    Some(MachineLinkCloseCode::Expired) | None => {
                        effects.push(LinkEffect::BackOff { attempt });
                        let freshness = if code == Some(MachineLinkCloseCode::Expired) {
                            DialLease::NewerThan(lease.version)
                        } else {
                            DialLease::Current
                        };
                        next.wait_for_lease(lease.binding.clone(), freshness, &mut effects);
                    }
                }
            }
            LinkInput::LeaseEnded { binding } => {
                if state.binding() != Some(&binding)
                    || matches!(state.phase, LinkPhase::EnrollmentEnded { .. })
                {
                    return Err(TransitionError::Stale);
                }
                next.end(binding, EnrollmentEnd::LeaseRefused, &mut effects);
            }
            LinkInput::LeaseFault { binding } => {
                if state.binding() != Some(&binding)
                    || matches!(state.phase, LinkPhase::EnrollmentEnded { .. })
                {
                    return Err(TransitionError::Stale);
                }
                if let LinkPhase::Dialing { attempt, .. } | LinkPhase::Welcomed { attempt, .. } =
                    &state.phase
                {
                    effects.push(LinkEffect::BackOff {
                        attempt: attempt.clone(),
                    });
                }
                next.phase = LinkPhase::Fault {
                    binding,
                    kind: LinkFault::LeaseSource,
                    retry_at: None,
                };
            }
            LinkInput::AddressCorrected => {
                let LinkPhase::Fault {
                    binding,
                    kind: LinkFault::MalformedAddress,
                    ..
                } = &state.phase
                else {
                    return Err(TransitionError::Stale);
                };
                next.wait_for_lease(binding.clone(), DialLease::Current, &mut effects);
            }
            LinkInput::AddressChanged { attempt } => {
                let (binding, freshness) = match &state.phase {
                    LinkPhase::Dialing {
                        attempt: current,
                        lease,
                        refresh,
                    } if current == &attempt => (lease.binding.clone(), refresh.clone()),
                    LinkPhase::Welcomed {
                        attempt: current,
                        lease,
                        ..
                    } if current == &attempt => (lease.binding.clone(), DialLease::Current),
                    _ => return Err(TransitionError::Stale),
                };
                effects.push(LinkEffect::BackOff { attempt });
                next.wait_for_lease(binding, freshness, &mut effects);
            }
        }
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        LINK_TRANSITIONS
    }
}

const LINK_TRANSITIONS: &[Transition] = &[
    transition("not_enrolled", "binding", "waiting"),
    transition("waiting", "binding", "waiting"),
    transition("dialing", "binding", "waiting"),
    transition("welcomed", "binding", "waiting"),
    transition("fault", "binding", "waiting"),
    transition("enrollment_ended", "binding", "waiting"),
    transition("waiting", "dial", "dialing"),
    transition("dialing", "dial_failed", "waiting"),
    transition("dialing", "dial_failed", "fault"),
    transition("dialing", "welcome", "welcomed"),
    transition("dialing", "welcome", "waiting"),
    transition("not_enrolled", "tick", "not_enrolled"),
    transition("waiting", "tick", "waiting"),
    transition("dialing", "tick", "dialing"),
    transition("welcomed", "tick", "welcomed"),
    transition("fault", "tick", "fault"),
    transition("enrollment_ended", "tick", "enrollment_ended"),
    transition("welcomed", "tick", "waiting"),
    transition("fault", "tick", "waiting"),
    transition("welcomed", "renewal_lease", "welcomed"),
    transition("welcomed", "renewal_accepted", "welcomed"),
    transition("welcomed", "renewal_accepted", "waiting"),
    transition("welcomed", "renewal_refused", "welcomed"),
    transition("welcomed", "renewal_unavailable", "welcomed"),
    transition("dialing", "socket_closed", "waiting"),
    transition("dialing", "socket_closed", "enrollment_ended"),
    transition("welcomed", "socket_closed", "waiting"),
    transition("welcomed", "socket_closed", "enrollment_ended"),
    transition("waiting", "lease_ended", "enrollment_ended"),
    transition("dialing", "lease_ended", "enrollment_ended"),
    transition("welcomed", "lease_ended", "enrollment_ended"),
    transition("fault", "lease_ended", "enrollment_ended"),
    transition("waiting", "lease_fault", "fault"),
    transition("dialing", "lease_fault", "fault"),
    transition("welcomed", "lease_fault", "fault"),
    transition("fault", "lease_fault", "fault"),
    transition("fault", "address_corrected", "waiting"),
    transition("dialing", "address_changed", "waiting"),
    transition("welcomed", "address_changed", "waiting"),
];

const fn transition(from: &'static str, event: &'static str, to: &'static str) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal: false,
    }
}

#[cfg(test)]
#[path = "link_policy_test.rs"]
mod tests;
