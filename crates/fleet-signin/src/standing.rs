//! The process-local standing of a stored organization stamp.
use crate::organization_authority::{StoredStampRead, VerifiedFleetOrganizationStatement};
use fleet_contracts::FleetOrganizationStatementRefusal;
use fleet_identity_protocol::UnixSeconds;
use fleet_runtime_ports::SyncTime;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition,
};
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StampHealth {
    Unchecked,
    Unprovisioned,
    Verified {
        issued_at: UnixSeconds,
        expires_at: UnixSeconds,
    },
    Refused,
}

/// What a guarded call may rely on right now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StampStanding {
    Unprovisioned,
    Valid,
    Refused,
}

pub struct StandingCache {
    health: Mutex<StampHealth>,
}

impl Default for StandingCache {
    fn default() -> Self {
        Self {
            health: Mutex::new(StampHealth::Unchecked),
        }
    }
}

#[derive(Clone)]
enum StandingEvent {
    Observed(StampHealth),
    Invalidate,
}

impl EventTag for StandingEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Observed(_) => "observed",
            Self::Invalidate => "invalidate",
        }
    }
}

impl StateTag for StampHealth {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Unchecked => "unchecked",
            Self::Unprovisioned => "unprovisioned",
            Self::Verified { .. } => "verified",
            Self::Refused => "refused",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

enum StandingRefusal {}
impl RefusalTag for StandingRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

struct StandingMachine;

impl StateMachine for StandingMachine {
    type State = StampHealth;
    type Event = StandingEvent;
    type Effect = ();
    type Error = StandingRefusal;

    fn transition(state: &StampHealth, event: StandingEvent) -> StateMachineResult<Self> {
        use StampHealth::{Refused, Unchecked, Unprovisioned, Verified};
        match (state, event) {
            (Unchecked | Unprovisioned | Verified { .. } | Refused, StandingEvent::Invalidate) => {
                Ok((Unchecked, vec![]))
            }
            (Unchecked, StandingEvent::Observed(observed)) => Ok((observed, vec![])),
            (Unprovisioned | Verified { .. } | Refused, StandingEvent::Observed(_)) => {
                Err(state_machine_core::TransitionError::Stale)
            }
        }
    }

    fn transitions() -> &'static [Transition] {
        &[
            Transition {
                from: "unchecked",
                event: "observed",
                to: "unchecked",
                terminal: false,
            },
            Transition {
                from: "unchecked",
                event: "observed",
                to: "unprovisioned",
                terminal: false,
            },
            Transition {
                from: "unchecked",
                event: "observed",
                to: "verified",
                terminal: false,
            },
            Transition {
                from: "unchecked",
                event: "observed",
                to: "refused",
                terminal: false,
            },
            Transition {
                from: "unchecked",
                event: "invalidate",
                to: "unchecked",
                terminal: false,
            },
            Transition {
                from: "unprovisioned",
                event: "invalidate",
                to: "unchecked",
                terminal: false,
            },
            Transition {
                from: "verified",
                event: "invalidate",
                to: "unchecked",
                terminal: false,
            },
            Transition {
                from: "refused",
                event: "invalidate",
                to: "unchecked",
                terminal: false,
            },
        ]
    }
}

impl StandingCache {
    pub(crate) fn needs_check(&self) -> bool {
        self.health
            .lock()
            .map(|health| *health == StampHealth::Unchecked)
            .unwrap_or(true)
    }

    pub(crate) fn record(
        &self,
        stored: &StoredStampRead,
        verified: Option<
            Result<&VerifiedFleetOrganizationStatement, FleetOrganizationStatementRefusal>,
        >,
    ) {
        let next = match (stored, verified) {
            (StoredStampRead::Unprovisioned, _) => StampHealth::Unprovisioned,
            (StoredStampRead::Stamped(_), Some(Ok(statement))) => StampHealth::Verified {
                issued_at: statement.issued_at(),
                expires_at: statement.expires_at(),
            },
            (
                StoredStampRead::Stamped(_),
                Some(Err(FleetOrganizationStatementRefusal::VerificationUnavailable(_))),
            ) => StampHealth::Unchecked,
            (StoredStampRead::Stamped(_), _) => StampHealth::Refused,
        };
        self.apply(StandingEvent::Observed(next));
    }

    pub fn forget(&self) {
        self.apply(StandingEvent::Invalidate);
    }

    fn apply(&self, event: StandingEvent) {
        if let Ok(mut health) = self.health.lock()
            && let Ok((next, _)) = StandingMachine::transition(&health, event)
        {
            *health = next;
        }
    }

    /// The stamp's standing at the caller's sampled time.
    pub fn standing(&self, now: SyncTime) -> StampStanding {
        let Ok(health) = self.health.lock() else {
            return StampStanding::Refused;
        };
        match *health {
            StampHealth::Unprovisioned => StampStanding::Unprovisioned,
            StampHealth::Verified {
                issued_at,
                expires_at,
            } => {
                let seconds = now.unix_millis().div_euclid(1000);
                if seconds < issued_at.0 || seconds >= expires_at.0 {
                    StampStanding::Refused
                } else {
                    StampStanding::Valid
                }
            }
            StampHealth::Unchecked | StampHealth::Refused => StampStanding::Refused,
        }
    }
}

#[cfg(test)]
#[path = "standing_test.rs"]
mod tests;
