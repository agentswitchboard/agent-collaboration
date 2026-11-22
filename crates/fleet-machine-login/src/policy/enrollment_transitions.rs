//! Enrollment changes after the policy has checked the caller and applying time.
use super::{
    decision_support::DecisionTime,
    enrollment_records::{EnrollmentChange, StoredEnrollment, StoredEnrollmentState},
};
use central_storage::StoreFailure;
use fleet_contracts::{ClientId, EnrollmentRevocationReason};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

#[derive(Clone)]
pub(super) enum EnrollmentEvent {
    ApprovalDeadlineElapsed(DecisionTime),
    FirstGrant {
        client_id: ClientId,
        time: DecisionTime,
    },
    ActiveGrant(DecisionTime),
    OwnerRevoked(DecisionTime),
    Superseded(DecisionTime),
    IdleDeadlineElapsed(DecisionTime),
}

impl EventTag for EnrollmentEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::ApprovalDeadlineElapsed(_) => "approvalDeadlineElapsed",
            Self::FirstGrant { .. } => "firstGrant",
            Self::ActiveGrant(_) => "activeGrant",
            Self::OwnerRevoked(_) => "ownerRevoked",
            Self::Superseded(_) => "superseded",
            Self::IdleDeadlineElapsed(_) => "idleDeadlineElapsed",
        }
    }
}

impl StateTag for StoredEnrollment {
    fn state_tag(&self) -> &'static str {
        match self.state {
            StoredEnrollmentState::Approved => "approved",
            StoredEnrollmentState::Enrolled { .. } => "enrolled",
            StoredEnrollmentState::Lapsed => "lapsed",
            StoredEnrollmentState::Revoked { .. } => "revoked",
            StoredEnrollmentState::Expired { .. } => "expired",
        }
    }

    fn is_terminal(&self) -> bool {
        match self.state {
            StoredEnrollmentState::Approved | StoredEnrollmentState::Enrolled { .. } => false,
            StoredEnrollmentState::Lapsed
            | StoredEnrollmentState::Revoked { .. }
            | StoredEnrollmentState::Expired { .. } => true,
        }
    }
}

#[derive(Debug)]
enum EnrollmentTransitionRefusal {}

impl RefusalTag for EnrollmentTransitionRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

struct EnrollmentLifecycle;

impl StateMachine for EnrollmentLifecycle {
    type State = StoredEnrollment;
    type Event = EnrollmentEvent;
    type Effect = ();
    type Error = EnrollmentTransitionRefusal;

    fn transition(row: &StoredEnrollment, event: EnrollmentEvent) -> StateMachineResult<Self> {
        use EnrollmentEvent::{
            ActiveGrant, ApprovalDeadlineElapsed, FirstGrant, IdleDeadlineElapsed, OwnerRevoked,
            Superseded,
        };
        use StoredEnrollmentState::{Approved, Enrolled, Expired, Lapsed, Revoked};

        let tag = event.event_tag();
        let (state, ended_at_ms) = match (&row.state, event) {
            (Approved, ApprovalDeadlineElapsed(time)) => (Lapsed, Some(time.now_ms)),
            (Approved, FirstGrant { client_id, time }) => (
                Enrolled {
                    client_id,
                    last_grant_at: time.seconds(),
                },
                None,
            ),
            (Enrolled { client_id, .. }, ActiveGrant(time)) => (
                Enrolled {
                    client_id: client_id.clone(),
                    last_grant_at: time.seconds(),
                },
                None,
            ),
            (Enrolled { client_id, .. }, OwnerRevoked(time)) => (
                Revoked {
                    client_id: client_id.clone(),
                    reason: EnrollmentRevocationReason::Owner {},
                },
                Some(time.now_ms),
            ),
            (Enrolled { client_id, .. }, Superseded(time)) => (
                Revoked {
                    client_id: client_id.clone(),
                    reason: EnrollmentRevocationReason::Superseded {},
                },
                Some(time.now_ms),
            ),
            (Enrolled { client_id, .. }, IdleDeadlineElapsed(time)) => (
                Expired {
                    client_id: client_id.clone(),
                },
                Some(time.now_ms),
            ),
            (
                Approved,
                ActiveGrant(_) | OwnerRevoked(_) | Superseded(_) | IdleDeadlineElapsed(_),
            )
            | (Enrolled { .. }, ApprovalDeadlineElapsed(_) | FirstGrant { .. })
            | (
                Lapsed | Revoked { .. } | Expired { .. },
                ApprovalDeadlineElapsed(_)
                | FirstGrant { .. }
                | ActiveGrant(_)
                | OwnerRevoked(_)
                | Superseded(_)
                | IdleDeadlineElapsed(_),
            ) => {
                return Err(TransitionError::NotAllowed {
                    state: row.state_tag(),
                    event: tag,
                });
            }
        };
        Ok((
            StoredEnrollment {
                state,
                ended_at_ms,
                ..row.clone()
            },
            Vec::new(),
        ))
    }

    fn transitions() -> &'static [Transition] {
        const TRANSITIONS: [Transition; 6] = [
            enrollment_transition("approved", "approvalDeadlineElapsed", "lapsed", true),
            enrollment_transition("approved", "firstGrant", "enrolled", false),
            enrollment_transition("enrolled", "activeGrant", "enrolled", false),
            enrollment_transition("enrolled", "ownerRevoked", "revoked", true),
            enrollment_transition("enrolled", "superseded", "revoked", true),
            enrollment_transition("enrolled", "idleDeadlineElapsed", "expired", true),
        ];
        &TRANSITIONS
    }
}

const fn enrollment_transition(
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

pub(super) fn replace_enrollment(
    state: &StoredEnrollment,
    event: EnrollmentEvent,
) -> Result<EnrollmentChange, StoreFailure> {
    EnrollmentLifecycle::transition(state, event)
        .map(|(next, _)| EnrollmentChange::ReplaceEnrollment(next))
        .map_err(|_| corrupt("login_machine_approvals.state"))
}

#[cfg(test)]
#[path = "enrollment_transitions_test.rs"]
mod tests;
