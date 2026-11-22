//! One allowance lifecycle per machine and workspace pair.
use central_storage::StoreFailure;
use fleet_signin::organization_change::MachineAllowanceState;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition,
};

#[derive(Clone, Copy)]
pub(super) enum AllowanceStatus {
    Absent,
    Active,
    Withdrawn,
}

impl StateTag for AllowanceStatus {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Active => "active",
            Self::Withdrawn => "withdrawn",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy)]
pub(super) enum AllowanceEvent {
    ApprovalSelectedWorkspace,
    EnrollmentEnded,
}

impl EventTag for AllowanceEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::ApprovalSelectedWorkspace => "approvalSelectedWorkspace",
            Self::EnrollmentEnded => "enrollmentEnded",
        }
    }
}

enum AllowanceRefusal {}

impl RefusalTag for AllowanceRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

struct AllowanceLifecycle;

impl StateMachine for AllowanceLifecycle {
    type State = AllowanceStatus;
    type Event = AllowanceEvent;
    type Effect = MachineAllowanceState;
    type Error = AllowanceRefusal;

    fn transition(state: &AllowanceStatus, event: AllowanceEvent) -> StateMachineResult<Self> {
        use AllowanceEvent::{ApprovalSelectedWorkspace, EnrollmentEnded};
        use AllowanceStatus::{Absent, Active, Withdrawn};
        Ok(match (*state, event) {
            (Absent | Withdrawn, ApprovalSelectedWorkspace) => {
                (Active, vec![MachineAllowanceState::Active])
            }
            (Active, EnrollmentEnded) => (Withdrawn, vec![MachineAllowanceState::Withdrawn]),
            (Active, ApprovalSelectedWorkspace) | (Absent | Withdrawn, EnrollmentEnded) => {
                (*state, Vec::new())
            }
        })
    }

    fn transitions() -> &'static [Transition] {
        const ALLOWANCE_TRANSITIONS: [Transition; 6] = [
            allowance_transition("absent", "approvalSelectedWorkspace", "active"),
            allowance_transition("active", "approvalSelectedWorkspace", "active"),
            allowance_transition("withdrawn", "approvalSelectedWorkspace", "active"),
            allowance_transition("absent", "enrollmentEnded", "absent"),
            allowance_transition("active", "enrollmentEnded", "withdrawn"),
            allowance_transition("withdrawn", "enrollmentEnded", "withdrawn"),
        ];
        &ALLOWANCE_TRANSITIONS
    }
}

const fn allowance_transition(
    from: &'static str,
    event: &'static str,
    to: &'static str,
) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal: false,
    }
}

pub(super) fn allowance_effect(
    state: AllowanceStatus,
    event: AllowanceEvent,
) -> Result<Option<MachineAllowanceState>, StoreFailure> {
    let (_, effects) =
        AllowanceLifecycle::transition(&state, event).map_err(|_| StoreFailure::Unavailable)?;
    match effects.as_slice() {
        [] => Ok(None),
        [effect] => Ok(Some(*effect)),
        _ => Err(StoreFailure::Unavailable),
    }
}

#[cfg(test)]
#[path = "allowance_transitions_test.rs"]
mod tests;
