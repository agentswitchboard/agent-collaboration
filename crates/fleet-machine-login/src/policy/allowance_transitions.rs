//! One allowance lifecycle per machine and workspace pair.
use super::enrollment_records::{AllowanceEffect, StoredAllowance, StoredAllowanceState};
use central_storage::StoreFailure;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AllowanceStatus {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PairChange {
    Insert,
    Reactivate,
    Withdraw,
}

struct AllowanceLifecycle;

impl StateMachine for AllowanceLifecycle {
    type State = AllowanceStatus;
    type Event = AllowanceEvent;
    type Effect = PairChange;
    type Error = AllowanceRefusal;

    fn transition(state: &AllowanceStatus, event: AllowanceEvent) -> StateMachineResult<Self> {
        use AllowanceEvent::{ApprovalSelectedWorkspace, EnrollmentEnded};
        use AllowanceStatus::{Absent, Active, Withdrawn};
        Ok(match (*state, event) {
            (Absent, ApprovalSelectedWorkspace) => (Active, vec![PairChange::Insert]),
            (Withdrawn, ApprovalSelectedWorkspace) => (Active, vec![PairChange::Reactivate]),
            (Active, EnrollmentEnded) => (Withdrawn, vec![PairChange::Withdraw]),
            (Active, ApprovalSelectedWorkspace) | (Withdrawn, EnrollmentEnded) => {
                (*state, Vec::new())
            }
            // Ending enumerates persisted pairs. A missing pair is outside that set.
            (Absent, EnrollmentEnded) => {
                return Err(TransitionError::NotAllowed {
                    state: state.state_tag(),
                    event: event.event_tag(),
                });
            }
        })
    }

    fn transitions() -> &'static [Transition] {
        const ALLOWANCE_TRANSITIONS: [Transition; 5] = [
            allowance_transition("absent", "approvalSelectedWorkspace", "active"),
            allowance_transition("active", "approvalSelectedWorkspace", "active"),
            allowance_transition("withdrawn", "approvalSelectedWorkspace", "active"),
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
    observed: Option<&StoredAllowance>,
    event: AllowanceEvent,
) -> Result<Option<AllowanceEffect>, StoreFailure> {
    let status = match observed.map(|row| row.state) {
        None => AllowanceStatus::Absent,
        Some(StoredAllowanceState::Active) => AllowanceStatus::Active,
        Some(StoredAllowanceState::Withdrawn) => AllowanceStatus::Withdrawn,
    };
    let (_, effects) =
        AllowanceLifecycle::transition(&status, event).map_err(|_| StoreFailure::Unavailable)?;
    let pair_id = || observed.map(|row| row.id).ok_or(StoreFailure::Unavailable);
    match effects.as_slice() {
        [] => Ok(None),
        [PairChange::Insert] => Ok(Some(AllowanceEffect::Insert)),
        [PairChange::Reactivate] => pair_id().map(|id| Some(AllowanceEffect::Reactivate(id))),
        [PairChange::Withdraw] => pair_id().map(|id| Some(AllowanceEffect::Withdraw(id))),
        _ => Err(StoreFailure::Unavailable),
    }
}

#[cfg(test)]
#[path = "allowance_transitions_test.rs"]
mod tests;
