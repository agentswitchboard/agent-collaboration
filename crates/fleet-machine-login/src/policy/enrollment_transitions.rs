//! Enrollment changes after the policy has checked the caller and applying time.
use super::{
    decision_support::DecisionTime,
    enrollment_records::{
        CurrentEnrollment, EnrollmentChange, EnrollmentGeneration, MachineRow, MachineState,
        StoredEnrollment, StoredEnrollmentState,
    },
};
use central_storage::StoreFailure;
use fleet_contracts::{ClientId, DecideDeviceLogin, EnrollmentRevocationReason};
use fleet_identity_protocol::EnrollmentId;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use message_board::MachineLabel;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::MachineId;

#[derive(Clone)]
pub(super) enum EnrollmentEvent {
    ReserveApproval(Box<ApprovalReservation>),
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
            Self::ReserveApproval(_) => "reserveApproval",
            Self::ApprovalDeadlineElapsed(_) => "approvalDeadlineElapsed",
            Self::FirstGrant { .. } => "firstGrant",
            Self::ActiveGrant(_) => "activeGrant",
            Self::OwnerRevoked(_) => "ownerRevoked",
            Self::Superseded(_) => "superseded",
            Self::IdleDeadlineElapsed(_) => "idleDeadlineElapsed",
        }
    }
}

#[derive(Clone)]
pub(super) struct ApprovalReservation {
    request: DecideDeviceLogin,
    id: EnrollmentId,
    base_generation: EnrollmentGeneration,
    time: DecisionTime,
}

#[derive(Clone)]

enum EnrollmentLifecycleState {
    Absent,
    Stored(Box<StoredEnrollment>),
}

impl StateTag for EnrollmentLifecycleState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Stored(row) => row.state_tag(),
        }
    }

    fn is_terminal(&self) -> bool {
        match self {
            Self::Absent => false,
            Self::Stored(row) => row.is_terminal(),
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
    type State = EnrollmentLifecycleState;
    type Event = EnrollmentEvent;
    type Effect = ();
    type Error = EnrollmentTransitionRefusal;

    fn transition(observed: &Self::State, event: EnrollmentEvent) -> StateMachineResult<Self> {
        use EnrollmentEvent::{
            ActiveGrant, ApprovalDeadlineElapsed, FirstGrant, IdleDeadlineElapsed, OwnerRevoked,
            ReserveApproval, Superseded,
        };
        use StoredEnrollmentState::{Approved, Enrolled, Expired, Lapsed, Revoked};

        let tag = event.event_tag();
        let row = match (observed, event) {
            (EnrollmentLifecycleState::Absent, ReserveApproval(reservation)) => {
                let ApprovalReservation {
                    request,
                    id,
                    base_generation,
                    time,
                } = *reservation;
                return Ok((
                    EnrollmentLifecycleState::Stored(Box::new(StoredEnrollment {
                        id,
                        machine_id: request.machine_id,
                        request_id: request.request_id,
                        approved_by: request.approver,
                        label: request.label,
                        allowed_workspaces: request.allowed_workspaces,
                        base_generation,
                        approved_at_ms: time.now_ms,
                        code_expires_at: request.code_expires_at,
                        state: Approved,
                        ended_at_ms: None,
                    })),
                    Vec::new(),
                ));
            }
            (EnrollmentLifecycleState::Stored(row), event) => (row, event),
            (EnrollmentLifecycleState::Absent, _) => {
                return Err(TransitionError::NotAllowed {
                    state: observed.state_tag(),
                    event: tag,
                });
            }
        };
        let (row, event) = row;
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
            (_, ReserveApproval(_))
            | (
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
            EnrollmentLifecycleState::Stored(Box::new(StoredEnrollment {
                state,
                ended_at_ms,
                ..row.as_ref().clone()
            })),
            Vec::new(),
        ))
    }

    fn transitions() -> &'static [Transition] {
        const TRANSITIONS: [Transition; 7] = [
            enrollment_transition("absent", "reserveApproval", "approved", false),
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

fn next_enrollment(
    state: EnrollmentLifecycleState,
    event: EnrollmentEvent,
) -> Result<StoredEnrollment, StoreFailure> {
    match EnrollmentLifecycle::transition(&state, event) {
        Ok((EnrollmentLifecycleState::Stored(next), _)) => Ok(*next),
        _ => Err(corrupt("login_machine_approvals.state")),
    }
}

pub(super) fn replace_enrollment(
    state: &StoredEnrollment,
    event: EnrollmentEvent,
) -> Result<EnrollmentChange, StoreFailure> {
    next_enrollment(
        EnrollmentLifecycleState::Stored(Box::new(state.clone())),
        event,
    )
    .map(EnrollmentChange::ReplaceEnrollment)
}

#[derive(Clone)]
enum MachineObservation {
    Absent,
    Stored(MachineRow),
}

impl StateTag for MachineObservation {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Stored(MachineRow {
                state: MachineState::Revoked,
                ..
            }) => "revoked",
            Self::Stored(MachineRow {
                state: MachineState::Enrolled,
                ..
            }) => "enrolled",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

#[derive(Clone)]
enum MachineEvent {
    Reserve {
        machine_id: MachineId,
        label: MachineLabel,
    },
    FirstGrant {
        machine_id: MachineId,
        base_generation: EnrollmentGeneration,
    },
    EndCurrent,
}

impl EventTag for MachineEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Reserve { .. } => "reserveApproval",
            Self::FirstGrant { .. } => "firstGrant",
            Self::EndCurrent => "endCurrent",
        }
    }
}

#[derive(Debug)]
enum MachineRefusal {
    DifferentMachine,
    DifferentGeneration,
    GenerationExhausted,
}

impl RefusalTag for MachineRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::DifferentMachine => "differentMachine",
            Self::DifferentGeneration => "differentGeneration",
            Self::GenerationExhausted => "generationExhausted",
        }
    }
}

struct MachineLifecycle;

impl StateMachine for MachineLifecycle {
    type State = MachineObservation;
    type Event = MachineEvent;
    type Effect = ();
    type Error = MachineRefusal;

    fn transition(observed: &MachineObservation, event: MachineEvent) -> StateMachineResult<Self> {
        use MachineEvent::{EndCurrent, FirstGrant, Reserve};
        use MachineObservation::{Absent, Stored};
        let tag = event.event_tag();
        let refusal = |reason| TransitionError::Domain(reason);
        let advance = |machine: &MachineRow| {
            machine
                .generation
                .next()
                .map_err(|_| refusal(MachineRefusal::GenerationExhausted))
        };
        let next = match (observed, event) {
            (Absent, Reserve { machine_id, label }) => MachineRow {
                machine_id,
                label,
                generation: EnrollmentGeneration::default(),
                state: MachineState::Revoked,
            },
            (Stored(machine), Reserve { machine_id, label }) => {
                if machine.machine_id != machine_id {
                    return Err(refusal(MachineRefusal::DifferentMachine));
                }
                MachineRow {
                    label,
                    ..machine.clone()
                }
            }
            (
                Stored(machine),
                FirstGrant {
                    machine_id,
                    base_generation,
                },
            ) => {
                if machine.machine_id != machine_id {
                    return Err(refusal(MachineRefusal::DifferentMachine));
                }
                if machine.generation != base_generation {
                    return Err(refusal(MachineRefusal::DifferentGeneration));
                }
                MachineRow {
                    state: MachineState::Enrolled,
                    generation: advance(machine)?,
                    ..machine.clone()
                }
            }
            (
                Stored(
                    machine @ MachineRow {
                        state: MachineState::Enrolled,
                        ..
                    },
                ),
                EndCurrent,
            ) => MachineRow {
                state: MachineState::Revoked,
                generation: advance(machine)?,
                ..machine.clone()
            },
            (Absent, FirstGrant { .. } | EndCurrent)
            | (
                Stored(MachineRow {
                    state: MachineState::Revoked,
                    ..
                }),
                EndCurrent,
            ) => {
                return Err(TransitionError::NotAllowed {
                    state: observed.state_tag(),
                    event: tag,
                });
            }
        };
        Ok((Stored(next), Vec::new()))
    }

    fn transitions() -> &'static [Transition] {
        const TRANSITIONS: [Transition; 6] = [
            enrollment_transition("absent", "reserveApproval", "revoked", false),
            enrollment_transition("revoked", "reserveApproval", "revoked", false),
            enrollment_transition("enrolled", "reserveApproval", "enrolled", false),
            enrollment_transition("revoked", "firstGrant", "enrolled", false),
            enrollment_transition("enrolled", "firstGrant", "enrolled", false),
            enrollment_transition("enrolled", "endCurrent", "revoked", false),
        ];
        &TRANSITIONS
    }
}

fn next_machine(
    observed: Option<&MachineRow>,
    event: MachineEvent,
) -> Result<MachineRow, StoreFailure> {
    let observed = observed.map_or(MachineObservation::Absent, |row| {
        MachineObservation::Stored(row.clone())
    });
    match MachineLifecycle::transition(&observed, event) {
        Ok((MachineObservation::Stored(next), _)) => Ok(next),
        Err(TransitionError::Domain(MachineRefusal::DifferentMachine)) => {
            Err(corrupt("login_machines.machine_id"))
        }
        Err(TransitionError::Domain(
            MachineRefusal::DifferentGeneration | MachineRefusal::GenerationExhausted,
        )) => Err(corrupt("login_machines.enrollment_generation")),
        Ok((MachineObservation::Absent, _)) | Err(_) => Err(corrupt("login_machines.state")),
    }
}

pub(super) fn reserve_enrollment(
    machine: Option<&MachineRow>,
    request: &DecideDeviceLogin,
    id: EnrollmentId,
    time: DecisionTime,
) -> Result<(MachineRow, StoredEnrollment), StoreFailure> {
    let machine = next_machine(
        machine,
        MachineEvent::Reserve {
            machine_id: request.machine_id.clone(),
            label: request.label.clone(),
        },
    )?;
    let enrollment = next_enrollment(
        EnrollmentLifecycleState::Absent,
        EnrollmentEvent::ReserveApproval(Box::new(ApprovalReservation {
            request: request.clone(),
            id,
            base_generation: machine.generation,
            time,
        })),
    )?;
    Ok((machine, enrollment))
}

pub(super) fn activate_enrollment(
    machine: &MachineRow,
    enrollment: &StoredEnrollment,
    client_id: ClientId,
    time: DecisionTime,
) -> Result<[EnrollmentChange; 2], StoreFailure> {
    let next = next_machine(
        Some(machine),
        MachineEvent::FirstGrant {
            machine_id: enrollment.machine_id.clone(),
            base_generation: enrollment.base_generation,
        },
    )?;
    Ok([
        replace_enrollment(enrollment, EnrollmentEvent::FirstGrant { client_id, time })?,
        EnrollmentChange::PutMachine(next),
    ])
}

pub(super) enum EnrollmentEnding {
    Owner(DecisionTime),
    Idle(DecisionTime),
}

pub(super) fn end_current_enrollment(
    current: &CurrentEnrollment,
    ending: EnrollmentEnding,
) -> Result<[EnrollmentChange; 2], StoreFailure> {
    let event = match ending {
        EnrollmentEnding::Owner(time) => EnrollmentEvent::OwnerRevoked(time),
        EnrollmentEnding::Idle(time) => EnrollmentEvent::IdleDeadlineElapsed(time),
    };
    let next = next_machine(Some(current.machine()), MachineEvent::EndCurrent)?;
    Ok([
        replace_enrollment(current.enrollment(), event)?,
        EnrollmentChange::PutMachine(next),
    ])
}

#[cfg(test)]
#[path = "enrollment_transitions_test.rs"]
mod tests;
