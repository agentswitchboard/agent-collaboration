use std::num::NonZeroU64;

use switchboard_central_link::{
    central_call_lane::{CallAdmissionRefusal, CentralCallHandle},
    link_policy::LinkStatus,
};
use switchboard_machine_enrollment::EnrollmentEnd;

use super::{
    CentralWorkNextMove as Move, CentralWorkRefusal, CentralWorkRefusalStatus as Status,
    WorkCallContext,
};
use crate::{
    EnrollmentPair, Generation, MessageSystemObservation, MessageSystemReason as Reason,
    MessageSystemStatus, State,
};

const ORGANIZATION: &str = "019f0000-0000-7000-8000-000000000001";

fn running_context(stored_ending: Option<EnrollmentEnd>) -> WorkCallContext {
    WorkCallContext {
        supervisor: State::Running {
            generation: Generation(NonZeroU64::MIN),
            pair: EnrollmentPair::new(
                ORGANIZATION.parse().unwrap(),
                "https://central.example/".parse().unwrap(),
            )
            .unwrap(),
        },
        lane: Some(CentralCallHandle::default()),
        stored_ending,
    }
}

fn connected_link() -> LinkStatus {
    LinkStatus::Connected {
        machine_id: "019f0000-0000-7000-8000-000000000002".parse().unwrap(),
    }
}

#[test]
fn withdrawn_gate_refuses_despite_connected_observation() {
    let context = running_context(None);
    let link = connected_link();
    let published = MessageSystemObservation::project(&context.supervisor, &link, None);
    assert_eq!(published.status, MessageSystemStatus::Connected);
    let refusal = CentralWorkRefusal::project(&context, &link, CallAdmissionRefusal::Unavailable);
    assert_eq!(
        refusal,
        CentralWorkRefusal {
            status: Status::Connecting,
            reason: Reason::NoReason
        }
    );
    assert_eq!(refusal.valid_move(), Move::Wait);
    assert_eq!(published.status, MessageSystemStatus::Connected);
}

#[test]
fn terminal_gate_projects_pending_then_stored_ending_even_with_connected_link() {
    for (stored_ending, reason) in [
        (None, Reason::EnrollmentEndingPending),
        (Some(EnrollmentEnd::Revoked), Reason::EnrollmentRevoked),
    ] {
        let refusal = CentralWorkRefusal::project(
            &running_context(stored_ending),
            &connected_link(),
            CallAdmissionRefusal::EnrollmentEnded,
        );
        assert_eq!(
            refusal,
            CentralWorkRefusal {
                status: Status::EnrollmentEnded,
                reason
            }
        );
        assert_eq!(refusal.valid_move(), Move::EnrollThisMacAgain);
    }
}

#[test]
fn each_refusal_status_has_its_authorized_move() {
    for (status, next_move) in [
        (Status::Starting, Move::Wait),
        (Status::Connecting, Move::Wait),
        (Status::Registering, Move::Wait),
        (Status::Restarting, Move::Wait),
        (Status::RegistrationFailed, Move::Wait),
        (Status::Fault, Move::Wait),
        (Status::EnrollmentUnreadable, Move::Wait),
        (Status::NotEnrolled, Move::EnrollThisMac),
        (Status::EnrollmentEnded, Move::EnrollThisMacAgain),
        (Status::EnrollmentMismatch, Move::EnrollThisMacAgain),
        (Status::RunRetired, Move::NoneForThisRun),
        (Status::Stopping, Move::NoneForThisRun),
        (Status::Stopped, Move::NoneForThisRun),
    ] {
        assert_eq!(
            CentralWorkRefusal {
                status,
                reason: Reason::NoReason
            }
            .valid_move(),
            next_move
        );
    }
}
