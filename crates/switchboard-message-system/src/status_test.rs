use std::num::NonZeroU64;

use super::{MessageSystemStatus as Status, message_system_status};
use crate::{
    EnrollmentDuringDrain, EnrollmentPair, FrontRefusal, Generation, ObservationPhase,
    QuiesceReason, Quiescing, ReadId, State, StopTarget,
};
use state_machine_core::StateMachine;
use switchboard_central_link::link_policy::{
    LinkFault, LinkInput, LinkPolicy, LinkState, LinkStatus, RegistrationFailure,
};

#[test]
fn status_projects_supervisor_state_and_current_link() {
    let pair = EnrollmentPair::new(
        "019f0000-0000-7000-8000-000000000001".parse().unwrap(),
        "https://central.example/".parse().unwrap(),
    )
    .unwrap();
    let observed = EnrollmentPair::new(
        "019f0000-0000-7000-8000-000000000002".parse().unwrap(),
        "https://central.example/".parse().unwrap(),
    )
    .unwrap();
    let generation = Generation(NonZeroU64::MIN);
    let machine_id = "019f0000-0000-7000-8000-000000000003"
        .parse::<switchboard_machine_enrollment::MachineId>()
        .unwrap();
    let welcomed = LinkStatus::Welcomed {
        machine_id: machine_id.clone(),
    };
    let statuses = status_inputs(machine_id);
    let connecting = LinkStatus::Connecting;
    let polling = |reason| State::Observing {
        phase: ObservationPhase::Polling(reason),
    };
    let running = State::Running {
        generation,
        pair: pair.clone(),
    };
    let cases = [
        (
            "first read",
            polling(FrontRefusal::AwaitingFirstRead),
            &connecting,
            Status::Starting,
        ),
        (
            "empty",
            polling(FrontRefusal::NotEnrolled),
            &welcomed,
            Status::NotEnrolled,
        ),
        (
            "ended",
            polling(FrontRefusal::EnrollmentEnded),
            &welcomed,
            Status::EnrollmentEnded,
        ),
        (
            "unreadable",
            polling(FrontRefusal::MetadataUnreadable),
            &connecting,
            Status::EnrollmentUnreadable,
        ),
        (
            "between generations",
            polling(FrontRefusal::Restarting),
            &connecting,
            Status::Restarting,
        ),
        (
            "cooldown",
            State::Observing {
                phase: ObservationPhase::RestartCooldown {
                    read: ReadId(NonZeroU64::MIN),
                },
            },
            &connecting,
            Status::Restarting,
        ),
        (
            "opening",
            State::Opening {
                generation,
                pair: pair.clone(),
            },
            &welcomed,
            Status::Connecting,
        ),
        (
            "running before welcome",
            running.clone(),
            &connecting,
            Status::Connecting,
        ),
        (
            "running after welcome, before registration",
            running,
            &welcomed,
            Status::Registering,
        ),
        (
            "draining ended enrollment",
            State::Quiescing(Quiescing::new(
                Some(generation),
                QuiesceReason::EnrollmentLost(EnrollmentDuringDrain::Ended),
            )),
            &welcomed,
            Status::Stopping,
        ),
        (
            "wrong enrollment",
            State::Faulted {
                pinned: pair,
                observed,
            },
            &welcomed,
            Status::EnrollmentMismatch,
        ),
        (
            "close with outstanding work",
            State::ClosedWithOutstanding {
                pieces: [StopTarget::EnrollmentStore].into(),
            },
            &welcomed,
            Status::Stopped,
        ),
        ("closed", State::Closed, &welcomed, Status::Stopped),
    ];

    for (case, state, link, expected) in cases {
        assert_eq!(message_system_status(&state, link), expected, "{case}");
        if !matches!(state, State::Running { .. }) {
            for transport in &statuses {
                assert_ne!(
                    message_system_status(&state, transport),
                    Status::Connected,
                    "{case}: {transport:?}"
                );
            }
        }
    }
}

#[test]
fn a_running_supervisor_projects_connected_if_and_only_if_the_link_is_connected() {
    let pair = EnrollmentPair::new(
        "019f0000-0000-7000-8000-000000000001".parse().unwrap(),
        "https://central.example/".parse().unwrap(),
    )
    .unwrap();
    let machine_id = "019f0000-0000-7000-8000-000000000003"
        .parse::<switchboard_machine_enrollment::MachineId>()
        .unwrap();
    let statuses = status_inputs(machine_id);
    let running = State::Running {
        generation: Generation(NonZeroU64::MIN),
        pair,
    };
    for status in statuses {
        let connected = match status {
            LinkStatus::Connected { .. } => true,
            LinkStatus::NotEnrolled
            | LinkStatus::Connecting
            | LinkStatus::Welcomed { .. }
            | LinkStatus::Registering
            | LinkStatus::RegistrationFailed { .. }
            | LinkStatus::RegistrationFault { .. }
            | LinkStatus::RunRetired
            | LinkStatus::Fault { .. }
            | LinkStatus::EnrollmentEnded { .. } => false,
        };
        assert_eq!(
            message_system_status(&running, &status) == Status::Connected,
            connected,
            "{status:?}"
        );
    }
}

fn status_inputs(machine_id: switchboard_machine_enrollment::MachineId) -> [LinkStatus; 10] {
    let (bound, _) = LinkPolicy::transition(
        &LinkState::new(machine_id.clone()),
        LinkInput::BindingAvailable(1_u8),
    )
    .unwrap();
    let (ended, _) = LinkPolicy::transition(&bound, LinkInput::LeaseEnded { binding: 1 }).unwrap();
    [
        LinkStatus::NotEnrolled,
        LinkStatus::Connecting,
        LinkStatus::Welcomed {
            machine_id: machine_id.clone(),
        },
        LinkStatus::Registering,
        LinkStatus::RegistrationFailed {
            reason: RegistrationFailure::TimedOut,
        },
        LinkStatus::RegistrationFault {
            reason: central_link_protocol::LinkCallRefusal::WrongTarget,
        },
        LinkStatus::RunRetired,
        LinkStatus::Connected { machine_id },
        LinkStatus::Fault {
            kind: LinkFault::LeaseSource,
        },
        ended.status(),
    ]
}
