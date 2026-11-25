use std::num::NonZeroU64;

use super::{MessageSystemStatus as Status, message_system_status};
use crate::{
    EnrollmentDuringDrain, EnrollmentPair, FrontRefusal, Generation, ObservationPhase,
    QuiesceReason, Quiescing, ReadId, State, StopTarget,
};
use switchboard_central_link::link_policy::LinkStatus;

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
    let machine_id = "019f0000-0000-7000-8000-000000000003".parse().unwrap();
    let welcomed = LinkStatus::Welcomed { machine_id };
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
    }
}
