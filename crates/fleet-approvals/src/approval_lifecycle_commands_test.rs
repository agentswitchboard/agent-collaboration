use super::super::fixtures::{
    approval_raise, holder_report, observer, other_run, parent, person_answer, router_run, session,
    session_answer, stop_event,
};
use super::{CHANGED_TITLE, FIRST_TITLE};
use crate::{ApprovalDestination, ApprovalEvent, FactorStanding, PassOnAuthority};
use approval_request_protocol::HolderReport;

#[derive(Clone, Copy, Debug)]
pub(super) enum Command {
    Raise { changed: bool },
    AllowFromParent,
    DeclineFromObserver,
    UnknownOption,
    KeptFromParent,
    OnceFromStalePerson,
    KeptFromStalePerson,
    KeptFromFreshPerson,
    OutsiderAnswers,
    ForwardFromParent,
    ForwardWithoutParent,
    HolderStops,
    ClockTicks,
    RunEnds,
    SessionEnds,
    EnrollmentEnds,
    Offer { foreign: bool },
    Report { foreign: bool, report: HolderReport },
}

pub(super) const COMMANDS: &[Command] = &[
    Command::Raise { changed: false },
    Command::Raise { changed: true },
    Command::AllowFromParent,
    Command::DeclineFromObserver,
    Command::UnknownOption,
    Command::KeptFromParent,
    Command::OnceFromStalePerson,
    Command::KeptFromStalePerson,
    Command::KeptFromFreshPerson,
    Command::OutsiderAnswers,
    Command::ForwardFromParent,
    Command::ForwardWithoutParent,
    Command::HolderStops,
    Command::ClockTicks,
    Command::RunEnds,
    Command::SessionEnds,
    Command::EnrollmentEnds,
    Command::Offer { foreign: false },
    Command::Offer { foreign: true },
    Command::Report {
        foreign: false,
        report: HolderReport::GoneBeforeHandover,
    },
    Command::Report {
        foreign: false,
        report: HolderReport::ReplyWrittenEffectNotConfirmed,
    },
    Command::Report {
        foreign: false,
        report: HolderReport::DeliveryUnknown,
    },
    Command::Report {
        foreign: false,
        report: HolderReport::Applied,
    },
    Command::Report {
        foreign: true,
        report: HolderReport::Applied,
    },
];

impl Command {
    pub(super) fn event(self, include_people: bool) -> ApprovalEvent {
        match self {
            Self::Raise { changed } => {
                let mut destinations = vec![
                    ApprovalDestination::Session(parent()),
                    ApprovalDestination::Session(observer()),
                    ApprovalDestination::Session(parent()),
                ];
                if include_people {
                    destinations.push(ApprovalDestination::People);
                }
                let title = if changed { CHANGED_TITLE } else { FIRST_TITLE };
                ApprovalEvent::Raise {
                    raise: Box::new(approval_raise(title)),
                    destinations,
                }
            }
            Self::AllowFromParent => session_answer(parent(), "once"),
            Self::DeclineFromObserver => session_answer(observer(), "decline"),
            Self::UnknownOption => session_answer(parent(), "unknown"),
            Self::KeptFromParent => session_answer(parent(), "kept"),
            Self::OnceFromStalePerson => person_answer(FactorStanding::AbsentOrStale, "once"),
            Self::KeptFromStalePerson => person_answer(FactorStanding::AbsentOrStale, "kept"),
            Self::KeptFromFreshPerson => person_answer(FactorStanding::Fresh, "kept"),
            Self::OutsiderAnswers => session_answer(session(9), "once"),
            Self::ForwardFromParent => ApprovalEvent::PassOn {
                authority: PassOnAuthority::LiveParent(parent()),
            },
            Self::ForwardWithoutParent => ApprovalEvent::PassOn {
                authority: PassOnAuthority::NotLiveParent,
            },
            Self::HolderStops => stop_event(),
            Self::ClockTicks => ApprovalEvent::ClockReached,
            Self::RunEnds => ApprovalEvent::RunRetired,
            Self::SessionEnds => ApprovalEvent::SessionEnded,
            Self::EnrollmentEnds => ApprovalEvent::EnrollmentEnded,
            Self::Offer { foreign } => ApprovalEvent::OfferAgain {
                router_run: if foreign { other_run() } else { router_run() },
            },
            Self::Report { foreign, report } => {
                holder_report(if foreign { other_run() } else { router_run() }, report)
            }
        }
    }
}

pub(super) fn command_name(command: Command) -> &'static str {
    match command {
        Command::Raise { .. } => "raise",
        Command::ForwardFromParent | Command::ForwardWithoutParent => "passOn",
        Command::HolderStops => "stop",
        Command::ClockTicks => "clockReached",
        Command::RunEnds => "runRetired",
        Command::SessionEnds => "sessionEnded",
        Command::EnrollmentEnds => "enrollmentEnded",
        Command::Offer { .. } => "offerAgain",
        Command::Report { .. } => "report",
        _ => "answer",
    }
}
