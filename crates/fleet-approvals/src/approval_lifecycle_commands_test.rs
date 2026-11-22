use super::super::fixtures::{
    approval_raise, observer, other_request_id, other_run, parent, person_answer, request_id,
    router_run, session, session_answer, stop_event,
};
use super::{CHANGED_TITLE, FIRST_TITLE};
use crate::{ApprovalDestination, ApprovalEvent, FactorStanding, PassOnAuthority};
use approval_request_protocol::{HolderReport, ReportDeliveryRequest};

#[derive(Clone, Copy, Debug)]
pub(super) enum Command {
    Raise {
        changed: bool,
    },
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
    Erase,
    Offer {
        foreign: bool,
    },
    Report {
        foreign_run: bool,
        foreign_request: bool,
        report: HolderReport,
    },
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
    Command::Erase,
    Command::Offer { foreign: false },
    Command::Offer { foreign: true },
    Command::Report {
        foreign_run: false,
        foreign_request: false,
        report: HolderReport::GoneBeforeHandover,
    },
    Command::Report {
        foreign_run: false,
        foreign_request: false,
        report: HolderReport::ReplyWrittenEffectNotConfirmed,
    },
    Command::Report {
        foreign_run: false,
        foreign_request: false,
        report: HolderReport::DeliveryUnknown,
    },
    Command::Report {
        foreign_run: false,
        foreign_request: false,
        report: HolderReport::Applied,
    },
    Command::Report {
        foreign_run: true,
        foreign_request: false,
        report: HolderReport::Applied,
    },
];

impl Command {
    pub(super) fn with_foreign_request(self, foreign_request: bool) -> Self {
        match self {
            Self::Report {
                foreign_run,
                report,
                ..
            } => Self::Report {
                foreign_run,
                foreign_request,
                report,
            },
            non_report => non_report,
        }
    }

    pub(super) const fn requires_live_holder(self) -> bool {
        matches!(
            self,
            Self::Raise { .. } | Self::HolderStops | Self::Offer { .. } | Self::Report { .. }
        )
    }

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
            Self::Erase => ApprovalEvent::Erase,
            Self::Offer { foreign } => ApprovalEvent::OfferAgain {
                router_run: if foreign { other_run() } else { router_run() },
            },
            Self::Report {
                foreign_run,
                foreign_request,
                report,
            } => ApprovalEvent::Report(ReportDeliveryRequest {
                request_id: if foreign_request {
                    other_request_id()
                } else {
                    request_id()
                },
                router_run: if foreign_run {
                    other_run()
                } else {
                    router_run()
                },
                report,
            }),
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
        Command::Erase => "erase",
        Command::Offer { .. } => "offerAgain",
        Command::Report { .. } => "report",
        _ => "answer",
    }
}

pub(super) fn history_prefix(start: u8) -> Vec<Command> {
    let raise = Command::Raise { changed: false };
    let report = Command::Report {
        foreign_run: false,
        foreign_request: false,
        report: HolderReport::Applied,
    };
    let prefix: &[Command] = match start {
        0 => &[],
        1 => &[raise, raise, Command::Raise { changed: true }],
        2 => &[
            raise,
            Command::ForwardFromParent,
            Command::ForwardFromParent,
        ],
        3 => &[Command::HolderStops, raise],
        4 => &[
            raise,
            Command::AllowFromParent,
            Command::DeclineFromObserver,
            report,
            report,
        ],
        5 => &[
            raise,
            Command::DeclineFromObserver,
            Command::AllowFromParent,
            report,
            report,
        ],
        6 => &[
            raise,
            Command::AllowFromParent,
            Command::HolderStops,
            Command::Offer { foreign: false },
        ],
        7 => &[
            raise,
            Command::HolderStops,
            Command::AllowFromParent,
            Command::Offer { foreign: false },
        ],
        8 => &[
            raise,
            Command::AllowFromParent,
            report.with_foreign_request(true),
            report,
        ],
        9 => &[
            raise,
            Command::AllowFromParent,
            report,
            Command::RunEnds,
            report,
        ],
        10 => &[raise, Command::AllowFromParent, Command::RunEnds, report],
        11 => &[Command::RunEnds, raise, Command::HolderStops],
        12 => &[
            raise,
            Command::AllowFromParent,
            Command::Erase,
            raise,
            Command::HolderStops,
            report,
            Command::Offer { foreign: false },
        ],
        13 => &[
            raise,
            Command::SessionEnds,
            Command::Erase,
            raise,
            Command::HolderStops,
            report,
        ],
        _ => &[
            raise,
            Command::AllowFromParent,
            report,
            Command::Erase,
            report,
        ],
    };
    prefix.to_vec()
}
