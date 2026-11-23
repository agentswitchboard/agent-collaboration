//! An independent reading of the lifecycle table, with complete receipts.
use chrono::SecondsFormat;
use fleet_identity_protocol::{
    CreateSessionRefusal, EndedSessionCause, NewAbandonmentCause, NewEndSettlement, SessionTime,
};
use hegel::generators as gs;
use workspace_ids::{CentralSessionId, SessionRef};

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Reservation {
    attempt: u8,
    deadline: i64,
    content: Content,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Content {
    Original,
    Changed,
}

impl Content {
    fn digest(self) -> CreateSessionDigest {
        match self {
            Self::Original => operation().contents_digest().unwrap(),
            Self::Changed => conflicting_digest(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Address {
    First,
    Second,
}

impl Address {
    fn reference(self) -> SessionRef {
        SessionRef {
            machine_id: "019ac1b0-0000-7000-8000-000000000003".parse().unwrap(),
            session_id: match self {
                Self::First => "first-harness",
                Self::Second => "second-harness",
            }
            .parse()
            .unwrap(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Ledger {
    Missing,
    Reserved(Reservation),
    Active(Reservation, Address, i64),
    Abandoned(Reservation, AbandonedSessionCause, i64),
    Ended(Reservation, Address, i64, EndedSessionCause, i64),
}

fn session_identity() -> CentralSessionId {
    "019ac1b0-0000-7000-8000-000000000002".parse().unwrap()
}

fn timestamp(millis: i64) -> SessionTime {
    when(millis)
        .to_rfc3339_opts(SecondsFormat::Millis, true)
        .try_into()
        .unwrap()
}

impl Ledger {
    fn reservation(&self) -> Option<&Reservation> {
        match self {
            Self::Missing => None,
            Self::Reserved(saved)
            | Self::Active(saved, ..)
            | Self::Abandoned(saved, ..)
            | Self::Ended(saved, ..) => Some(saved),
        }
    }

    fn effective(&self, now: i64, enrollment_ended_at: Option<i64>) -> Self {
        match self {
            Self::Reserved(saved) => {
                let ending =
                    enrollment_ended_at.map(|at| (at, AbandonedSessionCause::MachineRevoked {}));
                let expiry = (now >= saved.deadline || ending.is_some())
                    .then_some((saved.deadline, AbandonedSessionCause::TimeRanOut {}));
                match (expiry, ending) {
                    (Some(expiry), Some(ending)) if ending.0 < expiry.0 => {
                        Self::Abandoned(*saved, ending.1, ending.0)
                    }
                    (Some(expiry), _) => Self::Abandoned(*saved, expiry.1, expiry.0),
                    _ => self.clone(),
                }
            }
            Self::Active(saved, address, bound) => enrollment_ended_at.map_or_else(
                || self.clone(),
                |time| {
                    Self::Ended(
                        *saved,
                        *address,
                        *bound,
                        EndedSessionCause::MachineRevoked {},
                        time,
                    )
                },
            ),
            _ => self.clone(),
        }
    }

    fn settlement(&self) -> Option<SessionSettlement> {
        match self {
            Self::Abandoned(_, cause, at) => Some(SessionSettlement::Abandoned {
                cause: cause.clone(),
                time: timestamp(*at),
            }),
            Self::Ended(_, _, _, cause, at) => Some(SessionSettlement::Ended {
                cause: cause.clone(),
                time: timestamp(*at),
            }),
            _ => None,
        }
    }

    fn apply(&mut self, action: Action, now: i64, ended: Option<i64>) -> SessionReceipt {
        let effective = self.effective(now, ended);
        match action {
            Action::Create { attempt, content } => {
                use CreateFleetSessionReply as Reply;
                if self
                    .reservation()
                    .is_some_and(|saved| saved.content != content)
                {
                    return SessionReceipt::Create(Reply::Conflict {});
                }
                let reply = match effective {
                    Self::Active(_, address, _) => Reply::Active {
                        session_id: session_identity(),
                        address: address.reference(),
                    },
                    Self::Reserved(saved) if attempt <= saved.attempt => Reply::Superseded {},
                    Self::Missing | Self::Reserved(_) => {
                        let deadline = self.reservation().map_or(now + 8, |saved| saved.deadline);
                        *self = Self::Reserved(Reservation {
                            attempt,
                            deadline,
                            content,
                        });
                        Reply::Reserved {
                            session_id: session_identity(),
                            secret: SessionSecret::from_bytes(&[attempt; 32]),
                            reserved_until: timestamp(deadline),
                            workspace: fixture_project().workspace,
                        }
                    }
                    Self::Abandoned(..) | Self::Ended(..) => {
                        let settlement = effective.settlement().unwrap();
                        *self = effective;
                        Reply::Settled { settlement }
                    }
                };
                SessionReceipt::Create(reply)
            }
            Action::RefusedCreator => SessionReceipt::Create(CreateFleetSessionReply::NoSecret {}),
            Action::DeniedParent if matches!(self, Self::Missing) => {
                SessionReceipt::Create(CreateFleetSessionReply::Refused {
                    reason: CreateSessionRefusal::ParentNotValid {},
                })
            }
            Action::DeniedParent => self.apply(
                Action::Create {
                    attempt: 1,
                    content: Content::Original,
                },
                now,
                ended,
            ),
            Action::Bind {
                attempt,
                address,
                taken,
            } => {
                use BindFleetSessionReply as Reply;
                let reply = match effective {
                    Self::Missing => Reply::NoSuchStart {},
                    Self::Abandoned(..) | Self::Ended(..) => {
                        let settlement = effective.settlement().unwrap();
                        *self = effective;
                        Reply::Settled { settlement }
                    }
                    Self::Reserved(saved) | Self::Active(saved, ..) if saved.attempt != attempt => {
                        Reply::WrongSecret {}
                    }
                    Self::Reserved(_) if taken => Reply::AddressTaken {},
                    Self::Reserved(saved) => {
                        *self = Self::Active(saved, address, now);
                        Reply::Active {
                            address: address.reference(),
                        }
                    }
                    Self::Active(_, saved, _) if saved != address => {
                        Reply::AlreadyBoundToDifferentId {}
                    }
                    Self::Active(_, saved, _) => Reply::Active {
                        address: saved.reference(),
                    },
                };
                SessionReceipt::Bind(reply)
            }
            Action::End(target) => {
                use EndFleetSessionReply as Reply;
                if self.reservation().is_some_and(|saved| {
                    matches!(target, EndTarget::Operation(content) if content != saved.content)
                }) {
                    return SessionReceipt::End(Reply::NotYoursToEnd {});
                }
                let reply = match effective {
                    Self::Missing => Reply::NoSuchStart {},
                    Self::Reserved(saved) => {
                        *self = Self::Abandoned(saved, AbandonedSessionCause::GivenUp {}, now);
                        Reply::Settled {
                            settlement: NewEndSettlement::Abandoned {
                                cause: NewAbandonmentCause::GivenUp {},
                                time: timestamp(now),
                            },
                        }
                    }
                    Self::Active(..) if matches!(target, EndTarget::Operation(_)) => {
                        Reply::NotYoursToEnd {}
                    }
                    Self::Active(saved, address, bound) => {
                        *self =
                            Self::Ended(saved, address, bound, EndedSessionCause::GivenUp {}, now);
                        Reply::Settled {
                            settlement: NewEndSettlement::Ended {
                                cause: RouterEndCause::GivenUp {},
                                time: timestamp(now),
                            },
                        }
                    }
                    Self::Abandoned(..) | Self::Ended(..) => {
                        let settlement = effective.settlement().unwrap();
                        *self = effective;
                        Reply::AlreadySettled { settlement }
                    }
                };
                SessionReceipt::End(reply)
            }
        }
    }

    fn observe(state: &SessionState) -> Self {
        let reservation = |saved: &crate::ReservedSession| Reservation {
            attempt: saved.attempt.get().try_into().unwrap(),
            deadline: saved.reservation.reserved_until.timestamp_millis(),
            content: if saved.reservation.contents_digest == Content::Original.digest() {
                Content::Original
            } else {
                assert_eq!(saved.reservation.contents_digest, Content::Changed.digest());
                Content::Changed
            },
        };
        let address = |bound: &crate::BoundSession| {
            if bound.harness_session_id == Address::First.reference().session_id {
                Address::First
            } else {
                assert_eq!(
                    bound.harness_session_id,
                    Address::Second.reference().session_id
                );
                Address::Second
            }
        };
        match state {
            SessionState::Missing => Self::Missing,
            SessionState::Reserved(saved) => Self::Reserved(reservation(saved)),
            SessionState::Active(bound) => Self::Active(
                reservation(&bound.reserved),
                address(bound),
                bound.bound_at.timestamp_millis(),
            ),
            SessionState::Abandoned {
                reserved,
                cause,
                at,
            } => Self::Abandoned(reservation(reserved), cause.clone(), at.timestamp_millis()),
            SessionState::Ended { bound, cause, at } => Self::Ended(
                reservation(&bound.reserved),
                address(bound),
                bound.bound_at.timestamp_millis(),
                cause.clone(),
                at.timestamp_millis(),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum EndTarget {
    Central,
    Operation(Content),
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Create {
        attempt: u8,
        content: Content,
    },
    RefusedCreator,
    DeniedParent,
    Bind {
        attempt: u8,
        address: Address,
        taken: bool,
    },
    End(EndTarget),
}

impl Action {
    fn event(self, now: i64) -> SessionEvent {
        match self {
            Self::Create { attempt, content } => candidate(&content.digest(), attempt, now + 8),
            Self::RefusedCreator | Self::DeniedParent => {
                let refused = matches!(self, Self::RefusedCreator);
                SessionEvent::Create {
                    caller: if refused {
                        SessionCreatorAdmission::NoSecret
                    } else {
                        SessionCreatorAdmission::Admitted
                    },
                    first: (!refused).then(|| {
                        NewSessionAdmission::Refused(CreateSessionRefusal::ParentNotValid {})
                    }),
                    contents_digest: Content::Original.digest(),
                    attempt: 1.try_into().unwrap(),
                    candidate_secret: None,
                }
            }
            Self::Bind {
                attempt,
                address,
                taken,
            } => SessionEvent::Bind {
                harness_session_id: address.reference().session_id,
                secret: SessionSecret::from_bytes(&[attempt; 32]),
                address: if taken {
                    AddressAvailability::Taken
                } else {
                    AddressAvailability::Free
                },
            },
            Self::End(target) => end(match target {
                EndTarget::Central => None,
                EndTarget::Operation(content) => Some(content.digest()),
            }),
        }
    }
}

fn compare(
    ledger: &mut Ledger,
    state: &mut SessionState,
    action: Action,
    now: i64,
    ended: Option<i64>,
) {
    let expected = ledger.apply(action, now, ended);
    let (next, actual) = step(state, action.event(now), now, ended);
    assert_eq!(
        actual, expected,
        "{action:?} at {now} with ending {ended:?}"
    );
    assert_eq!(
        &Ledger::observe(&next),
        ledger,
        "stored facts after {action:?} at {now}"
    );
    *state = next;
}

#[hegel::test(test_cases = 320)]
fn histories_match_the_independent_start_ledger(case: hegel::TestCase) {
    let start = i64::from(case.draw(gs::integers::<u8>().max_value(30))) + 1;
    let mut tie_ledger = Ledger::Missing;
    let mut tie_state = SessionState::Missing;
    let create = Action::Create {
        attempt: 1,
        content: Content::Original,
    };
    let bind = Action::Bind {
        attempt: 2,
        address: Address::First,
        taken: false,
    };
    compare(&mut tie_ledger, &mut tie_state, create, start, None);
    compare(
        &mut tie_ledger,
        &mut tie_state,
        bind,
        start + 8,
        Some(start + 8),
    );
    compare(
        &mut tie_ledger,
        &mut tie_state,
        bind,
        start - 1,
        Some(start + 8),
    );

    let mut ledger = Ledger::Missing;
    let mut state = SessionState::Missing;
    let mut enrollment_end = None;
    for _ in 0..case.draw(gs::integers::<u8>().min_value(1).max_value(25)) {
        let now = i64::from(case.draw(gs::integers::<u8>().max_value(20)));
        if enrollment_end.is_none() && case.draw(gs::integers::<u8>().max_value(6)) == 0 {
            enrollment_end = Some(now);
        }
        let attempt = case.draw(gs::integers::<u8>().min_value(1).max_value(4));
        let action = match case.draw(gs::integers::<u8>().max_value(10)) {
            0..=2 => Action::Create {
                attempt,
                content: Content::Original,
            },
            3 => Action::Create {
                attempt,
                content: Content::Changed,
            },
            4 => Action::RefusedCreator,
            5 => Action::DeniedParent,
            6 => Action::End(EndTarget::Central),
            7 => Action::End(EndTarget::Operation(Content::Original)),
            8 => Action::End(EndTarget::Operation(Content::Changed)),
            index => Action::Bind {
                attempt,
                address: if index == 9 {
                    Address::First
                } else {
                    Address::Second
                },
                taken: case.draw(gs::integers::<u8>().max_value(1)) == 0,
            },
        };
        compare(&mut ledger, &mut state, action, now, enrollment_end);
    }
}
