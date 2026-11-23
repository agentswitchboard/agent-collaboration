//! Fixture from fleet-actor-identity's session reducer and service at 7403c5ee8.

use fleet_identity_protocol::{
    AbandonedSessionCause, BindFleetSession, BindFleetSessionReply, CreateFleetSession,
    CreateFleetSessionReply, CreateReplyContext, CreateSessionAttempt, CreateSessionDigest,
    EndFleetSession, EndFleetSessionReply, EndReplyContext, EndSessionTarget, EndedSessionCause,
    NewAbandonmentCause, NewEndSettlement, PresentedSessionSecret, RouterEndCause, SessionCreator,
    SessionSecret, SessionSettlement,
};
use switchboard_session_start::{
    BindContradiction, BindReplyKind, CloseResult, CreateAnswer, CreateContradiction,
    CreateReplyKind, DirectiveResult, EndLeg, Exposure, LinkResult, ObservedReservation,
    OpeningOutcome, PutSecretResult, SetUpResult, StartAnswer, StartDirective, StartNotification,
    StartRevision, StartStateKind,
};
use workspace_ids::{MachineId, SessionRef};

use super::fixtures::{self, Journey};

#[derive(Default)]
struct CentralFixture {
    reservation: Option<(CreateSessionDigest, CreateSessionAttempt, SessionSecret)>,
    active: Option<SessionRef>,
    settlement: Option<SessionSettlement>,
}

impl CentralFixture {
    fn create(&mut self, request: &CreateFleetSession) -> CreateFleetSessionReply {
        if matches!(
            request.creator,
            SessionCreator::Session {
                presented: PresentedSessionSecret::Nothing {},
                ..
            }
        ) {
            return CreateFleetSessionReply::NoSecret {};
        }
        let digest = request.contents_digest().unwrap();
        if self
            .reservation
            .as_ref()
            .is_some_and(|(held, ..)| held != &digest)
        {
            return CreateFleetSessionReply::Conflict {};
        }
        if let Some(settlement) = &self.settlement {
            return CreateFleetSessionReply::Settled {
                settlement: settlement.clone(),
            };
        }
        if let Some(address) = &self.active {
            return CreateFleetSessionReply::Active {
                session_id: fixtures::central_id(),
                address: address.clone(),
            };
        }
        if self
            .reservation
            .as_ref()
            .is_some_and(|(_, attempt, _)| attempt >= &request.attempt)
        {
            return CreateFleetSessionReply::Superseded {};
        }
        let next = if self.reservation.is_some() {
            fixtures::secret(2)
        } else {
            fixtures::secret(1)
        };
        self.reservation = Some((digest, request.attempt, next.clone()));
        CreateFleetSessionReply::Reserved {
            workspace: fixtures::workspace_id(),
            session_id: fixtures::central_id(),
            secret: next,
            reserved_until: fixtures::time(),
        }
    }

    fn bind(&mut self, request: &BindFleetSession, address_taken: bool) -> BindFleetSessionReply {
        if let Some(settlement) = &self.settlement {
            return BindFleetSessionReply::Settled {
                settlement: settlement.clone(),
            };
        }
        let Some((_, _, secret)) = &self.reservation else {
            return BindFleetSessionReply::NoSuchStart {};
        };
        if secret != &request.secret {
            return BindFleetSessionReply::WrongSecret {};
        }
        if let Some(address) = &self.active {
            return if address.session_id == request.harness_session_id {
                BindFleetSessionReply::Active {
                    address: address.clone(),
                }
            } else {
                BindFleetSessionReply::AlreadyBoundToDifferentId {}
            };
        }
        if address_taken {
            return BindFleetSessionReply::AddressTaken {};
        }
        let address = SessionRef {
            machine_id: fixtures::machine_id(1),
            session_id: request.harness_session_id.clone(),
        };
        self.active = Some(address.clone());
        BindFleetSessionReply::Active { address }
    }

    fn end(&mut self, request: &EndFleetSession) -> EndFleetSessionReply {
        assert_eq!(
            request.target,
            EndSessionTarget::CentralId {
                session_id: fixtures::central_id()
            }
        );
        assert_eq!(request.cause, RouterEndCause::GivenUp {});
        if let Some(settlement) = &self.settlement {
            return EndFleetSessionReply::AlreadySettled {
                settlement: settlement.clone(),
            };
        }
        if self.reservation.is_none() {
            return EndFleetSessionReply::NoSuchStart {};
        }
        let time = fixtures::time();
        let settlement = if self.active.is_some() {
            self.settlement = Some(SessionSettlement::Ended {
                cause: EndedSessionCause::GivenUp {},
                time: time.clone(),
            });
            NewEndSettlement::Ended {
                cause: RouterEndCause::GivenUp {},
                time,
            }
        } else {
            self.settlement = Some(SessionSettlement::Abandoned {
                cause: AbandonedSessionCause::GivenUp {},
                time: time.clone(),
            });
            NewEndSettlement::Abandoned {
                cause: NewAbandonmentCause::GivenUp {},
                time,
            }
        };
        EndFleetSessionReply::Settled { settlement }
    }
}

fn create_call(journey: &Journey) -> CreateFleetSession {
    let StartDirective::SendCreate(request) = &journey.pending.as_ref().unwrap().directive else {
        panic!("create expected")
    };
    request.clone()
}

pub(super) fn checked_create(
    request: &CreateFleetSession,
    reply: CreateFleetSessionReply,
) -> (DirectiveResult, Option<SessionSecret>) {
    use CreateFleetSessionReply as Wire;
    let answer = match &reply {
        Wire::Reserved {
            session_id,
            reserved_until,
            ..
        } => CreateAnswer::Reserved {
            session_id: session_id.clone(),
            reserved_until: reserved_until.clone(),
        },
        Wire::Superseded {} => CreateAnswer::Superseded,
        Wire::Active {
            session_id,
            address,
        } => CreateAnswer::Active {
            session_id: session_id.clone(),
            address: address.clone(),
        },
        Wire::Settled { settlement } => CreateAnswer::Settled {
            settlement: settlement.clone(),
        },
        Wire::Refused { reason } => CreateAnswer::Refused {
            reason: reason.clone(),
        },
        Wire::Conflict {} => CreateAnswer::Conflict,
        Wire::NoSecret {} => CreateAnswer::NoSecret,
        Wire::NoSuchSession {} => CreateAnswer::NoSuchSession,
        Wire::SessionEnded {} => CreateAnswer::SessionEnded,
        Wire::WrongSecret {} => CreateAnswer::WrongSecret,
    };
    match CreateReplyContext::from_request(request).check(reply) {
        Ok(checked) => {
            let pending = match checked.into_wire() {
                Wire::Reserved { secret, .. } => Some(secret),
                _ => None,
            };
            (
                DirectiveResult::SendCreate(LinkResult::Answered(answer)),
                pending,
            )
        }
        Err(_) => {
            let kind = CreateReplyKind::from(&answer);
            let reservation = match answer {
                CreateAnswer::Reserved {
                    session_id,
                    reserved_until,
                } => Some(ObservedReservation {
                    session_id,
                    reserved_until,
                }),
                _ => None,
            };
            (
                DirectiveResult::SendCreate(LinkResult::Contradicted(CreateContradiction {
                    reply: Some(kind),
                    reservation,
                })),
                None,
            )
        }
    }
}

fn checked_bind(
    request: &BindFleetSession,
    machine: &MachineId,
    reply: BindFleetSessionReply,
) -> DirectiveResult {
    let output = match &reply {
        BindFleetSessionReply::Active { address }
            if &address.machine_id != machine
                || address.session_id != request.harness_session_id =>
        {
            LinkResult::Contradicted(BindContradiction {
                reply: Some(BindReplyKind::Active),
            })
        }
        _ => LinkResult::Answered(reply),
    };
    DirectiveResult::SendBind(output)
}

fn issue_and_hold(journey: &mut Journey, reply: CreateFleetSessionReply) -> SessionSecret {
    let create = create_call(journey);
    let (result, pending) = checked_create(&create, reply);
    let received = pending.unwrap();
    journey.result(result);
    assert_eq!(
        journey.snapshot().kind(),
        switchboard_session_start::SnapshotKind::Reserved
    );
    journey.saved();
    let StartDirective::PutSecret { issue } = &journey.pending.as_ref().unwrap().directive else {
        panic!("expected secret put")
    };
    assert_eq!(issue.attempt, create.attempt);
    journey.result(DirectiveResult::PutSecret(PutSecretResult::Stored));
    assert!(!format!("{:?}", journey.machine).contains(received.expose_secret()));
    assert!(!format!("{:?}", journey.snapshot()).contains(received.expose_secret()));
    journey.saved();
    journey.result(DirectiveResult::Launch(
        switchboard_session_start::LaunchResult::Started {
            harness_session_id: fixtures::harness_id(),
        },
    ));
    journey.saved();
    received
}

fn bind_call(journey: &Journey, secret: &SessionSecret) -> BindFleetSession {
    let StartDirective::SendBind {
        session_id,
        harness_session_id,
        ..
    } = &journey.pending.as_ref().unwrap().directive
    else {
        panic!("bind expected")
    };
    BindFleetSession {
        session_id: session_id.clone(),
        harness_session_id: harness_session_id.clone(),
        secret: secret.clone(),
    }
}

#[test]
fn central_rotates_the_attempt_secret_and_replays_the_same_active_bind() {
    let mut journal = Journey::creating();
    let mut central = CentralFixture::default();
    let first = create_call(&journal);
    let first_reply = central.create(&first);
    let CreateFleetSessionReply::Reserved {
        session_id,
        secret,
        reserved_until,
        workspace,
    } = first_reply
    else {
        panic!("reservation expected")
    };
    assert_eq!(
        central.create(&first),
        CreateFleetSessionReply::Superseded {}
    );
    journal.result(DirectiveResult::SendCreate(LinkResult::AnswerLost));
    journal.saved();
    journal.ask();
    journal.saved();
    let later = create_call(&journal);
    assert_eq!(later.attempt, fixtures::attempt(2));
    assert_eq!(first.contents_digest(), later.contents_digest());
    let on_wire = serde_json::to_value(&later).unwrap();
    assert_eq!(on_wire.get("attempt"), Some(&serde_json::json!(2)));
    assert_eq!(
        on_wire.get("creator"),
        Some(&serde_json::json!({"kind":"routerOperator"}))
    );
    let rotated = central.create(&later);
    let CreateFleetSessionReply::Reserved {
        session_id: later_id,
        reserved_until: later_until,
        secret: later_secret,
        workspace: later_workspace,
    } = &rotated
    else {
        panic!("rotated issue expected")
    };
    assert_eq!(*later_id, session_id);
    assert_eq!(*later_until, reserved_until);
    assert_eq!(*later_workspace, workspace);
    assert_ne!(*later_secret, secret);
    assert_eq!(
        central.create(&first),
        CreateFleetSessionReply::Superseded {}
    );
    let held = issue_and_hold(&mut journal, rotated);
    journal.result(DirectiveResult::SetUp(SetUpResult::Done));
    journal.saved();
    let bind = bind_call(&journal, &held);
    let first_bind = central.bind(&bind, false);
    assert_eq!(
        first_bind,
        BindFleetSessionReply::Active {
            address: fixtures::address()
        }
    );
    journal.result(DirectiveResult::SendBind(LinkResult::AnswerLost));
    journal.saved();
    journal.ask();
    journal.saved();
    let retry = bind_call(&journal, &held);
    assert_eq!(retry, bind);
    let active = central.bind(&retry, true);
    assert_eq!(active, first_bind);
    assert!(matches!(
        central.create(&later),
        CreateFleetSessionReply::Active { .. }
    ));
    journal.result(checked_bind(&retry, &fixtures::machine_id(1), active));
    let StartDirective::CommitOpening { opening, .. } =
        &journal.pending.as_ref().unwrap().directive
    else {
        panic!("opening expected")
    };
    let persisted = opening.opened().clone();
    assert!(!journal.notices.iter().any(|notice| matches!(
        notice,
        StartNotification::Tell {
            answer: StartAnswer::Started { .. },
            ..
        }
    )));
    journal.result(DirectiveResult::CommitOpening(OpeningOutcome::Committed(
        StartRevision::new(99),
    )));
    let mut restarted = Journey::on(fixtures::restore(
        fixtures::instance(999),
        StartRevision::new(99),
        persisted,
    ));
    restarted.ask();
    assert_eq!(restarted.notices.last(), journal.notices.last());
    assert!(restarted.pending.is_none());
}

#[test]
fn every_half_of_a_wrong_active_address_is_delivered_as_a_contradiction() {
    for address in [
        SessionRef {
            machine_id: fixtures::machine_id(41),
            session_id: fixtures::harness_id(),
        },
        SessionRef {
            machine_id: fixtures::machine_id(1),
            session_id: "other-harness".parse().unwrap(),
        },
    ] {
        let mut journal = Journey::binding();
        let bind = bind_call(&journal, &fixtures::secret(17));
        let result = checked_bind(
            &bind,
            &fixtures::machine_id(1),
            BindFleetSessionReply::Active { address },
        );
        assert_eq!(
            result,
            DirectiveResult::SendBind(LinkResult::Contradicted(BindContradiction {
                reply: Some(BindReplyKind::Active)
            }))
        );
        journal.result(result);
        journal.result(DirectiveResult::CloseConversation(CloseResult::Closed));
        journal.saved();
        assert_eq!(journal.machine.kind(), StartStateKind::Unknown);
        assert_eq!(
            journal.machine.facts().unwrap().bind_exposure,
            Exposure::MayHaveReached
        );
        assert!(!journal.effects.contains(&"CommitOpening"));
        assert!(!journal.effects.contains(&"SendEnd"));
    }
}

#[test]
fn central_checks_settlement_then_secret_then_address_and_preserves_each_end() {
    for had_active in [false, true] {
        let mut journal = Journey::creating();
        let mut central = CentralFixture::default();
        let create = create_call(&journal);
        let reserved = central.create(&create);
        let secret = issue_and_hold(&mut journal, reserved);
        let mut bind = BindFleetSession {
            session_id: fixtures::central_id(),
            harness_session_id: fixtures::harness_id(),
            secret: fixtures::secret(99),
        };
        assert_eq!(
            central.bind(&bind, true),
            BindFleetSessionReply::WrongSecret {}
        );
        bind.secret = secret;
        assert_eq!(
            central.bind(&bind, true),
            BindFleetSessionReply::AddressTaken {}
        );
        if had_active {
            journal.result(DirectiveResult::SetUp(SetUpResult::Done));
            journal.saved();
            let active = central.bind(&bind, false);
            journal.result(checked_bind(&bind, &fixtures::machine_id(1), active));
            journal.result(DirectiveResult::CommitOpening(
                OpeningOutcome::NotCommitted(
                    switchboard_session_start::OpeningNotCommitted::PreparationFailed,
                ),
            ));
        } else {
            journal.result(DirectiveResult::SetUp(SetUpResult::Failed));
        }
        journal.saved();
        let StartDirective::SendEnd(request) = &journal.pending.as_ref().unwrap().directive else {
            panic!("end expected")
        };
        let request = request.clone();
        let settled = central.end(&request);
        let EndFleetSessionReply::Settled { settlement } = &settled else {
            panic!("exact settlement expected")
        };
        assert_eq!(
            matches!(settlement, NewEndSettlement::Ended { .. }),
            had_active
        );
        let settled_expected = settlement.clone();
        let settlement = central.settlement.clone().unwrap();
        assert_eq!(
            central.end(&request),
            EndFleetSessionReply::AlreadySettled {
                settlement: settlement.clone()
            }
        );
        bind.secret = fixtures::secret(99);
        assert_eq!(
            central.bind(&bind, true),
            BindFleetSessionReply::Settled {
                settlement: settlement.clone()
            }
        );
        let impossible = EndFleetSessionReply::Settled {
            settlement: NewEndSettlement::Ended {
                cause: RouterEndCause::EndedForGood {},
                time: fixtures::time(),
            },
        };
        assert!(
            EndReplyContext::from_request(&request)
                .check(impossible)
                .is_err()
        );
        let checked = EndReplyContext::from_request(&request)
            .check(settled)
            .unwrap();
        journal.result(DirectiveResult::SendEnd(LinkResult::Answered(
            checked.into_wire(),
        )));
        journal.result(DirectiveResult::CloseConversation(CloseResult::Closed));
        journal.saved();
        assert_eq!(journal.machine.kind(), StartStateKind::Settled);
        assert_eq!(
            journal.machine.facts().unwrap().end,
            EndLeg::Settled(settled_expected)
        );
        assert_eq!(
            journal.machine.facts().unwrap().settlement,
            Some(settlement)
        );
    }
}
