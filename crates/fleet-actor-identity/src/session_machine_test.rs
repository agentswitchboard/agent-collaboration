use central_engine_model::{Context, EngineTime, IdSource, Machine, StateTag, check_transitions};
use fleet_identity_protocol::{
    AbandonedSessionCause, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    CreateSessionAttempt, CreateSessionDigest, CreateSessionKind, EndFleetSessionReply,
    EnrollmentId, HarnessKind, RouterEndCause, SessionCreator, SessionSecret, SessionSettlement,
};
use hegel::generators as gs;

use crate::{
    AddressAvailability, NewSessionAdmission, RecordedSessionCreator, SessionApply,
    SessionCreatorAdmission, SessionEndIdentity, SessionEvent, SessionLifecycle, SessionReceipt,
    SessionReservation, SessionState,
};

struct NoIds;
impl IdSource for NoIds {
    fn next_id(&mut self) -> uuid::Uuid {
        panic!("the machine receives its ID before evaluation")
    }
}

fn when(millis: i64) -> EngineTime {
    EngineTime::from_timestamp_millis(millis).unwrap()
}
fn operation() -> CreateFleetSession {
    CreateFleetSession {
        operation_id: "019ac1b0-0000-7000-8000-000000000001".parse().unwrap(),
        attempt: 1.try_into().unwrap(),
        creator: SessionCreator::RouterOperator {},
        agent_program: HarnessKind::Codex,
        session_kind: CreateSessionKind::Ordinary {},
    }
}

fn candidate(digest: &CreateSessionDigest, sequence: u8, deadline: i64) -> SessionEvent {
    let reservation = SessionReservation {
        session_id: "019ac1b0-0000-7000-8000-000000000002".parse().unwrap(),
        machine_id: "019ac1b0-0000-7000-8000-000000000003".parse().unwrap(),
        enrollment_id: EnrollmentId::try_from("019ac1b0-0000-7000-8000-000000000004".to_owned())
            .unwrap(),
        operation_id: operation().operation_id,
        contents_digest: digest.clone(),
        creator: RecordedSessionCreator::Operator,
        agent_program: HarnessKind::Codex,
        kind: CreateSessionKind::Ordinary {},
        reserved_until: when(deadline),
    };
    SessionEvent::Create {
        caller: SessionCreatorAdmission::Admitted,
        first: Some(Box::new(NewSessionAdmission::Permitted(reservation))),
        contents_digest: digest.clone(),
        attempt: CreateSessionAttempt::try_from(u64::from(sequence)).unwrap(),
        candidate_secret: Some(SessionSecret::from_bytes(&[sequence; 32])),
    }
}

fn step(
    state: &SessionState,
    event: SessionEvent,
    at: i64,
    ended: Option<i64>,
) -> (SessionState, SessionReceipt) {
    let (next, mut receipts) = SessionLifecycle::transition(
        state,
        SessionApply {
            event,
            enrollment_ended_at: ended.map(when),
        },
        &mut Context {
            now: when(at),
            ids: &mut NoIds,
        },
    )
    .unwrap();
    assert_eq!(receipts.len(), 1);
    (next, receipts.pop().unwrap())
}

fn bind(secret: u8) -> SessionEvent {
    SessionEvent::Bind {
        harness_session_id: "harness-session".parse().unwrap(),
        secret: SessionSecret::from_bytes(&[secret; 32]),
        address: AddressAvailability::Free,
    }
}

fn end(digest: Option<CreateSessionDigest>) -> SessionEvent {
    SessionEvent::End {
        by: digest.map_or(
            SessionEndIdentity::CentralId,
            SessionEndIdentity::OperationDigest,
        ),
        cause: RouterEndCause::GivenUp {},
    }
}

#[test]
fn reservation_attempts_rotate_only_the_current_secret() {
    let digest = operation().contents_digest().unwrap();
    let (reserved, _) = step(&SessionState::Missing, candidate(&digest, 1, 20), 0, None);
    let (rotated, receipt) = step(&reserved, candidate(&digest, 2, 21), 1, None);
    let SessionReceipt::Create(CreateFleetSessionReply::Reserved { reserved_until, .. }) = receipt
    else {
        panic!("expected rotated credentials");
    };
    assert_eq!(reserved_until.as_str(), "1970-01-01T00:00:00.020Z");
    assert_eq!(
        step(&rotated, candidate(&digest, 1, 22), 2, None),
        (
            rotated.clone(),
            SessionReceipt::Create(CreateFleetSessionReply::Superseded {})
        )
    );
    assert_eq!(
        step(&rotated, bind(1), 3, None),
        (
            rotated.clone(),
            SessionReceipt::Bind(BindFleetSessionReply::WrongSecret {})
        )
    );
    assert_eq!(step(&rotated, bind(2), 4, None).0.state_tag(), "active");
}

#[test]
fn exact_deadline_wins_tied_enrollment_end_and_persisted_settlement_survives_rollback() {
    let digest = operation().contents_digest().unwrap();
    let (reserved, _) = step(&SessionState::Missing, candidate(&digest, 1, 20), 0, None);
    let (settled, receipt) = step(&reserved, bind(1), 1, Some(20));
    let SessionReceipt::Bind(BindFleetSessionReply::Settled {
        settlement: SessionSettlement::Abandoned { cause, time },
    }) = receipt
    else {
        panic!("expected settlement");
    };
    assert_eq!(cause, AbandonedSessionCause::TimeRanOut {});
    assert_eq!(time.as_str(), "1970-01-01T00:00:00.020Z");
    let (unchanged, _) = step(&settled, bind(1), 0, Some(19));
    assert_eq!(unchanged, settled);
}

#[test]
fn samples_witness_each_declared_transition_and_only_declared_transitions() {
    let digest = operation().contents_digest().unwrap();
    let (reserved, _) = step(&SessionState::Missing, candidate(&digest, 1, 20), 0, None);
    let (active, _) = step(&reserved, bind(1), 1, None);
    let (abandoned, _) = step(&reserved, end(None), 2, None);
    let (ended, _) = step(&active, end(None), 2, None);
    let mut no_admission = candidate(&digest, 1, 20);
    if let SessionEvent::Create { caller, .. } = &mut no_admission {
        *caller = SessionCreatorAdmission::NoSecret;
    }
    let events = [
        candidate(&digest, 1, 20),
        candidate(&digest, 2, 20),
        no_admission,
        bind(1),
        bind(2),
        end(None),
        end(Some(digest)),
        end(Some(conflicting_digest())),
    ];
    let states = [SessionState::Missing, reserved, active, abandoned, ended];
    let mut cases = Vec::new();
    for state in states {
        for enrollment_ended_at in [None, Some(when(3)), Some(when(20))] {
            cases.extend(events.iter().cloned().map(|event| {
                (
                    state.clone(),
                    SessionApply {
                        event,
                        enrollment_ended_at,
                    },
                )
            }));
        }
    }
    check_transitions::<SessionLifecycle>(
        cases,
        &mut Context {
            now: when(4),
            ids: &mut NoIds,
        },
    )
    .unwrap();
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ExpectedReply {
    CreateReserved,
    CreateSuperseded,
    CreateActive,
    CreateSettled,
    CreateConflict,
    Bound,
    BindWrongSecret,
    BindSettled,
    EndSettled,
    EndAlreadySettled,
    EndNotYours,
}

#[derive(Clone, Copy)]
enum Model {
    Reserved { attempt: u8 },
    Active { attempt: u8 },
    Abandoned { attempt: u8 },
    Ended { attempt: u8 },
}

impl Model {
    fn attempt(self) -> u8 {
        match self {
            Self::Reserved { attempt }
            | Self::Active { attempt }
            | Self::Abandoned { attempt }
            | Self::Ended { attempt } => attempt,
        }
    }

    fn at(self, now: i64, enrollment_ended_at: Option<i64>) -> Self {
        match self {
            Self::Reserved { attempt } if now >= 8 || enrollment_ended_at.is_some() => {
                Self::Abandoned { attempt }
            }
            Self::Active { attempt } if enrollment_ended_at.is_some() => Self::Ended { attempt },
            state => state,
        }
    }

    fn apply(
        self,
        action: ModelAction,
        now: i64,
        enrollment_ended_at: Option<i64>,
    ) -> (Self, ExpectedReply) {
        use ExpectedReply as Reply;

        if let ModelAction::Bind(secret) = action
            && secret != self.attempt()
        {
            return (self, Reply::BindWrongSecret);
        }
        match (self.at(now, enrollment_ended_at), action) {
            (_, ModelAction::Conflict) => (self, Reply::CreateConflict),
            (Self::Reserved { attempt }, ModelAction::Retry(new)) if new > attempt => {
                (Self::Reserved { attempt: new }, Reply::CreateReserved)
            }
            (Self::Reserved { .. }, ModelAction::Retry(_)) => (self, Reply::CreateSuperseded),
            (Self::Active { .. }, ModelAction::Retry(_)) => (self, Reply::CreateActive),
            (state @ (Self::Abandoned { .. } | Self::Ended { .. }), ModelAction::Retry(_)) => {
                (state, Reply::CreateSettled)
            }
            (Self::Reserved { attempt }, ModelAction::Bind(_)) => {
                (Self::Active { attempt }, Reply::Bound)
            }
            (Self::Active { .. }, ModelAction::Bind(_)) => (self, Reply::Bound),
            (state @ (Self::Abandoned { .. } | Self::Ended { .. }), ModelAction::Bind(_)) => {
                (state, Reply::BindSettled)
            }
            (Self::Reserved { attempt }, ModelAction::EndByOperation | ModelAction::EndById) => {
                (Self::Abandoned { attempt }, Reply::EndSettled)
            }
            (Self::Active { .. }, ModelAction::EndByOperation) => (self, Reply::EndNotYours),
            (Self::Active { attempt }, ModelAction::EndById) => {
                (Self::Ended { attempt }, Reply::EndSettled)
            }
            (
                state @ (Self::Abandoned { .. } | Self::Ended { .. }),
                ModelAction::EndByOperation | ModelAction::EndById,
            ) => (state, Reply::EndAlreadySettled),
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::Reserved { .. } => "reserved",
            Self::Active { .. } => "active",
            Self::Abandoned { .. } => "abandoned",
            Self::Ended { .. } => "ended",
        }
    }
}

#[derive(Clone, Copy)]
enum ModelAction {
    Retry(u8),
    Bind(u8),
    EndByOperation,
    EndById,
    Conflict,
}

fn observe_reply(receipt: &SessionReceipt) -> Option<ExpectedReply> {
    use ExpectedReply as Reply;

    Some(match receipt {
        SessionReceipt::Create(CreateFleetSessionReply::Reserved { .. }) => Reply::CreateReserved,
        SessionReceipt::Create(CreateFleetSessionReply::Superseded {}) => Reply::CreateSuperseded,
        SessionReceipt::Create(CreateFleetSessionReply::Active { .. }) => Reply::CreateActive,
        SessionReceipt::Create(CreateFleetSessionReply::Settled { .. }) => Reply::CreateSettled,
        SessionReceipt::Create(CreateFleetSessionReply::Conflict {}) => Reply::CreateConflict,
        SessionReceipt::Bind(BindFleetSessionReply::Active { .. }) => Reply::Bound,
        SessionReceipt::Bind(BindFleetSessionReply::WrongSecret {}) => Reply::BindWrongSecret,
        SessionReceipt::Bind(BindFleetSessionReply::Settled { .. }) => Reply::BindSettled,
        SessionReceipt::End(EndFleetSessionReply::Settled { .. }) => Reply::EndSettled,
        SessionReceipt::End(EndFleetSessionReply::AlreadySettled { .. }) => {
            Reply::EndAlreadySettled
        }
        SessionReceipt::End(EndFleetSessionReply::NotYoursToEnd {}) => Reply::EndNotYours,
        _ => return None,
    })
}

fn conflicting_digest() -> CreateSessionDigest {
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        .to_owned()
        .try_into()
        .unwrap()
}

#[hegel::test(test_cases = 320)]
fn histories_match_the_independent_start_ledger(case: hegel::TestCase) {
    let digest = operation().contents_digest().unwrap();
    let other = conflicting_digest();
    let (mut state, first_reply) = step(&SessionState::Missing, candidate(&digest, 1, 8), 0, None);
    assert_eq!(
        observe_reply(&first_reply),
        Some(ExpectedReply::CreateReserved)
    );
    let mut ledger = Model::Reserved { attempt: 1 };
    let mut enrollment_end = None;
    for _ in 0..case.draw(gs::integers::<u8>().min_value(1).max_value(25)) {
        let now = i64::from(case.draw(gs::integers::<u8>().max_value(11)));
        if enrollment_end.is_none() && case.draw(gs::integers::<u8>().max_value(6)) == 0 {
            enrollment_end = Some(now);
        }
        let attempt = case.draw(gs::integers::<u8>().min_value(1).max_value(4));
        let (action, event) = match case.draw(gs::integers::<u8>().max_value(4)) {
            0 => (ModelAction::Retry(attempt), candidate(&digest, attempt, 70)),
            1 => (ModelAction::Bind(attempt), bind(attempt)),
            2 => (ModelAction::EndByOperation, end(Some(digest.clone()))),
            3 => (ModelAction::EndById, end(None)),
            _ => (ModelAction::Conflict, candidate(&other, attempt, 70)),
        };
        let (expected_next, expected_reply) = ledger.apply(action, now, enrollment_end);
        let (next, reply) = step(&state, event, now, enrollment_end);
        assert_eq!(observe_reply(&reply), Some(expected_reply), "{reply:?}");
        assert_eq!(next.state_tag(), expected_next.tag());
        state = next;
        ledger = expected_next;
    }
}
