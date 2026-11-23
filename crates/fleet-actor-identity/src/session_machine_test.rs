use central_engine_model::{Context, EngineTime, IdSource, Machine, StateTag, check_transitions};
use fleet_identity_protocol::{
    AbandonedSessionCause, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    CreateSessionAttempt, CreateSessionDigest, CreateSessionKind, EndFleetSessionReply,
    EnrollmentId, HarnessKind, RouterEndCause, SessionCreator, SessionSecret, SessionSettlement,
};

use crate::{
    AddressAvailability, NewSessionAdmission, RecordedSessionCreator, SessionApply,
    SessionCreateApplication, SessionCreatorAdmission, SessionEndIdentity, SessionEvent,
    SessionLifecycle, SessionReceipt, SessionReservation, SessionState,
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
        project: fleet_identity_protocol::StartProject::Named {
            project_ref: fleet_identity_protocol::ProjectRef {
                workspace: "019a3dce-2000-7000-8000-000000000004".parse().unwrap(),
                project: "019a3dce-2000-7000-8000-000000000005"
                    .to_owned()
                    .try_into()
                    .unwrap(),
            },
        },
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
    let SessionReceipt::Create(SessionCreateApplication::Reserved { reserved_until, .. }) = receipt
    else {
        panic!("expected rotated credentials");
    };
    assert_eq!(reserved_until.as_str(), "1970-01-01T00:00:00.020Z");
    assert_eq!(
        step(&rotated, candidate(&digest, 1, 22), 2, None),
        (
            rotated.clone(),
            SessionReceipt::Create(SessionCreateApplication::ExistingOrRefused(
                CreateFleetSessionReply::Superseded {}
            ))
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

fn conflicting_digest() -> CreateSessionDigest {
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        .to_owned()
        .try_into()
        .unwrap()
}

#[path = "session_ledger_test.rs"]
mod session_ledger;
