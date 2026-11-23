use central_storage::StoreFailure;
use fleet_actor_identity::{SessionAvailability, SessionCallOutcome, SessionServiceError};
use fleet_identity_protocol::{
    AbandonedSessionCause, BindFleetSession, BindFleetSessionReply, CreateFleetSessionReply,
    EndedSessionCause, EnrollmentId, SessionSettlement,
};
use sqlx::Connection;

use super::fixtures::{
    FIXED_TIME, Operation, Receipt, SessionStoreFixture, create, id, reservation,
};

pub(super) async fn replace_enrollment(
    store: &mut SessionStoreFixture,
    cause: &str,
    ended_at: &str,
) {
    let mut connection = store.open().await;
    let mut transaction = connection.begin().await.unwrap();
    let new_enrollment: EnrollmentId = id(95).try_into().unwrap();
    sqlx::query("UPDATE login_machine_approvals SET state='ended',end_reason=?,last_grant_at=NULL,ended_at=?,updated_at=? WHERE approval_id=?")
        .bind(cause).bind(ended_at).bind(ended_at).bind(store.caller.enrollment_id.as_str())
        .execute(&mut *transaction).await.unwrap();
    sqlx::query(
        "UPDATE login_machines SET enrollment_generation=3,updated_at=? WHERE machine_id=?",
    )
    .bind(ended_at)
    .bind(store.caller.machine_id.as_str())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query("INSERT INTO login_machine_approvals (approval_id,request_id,machine_id,label,allowed_workspaces,state,approved_by_person_id,permission_checked,base_generation,code_expires_at,machine_client_id,last_grant_at,created_at,updated_at) VALUES (?,?,?,'Test machine','[]','enrolled',?,'machine:enroll',2,?,?,?,?,?)")
        .bind(new_enrollment.as_str()).bind(id(96)).bind(store.caller.machine_id.as_str())
        .bind(id(4)).bind(FIXED_TIME).bind(new_enrollment.as_str()).bind(FIXED_TIME)
        .bind(ended_at).bind(ended_at).execute(&mut *transaction).await.unwrap();
    transaction.commit().await.unwrap();
    connection.close().await.unwrap();
    store.caller.enrollment_id = new_enrollment;
}

#[tokio::test]
async fn all_terminal_enrollment_endings_settle_bound_sessions_at_the_stored_time() {
    let ended_at = "2026-11-22T10:00:02.000Z";
    for cause in ["superseded", "owner", "idle"] {
        let mut store = SessionStoreFixture::new().await;
        let (session_id, secret) = reservation(
            store
                .apply(Operation::Create(create(5, 1)), 0)
                .await
                .unwrap(),
        );
        let bind = BindFleetSession {
            session_id: session_id.clone(),
            harness_session_id: "session-address".parse().unwrap(),
            secret,
        };
        store.apply(Operation::Bind(bind.clone()), 1).await.unwrap();
        let old_enrollment = store.caller.enrollment_id.clone();
        let old_connection = store.capture_connection(1).await;
        replace_enrollment(&mut store, cause, ended_at).await;

        let current = store.caller.enrollment_id.clone();
        store.caller.enrollment_id = old_enrollment;
        assert_eq!(
            store.facts(&session_id).await,
            SessionAvailability::Unavailable
        );
        assert_eq!(
            store
                .apply_with_connection(Operation::Bind(bind.clone()), 2_001, Some(&old_connection))
                .await
                .unwrap(),
            Receipt::Bind(SessionCallOutcome::Revoked)
        );
        store.caller.enrollment_id = current;
        let Receipt::Create(SessionCallOutcome::Answered(CreateFleetSessionReply::Settled {
            settlement: SessionSettlement::Ended { cause, time },
        })) = store
            .apply(Operation::Create(create(5, 2)), 2_001)
            .await
            .unwrap()
        else {
            panic!("a same-machine replay on a new enrollment must settle the old session")
        };
        assert_eq!(cause, EndedSessionCause::MachineRevoked {});
        assert_eq!(time.as_str(), ended_at);
        assert_eq!(
            store.apply(Operation::Bind(bind), 1_000).await.unwrap(),
            Receipt::Bind(SessionCallOutcome::Answered(
                BindFleetSessionReply::Settled {
                    settlement: SessionSettlement::Ended { cause, time },
                }
            ))
        );
        assert_eq!(
            store
                .count("SELECT COUNT(*) FROM actor_sessions WHERE session_state='ended'")
                .await,
            1
        );
    }
}

#[tokio::test]
async fn settlement_uses_deadline_on_a_tie_and_does_not_escape_a_failed_write() {
    let deadline = "2026-11-22T10:10:00.000Z";
    let mut store = SessionStoreFixture::new().await;
    let (session_id, secret) = reservation(
        store
            .apply(Operation::Create(create(5, 1)), 0)
            .await
            .unwrap(),
    );
    replace_enrollment(&mut store, "idle", deadline).await;
    let bind = BindFleetSession {
        session_id,
        harness_session_id: "left-unbound".parse().unwrap(),
        secret,
    };
    let mut connection = store.open().await;
    sqlx::query("CREATE TRIGGER refuse_settlement BEFORE UPDATE ON actor_sessions BEGIN SELECT RAISE(ABORT, 'fixture'); END")
        .execute(&mut connection).await.unwrap();
    assert!(matches!(
        store.apply(Operation::Bind(bind.clone()), 600_001).await,
        Err(SessionServiceError::Store(StoreFailure::Unavailable))
    ));
    assert_eq!(
        store
            .count("SELECT COUNT(*) FROM actor_sessions WHERE session_state='reserved'")
            .await,
        1
    );
    sqlx::query("DROP TRIGGER refuse_settlement")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();

    let Receipt::Bind(SessionCallOutcome::Answered(BindFleetSessionReply::Settled {
        settlement: SessionSettlement::Abandoned { cause, time },
    })) = store.apply(Operation::Bind(bind), 599_999).await.unwrap()
    else {
        panic!("a stored ending tied with the deadline settles even after clock rollback")
    };
    assert_eq!(cause, AbandonedSessionCause::TimeRanOut {});
    assert_eq!(time.as_str(), deadline);
    assert_eq!(
        store
            .count("SELECT COUNT(*) FROM actor_sessions WHERE session_state='abandoned'")
            .await,
        1
    );
}
