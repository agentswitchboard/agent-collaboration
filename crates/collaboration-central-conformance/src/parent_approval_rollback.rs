//! Adjacent-owner writes and approval settlement must roll back in one loan.
use approval_request_protocol::{CancelReason, Ending, Raise};
use central_link_protocol::{
    LinkCallRefusal, TypedCallResult, session_lifecycle_rpc::EndFleetSessionRpc,
};
use central_storage::{SqlValue, Statement};
use fleet_identity_protocol::{EndFleetSessionReply, NewEndSettlement};

use super::{ApprovalLinkFixture, Chain, ParentSetup, Result, heartbeat, text};
use crate::{
    approval_link_scenarios::{EnrollmentLoginEnd, next_ending, read_request},
    session_link_scenarios::{answer as session_answer, call as session_call, end},
};

pub async fn session_end_and_approval_rollback(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let session = &setup.child.session_id;
    let target = &setup.target;
    let before = session_snapshot(fixture, session.as_str(), &setup.approval).await?;
    assert_eq!(
        read_request(fixture, &setup.approval).await?,
        [text("waiting"), SqlValue::Null]
    );

    fixture.fail_next_applying_write().await?;
    assert_eq!(
        session_call::<EndFleetSessionRpc>(
            &mut setup.socket,
            &target.organization_id,
            end(session.clone()),
        )
        .await?,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::CentralFault
        },
    );
    heartbeat(&mut setup.socket).await?;
    assert_eq!(
        session_snapshot(fixture, session.as_str(), &setup.approval).await?,
        before
    );

    let retried = session_answer::<EndFleetSessionRpc>(
        session_call::<EndFleetSessionRpc>(
            &mut setup.socket,
            &target.organization_id,
            end(session.clone()),
        )
        .await?,
        &target.organization_id,
    )?;
    assert!(matches!(
        retried,
        EndFleetSessionReply::Settled {
            settlement: NewEndSettlement::Ended { .. }
        }
    ));
    let (_, ending) = next_ending(&mut setup.socket, target).await?;
    assert_eq!(ending.request_id(), setup.approval.request_id());
    assert_eq!(
        ending.into_ending(),
        Ending::Cancelled {
            reason: CancelReason::SessionEnded {}
        }
    );
    let after = session_snapshot(fixture, session.as_str(), &setup.approval).await?;
    assert_ne!(after, before);
    assert_eq!(
        read_request(fixture, &setup.approval).await?,
        [text("ended"), text("owed")]
    );
    Ok(())
}

pub async fn enrollment_and_approval_rollback(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let before = enrollment_snapshot(fixture, &setup).await?;
    assert_eq!(
        read_request(fixture, &setup.approval).await?,
        [text("waiting"), SqlValue::Null]
    );

    fixture.fail_next_applying_write().await?;
    let failure = match fixture
        .end_enrollment_through_login(EnrollmentLoginEnd::Revoke)
        .await
    {
        Err(failure) => failure,
        Ok(()) => return Err("post-write fault did not reject the enrollment apply".into()),
    };
    assert!(
        matches!(
            failure.downcast_ref::<central_storage::StoreFailure>(),
            Some(central_storage::StoreFailure::Unavailable),
        ),
        "unexpected fixture revoke refusal {failure:?}"
    );
    heartbeat(&mut setup.socket).await?;
    assert_eq!(enrollment_snapshot(fixture, &setup).await?, before);

    fixture
        .end_enrollment_through_login(EnrollmentLoginEnd::Revoke)
        .await?;
    let after = enrollment_snapshot(fixture, &setup).await?;
    assert_ne!(after, before);
    let enrollment = fixture
        .read(Statement {
            sql: "SELECT state FROM login_machine_approvals WHERE approval_id=?",
            args: vec![text(setup.target.enrollment_id.as_str())],
        })
        .await?;
    assert_eq!(enrollment.rows, [[text("ended")]]);
    let runs = fixture
        .read(Statement {
            sql: "SELECT retired_at FROM approval_router_runs WHERE router_run_id=?",
            args: vec![text(setup.target.router_run.as_str())],
        })
        .await?;
    assert!(matches!(runs.rows.as_slice(), [row] if matches!(row.as_slice(), [SqlValue::Text(_)])));
    assert_eq!(
        read_request(fixture, &setup.approval).await?,
        [text("ended"), text("closed_without_receipt")]
    );
    let serialized = fixture
        .read(Statement {
            sql: "SELECT ending_json FROM approval_requests WHERE request_id=?",
            args: vec![text(setup.approval.request_id().as_str())],
        })
        .await?;
    let [row] = serialized.rows.as_slice() else {
        return Err("missing settled approval".into());
    };
    let [SqlValue::Text(ending)] = row.as_slice() else {
        return Err("missing stored ending".into());
    };
    let ending: approval_request_protocol::DeliverEndingRequest = serde_json::from_str(ending)?;
    assert_eq!(
        ending.into_ending(),
        Ending::Cancelled {
            reason: CancelReason::EnrollmentEnded {}
        }
    );
    Ok(())
}

async fn session_snapshot(
    fixture: &impl ApprovalLinkFixture,
    session: &str,
    approval: &Raise,
) -> Result<Vec<Vec<SqlValue>>> {
    let result = fixture.read(Statement {
        sql: "SELECT s.session_state,s.settled_at,s.updated_at,a.record_state,a.ending_json,a.delivery_state,a.updated_at,(SELECT COUNT(*) FROM engine_due_entries d WHERE d.work_kind='approval_request' AND d.subject_key=a.approval_row_id) AS pending_due FROM actor_sessions s,approval_requests a WHERE s.actor_id=? AND a.request_id=?",
        args: vec![text(session), text(approval.request_id().as_str())],
    }).await?;
    if result.rows.len() != 1 {
        return Err("missing session or approval proof row".into());
    }
    Ok(result.rows)
}

async fn enrollment_snapshot(
    fixture: &impl ApprovalLinkFixture,
    setup: &ParentSetup,
) -> Result<Vec<Vec<SqlValue>>> {
    let rows = fixture.read(Statement {
        sql: "SELECT m.state,m.end_reason,m.updated_at,r.retired_at,r.updated_at,a.record_state,a.ending_json,a.delivery_state,a.updated_at,(SELECT COUNT(*) FROM signin_change_records) AS signin_changes,(SELECT COUNT(*) FROM engine_due_entries d WHERE d.work_kind='approval_request' AND d.subject_key=a.approval_row_id) AS pending_due FROM login_machine_approvals m,approval_router_runs r,approval_requests a WHERE m.approval_id=? AND r.router_run_id=? AND a.request_id=?",
        args: vec![
            text(setup.target.enrollment_id.as_str()),
            text(setup.target.router_run.as_str()),
            text(setup.approval.request_id().as_str()),
        ],
    }).await?.rows;
    if rows.len() != 1 {
        return Err("missing enrollment, run, or approval proof row".into());
    }
    Ok(rows)
}
