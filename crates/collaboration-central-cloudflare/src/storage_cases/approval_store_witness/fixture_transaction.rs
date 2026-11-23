use central_storage::{ReadOutcome, Statement, StoreFailure};
use fleet_approvals::{
    ApprovalEnrollment, ApprovalEnrollmentScope, ApprovalEnrollmentStanding, ApprovalRouting,
    ApprovalSession, ApprovalSessionFacts, ApprovalStoreTransaction,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_durable_object::{Do, DoTransaction};
use std::{cell::RefCell, rc::Rc};

use crate::{statement_store, store_wire::OwnedStatement};

#[derive(Default)]
pub(super) struct FixtureEvidence {
    pub write_count: usize,
    pub rows_changed: u64,
}

pub(super) struct WorkerApprovalTransaction {
    pub transaction: DoTransaction,
    pub fail_after_write: Option<usize>,
    pub evidence: Rc<RefCell<FixtureEvidence>>,
}

impl CentralTransaction for WorkerApprovalTransaction {
    type Database = Do;

    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = Do> {
        &self.transaction
    }

    fn rows_affected(result: <Do as sqlx::Database>::QueryResult) -> u64 {
        result.rows_affected()
    }
}

impl ApprovalStoreTransaction for WorkerApprovalTransaction {
    async fn read_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<ReadOutcome, StoreFailure> {
        let rows = statement_store::bound(&OwnedStatement::from(statement))
            .fetch_all(&self.transaction)
            .await
            .map_err(sql_failure)?;
        statement_store::read_outcome(&rows)
    }

    async fn execute_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<u64, StoreFailure> {
        let rows = statement_store::bound(&OwnedStatement::from(statement))
            .execute(&self.transaction)
            .await
            .map_err(sql_failure)?
            .rows_affected();
        let mut evidence = self.evidence.borrow_mut();
        evidence.write_count += 1;
        evidence.rows_changed += rows;
        if Some(evidence.write_count) == self.fail_after_write {
            return Err(StoreFailure::Unavailable);
        }
        Ok(rows)
    }

    async fn approval_enrollment_standing(
        &mut self,
        scope: &ApprovalEnrollmentScope,
    ) -> Result<ApprovalEnrollmentStanding, StoreFailure> {
        let standing = sqlx::query_scalar::<Do, String>(
            "SELECT standing FROM fixture_enrollments
             WHERE organization_id=?1 AND machine_id=?2 AND enrollment_id=?3",
        )
        .bind(scope.organization_id.as_str())
        .bind(scope.enrollment.machine_id.as_str())
        .bind(scope.enrollment.enrollment_id.as_str())
        .fetch_optional(&self.transaction)
        .await
        .map_err(sql_failure)?;
        match standing.as_deref() {
            Some("live") => Ok(ApprovalEnrollmentStanding::Live),
            Some("ended") => Ok(ApprovalEnrollmentStanding::Ended),
            None => Ok(ApprovalEnrollmentStanding::Unavailable),
            Some(_) => Err(corrupt("fixture_enrollments.standing")),
        }
    }
}

pub(super) struct FixtureSessionFacts;

#[derive(sqlx::FromRow)]
struct StoredSessionRoute {
    parent_machine: Option<String>,
    parent_enrollment: Option<String>,
    parent_session: Option<String>,
}

impl ApprovalSessionFacts<WorkerApprovalTransaction> for FixtureSessionFacts {
    async fn session_initial_route(
        &self,
        transaction: &mut WorkerApprovalTransaction,
        organization: &OrganizationId,
        requester: &ApprovalSession,
    ) -> Result<Option<ApprovalRouting>, StoreFailure> {
        let row = sqlx::query_as::<Do, StoredSessionRoute>(
            "SELECT parent_machine, parent_enrollment, parent_session FROM fixture_sessions
             WHERE organization_id=?1 AND machine_id=?2 AND enrollment_id=?3 AND session_id=?4",
        )
        .bind(organization.as_str())
        .bind(requester.enrollment.machine_id.as_str())
        .bind(requester.enrollment.enrollment_id.as_str())
        .bind(requester.session_id.as_str())
        .fetch_optional(&transaction.transaction)
        .await
        .map_err(sql_failure)?;
        row.map(|route| {
            match (
                route.parent_machine,
                route.parent_enrollment,
                route.parent_session,
            ) {
                (None, None, None) => Ok(ApprovalRouting::Owner),
                (Some(machine), Some(enrollment), Some(session)) => Ok(ApprovalRouting::Delegate {
                    admitted_parent: ApprovalSession {
                        enrollment: ApprovalEnrollment {
                            machine_id: machine
                                .parse()
                                .map_err(|_| corrupt("fixture_sessions.parent_machine"))?,
                            enrollment_id: EnrollmentId::try_from(enrollment)
                                .map_err(|_| corrupt("fixture_sessions.parent_enrollment"))?,
                        },
                        session_id: session
                            .parse()
                            .map_err(|_| corrupt("fixture_sessions.parent_session"))?,
                    },
                }),
                _ => Err(corrupt("fixture_sessions.parent_route")),
            }
        })
        .transpose()
    }

    async fn session_is_live(
        &self,
        transaction: &mut WorkerApprovalTransaction,
        organization: &OrganizationId,
        session: &ApprovalSession,
    ) -> Result<bool, StoreFailure> {
        sqlx::query_scalar::<Do, i64>(
            "SELECT EXISTS(SELECT 1 FROM fixture_sessions
             WHERE organization_id=?1 AND machine_id=?2 AND enrollment_id=?3 AND session_id=?4 AND live=1)",
        )
        .bind(organization.as_str())
        .bind(session.enrollment.machine_id.as_str())
        .bind(session.enrollment.enrollment_id.as_str())
        .bind(session.session_id.as_str())
        .fetch_one(&transaction.transaction).await
        .map(|live| live == 1).map_err(sql_failure)
    }
}

fn sql_failure(error: sqlx::Error) -> StoreFailure {
    worker::console_warn!("approvals fixture SQL: {error}");
    StoreFailure::Unavailable
}
