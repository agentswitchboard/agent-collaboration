use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};

use central_engine_model::{
    AlarmInputs, DueBatchPolicy, DueListKind, DueRow, EngineTime, IdSource, due_list_query,
};
use central_storage::{ReadOutcome, SafeInteger, SqlValue, Statement, StoreFailure};
use fleet_approvals::{
    ApprovalActor, ApprovalApplyFailure, ApprovalApplyingStore, ApprovalCommitOwner,
    ApprovalCommitProof, ApprovalCommitScope, ApprovalEnrollment, ApprovalEnrollmentScope,
    ApprovalEnrollmentStanding, ApprovalHolderAdmission, ApprovalHolderScope, ApprovalRouting,
    ApprovalSession, ApprovalSessionFacts, ApprovalStoreTransaction, PendingApprovalHolderEffects,
    PendingApprovalReply, complete_approval_commit,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_lent_transaction::{CentralTransaction, OrganizationTransaction};
use fleet_runtime_ports::{Clock, SyncTime, workspace_decode_error::corrupt};
use sqlx::{
    Column, Row, Sqlite, SqlitePool, Transaction, ValueRef,
    query::Query,
    sqlite::{SqliteArguments, SqliteConnectOptions, SqlitePoolOptions},
};
use workspace_ids::ActorIdentityId;

#[path = "native_approval_store_commit.rs"]
mod native_approval_store_commit;
use native_approval_store_commit::NativeApprovalCommit;
pub use native_approval_store_commit::{FixtureCommittedApply, FixtureHolderOffer};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(1);

pub struct StoreClock(AtomicI64);

impl StoreClock {
    pub fn set_millis(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

impl Clock for StoreClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.0.load(Ordering::Relaxed))
    }
}

struct StoreFixtureIds<'fixture>(&'fixture AtomicU64);

impl IdSource for StoreFixtureIds<'_> {
    fn next_id(&mut self) -> uuid::Uuid {
        let number = self.0.fetch_add(1, Ordering::Relaxed);
        uuid::Uuid::from_u128(0x019a_c19f_aa00_7000_8000_0000_0000_0000 + u128::from(number))
    }
}

#[derive(Debug, Default)]
pub struct ApprovalTransactionEvidence {
    pub sql_reads: Vec<&'static str>,
    pub sql_writes: Vec<(&'static str, u64)>,
    pub session_route_reads: Vec<ApprovalSession>,
    pub session_liveness_reads: Vec<ApprovalSession>,
}

pub struct NativeApprovalTransaction {
    organization_id: OrganizationId,
    transaction: Transaction<'static, Sqlite>,
    fail_after_write: Option<usize>,
    evidence: ApprovalTransactionEvidence,
}

impl OrganizationTransaction for NativeApprovalTransaction {
    fn organization_id(&self) -> &OrganizationId {
        &self.organization_id
    }
}

fn fixture_organization() -> OrganizationId {
    OrganizationId::try_from("019ac19f-aa00-7000-8000-00000000a100".to_owned()).unwrap()
}

impl CentralTransaction for NativeApprovalTransaction {
    type Database = Sqlite;

    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = Self::Database> {
        &mut *self.transaction
    }

    fn rows_affected(result: <Self::Database as sqlx::Database>::QueryResult) -> u64 {
        result.rows_affected()
    }
}

impl ApprovalStoreTransaction for NativeApprovalTransaction {
    async fn approval_decision_caller(
        &mut self,
        _organization: &OrganizationId,
        actor: &ApprovalActor,
    ) -> Result<ActorIdentityId, StoreFailure> {
        // The fixture uses disjoint issued person and session identities.
        let fixture_id = match actor {
            ApprovalActor::Person(person) => person.as_str(),
            ApprovalActor::Session(session) => session.session_id.as_str(),
        };
        fixture_id
            .parse()
            .map_err(|_| corrupt("fixture_actor_identity"))
    }

    async fn read_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<ReadOutcome, StoreFailure> {
        self.evidence.sql_reads.push(statement.sql);
        let query = bind_statement(statement);
        let rows = query
            .fetch_all(&mut *self.transaction)
            .await
            .map_err(sql_failure)?;
        let columns = rows.first().map_or_else(Vec::new, |row| {
            row.columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect()
        });
        let rows = rows
            .iter()
            .map(|row| {
                row.columns()
                    .iter()
                    .map(|column| {
                        let name = column.name();
                        let raw = row.try_get_raw(name).map_err(|_| corrupt(name))?;
                        if raw.is_null() {
                            Ok(SqlValue::Null)
                        } else if let Ok(number) = row.try_get::<i64, _>(name) {
                            SafeInteger::new(number)
                                .map(SqlValue::Integer)
                                .map_err(|_| corrupt(name))
                        } else {
                            row.try_get::<String, _>(name)
                                .map(SqlValue::Text)
                                .map_err(|_| corrupt(name))
                        }
                    })
                    .collect()
            })
            .collect::<Result<Vec<Vec<_>>, _>>()?;
        Ok(ReadOutcome { columns, rows })
    }

    async fn execute_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<u64, StoreFailure> {
        let statement_text = statement.sql;
        let rows = bind_statement(statement)
            .execute(&mut *self.transaction)
            .await
            .map_err(sql_failure)?
            .rows_affected();
        self.evidence.sql_writes.push((statement_text, rows));
        if Some(self.evidence.sql_writes.len()) == self.fail_after_write {
            return Err(StoreFailure::Unavailable);
        }
        Ok(rows)
    }

    async fn approval_enrollment_standing(
        &mut self,
        scope: &ApprovalEnrollmentScope,
    ) -> Result<ApprovalEnrollmentStanding, StoreFailure> {
        let standing: Option<String> = sqlx::query_scalar(
            "SELECT standing FROM fixture_enrollments
             WHERE organization_id=?1 AND machine_id=?2 AND enrollment_id=?3",
        )
        .bind(scope.organization_id.as_str())
        .bind(scope.enrollment.machine_id.as_str())
        .bind(scope.enrollment.enrollment_id.as_str())
        .fetch_optional(&mut *self.transaction)
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

pub struct StoredSessionFacts;

#[derive(sqlx::FromRow)]
struct StoredSessionRoute {
    parent_machine: Option<String>,
    parent_enrollment: Option<String>,
    parent_session: Option<String>,
}

impl ApprovalSessionFacts<NativeApprovalTransaction> for StoredSessionFacts {
    async fn session_initial_route(
        &self,
        transaction: &mut NativeApprovalTransaction,
        requester: &ApprovalSession,
    ) -> Result<Option<ApprovalRouting>, StoreFailure> {
        transaction
            .evidence
            .session_route_reads
            .push(requester.clone());
        let row = sqlx::query_as::<_, StoredSessionRoute>(
            "SELECT parent_machine, parent_enrollment, parent_session FROM fixture_sessions
             WHERE organization_id=?1 AND machine_id=?2 AND enrollment_id=?3 AND session_id=?4",
        )
        .bind(transaction.organization_id().as_str().to_owned())
        .bind(requester.enrollment.machine_id.as_str())
        .bind(requester.enrollment.enrollment_id.as_str())
        .bind(requester.session_id.as_str())
        .fetch_optional(&mut *transaction.transaction)
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
        transaction: &mut NativeApprovalTransaction,
        session: &ApprovalSession,
    ) -> Result<bool, StoreFailure> {
        transaction
            .evidence
            .session_liveness_reads
            .push(session.clone());
        sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM fixture_sessions
             WHERE organization_id=?1 AND machine_id=?2 AND enrollment_id=?3 AND session_id=?4 AND live=1)",
        )
        .bind(transaction.organization_id().as_str().to_owned())
        .bind(session.enrollment.machine_id.as_str())
        .bind(session.enrollment.enrollment_id.as_str())
        .bind(session.session_id.as_str())
        .fetch_one(&mut *transaction.transaction)
        .await
        .map(|live| live == 1)
        .map_err(sql_failure)
    }
}

pub struct NativeApprovalStoreFixture {
    workspace: PathBuf,
    pub clock: StoreClock,
    ids: AtomicU64,
}

impl NativeApprovalStoreFixture {
    pub async fn open_fresh() -> Self {
        let workspace = std::env::temp_dir().join(format!(
            "fleet-approvals-proof-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&workspace).unwrap();
        let fixture = Self {
            workspace,
            clock: StoreClock(AtomicI64::new(0)),
            ids: AtomicU64::new(1_000_000),
        };
        let pool = fixture.reopen_pool().await;
        fleet_storage_schema::workspace_migrations::central_migrator()
            .run(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../fixtures/approval_authority_records.sql"))
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        fixture
    }

    async fn reopen_pool(&self) -> SqlitePool {
        let options = SqliteConnectOptions::new()
            .filename(self.workspace.join("approvals.sqlite"))
            .create_if_missing(true)
            .foreign_keys(true);
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap()
    }

    pub async fn apply<Reply>(
        &self,
        fail_after_write: Option<usize>,
        run_command: impl AsyncFnOnce(
            &mut ApprovalApplyingStore<'_, NativeApprovalTransaction, StoredSessionFacts>,
        )
            -> Result<PendingApprovalReply<Reply>, ApprovalApplyFailure>,
    ) -> (
        Result<FixtureCommittedApply<Reply>, ApprovalApplyFailure>,
        ApprovalTransactionEvidence,
    ) {
        let (committed, evidence) = self.commit_pending(fail_after_write, run_command).await;
        let result = committed.map(|(proof, (reply, holder_effects))| FixtureCommittedApply {
            reply: reply.take_fixture_reply(&proof).unwrap(),
            ending_offers: holder_effects
                .release(&proof)
                .unwrap()
                .into_iter()
                .map(|offer| {
                    let holder = ApprovalHolderAdmission::for_fixture_scope(offer.holder_address());
                    FixtureHolderOffer {
                        request: offer.take_for_holder(&holder).unwrap(),
                    }
                })
                .collect(),
        });
        (result, evidence)
    }

    pub async fn commit_pending<Reply>(
        &self,
        fail_after_write: Option<usize>,
        run_command: impl AsyncFnOnce(
            &mut ApprovalApplyingStore<'_, NativeApprovalTransaction, StoredSessionFacts>,
        )
            -> Result<PendingApprovalReply<Reply>, ApprovalApplyFailure>,
    ) -> (
        Result<
            (
                ApprovalCommitProof,
                (PendingApprovalReply<Reply>, PendingApprovalHolderEffects),
            ),
            ApprovalApplyFailure,
        >,
        ApprovalTransactionEvidence,
    ) {
        let mut evidence = ApprovalTransactionEvidence::default();
        let committed = complete_approval_commit(NativeApprovalCommit {
            fixture: self,
            fail_after_write,
            run_command,
            evidence: &mut evidence,
        })
        .await;

        (committed, evidence)
    }

    pub async fn store_enrollment(&self, holder: &ApprovalHolderScope, standing: &str) {
        let pool = self.reopen_pool().await;
        sqlx::query(
            "INSERT INTO fixture_enrollments VALUES (?1,?2,?3,?4)
             ON CONFLICT (organization_id,machine_id,enrollment_id) DO UPDATE SET standing=excluded.standing",
        )
        .bind(holder.organization_id.as_str())
        .bind(holder.run.enrollment.machine_id.as_str())
        .bind(holder.run.enrollment.enrollment_id.as_str())
        .bind(standing)
        .execute(&pool).await.unwrap();
        pool.close().await;
    }

    pub async fn store_session(
        &self,
        organization: &OrganizationId,
        session: &ApprovalSession,
        parent: Option<&ApprovalSession>,
        live: bool,
    ) {
        let pool = self.reopen_pool().await;
        sqlx::query(
            "INSERT INTO fixture_sessions VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
             ON CONFLICT (organization_id,machine_id,enrollment_id,session_id)
             DO UPDATE SET live=excluded.live, parent_machine=excluded.parent_machine,
                 parent_enrollment=excluded.parent_enrollment, parent_session=excluded.parent_session",
        )
        .bind(organization.as_str())
        .bind(session.enrollment.machine_id.as_str())
        .bind(session.enrollment.enrollment_id.as_str())
        .bind(session.session_id.as_str())
        .bind(live)
        .bind(parent.map(|value| value.enrollment.machine_id.as_str()))
        .bind(parent.map(|value| value.enrollment.enrollment_id.as_str()))
        .bind(parent.map(|value| value.session_id.as_str()))
        .execute(&pool).await.unwrap();
        pool.close().await;
    }

    pub async fn due_rows(&self, cutoff: EngineTime, cap: usize) -> Vec<DueRow> {
        let pool = self.reopen_pool().await;
        let mut transaction = NativeApprovalTransaction {
            organization_id: fixture_organization(),

            transaction: pool.begin().await.unwrap(),
            fail_after_write: None,
            evidence: ApprovalTransactionEvidence::default(),
        };
        let bounds = due_list_query(DueListKind::ApprovalExpiries, cutoff, cap).unwrap();
        let result = fleet_engine::read_timed_due_rows(&mut transaction, bounds)
            .await
            .unwrap();
        transaction.transaction.rollback().await.unwrap();
        pool.close().await;
        result
    }

    pub async fn compare_due_table(
        &self,
        now: EngineTime,
        cutoff: EngineTime,
    ) -> Vec<fleet_engine::DueTableDifference> {
        let pool = self.reopen_pool().await;
        let mut transaction = NativeApprovalTransaction {
            organization_id: fixture_organization(),

            transaction: pool.begin().await.unwrap(),
            fail_after_write: None,
            evidence: ApprovalTransactionEvidence::default(),
        };
        let inputs = AlarmInputs {
            now,
            last_batch_start: None,
            leftovers_beyond_cap: false,
            earliest_on_grid_due: None,
            earliest_legacy_due: None,
        };
        let policy = DueBatchPolicy::DEFAULT;
        let differences = fleet_engine::compare_due_table(
            &mut transaction,
            cutoff,
            policy.per_kind_cap,
            inputs,
            &policy,
        )
        .await
        .unwrap();
        transaction.transaction.rollback().await.unwrap();
        pool.close().await;
        differences
    }

    pub async fn seed_due_population(&self, waiting: i64) {
        let pool = self.reopen_pool().await;
        let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query(include_str!("../fixtures/approval_noise_requests.sql"))
            .bind(waiting)
            .execute(&mut *transaction)
            .await
            .unwrap();
        sqlx::query(include_str!("../fixtures/approval_noise_due_entries.sql"))
            .execute(&mut *transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        pool.close().await;
    }

    pub async fn due_rows_with_progress(&self, cutoff: EngineTime) -> (Vec<DueRow>, u64) {
        let pool = self.reopen_pool().await;
        let mut transaction = NativeApprovalTransaction {
            organization_id: fixture_organization(),

            transaction: pool.begin().await.unwrap(),
            fail_after_write: None,
            evidence: ApprovalTransactionEvidence::default(),
        };
        let bounds = due_list_query(DueListKind::ApprovalExpiries, cutoff, 1).unwrap();
        let warmed = fleet_engine::read_timed_due_rows(&mut transaction, bounds)
            .await
            .unwrap();
        let steps = Arc::new(AtomicU64::new(0));
        let observed = Arc::clone(&steps);
        transaction
            .transaction
            .lock_handle()
            .await
            .unwrap()
            .set_progress_handler(1, move || {
                observed.fetch_add(1, Ordering::Relaxed);
                true
            });
        let result = fleet_engine::read_timed_due_rows(&mut transaction, bounds)
            .await
            .unwrap();
        assert_eq!(result, warmed);
        transaction
            .transaction
            .lock_handle()
            .await
            .unwrap()
            .remove_progress_handler();
        transaction.transaction.rollback().await.unwrap();
        pool.close().await;
        (result, steps.load(Ordering::Relaxed))
    }

    pub async fn inspect_rows(&self, query: &'static str) -> Vec<String> {
        let pool = self.reopen_pool().await;
        let result = sqlx::query_scalar(query).fetch_all(&pool).await.unwrap();
        pool.close().await;
        result
    }

    pub async fn alter_fixture(&self, sql: &'static str) {
        let pool = self.reopen_pool().await;
        sqlx::raw_sql(sql).execute(&pool).await.unwrap();
        pool.close().await;
    }
}

impl Drop for NativeApprovalStoreFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.workspace);
    }
}

fn sql_failure(error: sqlx::Error) -> StoreFailure {
    eprintln!("approval test statement rejected: {error}");
    StoreFailure::Unavailable
}

fn bind_statement(statement: Statement) -> Query<'static, Sqlite, SqliteArguments> {
    let mut query = sqlx::query(statement.sql);
    for value in statement.args {
        query = match value {
            SqlValue::Null => query.bind(None::<String>),
            SqlValue::Text(value) => query.bind(value),
            SqlValue::Integer(value) => query.bind(value.get()),
            SqlValue::Blob(value) => query.bind(value),
        };
    }
    query
}
