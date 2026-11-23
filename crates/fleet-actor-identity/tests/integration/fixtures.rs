use std::{
    ops::ControlFlow,
    path::{Path, PathBuf},
};

use central_engine_model::{Context, EngineTime, IdSource};
use fleet_actor_identity::{
    ActorApprovalSessionFacts, SessionAvailability, SessionCallOutcome, SessionMachineCaller,
    SessionSecretSource, SessionService, SessionServiceError,
};
use fleet_approvals::{ApprovalEnrollment, ApprovalRouting, ApprovalSession, ApprovalSessionFacts};
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    CreateSessionKind, EndFleetSession, EndFleetSessionReply, EnrollmentId, HarnessKind,
    OrganizationId, SessionCreator, SessionSecret,
};
use fleet_lent_transaction::CentralTransaction;
use fleet_store_sqlx::SqliteObjectStore;
use sqlx::{Connection, Sqlite, SqliteConnection, sqlite::SqliteConnectOptions};

pub const FIXED_TIME: &str = "2026-11-22T10:00:00.000Z";

pub fn id(number: u16) -> String {
    format!("019ac1c0-0000-7000-8000-00000000{number:04x}")
}

pub struct HeldIds(u16);
impl IdSource for HeldIds {
    fn next_id(&mut self) -> uuid::Uuid {
        self.0 += 1;
        id(self.0).parse().unwrap()
    }
}

struct HeldSecrets {
    seed: u8,
    available: bool,
}
impl SessionSecretSource for HeldSecrets {
    fn new_session_secret(&mut self) -> Result<SessionSecret, SessionServiceError> {
        if !self.available {
            return Err(SessionServiceError::SecretUnavailable);
        }
        self.seed += 1;
        Ok(secret(self.seed))
    }
}

pub fn secret(seed: u8) -> SessionSecret {
    SessionSecret::from_bytes(&[seed; 32])
}

pub fn time(offset_millis: i64) -> EngineTime {
    let base = FIXED_TIME.parse::<EngineTime>().unwrap().timestamp_millis();
    EngineTime::from_timestamp_millis(base + offset_millis).unwrap()
}

struct SqliteTransaction<'a>(&'a mut SqliteConnection);
impl CentralTransaction for SqliteTransaction<'_> {
    type Database = Sqlite;
    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = Sqlite> {
        &mut *self.0
    }
    fn rows_affected(result: <Sqlite as sqlx::Database>::QueryResult) -> u64 {
        result.rows_affected()
    }
}

pub enum Operation {
    Create(CreateFleetSession),
    Bind(BindFleetSession),
    End(EndFleetSession),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Receipt {
    Create(SessionCallOutcome<CreateFleetSessionReply>),
    Bind(SessionCallOutcome<BindFleetSessionReply>),
    End(SessionCallOutcome<EndFleetSessionReply>),
}

pub struct SessionStoreFixture {
    _root: tempfile::TempDir,
    path: PathBuf,
    pub caller: SessionMachineCaller,
    ids: HeldIds,
    secrets: HeldSecrets,
}

impl SessionStoreFixture {
    pub async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("actor-identity.sqlite");
        let mut connection = Self::open_path(&path).await;
        fleet_storage_schema::workspace_migrations::central_migrator()
            .run(&mut connection)
            .await
            .unwrap();
        let caller = SessionMachineCaller {
            machine_id: id(1).parse().unwrap(),
            enrollment_id: EnrollmentId::try_from(id(2)).unwrap(),
        };
        sqlx::query("INSERT INTO login_machines (machine_id,label,state,enrollment_generation,created_at,updated_at) VALUES (?,'Test machine','enrolled',1,?,?)")
            .bind(caller.machine_id.as_str()).bind(FIXED_TIME).bind(FIXED_TIME)
            .execute(&mut connection).await.unwrap();
        sqlx::query("INSERT INTO login_machine_approvals (approval_id,request_id,machine_id,label,allowed_workspaces,state,approved_by_person_id,permission_checked,base_generation,code_expires_at,machine_client_id,last_grant_at,created_at,updated_at) VALUES (?,?,?,'Test machine','[]','enrolled',?,'machine:enroll',0,?,?,?, ?,?)")
            .bind(caller.enrollment_id.as_str()).bind(id(3)).bind(caller.machine_id.as_str())
            .bind(id(4)).bind(FIXED_TIME).bind(caller.enrollment_id.as_str()).bind(FIXED_TIME)
            .bind(FIXED_TIME).bind(FIXED_TIME).execute(&mut connection).await.unwrap();
        connection.close().await.unwrap();
        Self {
            _root: root,
            path,
            caller,
            ids: HeldIds(100),
            secrets: HeldSecrets {
                seed: 0,
                available: true,
            },
        }
    }

    async fn open_path(path: &Path) -> SqliteConnection {
        SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
                .foreign_keys(true),
        )
        .await
        .unwrap()
    }

    pub fn refuse_future_secrets(&mut self) {
        self.secrets.available = false;
    }

    pub fn database_path(&self) -> &Path {
        &self.path
    }

    pub async fn open(&self) -> SqliteConnection {
        Self::open_path(&self.path).await
    }

    pub async fn apply(
        &mut self,
        operation: Operation,
        at: i64,
    ) -> Result<Receipt, SessionServiceError> {
        let store = self.open_tokio_store().await;
        let result = store
            .run_transaction(async |connection| {
                let mut lent = SqliteTransaction(connection);
                let mut service = SessionService::in_transaction(&mut lent);
                let mut context = Context {
                    now: time(at),
                    ids: &mut self.ids,
                };
                let result = match operation {
                    Operation::Create(request) => service
                        .create(&self.caller, request, &mut context, &mut self.secrets)
                        .await
                        .map(Receipt::Create),
                    Operation::Bind(request) => service
                        .bind(&self.caller, request, &mut context)
                        .await
                        .map(Receipt::Bind),
                    Operation::End(request) => service
                        .end(&self.caller, request, &mut context)
                        .await
                        .map(Receipt::End),
                };
                Ok(match result {
                    Ok(receipt) => ControlFlow::Continue(receipt),
                    Err(failure) => ControlFlow::Break(failure),
                })
            })
            .await;
        store.close().await;
        match result? {
            ControlFlow::Continue(receipt) => Ok(receipt),
            ControlFlow::Break(failure) => Err(failure),
        }
    }

    pub async fn facts(&self, session_id: &workspace_ids::CentralSessionId) -> SessionAvailability {
        let store = self.open_tokio_store().await;
        let result = store
            .run_transaction(async |connection| {
                let mut lent = SqliteTransaction(connection);
                let facts = SessionService::in_transaction(&mut lent)
                    .session_facts(&self.caller, session_id)
                    .await?;
                Ok(ControlFlow::<std::convert::Infallible, _>::Continue(facts))
            })
            .await;
        store.close().await;
        match result.unwrap() {
            ControlFlow::Continue(facts) => facts,
            ControlFlow::Break(never) => match never {},
        }
    }

    pub async fn approval_facts(
        &self,
        session_id: &workspace_ids::CentralSessionId,
        store_organization: &OrganizationId,
        requested_organization: &OrganizationId,
    ) -> (bool, Option<ApprovalRouting>) {
        let reader = ActorApprovalSessionFacts::for_organization(store_organization.clone());
        let subject = ApprovalSession {
            enrollment: ApprovalEnrollment {
                machine_id: self.caller.machine_id.clone(),
                enrollment_id: self.caller.enrollment_id.clone(),
            },
            session_id: session_id.clone(),
        };
        let store = self.open_tokio_store().await;
        let result = store
            .run_transaction(async |connection| {
                let mut lent = SqliteTransaction(connection);
                let live = reader
                    .session_is_live(&mut lent, requested_organization, &subject)
                    .await?;
                let route = reader
                    .session_initial_route(&mut lent, requested_organization, &subject)
                    .await?;
                Ok(ControlFlow::<std::convert::Infallible, _>::Continue((
                    live, route,
                )))
            })
            .await;
        store.close().await;
        match result.unwrap() {
            ControlFlow::Continue(facts) => facts,
            ControlFlow::Break(never) => match never {},
        }
    }

    async fn open_tokio_store(&self) -> SqliteObjectStore {
        SqliteObjectStore::open_and_migrate(
            &self.path,
            &fleet_storage_schema::workspace_migrations::central_migrator(),
        )
        .await
        .unwrap()
    }

    pub async fn count(&self, sql: &'static str) -> i64 {
        let mut connection = self.open().await;
        let count = sqlx::query_scalar(sql)
            .fetch_one(&mut connection)
            .await
            .unwrap();
        connection.close().await.unwrap();
        count
    }
}

pub fn create(operation: u16, attempt: u64) -> CreateFleetSession {
    CreateFleetSession {
        operation_id: id(operation).parse().unwrap(),
        attempt: attempt.try_into().unwrap(),
        creator: SessionCreator::RouterOperator {},
        agent_program: HarnessKind::Codex,
        session_kind: CreateSessionKind::Ordinary {},
    }
}

pub fn reservation(receipt: Receipt) -> (workspace_ids::CentralSessionId, SessionSecret) {
    let Receipt::Create(SessionCallOutcome::Answered(CreateFleetSessionReply::Reserved {
        session_id,
        secret,
        ..
    })) = receipt
    else {
        panic!("expected a reservation, got {receipt:?}");
    };
    (session_id, secret)
}
