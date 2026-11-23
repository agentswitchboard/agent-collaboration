use super::statement_observer::{ObservedRead, ObservedReads, StatementObserver};
use std::{
    ops::ControlFlow,
    path::{Path, PathBuf},
};

use central_engine_model::{Context, EngineTime, IdSource};
use fleet_actor_identity::{
    AuthenticatedSession, SessionAvailability, SessionCallOutcome, SessionLookupScope,
    SessionSecretSource, SessionService, SessionServiceError,
};
use fleet_contracts::{ClientId, MachineConnectionAttachment};
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    CreateSessionKind, EndFleetSession, EndFleetSessionReply, EnrollmentId, HarnessKind,
    OrganizationId, PresentedSessionSecret, SessionCreator, SessionSecret, UnixSeconds,
};
use fleet_lent_transaction::{CentralTransaction, OrganizationTransaction};
use fleet_machine_login::link_authority::{ValidatedMachineConnection, restore_fixture_link};
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

struct SqliteTransaction<'a> {
    connection: &'a mut SqliteConnection,
    organization: &'a OrganizationId,
    reads: std::sync::Arc<ObservedReads>,
}

impl OrganizationTransaction for SqliteTransaction<'_> {
    fn organization_id(&self) -> &OrganizationId {
        self.organization
    }
}
impl CentralTransaction for SqliteTransaction<'_> {
    type Database = Sqlite;
    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = Sqlite> {
        StatementObserver {
            connection: self.connection,
            reads: self.reads.clone(),
        }
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
    organization: OrganizationId,
    pub caller: SessionLookupScope,
    ids: HeldIds,
    secrets: HeldSecrets,
    reads: std::sync::Arc<ObservedReads>,
}

impl SessionStoreFixture {
    pub async fn new() -> Self {
        Self::for_organization(id(700).try_into().unwrap()).await
    }

    pub async fn for_organization(organization: OrganizationId) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root
            .path()
            .join(format!("{}.sqlite", organization.as_str()));
        let mut connection = Self::open_path(&path).await;
        fleet_storage_schema::workspace_migrations::central_migrator()
            .run(&mut connection)
            .await
            .unwrap();
        let caller = SessionLookupScope {
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

        sqlx::query("INSERT INTO directory_workspaces (workspace_id,name,state,created_at,updated_at) VALUES ('019a3dce-2000-7000-8000-000000000004','Session workspace','active',?,?)")
        .bind(FIXED_TIME).bind(FIXED_TIME).execute(&mut connection).await.unwrap();
        sqlx::query("INSERT INTO login_machine_allowances (allowance_id,machine_id,workspace_id,state,change_request_id,change_origin,created_at,updated_at) VALUES ('019a3dce-2000-7000-8000-000000000007',?,'019a3dce-2000-7000-8000-000000000004','active','019a3dce-2000-7000-8000-000000000007','automatic',0,0)")
        .bind(caller.machine_id.as_str()).execute(&mut connection).await.unwrap();
        connection.close().await.unwrap();
        Self {
            _root: root,
            path,
            organization,
            caller,
            ids: HeldIds(100),
            reads: std::sync::Arc::default(),
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

    pub fn dispatched_reads(&self) -> Vec<ObservedRead> {
        std::mem::take(&mut *self.reads.lock().unwrap())
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
        self.apply_with_connection(operation, at, None).await
    }

    pub async fn capture_connection(&self, at: i64) -> ValidatedMachineConnection {
        let attachment = self.attachment(at);
        let store = self.open_tokio_store().await;
        let result = store
            .run_transaction(async |connection| {
                let mut lender = SqliteTransaction {
                    connection,
                    organization: &self.organization,
                    reads: self.reads.clone(),
                };
                let authority = restore_fixture_link(
                    &mut lender,
                    &attachment,
                    &self.organization,
                    time(at).timestamp_millis(),
                )
                .await?;
                Ok(ControlFlow::<std::convert::Infallible, _>::Continue(
                    authority,
                ))
            })
            .await;
        store.close().await;
        match result.unwrap() {
            ControlFlow::Continue(Ok(authority)) => authority,
            ControlFlow::Continue(Err(_)) => panic!("fixture link admission was refused"),
            ControlFlow::Break(never) => match never {},
        }
    }

    fn attachment(&self, at: i64) -> MachineConnectionAttachment {
        MachineConnectionAttachment::from_token_claims(
            self.organization.clone(),
            self.caller.machine_id.clone(),
            self.caller.enrollment_id.clone(),
            ClientId::try_from(self.caller.enrollment_id.as_str().to_owned()).unwrap(),
            UnixSeconds(time(at).timestamp() + 60),
        )
    }

    pub async fn apply_with_connection(
        &mut self,
        operation: Operation,
        at: i64,
        admitted: Option<&ValidatedMachineConnection>,
    ) -> Result<Receipt, SessionServiceError> {
        let store = self.open_tokio_store().await;
        let result = store
            .run_transaction(async |connection| {
                let mut lent = SqliteTransaction {
                    connection,
                    organization: &self.organization,
                    reads: self.reads.clone(),
                };
                let admitted = match admitted {
                    Some(connection) => Ok(connection.clone()),
                    None => {
                        restore_fixture_link(
                            &mut lent,
                            &self.attachment(at),
                            &self.organization,
                            time(at).timestamp_millis(),
                        )
                        .await?
                    }
                };
                let Ok(admitted) = admitted else {
                    return Ok(ControlFlow::Continue(match operation {
                        Operation::Create(_) => Receipt::Create(SessionCallOutcome::Revoked),
                        Operation::Bind(_) => Receipt::Bind(SessionCallOutcome::Revoked),
                        Operation::End(_) => Receipt::End(SessionCallOutcome::Revoked),
                    }));
                };
                let mut service = SessionService::in_transaction(&mut lent);
                let mut context = Context {
                    now: time(at),
                    ids: &mut self.ids,
                };
                let result = match operation {
                    Operation::Create(request) => {
                        let project_lookup = match &request.project {
                            fleet_identity_protocol::StartProject::Named { project_ref } => {
                                fleet_runtime_ports::SessionProjectLookup::Named {
                                    workspace: project_ref.workspace.clone(),
                                    project: project_ref.project.clone(),
                                    result: Ok(true),
                                }
                            }
                            fleet_identity_protocol::StartProject::SameAsParent {} => {
                                fleet_runtime_ports::SessionProjectLookup::NotRead
                            }
                        };
                        service
                            .create(
                                &admitted,
                                request,
                                project_lookup,
                                &mut context,
                                &mut self.secrets,
                            )
                            .await
                            .map(Receipt::Create)
                    }
                    Operation::Bind(request) => service
                        .bind(&admitted, request, &mut context)
                        .await
                        .map(Receipt::Bind),
                    Operation::End(request) => service
                        .end(&admitted, request, &mut context)
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
                let mut lent = SqliteTransaction {
                    connection,
                    organization: &self.organization,
                    reads: self.reads.clone(),
                };
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

    pub async fn checked_facts(
        &self,
        session_id: &workspace_ids::CentralSessionId,
    ) -> Result<(bool, SessionAvailability), central_storage::StoreFailure> {
        let store = self.open_tokio_store().await;
        let result = store
            .run_transaction(async |connection| {
                let mut lent = SqliteTransaction {
                    connection,
                    organization: &self.organization,
                    reads: self.reads.clone(),
                };
                let mut service = SessionService::in_transaction(&mut lent);
                let live = service.session_is_live(&self.caller, session_id).await?;
                let facts = service.session_facts(&self.caller, session_id).await?;
                Ok(ControlFlow::<std::convert::Infallible, _>::Continue((
                    live, facts,
                )))
            })
            .await;
        store.close().await;
        let ControlFlow::Continue(facts) = result?;
        Ok(facts)
    }

    pub async fn authenticate(
        &self,
        connection: &ValidatedMachineConnection,
        session_id: &workspace_ids::CentralSessionId,
        presented: &PresentedSessionSecret,
    ) -> Option<AuthenticatedSession> {
        let store = self.open_tokio_store().await;
        let result = store
            .run_transaction(async |executor| {
                let mut lent = SqliteTransaction {
                    connection: executor,
                    organization: &self.organization,
                    reads: self.reads.clone(),
                };
                let admitted = SessionService::in_transaction(&mut lent)
                    .authenticate_live_session(connection, session_id, presented)
                    .await?;
                Ok(ControlFlow::<std::convert::Infallible, _>::Continue(
                    admitted,
                ))
            })
            .await;
        store.close().await;
        let ControlFlow::Continue(admitted) = result.unwrap();
        admitted
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
