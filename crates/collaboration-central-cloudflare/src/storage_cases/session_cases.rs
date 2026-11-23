//! Test-only application and observation of the session service on Durable Object SQLite.

use central_engine_model::{Context, IdSource};
use central_link_call_composition::OrganizationLoan;
use chrono::{DateTime, SecondsFormat};
use fleet_actor_identity::{
    ActorApprovalSessionFacts, LiveSessionFacts, OrganizationTransaction, SessionAvailability,
    SessionCallOutcome, SessionLookupScope, SessionSecretSource, SessionService,
    SessionServiceError,
};
use fleet_approvals::{ApprovalEnrollment, ApprovalRouting, ApprovalSession, ApprovalSessionFacts};
use fleet_contracts::{ClientId, MachineConnectionAttachment};
use fleet_identity_protocol::{
    BindFleetSession, CreateFleetSession, EndFleetSession, EnrollmentId, OrganizationId,
    SessionSecret, UnixSeconds,
};
use fleet_machine_login::link_authority::{ValidatedMachineConnection, restore_fixture_link};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx_core::{query::query, query_scalar::query_scalar};
use sqlx_durable_object::{DoConnection, DoTransaction};
use std::sync::{Arc, Mutex};
use worker::{Response, Result, Storage};
use workspace_ids::{CentralSessionId, MachineId};

use crate::statement_store::WorkerExecutorLender;

const FIXED_TIME: &str = "2026-11-22T10:00:00.000Z";
const ADDRESS_QUERY: &str =
    include_str!("../../../fleet-actor-identity/src/queries/session_at_address.sql");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SessionCase {
    machine_id: MachineId,
    enrollment_id: EnrollmentId,
    now_ms: i64,
    command: SessionCommand,
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum SessionCommand {
    Seed {
        unrelated_sessions: i64,
    },
    ReplaceIdleEnrollment {
        replacement_id: EnrollmentId,
    },
    Create {
        request: CreateFleetSession,
        candidate_id: uuid::Uuid,
        secret_seed: u8,
    },
    Bind {
        request: BindFleetSession,
    },
    End {
        request: EndFleetSession,
    },
    Facts {
        session_id: CentralSessionId,
    },
    ApprovalFacts {
        session_id: CentralSessionId,
        requested_organization: OrganizationId,
    },
    Digest {
        request: CreateFleetSession,
    },
    CountActor {
        session_id: CentralSessionId,
    },
    CorruptState {
        session_id: CentralSessionId,
    },
    FailSessionInsert,
    FailDelegateInsert,
    FailSessionUpdate,
    ClearFailures,
}

struct HeldSessionId(uuid::Uuid);
impl IdSource for HeldSessionId {
    fn next_id(&mut self) -> uuid::Uuid {
        self.0
    }
}

struct HeldSessionSecret(u8);
impl SessionSecretSource for HeldSessionSecret {
    fn new_session_secret(&mut self) -> std::result::Result<SessionSecret, SessionServiceError> {
        Ok(SessionSecret::from_bytes(&[self.0; 32]))
    }
}

#[derive(Debug, thiserror::Error)]
enum SessionCaseError {
    #[error(transparent)]
    Sql(#[from] sqlx_core::Error),
    #[error(transparent)]
    Service(#[from] SessionServiceError),
    #[error("the fixture supplied an invalid date, enrollment or request")]
    InvalidFixture,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryRowsRead {
    statement: &'static str,
    rows_read: u64,
}

pub(super) async fn apply(
    storage: Storage,
    organization: OrganizationId,
    input: SessionCase,
) -> Result<Response> {
    let observed_reads = Arc::new(Mutex::new(Vec::new()));
    let capture_reads = observed_reads.clone();
    let connection = DoConnection::new(storage).with_fetch_observer(move |sql, rows_read| {
        capture_reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(QueryRowsRead {
                statement: if sql == ADDRESS_QUERY {
                    "address"
                } else {
                    "other"
                },
                rows_read,
            });
    });
    let outcome = connection
        .transaction(move |transaction| async move {
            apply_command(&transaction, &organization, input).await
        })
        .await;
    let reads = observed_reads
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match outcome {
        Ok(reply) => Response::from_json(&json!({ "reply": reply, "reads": *reads })),
        Err(error) => {
            Ok(Response::from_json(&json!({ "failure": error.to_string() }))?.with_status(409))
        }
    }
}

async fn apply_command(
    transaction: &DoTransaction,
    organization: &OrganizationId,
    input: SessionCase,
) -> std::result::Result<Value, SessionCaseError> {
    let caller = SessionLookupScope {
        machine_id: input.machine_id,
        enrollment_id: input.enrollment_id,
    };
    let now =
        DateTime::from_timestamp_millis(input.now_ms).ok_or(SessionCaseError::InvalidFixture)?;
    let mut lender = OrganizationLoan::new(
        organization,
        WorkerExecutorLender::from_storage_executor(transaction),
    );
    let mut ids = HeldSessionId(uuid::Uuid::nil());
    let mut context = Context { now, ids: &mut ids };
    match input.command {
        SessionCommand::Seed { unrelated_sessions } => {
            seed(transaction, &caller, unrelated_sessions).await?;
            Ok(json!({ "seeded": unrelated_sessions }))
        }
        SessionCommand::ReplaceIdleEnrollment { replacement_id } => {
            let ended_at = now.to_rfc3339_opts(SecondsFormat::Millis, true);
            replace_idle_enrollment(transaction, &caller, &replacement_id, &ended_at).await?;
            Ok(json!({ "replaced": true }))
        }
        SessionCommand::Create {
            request,
            candidate_id,
            secret_seed,
        } => {
            let mut ids = HeldSessionId(candidate_id);
            context.ids = &mut ids;
            let Some(admitted) = admit_fixture(&mut lender, &caller, now).await? else {
                return Ok(json!({ "result": "revoked" }));
            };
            let reply = SessionService::in_transaction(&mut lender)
                .create(
                    &admitted,
                    request,
                    &mut context,
                    &mut HeldSessionSecret(secret_seed),
                )
                .await?;
            Ok(call_reply(reply))
        }
        SessionCommand::Bind { request } => {
            let Some(admitted) = admit_fixture(&mut lender, &caller, now).await? else {
                return Ok(json!({ "result": "revoked" }));
            };
            Ok(call_reply(
                SessionService::in_transaction(&mut lender)
                    .bind(&admitted, request, &mut context)
                    .await?,
            ))
        }
        SessionCommand::End { request } => {
            let Some(admitted) = admit_fixture(&mut lender, &caller, now).await? else {
                return Ok(json!({ "result": "revoked" }));
            };
            Ok(call_reply(
                SessionService::in_transaction(&mut lender)
                    .end(&admitted, request, &mut context)
                    .await?,
            ))
        }
        SessionCommand::Facts { session_id } => {
            let availability = SessionService::in_transaction(&mut lender)
                .session_facts(&caller, &session_id)
                .await
                .map_err(SessionServiceError::from)?;
            Ok(match availability {
                SessionAvailability::Unavailable => json!({ "state": "unavailable" }),
                SessionAvailability::Live(LiveSessionFacts::Owner) => json!({ "state": "live" }),
                SessionAvailability::Live(LiveSessionFacts::Delegate { parent }) => json!({
                    "state": "live",
                    "parent": {
                        "sessionId": parent.session_id,
                        "machineId": parent.machine_id,
                        "enrollmentId": parent.enrollment_id,
                    },
                }),
            })
        }
        SessionCommand::ApprovalFacts {
            session_id,
            requested_organization,
        } => {
            let facts = ActorApprovalSessionFacts;
            let subject = ApprovalSession {
                enrollment: ApprovalEnrollment {
                    machine_id: caller.machine_id,
                    enrollment_id: caller.enrollment_id,
                },
                session_id,
            };
            let live = facts
                .session_is_live(&mut lender, &requested_organization, &subject)
                .await
                .map_err(SessionServiceError::from)?;
            let route = facts
                .session_initial_route(&mut lender, &requested_organization, &subject)
                .await
                .map_err(SessionServiceError::from)?;
            Ok(approval_facts_reply(live, route))
        }
        SessionCommand::Digest { request } => {
            let digest = request
                .contents_digest()
                .map_err(|_| SessionCaseError::InvalidFixture)?;
            Ok(json!({ "digest": digest }))
        }
        SessionCommand::CountActor { session_id } => {
            let count: i64 =
                query_scalar("SELECT COUNT(*) FROM actor_identities WHERE actor_id = ?")
                    .bind(session_id.as_str())
                    .fetch_one(transaction)
                    .await?;
            Ok(json!({ "count": count }))
        }
        SessionCommand::CorruptState { session_id } => {
            query("UPDATE actor_sessions SET session_state = 'unrecognized' WHERE actor_id = ?")
                .bind(session_id.as_str())
                .execute(transaction)
                .await?;
            Ok(json!({ "corrupted": true }))
        }
        SessionCommand::FailSessionInsert => {
            query("CREATE TRIGGER fixture_reject_session_insert BEFORE INSERT ON actor_sessions BEGIN SELECT RAISE(ABORT, 'injected between actor and session inserts'); END")
                .execute(transaction).await?;
            Ok(json!({ "armed": true }))
        }
        SessionCommand::FailDelegateInsert => {
            query("CREATE TRIGGER fixture_reject_delegate_insert BEFORE INSERT ON actor_delegates BEGIN SELECT RAISE(ABORT, 'injected before delegate insert'); END")
                .execute(transaction).await?;
            Ok(json!({ "armed": true }))
        }
        SessionCommand::FailSessionUpdate => {
            query("CREATE TRIGGER fixture_reject_session_update BEFORE UPDATE ON actor_sessions BEGIN SELECT RAISE(ABORT, 'injected before session update'); END")
                .execute(transaction).await?;
            Ok(json!({ "armed": true }))
        }
        SessionCommand::ClearFailures => {
            query("DROP TRIGGER IF EXISTS fixture_reject_session_insert")
                .execute(transaction)
                .await?;
            query("DROP TRIGGER IF EXISTS fixture_reject_delegate_insert")
                .execute(transaction)
                .await?;
            query("DROP TRIGGER IF EXISTS fixture_reject_session_update")
                .execute(transaction)
                .await?;
            Ok(json!({ "cleared": true }))
        }
    }
}

type OrganizationLender<'loan> =
    OrganizationLoan<'loan, WorkerExecutorLender<'loan, DoTransaction>>;

async fn admit_fixture(
    lender: &mut OrganizationLender<'_>,
    scope: &SessionLookupScope,
    now: DateTime<chrono::Utc>,
) -> std::result::Result<Option<ValidatedMachineConnection>, SessionCaseError> {
    let organization = lender.organization_id().clone();
    let attachment = MachineConnectionAttachment::from_token_claims(
        organization.clone(),
        scope.machine_id.clone(),
        scope.enrollment_id.clone(),
        ClientId::try_from(scope.enrollment_id.as_str().to_owned())
            .map_err(|_| SessionCaseError::InvalidFixture)?,
        UnixSeconds(now.timestamp() + 60),
    );
    Ok(
        restore_fixture_link(lender, &attachment, &organization, now.timestamp_millis())
            .await
            .map_err(SessionServiceError::from)?
            .ok(),
    )
}

fn approval_facts_reply(live: bool, route: Option<ApprovalRouting>) -> Value {
    let route = match route {
        Some(ApprovalRouting::Owner) => json!({ "kind": "owner" }),
        Some(ApprovalRouting::Delegate { admitted_parent }) => json!({
            "kind": "delegate",
            "parent": {
                "sessionId": admitted_parent.session_id,
                "machineId": admitted_parent.enrollment.machine_id,
                "enrollmentId": admitted_parent.enrollment.enrollment_id,
            }
        }),
        None => Value::Null,
    };
    json!({ "live": live, "route": route })
}

fn call_reply<Reply: Serialize>(outcome: SessionCallOutcome<Reply>) -> Value {
    match outcome {
        SessionCallOutcome::Answered(reply) => json!(reply),
        SessionCallOutcome::Revoked => json!({ "result": "revoked" }),
    }
}

async fn seed(
    transaction: &DoTransaction,
    caller: &SessionLookupScope,
    unrelated_sessions: i64,
) -> std::result::Result<(), SessionCaseError> {
    query("INSERT INTO login_machines (machine_id,label,state,enrollment_generation,created_at,updated_at) VALUES (?,'Test machine','enrolled',1,?,?)")
        .bind(caller.machine_id.as_str()).bind(FIXED_TIME).bind(FIXED_TIME)
        .execute(transaction).await?;
    query("INSERT INTO login_machine_approvals (approval_id,request_id,machine_id,label,allowed_workspaces,state,approved_by_person_id,permission_checked,base_generation,code_expires_at,machine_client_id,last_grant_at,created_at,updated_at) VALUES (?,'019ac1c0-0000-7000-8000-000000000003',?,'Test machine','[]','enrolled','019ac1c0-0000-7000-8000-000000000004','machine:enroll',0,?,?,?,?,?)")
        .bind(caller.enrollment_id.as_str()).bind(caller.machine_id.as_str())
        .bind(FIXED_TIME).bind(caller.enrollment_id.as_str()).bind(FIXED_TIME)
        .bind(FIXED_TIME).bind(FIXED_TIME).execute(transaction).await?;
    if !(0..=10_000).contains(&unrelated_sessions) {
        return Err(SessionCaseError::InvalidFixture);
    }
    if unrelated_sessions > 0 {
        query("WITH RECURSIVE fixture_seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM fixture_seq WHERE n < ?) INSERT INTO actor_identities (actor_id,actor_kind,created_at,updated_at) SELECT printf('019bc1c0-0000-7000-8000-%012x', n),'session',?,? FROM fixture_seq")
            .bind(unrelated_sessions).bind(FIXED_TIME).bind(FIXED_TIME).execute(transaction).await?;
        query("WITH RECURSIVE fixture_seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM fixture_seq WHERE n < ?) INSERT INTO actor_sessions (actor_id,machine_id,enrollment_id,harness_session_id,create_operation_id,create_contents_digest,create_attempt,agent_program,session_kind,creator_kind,secret_hash,session_state,reserved_until,bound_at,created_at,updated_at) SELECT printf('019bc1c0-0000-7000-8000-%012x', n),?,?,printf('unrelated-%d', n),printf('019cc1c0-0000-7000-8000-%012x', n),'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',1,'codex','ordinary','operator','AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA','active',?,?,?,? FROM fixture_seq")
            .bind(unrelated_sessions).bind(caller.machine_id.as_str()).bind(caller.enrollment_id.as_str())
            .bind("2026-11-22T10:10:00.000Z").bind(FIXED_TIME).bind(FIXED_TIME).bind(FIXED_TIME)
            .execute(transaction).await?;
    }
    Ok(())
}

async fn replace_idle_enrollment(
    transaction: &DoTransaction,
    caller: &SessionLookupScope,
    replacement_id: &EnrollmentId,
    ended_at: &str,
) -> std::result::Result<(), SessionCaseError> {
    query("UPDATE login_machine_approvals SET state='ended',end_reason='idle',last_grant_at=NULL,ended_at=?,updated_at=? WHERE approval_id=?")
        .bind(ended_at).bind(ended_at).bind(caller.enrollment_id.as_str())
        .execute(transaction).await?;
    query("UPDATE login_machines SET enrollment_generation=3,updated_at=? WHERE machine_id=?")
        .bind(ended_at)
        .bind(caller.machine_id.as_str())
        .execute(transaction)
        .await?;
    query("INSERT INTO login_machine_approvals (approval_id,request_id,machine_id,label,allowed_workspaces,state,approved_by_person_id,permission_checked,base_generation,code_expires_at,machine_client_id,last_grant_at,created_at,updated_at) VALUES (?,'019ac1c0-0000-7000-8000-000000000096',?,'Test machine','[]','enrolled','019ac1c0-0000-7000-8000-000000000004','machine:enroll',2,?,?,?,?,?)")
        .bind(replacement_id.as_str()).bind(caller.machine_id.as_str())
        .bind(FIXED_TIME).bind(replacement_id.as_str()).bind(FIXED_TIME)
        .bind(ended_at).bind(ended_at).execute(transaction).await?;
    Ok(())
}
