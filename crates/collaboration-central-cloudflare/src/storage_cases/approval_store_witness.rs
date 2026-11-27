//! V3 applies the real approvals service in a test-only SQLite Durable Object.
use std::{
    cell::Cell,
    future::Future,
    num::NonZeroU16,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
};

use approval_request_protocol::CentralTime;
use central_engine_model::{DueListKind, IdSource, InstanceKey, due_list_query};
use fleet_approvals::{
    AdmittedDecider, ApprovalApplyFailure, ApprovalApplyingStore, ApprovalCommitOwner,
    ApprovalCommitProof, ApprovalCommitScope, ApprovalDecideInput, ApprovalDecisionReply,
    ApprovalEnrollmentScope, ApprovalHolderAdmission, ApprovalHolderCommand, ApprovalHolderReply,
    ApprovalHolderScope, PendingApprovalEffects, PendingApprovalReply, complete_approval_commit,
};
use fleet_runtime_ports::{Clock, SyncTime};
use serde_json::{Value, json};
use sqlx_durable_object::DoConnection;
use uuid::{NoContext, Timestamp, Uuid};
use worker::{Response, State};

#[path = "approval_store_witness/fixture_commands.rs"]
mod fixture_commands;
#[path = "approval_store_witness/fixture_transaction.rs"]
mod fixture_transaction;
#[path = "approval_store_witness/session_authority.rs"]
mod session_authority;

use fixture_commands::{
    ApprovalWitnessCommand, ApprovalWitnessInput, StoredCorruption, WitnessDecider,
};
use fixture_transaction::{FixtureEvidence, FixtureSessionFacts, WorkerApprovalTransaction};

const AUTHORITY_SCHEMA: &str =
    include_str!("../../../fleet-approvals/tests/fixtures/approval_authority_records.sql");
const SNAPSHOT: &str =
    include_str!("../../../fleet-approvals/tests/fixtures/approval_store_snapshot.sql");
const NOISE_REQUESTS: &str =
    include_str!("../../../fleet-approvals/tests/fixtures/approval_noise_requests.sql");
const NOISE_DUE_ENTRIES: &str =
    include_str!("../../../fleet-approvals/tests/fixtures/approval_noise_due_entries.sql");
const INDEXED_DUE_QUERY: &str =
    include_str!("../../../fleet-engine/src/queries/read_projected_due_items.sql");

struct WitnessClock(i64);
impl Clock for WitnessClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.0)
    }
}

struct WitnessIds(i64);
impl IdSource for WitnessIds {
    fn next_id(&mut self) -> Uuid {
        let millis = u64::try_from(self.0).unwrap_or(0);
        Uuid::new_v7(Timestamp::from_unix(
            NoContext,
            millis / 1000,
            u32::try_from(millis % 1000).unwrap_or(0) * 1_000_000,
        ))
    }
}

#[derive(Debug)]
enum WitnessFailure {
    Apply(ApprovalApplyFailure),
    Storage,
    Clock,
    EmptyPage,
    Release,
}

impl From<ApprovalApplyFailure> for WitnessFailure {
    fn from(failure: ApprovalApplyFailure) -> Self {
        Self::Apply(failure)
    }
}

impl From<sqlx::Error> for WitnessFailure {
    fn from(error: sqlx::Error) -> Self {
        worker::console_warn!("approval witness transaction: {error}");
        Self::Storage
    }
}

pub(super) async fn apply_fixture(
    state: &State,
    connection: &DoConnection,
    opened: &Cell<bool>,
    mut request: worker::Request,
) -> worker::Result<Response> {
    let input: ApprovalWitnessInput = request.json().await?;
    if !opened.get() {
        crate::statement_store::migrate(
            connection,
            Arc::new(fleet_storage_schema::workspace_migrations::central_migrator()),
        )
        .await
        .map_err(witness_error)?;
        connection.sql().exec(AUTHORITY_SCHEMA, None)?;
        opened.set(true);
    }
    if let ApprovalWitnessCommand::DueRows { cutoff_ms, limit } = input.command {
        return observe_due_rows(state, cutoff_ms, limit).await;
    }
    let evidence = Arc::new(Mutex::new(FixtureEvidence::default()));
    let outcome = complete_approval_commit(WorkerApprovalCommit {
        connection,
        input,
        evidence: Arc::clone(&evidence),
    })
    .await
    .and_then(|(proof, (reply, holder_effects))| {
        let offers = holder_effects
            .map_or_else(
                || Ok(Vec::new()),
                |effects| {
                    effects
                        .release(&proof)
                        .map(|released| released.endings)
                        .map_err(|_| WitnessFailure::Release)
                },
            )?
            .into_iter()
            .map(|offer| {
                let holder = ApprovalHolderAdmission::for_fixture_scope(offer.holder_address());
                offer
                    .take_for_holder(&holder)
                    .map_err(|_| WitnessFailure::Release)
            })
            .collect::<Result<Vec<_>, _>>()?;
        reply(&proof, offers)
    });
    let evidence = evidence.lock().unwrap_or_else(PoisonError::into_inner);
    let (reply, error) = match outcome {
        Ok(value) => (Some(value), None),
        Err(WitnessFailure::Apply(failure)) => (None, Some(format!("{failure:?}"))),
        Err(other) => (None, Some(format!("{other:?}"))),
    };
    Response::from_json(&json!({
        "reply": reply,
        "error": error,
        "writes": evidence.write_count,
        "rowsChanged": evidence.rows_changed,
        "committed": error.is_none(),
    }))
}

async fn observe_due_rows(state: &State, cutoff_ms: i64, limit: usize) -> worker::Result<Response> {
    let cutoff = CentralTime::from_timestamp_millis(cutoff_ms)
        .ok_or_else(|| witness_error("deadline cutoff is out of range"))?;
    let bounds =
        due_list_query(DueListKind::ApprovalExpiries, cutoff, limit).map_err(witness_error)?;
    let billed_rows = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&billed_rows);
    let connection =
        DoConnection::new(state.storage()).with_fetch_observer(move |sql, rows_read| {
            if sql == INDEXED_DUE_QUERY
                && let Ok(mut observations) = observed.lock()
            {
                observations.push(rows_read);
            }
        });
    let rows = crate::statement_store::read_timed_due_rows(&connection, bounds)
        .await
        .map_err(witness_error)?;
    let reads = billed_rows.lock().map_err(witness_error)?;
    let rows: Vec<_> = rows
        .into_iter()
        .map(|row| {
            json!({
                "subject": row.key.0,
                "occurrenceMs": row.occurrence_ms,
                "effectiveDueMs": row.effective_due_ms,
            })
        })
        .collect();
    Response::from_json(&json!({ "rows": rows, "rowsRead": *reads }))
}

async fn apply_command(
    transaction: &mut WorkerApprovalTransaction,
    input: ApprovalWitnessInput,
    commit: &ApprovalCommitScope,
) -> Result<PendingWitnessApply, WitnessFailure> {
    let command = match input.command {
        ApprovalWitnessCommand::Authenticated { holder, operation } => {
            return session_authority::apply_authenticated(
                transaction,
                commit,
                input.at_ms,
                holder,
                operation,
            )
            .await;
        }
        fixture @ (ApprovalWitnessCommand::StoreEnrollment { .. }
        | ApprovalWitnessCommand::StoreSession { .. }
        | ApprovalWitnessCommand::Inspect
        | ApprovalWitnessCommand::Corrupt { .. }
        | ApprovalWitnessCommand::SeedDuePopulation { .. }) => {
            let value = apply_fixture_sql(transaction, fixture).await?;
            return Ok((Box::new(move |_, _| Ok(value)), None));
        }
        command => command,
    };
    let clock = WitnessClock(input.at_ms);
    let mut ids = WitnessIds(input.at_ms);
    let mut applying_store =
        ApprovalApplyingStore::new(commit, transaction, &FixtureSessionFacts, &clock, &mut ids);
    let reply = match command {
        ApprovalWitnessCommand::Register { holder } => {
            let applied = applying_store
                .register_approval_run(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder.into()),
                )
                .await?;
            applied_json(applied, |reply| json!(reply))
        }
        ApprovalWitnessCommand::Raise { holder, request } => {
            let applied = applying_store
                .apply_holder_command(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder.into()),
                    ApprovalHolderCommand::Raise(request),
                )
                .await?;
            applied_json(applied, holder_json)
        }
        ApprovalWitnessCommand::Stop { holder, request } => {
            let applied = applying_store
                .apply_holder_command(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder.into()),
                    ApprovalHolderCommand::Stop(request),
                )
                .await?;
            applied_json(applied, holder_json)
        }
        ApprovalWitnessCommand::Deliver {
            holder,
            request,
            reply,
        } => {
            let applied = applying_store
                .apply_holder_command(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder.into()),
                    ApprovalHolderCommand::DeliveryReply { request, reply },
                )
                .await?;
            applied_json(applied, holder_json)
        }
        ApprovalWitnessCommand::Reoffer { holder, request_id } => {
            let applied = applying_store
                .apply_holder_command(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder.into()),
                    ApprovalHolderCommand::Reoffer { request_id },
                )
                .await?;
            applied_json(applied, holder_json)
        }
        ApprovalWitnessCommand::Decide {
            organization,
            actor,
            request_id,
            operation_id,
            value,
        } => {
            let decider = match actor {
                WitnessDecider::Person { person_id } => AdmittedDecider::Person { person_id },
                WitnessDecider::Session { session } => AdmittedDecider::Session(session.into()),
            };
            let request = ApprovalDecideInput {
                request_id,
                operation_id,
                value,
            };
            let applied = applying_store
                .fixture_decide_approval(&organization, decider, request)
                .await?;
            applied_json(applied, decision_json)
        }
        ApprovalWitnessCommand::PassOn {
            organization,
            parent,
            request_id,
        } => {
            let applied = applying_store
                .fixture_pass_on_approval(&organization, parent.into(), &request_id)
                .await?;
            applied_json(applied, |reply| json!(reply))
        }
        ApprovalWitnessCommand::Read {
            organization,
            person,
            request_id,
        } => {
            let applied = applying_store
                .fixture_read_person_approval(&organization, &person, &request_id)
                .await?;
            applied_json(applied, |reply| json!(reply))
        }
        ApprovalWitnessCommand::List {
            organization,
            person,
            limit,
        } => {
            let limit = NonZeroU16::new(limit).ok_or(WitnessFailure::EmptyPage)?;
            let applied = applying_store
                .fixture_list_person_approvals(&organization, &person, None, limit)
                .await?;
            applied_json(applied, |reply| {
                json!({
                    "requests": reply.requests, "hasNextPage": reply.next_after.is_some(),
                })
            })
        }
        ApprovalWitnessCommand::Deadline {
            subject,
            deadline_ms,
        } => {
            let deadline =
                CentralTime::from_timestamp_millis(deadline_ms).ok_or(WitnessFailure::Clock)?;
            let offers = applying_store
                .apply_approval_deadline(&InstanceKey(subject), deadline)
                .await?;
            witness_json(offers, |(), offers| json!(offers))
        }
        ApprovalWitnessCommand::Retire { holder } => {
            let applied = applying_store
                .retire_approval_run(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder.into()),
                )
                .await?;
            witness_json(applied, |reply, _| json!(reply))
        }
        ApprovalWitnessCommand::EndSession {
            organization,
            session,
        } => {
            let offers = applying_store
                .end_approval_session(&organization, &session.into())
                .await?;
            witness_json(offers, |(), offers| json!(offers))
        }
        ApprovalWitnessCommand::EndEnrollment { holder } => {
            let holder: ApprovalHolderScope = holder.into();
            let applied = applying_store
                .end_approval_enrollment(&ApprovalEnrollmentScope {
                    organization_id: holder.organization_id,
                    enrollment: holder.run.enrollment,
                })
                .await?;
            witness_json(applied, |(), _| Value::Null)
        }
        ApprovalWitnessCommand::Recover { holder } => {
            let applied = applying_store
                .recover_owed_approval_endings(&ApprovalHolderAdmission::for_fixture_scope(
                    &holder.into(),
                ))
                .await?;
            applied_json(applied, |reply| json!(reply))
        }
        _ => return Err(WitnessFailure::Storage),
    };
    Ok((reply, Some(applying_store.finish_effects())))
}

async fn apply_fixture_sql(
    transaction: &mut WorkerApprovalTransaction,
    command: ApprovalWitnessCommand,
) -> Result<Value, WitnessFailure> {
    let reply = match command {
        ApprovalWitnessCommand::StoreEnrollment { holder, live } => {
            let holder: ApprovalHolderScope = holder.into();
            sqlx::query("INSERT INTO fixture_enrollments VALUES (?1,?2,?3,?4)
                ON CONFLICT (organization_id,machine_id,enrollment_id) DO UPDATE SET standing=excluded.standing")
                .bind(holder.organization_id.as_str())
                .bind(holder.run.enrollment.machine_id.as_str())
                .bind(holder.run.enrollment.enrollment_id.as_str())
                .bind(if live { "live" } else { "ended" })
                .execute(&transaction.transaction).await?;
            Value::Null
        }
        ApprovalWitnessCommand::StoreSession {
            organization,
            session,
            parent,
            live,
        } => {
            sqlx::query("INSERT INTO fixture_sessions VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
                ON CONFLICT (organization_id,machine_id,enrollment_id,session_id)
                DO UPDATE SET live=excluded.live, parent_machine=excluded.parent_machine,
                    parent_enrollment=excluded.parent_enrollment, parent_session=excluded.parent_session")
                .bind(organization.as_str())
                .bind(session.machine.as_str())
                .bind(session.enrollment.as_str())
                .bind(session.session_id.as_str())
                .bind(i64::from(live))
                .bind(parent.as_ref().map(|route| route.machine.as_str()))
                .bind(parent.as_ref().map(|route| route.enrollment.as_str()))
                .bind(parent.as_ref().map(|route| route.session_id.as_str()))
                .execute(&transaction.transaction).await?;
            Value::Null
        }
        ApprovalWitnessCommand::Inspect => {
            let rows: Vec<String> = sqlx::query_scalar(SNAPSHOT)
                .fetch_all(&transaction.transaction)
                .await?;
            json!(rows)
        }
        ApprovalWitnessCommand::Corrupt { request_id, field } => {
            let sql = match field {
                StoredCorruption::RequestKind => {
                    "UPDATE approval_requests SET request_kind='future_kind' WHERE request_id=?1"
                }
                StoredCorruption::RecordState => {
                    "UPDATE approval_requests SET record_state='future_state' WHERE request_id=?1"
                }
                StoredCorruption::Ending => {
                    "UPDATE approval_requests SET ending_json=json_set(ending_json,'$.ending.kind','future_ending') WHERE request_id=?1"
                }
                StoredCorruption::DeliveryState => {
                    "UPDATE approval_requests SET delivery_state='future_report' WHERE request_id=?1"
                }
                StoredCorruption::RequestKindMismatch => {
                    "UPDATE approval_requests SET request_kind='provider_question' WHERE request_id=?1"
                }
                StoredCorruption::EndingBinding => {
                    // Both decision families project to the same compact ending.
                    "UPDATE approval_requests SET ending_json=json_set(ending_json,'$.ending',
                       json_object('kind','unprovenCall','ending',
                         json_object('kind','decided','decision','allow',
                                   'personId',json_extract(ending_json,'$.ending.ending.by.personId'))))
                     WHERE request_id=?1"
                }
                StoredCorruption::ProviderMarkerWithCallCause => {
                    "UPDATE approval_requests SET stop_cause='call_withdrawn' WHERE request_id=?1 AND stop_family='provider'"
                }
                StoredCorruption::CallMarkerWithProviderCause => {
                    "UPDATE approval_requests SET stop_cause='ask_replaced' WHERE request_id=?1 AND stop_family='unproven_call'"
                }
            };
            sqlx::query(sql)
                .bind(request_id.as_str())
                .execute(&transaction.transaction)
                .await?;
            Value::Null
        }
        ApprovalWitnessCommand::SeedDuePopulation { waiting } => {
            sqlx::query(NOISE_REQUESTS)
                .bind(i64::from(waiting))
                .execute(&transaction.transaction)
                .await?;
            sqlx::query(NOISE_DUE_ENTRIES)
                .execute(&transaction.transaction)
                .await?;
            let rows: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM approval_requests WHERE record_state = 'waiting'",
            )
            .fetch_one(&transaction.transaction)
            .await?;
            json!(rows)
        }
        _ => return Err(WitnessFailure::Storage),
    };
    Ok(reply)
}

type WitnessOffers = Vec<Box<approval_request_protocol::DeliverEndingRequest>>;
type WitnessReply =
    Box<dyn FnOnce(&ApprovalCommitProof, WitnessOffers) -> Result<Value, WitnessFailure>>;
type PendingWitnessApply = (WitnessReply, Option<PendingApprovalEffects>);

struct WorkerApprovalCommit<'storage> {
    connection: &'storage DoConnection,
    input: ApprovalWitnessInput,
    evidence: Arc<Mutex<FixtureEvidence>>,
}

impl<'owner> ApprovalCommitOwner<PendingWitnessApply> for WorkerApprovalCommit<'owner> {
    type Error = WitnessFailure;

    type Commit =
        Pin<Box<dyn Future<Output = Result<PendingWitnessApply, Self::Error>> + Send + 'owner>>;

    fn apply_and_commit(self, commit: ApprovalCommitScope) -> Self::Commit {
        Box::pin(async move {
            let Self {
                connection,
                input,
                evidence,
            } = self;
            connection
                .transaction(move |transaction| async move {
                    let mut transaction = WorkerApprovalTransaction {
                        organization_id: fleet_identity_protocol::OrganizationId::try_from(
                            "019ac19f-aa00-7000-8000-0000000013ed".to_owned(),
                        )
                        .map_err(|_| WitnessFailure::Storage)?,
                        transaction,
                        fail_after_write: input.fail_after_write,
                        evidence,
                    };
                    apply_command(&mut transaction, input, &commit).await
                })
                .await
        })
    }
}

fn witness_json<Reply: 'static>(
    pending: PendingApprovalReply<Reply>,
    project: impl FnOnce(Reply, WitnessOffers) -> Value + 'static,
) -> WitnessReply {
    Box::new(move |proof, offers| {
        let reply = pending
            .take_fixture_reply(proof)
            .map_err(|_| WitnessFailure::Release)?;
        Ok(project(reply, offers))
    })
}

fn applied_json<Reply: 'static>(
    pending: PendingApprovalReply<Reply>,
    project: impl FnOnce(Reply) -> Value + 'static,
) -> WitnessReply {
    witness_json(
        pending,
        move |reply, offers| json!({ "result": project(reply), "offers": offers }),
    )
}

fn holder_json(reply: ApprovalHolderReply) -> Value {
    match reply {
        ApprovalHolderReply::Raised(reply) => json!(reply),
        ApprovalHolderReply::Stopped(reply) => json!(reply),
        ApprovalHolderReply::NoReply => Value::Null,
    }
}

fn decision_json(reply: ApprovalDecisionReply) -> Value {
    match reply {
        ApprovalDecisionReply::SavedDecision { answer, by } => {
            json!({"kind":"savedDecision","answer":answer,"by":by})
        }
        ApprovalDecisionReply::Standing { ending } => {
            json!({"kind":"standing","ending":ending})
        }
        ApprovalDecisionReply::NotVisible {} => json!({"kind":"notVisible"}),
        ApprovalDecisionReply::Invalid {} => json!({"kind":"invalid"}),
        ApprovalDecisionReply::TooLate {} => json!({"kind":"tooLate"}),
        ApprovalDecisionReply::OperationConflict {} => json!({"kind":"operationConflict"}),
    }
}

fn witness_error(error: impl std::fmt::Display) -> worker::Error {
    worker::Error::RustError(error.to_string())
}
