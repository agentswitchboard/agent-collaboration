//! Attempt-only administration and host-bound discard.
use crate::{
    admin_failure_class::EngineFailureClass,
    attempt_records::decode_attempt,
    engine_host_binding::{EngineHost, FleetEngine},
    event_source_policy::{EventSource, ReceiptStatements, SourcePolicy},
    machine_event_execution::MachineEventOutcome,
    machine_store_adapter::{
        MachineStoreAdapter, SubjectLoadFailure, VersionedSubjectState,
        classify_subject_read_failure,
    },
};
use central_engine_model::{AttemptState, EngineTime, InstanceKey, Machine};
use central_storage::{GuardedStatement, ReadOutcome, SqlValue, Statement, StoreFailure};
use fleet_runtime_ports::CentralStore as _;
use fleet_runtime_ports::{
    guarded_statements::{exact, text},
    stored_values::column,
    workspace_decode_error::corrupt,
};

pub struct AdminMachineKindRegistry<'a, Kind> {
    pub visible_machine_kinds: &'a [(&'static str, Kind)],
    pub hidden_machine_kind_tags: &'a [&'static str],
}

/// A retired attempt passed only to the preparing runtime's discard registration.
pub struct PreparedAttemptDiscard<'runtime, R: EngineHost> {
    runtime: &'runtime R,
    attempt_id: uuid::Uuid,
    kind: R::AdminMachineKind,
    stored_kind: String,
    key: InstanceKey,
    admission_and_receipt: Vec<GuardedStatement>,
}

type DecodeFinishingRow<State> =
    fn(&ReadOutcome) -> Result<Option<VersionedSubjectState<State>>, SubjectLoadFailure>;
type PlanFinishingWrite<State> =
    fn(&VersionedSubjectState<State>, &State, EngineTime) -> Result<GuardedStatement, StoreFailure>;

pub struct DiscardSubjectStatements<M: Machine> {
    pub read_discard_subject_sql: &'static str,
    pub decode_discard_subject: DecodeFinishingRow<M::State>,
    pub plan_discard_subject_write: PlanFinishingWrite<M::State>,
    pub plan_discard_effect_writes: fn(&M::Effect) -> Result<Vec<GuardedStatement>, StoreFailure>,
    pub write_discard_attempt_failure_sql: &'static str,
}

/// Both finishing reads and the applying batch use the runtime from preparation.
struct BoundDiscardStore<'a, R: EngineHost, M: Machine> {
    runtime: &'a R,
    store: DiscardSubjectStatements<M>,
}
impl<R: EngineHost, M: Machine> MachineStoreAdapter<M> for BoundDiscardStore<'_, R, M> {
    async fn load_subject_state(
        &self,
        key: &InstanceKey,
    ) -> Result<Option<VersionedSubjectState<M::State>>, SubjectLoadFailure> {
        let rows = self
            .runtime
            .central_store()
            .read(Statement {
                sql: self.store.read_discard_subject_sql,
                args: vec![text(&key.0)],
            })
            .await
            .map_err(classify_subject_read_failure)?;
        (self.store.decode_discard_subject)(&rows)
    }
    fn plan_subject_state_write(
        &self,
        _key: &InstanceKey,
        prior: &VersionedSubjectState<M::State>,
        next: &M::State,
    ) -> Result<GuardedStatement, StoreFailure> {
        (self.store.plan_discard_subject_write)(prior, next, self.runtime.current_engine_time()?)
    }
    fn plan_machine_effect_writes(
        &self,
        _key: &InstanceKey,
        effect: &M::Effect,
    ) -> Result<Vec<GuardedStatement>, StoreFailure> {
        (self.store.plan_discard_effect_writes)(effect)
    }
    fn write_attempt_failure_sql(&self) -> &'static str {
        self.store.write_discard_attempt_failure_sql
    }
}

const READ_BY_ID: &str = "SELECT attempt_id,machine_kind,subject_key,state,failure_count,next_try_at,last_failure,last_failure_detail,first_failed_at,last_failed_at,retired_at,created_at,updated_at FROM engine_work_attempts WHERE attempt_id=?";
const READ_RETIRED_PAGE: &str = "SELECT attempt_id,machine_kind,subject_key,state,failure_count,next_try_at,last_failure,last_failure_detail,first_failed_at,last_failed_at,retired_at,created_at,updated_at FROM engine_work_attempts WHERE state='retired' AND (?1 IS NULL OR attempt_id>?1) ORDER BY attempt_id LIMIT ?2";
const READ_HEALTH: &str = "SELECT machine_kind,SUM(CASE WHEN state='failing' THEN 1 ELSE 0 END) AS failing,SUM(CASE WHEN state='retired' THEN 1 ELSE 0 END) AS retired,MIN(first_failed_at) AS oldest,(SELECT b.last_failure FROM engine_work_attempts b WHERE b.machine_kind=a.machine_kind ORDER BY b.last_failed_at DESC,b.attempt_id DESC LIMIT 1) AS last_failure FROM engine_work_attempts a GROUP BY machine_kind";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MachineKindAttemptHealth<Kind> {
    pub machine_kind: Kind,
    pub failing_attempt_count: u32,
    pub retired_attempt_count: u32,
    pub oldest_first_failed_at: Option<EngineTime>,
    pub latest_failure_class: Option<EngineFailureClass>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineHealthFacts<Kind> {
    pub health_by_machine_kind: Vec<MachineKindAttemptHealth<Kind>>,
    pub published_alarm_at: Option<EngineTime>,
    pub alarm_missing: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetiredAttemptSummary<Kind> {
    pub attempt_id: uuid::Uuid,
    pub machine_kind: Kind,
    pub subject_key: String,
    pub failure_class: EngineFailureClass,
    pub failure_count: u32,
    pub retired_at: EngineTime,
    pub last_failure_detail: Option<String>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptRefusal {
    NotFound,
    NotRetired,
    DiscardUnsupported,
    DiscardSuperseded,
    SubjectUnreadable,
}
pub enum DiscardOutcome {
    /// The subject's Discard transition, the guarded record delete and the
    /// caller's statements committed in one batch.
    Committed,
    Refused(AttemptRefusal),
}

/// An admin-visible kind, a hidden test kind (None), or a corrupt stored value.
fn registered_kind<Kind: Copy>(
    raw: &str,
    kinds: &AdminMachineKindRegistry<'_, Kind>,
) -> Result<Option<Kind>, StoreFailure> {
    if let Some((_, kind)) = kinds
        .visible_machine_kinds
        .iter()
        .find(|(stored, _)| *stored == raw)
    {
        return Ok(Some(*kind));
    }
    if kinds.hidden_machine_kind_tags.contains(&raw) {
        return Ok(None);
    }
    Err(corrupt("engine_work_attempts.machine_kind"))
}
fn failure_class(tag: &str) -> Result<EngineFailureClass, StoreFailure> {
    Ok(match tag {
        "corrupt_row" => EngineFailureClass::CorruptRow,
        "constraint_violated" => EngineFailureClass::ConstraintViolated,
        "statement_rejected" => EngineFailureClass::StatementRejected,
        "refused" => EngineFailureClass::Refused,
        "other_store_unavailable" => EngineFailureClass::OtherStoreUnavailable,
        _ => return Err(corrupt("engine_work_attempts.last_failure")),
    })
}
fn integer(rows: &ReadOutcome, row: &[SqlValue], name: &str) -> Result<u32, StoreFailure> {
    match column(rows, row, name)? {
        SqlValue::Integer(value) => u32::try_from(value.get()).map_err(|_| corrupt(name)),
        _ => Err(corrupt(name)),
    }
}
fn optional_text(
    rows: &ReadOutcome,
    row: &[SqlValue],
    name: &str,
) -> Result<Option<String>, StoreFailure> {
    match column(rows, row, name)? {
        SqlValue::Null => Ok(None),
        SqlValue::Text(value) => Ok(Some(value.clone())),
        _ => Err(corrupt(name)),
    }
}
fn one_row(rows: &ReadOutcome, row: &[SqlValue]) -> ReadOutcome {
    ReadOutcome {
        columns: rows.columns.clone(),
        rows: vec![row.to_vec()],
    }
}
/// The guarded delete that retry and discard both commit: only a still-retired
/// record with this id goes, so a concurrent second change finds nothing.
fn delete_retired(attempt_id: uuid::Uuid) -> GuardedStatement {
    exact(
        "DELETE FROM engine_work_attempts WHERE attempt_id=? AND state='retired'",
        vec![text(attempt_id)],
    )
}

/// Discard's receipt statements are the caller's admission writes and answer
/// receipt plus the guarded delete; a refused transition writes none of them.
struct DiscardSource(Vec<GuardedStatement>);
impl<M: Machine> EventSource<M> for DiscardSource {
    fn policy(&self) -> SourcePolicy {
        SourcePolicy::Discard
    }
    fn accepted(&self, _state: &M::State) -> Result<ReceiptStatements, StoreFailure> {
        Ok(ReceiptStatements::Admin(self.0.clone()))
    }
}

impl<'runtime, R: EngineHost> FleetEngine<'runtime, R> {
    pub async fn read_engine_attempt_health<Kind: Copy + Eq + Send + Sync>(
        &self,
        registry: &AdminMachineKindRegistry<'_, Kind>,
    ) -> Result<EngineHealthFacts<Kind>, StoreFailure> {
        let rows = self
            .host()
            .read(Statement {
                sql: READ_HEALTH,
                args: vec![],
            })
            .await?;
        let mut kinds = registry
            .visible_machine_kinds
            .iter()
            .map(|(_, kind)| MachineKindAttemptHealth {
                machine_kind: *kind,
                failing_attempt_count: 0,
                retired_attempt_count: 0,
                oldest_first_failed_at: None,
                latest_failure_class: None,
            })
            .collect::<Vec<_>>();
        for row in &rows.rows {
            let kind = match column(&rows, row, "machine_kind")? {
                SqlValue::Text(kind) => registered_kind(kind, registry)?,
                _ => return Err(corrupt("engine_work_attempts.machine_kind")),
            };
            let Some(kind) = kind else { continue };
            let oldest = optional_text(&rows, row, "oldest")?
                .map(|time| {
                    time.parse::<EngineTime>()
                        .map_err(|_| corrupt("engine_work_attempts.first_failed_at"))
                })
                .transpose()?;
            let entry = kinds
                .iter_mut()
                .find(|entry| entry.machine_kind == kind)
                .ok_or_else(|| corrupt("engine_work_attempts.machine_kind"))?;
            *entry = MachineKindAttemptHealth {
                machine_kind: kind,
                failing_attempt_count: integer(&rows, row, "failing")?,
                retired_attempt_count: integer(&rows, row, "retired")?,
                oldest_first_failed_at: oldest,
                latest_failure_class: optional_text(&rows, row, "last_failure")?
                    .map(|tag| failure_class(&tag))
                    .transpose()?,
            };
        }
        let alarm = self.host().published_alarm().await?;
        let pending = self.host().engine_alarm_inputs().await?;
        let timed_work_pending = pending.earliest_on_grid_due_unix_millis.is_some()
            || pending.earliest_legacy_due_unix_millis.is_some();
        Ok(EngineHealthFacts {
            health_by_machine_kind: kinds,
            published_alarm_at: alarm,
            alarm_missing: alarm.is_none() && timed_work_pending,
        })
    }

    /// One page ascending by attempt id, `after` exclusive, plus whether more exist.
    pub async fn read_retired_attempt_page<Kind: Copy + Eq + Send + Sync>(
        &self,
        registry: &AdminMachineKindRegistry<'_, Kind>,
        after: Option<uuid::Uuid>,
        limit: u16,
    ) -> Result<(Vec<RetiredAttemptSummary<Kind>>, Option<uuid::Uuid>), StoreFailure> {
        let rows = self
            .host()
            .read(Statement {
                sql: READ_RETIRED_PAGE,
                args: vec![
                    after.map_or(SqlValue::Null, text),
                    SqlValue::Integer(
                        (i64::from(limit) + 1)
                            .try_into()
                            .map_err(|_| corrupt("limit"))?,
                    ),
                ],
            })
            .await?;
        let mut items = Vec::new();
        for row in &rows.rows {
            let stored = decode_attempt(&one_row(&rows, row))?
                .ok_or_else(|| corrupt("engine_work_attempts.attempt_id"))?;
            let record = stored
                .record
                .map_err(|corruption| corrupt(&corruption.column))?;
            let AttemptState::Retired { retired_at } = record.state else {
                return Err(corrupt("engine_work_attempts.state"));
            };
            let Some(kind) = (match column(&rows, row, "machine_kind")? {
                SqlValue::Text(kind) => registered_kind(kind, registry)?,
                _ => return Err(corrupt("engine_work_attempts.machine_kind")),
            }) else {
                continue;
            };
            items.push(RetiredAttemptSummary {
                attempt_id: record.attempt_id,
                machine_kind: kind,
                subject_key: match column(&rows, row, "subject_key")? {
                    SqlValue::Text(key) => key.clone(),
                    _ => return Err(corrupt("engine_work_attempts.subject_key")),
                },
                failure_class: failure_class(record.last_failure.storage_tag())?,
                failure_count: record.failure_count.get(),
                retired_at,
                last_failure_detail: crate::attempt_records::failure_detail(&record.last_failure),
            });
        }
        let next = if items.len() > usize::from(limit) {
            items.truncate(usize::from(limit));
            items.last().map(|item| item.attempt_id)
        } else {
            None
        };
        Ok((items, next))
    }

    async fn retired_attempt<Kind: Copy + Eq + Send + Sync>(
        &self,
        registry: &AdminMachineKindRegistry<'_, Kind>,
        attempt_id: uuid::Uuid,
    ) -> Result<Result<(Kind, String, InstanceKey), AttemptRefusal>, StoreFailure> {
        let rows = self
            .host()
            .read(Statement {
                sql: READ_BY_ID,
                args: vec![text(attempt_id)],
            })
            .await?;
        let Some(row) = rows.rows.first() else {
            return Ok(Err(AttemptRefusal::NotFound));
        };
        let raw_kind = match column(&rows, row, "machine_kind")? {
            SqlValue::Text(kind) => kind,
            _ => return Err(corrupt("engine_work_attempts.machine_kind")),
        };
        let Some(kind) = registered_kind(raw_kind, registry)? else {
            return Ok(Err(AttemptRefusal::NotFound));
        };
        let key = match column(&rows, row, "subject_key")? {
            SqlValue::Text(key) => key.clone(),
            _ => return Err(corrupt("engine_work_attempts.subject_key")),
        };
        let stored = decode_attempt(&one_row(&rows, row))?
            .ok_or_else(|| corrupt("engine_work_attempts.attempt_id"))?;
        match &stored.record {
            Ok(record) if matches!(record.state, AttemptState::Retired { .. }) => {
                Ok(Ok((kind, raw_kind.clone(), InstanceKey(key))))
            }
            Ok(_) | Err(_) => Ok(Err(AttemptRefusal::NotRetired)),
        }
    }

    /// Retry is the guarded delete in the caller's batch, which also re-arms.
    pub async fn plan_retired_attempt_retry<Kind: Copy + Eq + Send + Sync>(
        &self,
        registry: &AdminMachineKindRegistry<'_, Kind>,
        attempt_id: uuid::Uuid,
    ) -> Result<Result<GuardedStatement, AttemptRefusal>, StoreFailure> {
        Ok(self
            .retired_attempt(registry, attempt_id)
            .await?
            .map(|_| delete_retired(attempt_id)))
    }

    /// Resolves a retired attempt and runs its preparing runtime's registered finisher.
    ///
    /// A same-kind descriptor cannot be substituted at the admin entry.
    ///
    /// ```compile_fail,E0061
    /// use central_engine_model::Machine;
    /// use fleet_engine::{DiscardSubjectStatements, FleetEngine, EngineHost};
    ///
    /// async fn replace_finisher<R: EngineHost, M: Machine>(
    ///     engine: &FleetEngine<'_, R>,
    ///     attempt_id: uuid::Uuid,
    ///     mut same_kind: DiscardSubjectStatements<M>,
    ///     event: M::Event,
    /// ) {
    ///     same_kind.read_discard_subject_sql = "SELECT 1";
    ///     let _ = engine.discard_retired_attempt(attempt_id, vec![], same_kind, event).await;
    /// }
    /// ```
    pub async fn discard_retired_attempt(
        &self,
        attempt_id: uuid::Uuid,
        admission_and_receipt: Vec<GuardedStatement>,
    ) -> Result<DiscardOutcome, StoreFailure> {
        let (kind, stored_kind, key) = match self
            .retired_attempt(self.0.admin_machine_kind_registry(), attempt_id)
            .await?
        {
            Ok(attempt) => attempt,
            Err(refusal) => return Ok(DiscardOutcome::Refused(refusal)),
        };
        self.0
            .finish_registered_discard(PreparedAttemptDiscard {
                runtime: self.0,
                attempt_id,
                kind,
                stored_kind,
                key,
                admission_and_receipt,
            })
            .await
    }

    /// Discard reads only the finishing view: a corrupt payload does not stop it;
    /// only a lifecycle state that will not decode answers SubjectUnreadable,
    /// with nothing changed (Retry still works).
    async fn discard_subject<M, Adapter>(
        &self,
        adapter: &Adapter,
        key: InstanceKey,
        event: M::Event,
        source: &DiscardSource,
    ) -> Result<DiscardOutcome, StoreFailure>
    where
        M: Machine,
        M::State: Send + Sync,
        M::Event: Send + Sync,
        M::Effect: Send + Sync,
        M::Error: Send + Sync,
        Adapter: crate::machine_store_adapter::MachineStoreAdapter<M>,
    {
        match adapter.load_subject_state(&key).await {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(DiscardOutcome::Refused(AttemptRefusal::DiscardSuperseded)),
            Err(crate::machine_store_adapter::SubjectLoadFailure::CorruptSubjectRow { .. }) => {
                return Ok(DiscardOutcome::Refused(AttemptRefusal::SubjectUnreadable));
            }
            Err(crate::machine_store_adapter::SubjectLoadFailure::StorageAnswerLost) => {
                return Err(StoreFailure::AnswerLost);
            }
            Err(crate::machine_store_adapter::SubjectLoadFailure::StoreReadFailed(failure)) => {
                return Err(failure);
            }
        }
        discard_outcome(
            self.handle::<M, Adapter, _>(adapter, key, event, source)
                .await,
        )
    }
}

impl<R: EngineHost> PreparedAttemptDiscard<'_, R> {
    pub fn admin_machine_kind(&self) -> R::AdminMachineKind {
        self.kind
    }

    /// Runs the typed finisher selected inside the host registration callback.
    pub async fn commit_subject_discard<M>(
        mut self,
        store: DiscardSubjectStatements<M>,
        event: M::Event,
    ) -> Result<DiscardOutcome, StoreFailure>
    where
        M: Machine,
        M::State: Send + Sync,
        M::Event: Send + Sync,
        M::Effect: Send + Sync,
        M::Error: Send + Sync,
    {
        if self.stored_kind != M::KIND.0 {
            return Ok(DiscardOutcome::Refused(AttemptRefusal::DiscardUnsupported));
        }
        self.admission_and_receipt
            .insert(0, delete_retired(self.attempt_id));
        FleetEngine::new(self.runtime)
            .discard_subject::<M, _>(
                &BoundDiscardStore {
                    runtime: self.runtime,
                    store,
                },
                self.key,
                event,
                &DiscardSource(self.admission_and_receipt),
            )
            .await
    }
}

fn discard_outcome<State, Error>(
    outcome: MachineEventOutcome<State, Error>,
) -> Result<DiscardOutcome, StoreFailure> {
    match outcome {
        MachineEventOutcome::TransitionCommitted(_) => Ok(DiscardOutcome::Committed),
        // The subject moved, finished, vanished or is unreadable: nothing changed.
        MachineEventOutcome::EventSuperseded
        | MachineEventOutcome::TransitionRefused(_)
        | MachineEventOutcome::SubjectMissing
        | MachineEventOutcome::AttemptFailureRecorded(_)
        | MachineEventOutcome::RequestItemFailed(_) => {
            Ok(DiscardOutcome::Refused(AttemptRefusal::DiscardSuperseded))
        }
        MachineEventOutcome::StorageAnswerLost => Err(StoreFailure::AnswerLost),
        MachineEventOutcome::StorageFailed(failure) => Err(failure),
    }
}
