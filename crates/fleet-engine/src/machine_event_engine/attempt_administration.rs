//! Attempt-only administration and host-bound discard.
#[cfg(test)]
use super::attempt_records::CheckedFullAttemptProjection;
use super::{
    admin_failure_class::EngineFailureClass,
    attempt_bindings::attempt_statement,
    attempt_projection::{AttemptCorruption, attempt_projection},
    attempt_records::{FullAttemptProjection, encode_attempt_failure_detail},
    engine_host_binding::{EngineHost, FleetEngine},
    event_source_policy::{EventExecutionPolicy, EventSource, ReceiptStatements},
    machine_event_execution::MachineEventOutcome,
    machine_store_adapter::{
        MachineStoreAdapter, SubjectLoadFailure, VersionedSubjectState,
        classify_subject_read_failure,
    },
};
use central_engine_model::{AttemptState, EngineTime, InstanceKey, Machine};
use central_storage::{
    GuardedStatement, ReadOutcome, RowExpectation, SafeInteger, Statement, StoreFailure,
};
use fleet_runtime_ports::CentralStore as _;
use fleet_runtime_ports::{guarded_statements::text, workspace_decode_error::corrupt};

pub struct AdminMachineKindRegistry<'a, AdminMachineKind> {
    pub visible_machine_kinds: &'a [(&'static str, AdminMachineKind)],
    pub hidden_machine_kind_tags: &'a [&'static str],
}

/// A retired attempt passed only to the preparing host's discard registration.
pub struct PreparedAttemptDiscard<'preparing_host, Host: EngineHost> {
    preparing_host: &'preparing_host Host,
    attempt_id: uuid::Uuid,
    admin_machine_kind: Host::AdminMachineKind,
    stored_machine_kind_tag: String,
    subject_key: InstanceKey,
    admission_and_receipt: Vec<GuardedStatement>,
}

type DecodeDiscardSubject<State> =
    fn(&ReadOutcome) -> Result<Option<VersionedSubjectState<State>>, SubjectLoadFailure>;
type PlanDiscardSubjectWrite<State> =
    fn(&VersionedSubjectState<State>, &State, EngineTime) -> Result<GuardedStatement, StoreFailure>;

pub struct DiscardSubjectStatements<SubjectMachine: Machine> {
    pub read_discard_subject_sql: &'static str,
    pub decode_discard_subject: DecodeDiscardSubject<SubjectMachine::State>,
    pub plan_discard_subject_write: PlanDiscardSubjectWrite<SubjectMachine::State>,
    pub plan_discard_effect_writes:
        fn(&SubjectMachine::Effect) -> Result<Vec<GuardedStatement>, StoreFailure>,
    pub write_discard_attempt_failure_sql: &'static str,
}

/// Both finishing reads and the applying batch use the preparing host.
struct BoundDiscardStore<'a, Host: EngineHost, SubjectMachine: Machine> {
    preparing_host: &'a Host,
    discard_subject_statements: DiscardSubjectStatements<SubjectMachine>,
}
impl<Host: EngineHost, SubjectMachine: Machine> MachineStoreAdapter<SubjectMachine>
    for BoundDiscardStore<'_, Host, SubjectMachine>
{
    async fn load_subject_state(
        &self,
        key: &InstanceKey,
    ) -> Result<Option<VersionedSubjectState<SubjectMachine::State>>, SubjectLoadFailure> {
        let rows = self
            .preparing_host
            .central_store()
            .read(Statement {
                sql: self.discard_subject_statements.read_discard_subject_sql,
                args: vec![text(&key.0)],
            })
            .await
            .map_err(classify_subject_read_failure)?;
        (self.discard_subject_statements.decode_discard_subject)(&rows)
    }
    fn plan_subject_state_write(
        &self,
        _key: &InstanceKey,
        prior: &VersionedSubjectState<SubjectMachine::State>,
        next: &SubjectMachine::State,
    ) -> Result<GuardedStatement, StoreFailure> {
        (self.discard_subject_statements.plan_discard_subject_write)(
            prior,
            next,
            self.preparing_host.current_engine_time()?,
        )
    }
    fn plan_machine_effect_writes(
        &self,
        _key: &InstanceKey,
        effect: &SubjectMachine::Effect,
    ) -> Result<Vec<GuardedStatement>, StoreFailure> {
        (self.discard_subject_statements.plan_discard_effect_writes)(effect)
    }
    fn write_attempt_failure_sql(&self) -> &'static str {
        self.discard_subject_statements
            .write_discard_attempt_failure_sql
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MachineKindAttemptHealth<AdminMachineKind> {
    pub machine_kind: AdminMachineKind,
    pub failing_attempt_count: u32,
    pub retired_attempt_count: u32,
    pub oldest_first_failed_at: Option<EngineTime>,
    pub latest_failure_class: Option<EngineFailureClass>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineHealthFacts<AdminMachineKind> {
    pub health_by_machine_kind: Vec<MachineKindAttemptHealth<AdminMachineKind>>,
    pub published_alarm_at: Option<EngineTime>,
    pub alarm_missing: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetiredAttemptSummary<AdminMachineKind> {
    pub attempt_id: uuid::Uuid,
    pub machine_kind: AdminMachineKind,
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
fn decode_registered_admin_kind<AdminMachineKind: Copy>(
    stored_kind_tag: &str,
    registry: &AdminMachineKindRegistry<'_, AdminMachineKind>,
) -> Result<Option<AdminMachineKind>, StoreFailure> {
    if let Some((_, admin_kind)) = registry
        .visible_machine_kinds
        .iter()
        .find(|(stored, _)| *stored == stored_kind_tag)
    {
        return Ok(Some(*admin_kind));
    }
    if registry.hidden_machine_kind_tags.contains(&stored_kind_tag) {
        return Ok(None);
    }
    Err(corrupt("engine_work_attempts.machine_kind"))
}
fn decode_attempt_failure_class(tag: &str) -> Result<EngineFailureClass, StoreFailure> {
    Ok(match tag {
        "corrupt_row" => EngineFailureClass::CorruptRow,
        "constraint_violated" => EngineFailureClass::ConstraintViolated,
        "statement_rejected" => EngineFailureClass::StatementRejected,
        "refused" => EngineFailureClass::Refused,
        "other_store_unavailable" => EngineFailureClass::OtherStoreUnavailable,
        _ => return Err(corrupt("engine_work_attempts.last_failure")),
    })
}

fn attempt_column_corruption(error: AttemptCorruption) -> StoreFailure {
    corrupt(&error.column_path)
}

/// The guarded delete that retry and discard both commit: only a still-retired
/// record with this id goes, so a concurrent second change finds nothing.
fn plan_retired_attempt_delete(attempt_id: uuid::Uuid) -> GuardedStatement {
    GuardedStatement {
        statement: attempt_statement!(
            query_file,
            [],
            "src/queries/delete_retired_attempt.sql",
            attempt_id = attempt_id.to_string(),
        ),
        expect: RowExpectation::Exactly(1),
    }
}

/// Discard's receipt statements are the caller's admission writes and answer
/// receipt plus the guarded delete; a refused transition writes none of them.
struct DiscardEventSource(Vec<GuardedStatement>);
impl<SubjectMachine: Machine> EventSource<SubjectMachine> for DiscardEventSource {
    fn event_execution_policy(&self) -> EventExecutionPolicy {
        EventExecutionPolicy::AdminDiscard
    }
    fn plan_accepted_receipt(
        &self,
        _state: &SubjectMachine::State,
    ) -> Result<ReceiptStatements, StoreFailure> {
        Ok(ReceiptStatements::AdminCommit(self.0.clone()))
    }
}

const RETIRED_ATTEMPT_SCAN_ROWS_PER_ITEM: u16 = 4;

fn plan_retired_attempt_page(after: Option<uuid::Uuid>, read_limit: SafeInteger) -> Statement {
    match after {
        None => attempt_statement!(
            query_file_as,
            [CheckedFullAttemptProjection],
            "src/queries/read_retired_attempt_page.sql",
            scan_limit = read_limit,
        ),
        Some(after) => attempt_statement!(
            query_file_as,
            [CheckedFullAttemptProjection],
            "src/queries/read_retired_attempt_continuation.sql",
            exclusive_attempt_id = after.to_string(),
            scan_limit = read_limit,
        ),
    }
}

attempt_projection!(AttemptHealthProjection {
    machine_kind: Option<String>,
    failing: Option<i64>,
    retired: Option<i64>,
    oldest: Option<String>,
    last_failure: Option<String>,
});

#[cfg(test)]
type CheckedAttemptHealthProjection =
    AttemptHealthProjection<super::attempt_projection::CheckedStorage>;

struct DecodedAttemptHealth {
    oldest_failure_at: Option<EngineTime>,
    failing_attempt_count: u32,
    retired_attempt_count: u32,
    latest_failure_class: Option<EngineFailureClass>,
}

impl AttemptHealthProjection {
    fn decode_visible_health(&self) -> Result<DecodedAttemptHealth, StoreFailure> {
        let oldest_failure_at = self
            .oldest
            .nullable_text()
            .map_err(|_| corrupt("oldest"))?
            .map(|time| {
                time.parse::<EngineTime>()
                    .map_err(|_| corrupt("engine_work_attempts.first_failed_at"))
            })
            .transpose()?;
        let count = |column: &super::attempt_projection::ObservedColumn<Option<i64>>, name| {
            let value = column.integer().map_err(|_| corrupt(name))?;
            u32::try_from(value).map_err(|_| corrupt(name))
        };
        Ok(DecodedAttemptHealth {
            oldest_failure_at,
            failing_attempt_count: count(&self.failing, "failing")?,
            retired_attempt_count: count(&self.retired, "retired")?,
            latest_failure_class: self
                .last_failure
                .nullable_text()
                .map_err(|_| corrupt("last_failure"))?
                .map(decode_attempt_failure_class)
                .transpose()?,
        })
    }
}

impl<'host, Host: EngineHost> FleetEngine<'host, Host> {
    pub async fn read_engine_attempt_health<AdminMachineKind: Copy + Eq + Send + Sync>(
        &self,
        registry: &AdminMachineKindRegistry<'_, AdminMachineKind>,
    ) -> Result<EngineHealthFacts<AdminMachineKind>, StoreFailure> {
        let rows = self
            .central_store()
            .read(attempt_statement!(
                query_file_as,
                [CheckedAttemptHealthProjection],
                "src/queries/read_attempt_health_by_kind.sql",
            ))
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
            let projection = AttemptHealthProjection::read_named(&rows, row)?;
            let machine_kind = projection
                .machine_kind
                .text()
                .map_err(attempt_column_corruption)?;
            let admin_kind = decode_registered_admin_kind(machine_kind, registry)?;
            let Some(admin_kind) = admin_kind else {
                continue;
            };
            let stored_health = projection.decode_visible_health()?;
            let entry = kinds
                .iter_mut()
                .find(|entry| entry.machine_kind == admin_kind)
                .ok_or_else(|| corrupt("engine_work_attempts.machine_kind"))?;
            *entry = MachineKindAttemptHealth {
                machine_kind: admin_kind,
                failing_attempt_count: stored_health.failing_attempt_count,
                retired_attempt_count: stored_health.retired_attempt_count,
                oldest_first_failed_at: stored_health.oldest_failure_at,
                latest_failure_class: stored_health.latest_failure_class,
            };
        }
        let alarm = self.central_store().published_alarm().await?;
        let alarm_inputs = self.central_store().engine_alarm_inputs().await?;
        let timed_work_pending = alarm_inputs.earliest_on_grid_due_unix_millis.is_some()
            || alarm_inputs.earliest_legacy_due_unix_millis.is_some();
        Ok(EngineHealthFacts {
            health_by_machine_kind: kinds,
            published_alarm_at: alarm,
            alarm_missing: alarm.is_none() && timed_work_pending,
        })
    }

    /// A bounded ascending scan. Continue from the last examined ID until there
    /// is no continuation, even when a page contains no visible retired rows.
    pub async fn read_retired_attempt_page<AdminMachineKind: Copy + Eq + Send + Sync>(
        &self,
        registry: &AdminMachineKindRegistry<'_, AdminMachineKind>,
        after: Option<uuid::Uuid>,
        limit: u16,
    ) -> Result<
        (
            Vec<RetiredAttemptSummary<AdminMachineKind>>,
            Option<uuid::Uuid>,
        ),
        StoreFailure,
    > {
        let scan_limit = usize::from(limit) * usize::from(RETIRED_ATTEMPT_SCAN_ROWS_PER_ITEM);
        let read_limit = (i64::from(limit) * i64::from(RETIRED_ATTEMPT_SCAN_ROWS_PER_ITEM))
            .try_into()
            .map_err(|_| corrupt("limit"))?;
        let rows = self
            .central_store()
            .read(plan_retired_attempt_page(after, read_limit))
            .await?;
        let mut pending_rows = rows.rows.iter().peekable();
        let mut last_examined = None;
        let mut items = Vec::new();
        while items.len() < usize::from(limit) {
            let Some(row) = pending_rows.next() else {
                break;
            };
            let projection = FullAttemptProjection::read_named(&rows, row)?;
            let raw_id = projection
                .attempt_id
                .text()
                .map_err(attempt_column_corruption)?;
            let attempt_id = raw_id
                .parse::<uuid::Uuid>()
                .ok()
                .filter(|id| id.get_version_num() == 7 && id.to_string() == raw_id)
                .ok_or_else(|| corrupt("engine_work_attempts.attempt_id"))?;
            last_examined = Some(attempt_id);
            // The row cap precedes the state filter, so even a long run of
            // active attempts can only consume one bounded index range.
            if !projection
                .state
                .text()
                .is_ok_and(|state| state == "retired")
            {
                continue;
            }
            let admin_kind = projection
                .machine_kind
                .text()
                .map_err(attempt_column_corruption)
                .and_then(|kind| decode_registered_admin_kind(kind, registry));
            let subject_key = projection.subject_key.text().map(str::to_owned);
            let record = projection
                .into_record_projection()
                .decode_attempt_record()
                .map_err(attempt_column_corruption)?;
            let AttemptState::Retired { retired_at } = record.state else {
                return Err(corrupt("engine_work_attempts.state"));
            };
            let Some(admin_kind) = admin_kind? else {
                continue;
            };
            items.push(RetiredAttemptSummary {
                attempt_id,
                machine_kind: admin_kind,
                subject_key: subject_key.map_err(attempt_column_corruption)?,
                failure_class: decode_attempt_failure_class(record.last_failure.storage_tag())?,
                failure_count: record.failure_count.get(),
                retired_at,
                last_failure_detail: encode_attempt_failure_detail(&record.last_failure),
            });
        }
        let may_have_more = pending_rows.peek().is_some() || rows.rows.len() == scan_limit;
        Ok((items, last_examined.filter(|_| may_have_more)))
    }

    async fn read_retired_attempt_identity<AdminMachineKind: Copy + Eq + Send + Sync>(
        &self,
        registry: &AdminMachineKindRegistry<'_, AdminMachineKind>,
        attempt_id: uuid::Uuid,
    ) -> Result<Result<(AdminMachineKind, String, InstanceKey), AttemptRefusal>, StoreFailure> {
        let rows = self
            .central_store()
            .read(attempt_statement!(
                query_file_as,
                [CheckedFullAttemptProjection],
                "src/queries/read_admin_attempt_by_id.sql",
                attempt_id = attempt_id.to_string(),
            ))
            .await?;
        let Some(row) = rows.rows.first() else {
            return Ok(Err(AttemptRefusal::NotFound));
        };
        let projection = FullAttemptProjection::read_named(&rows, row)?;
        let stored_kind_tag = projection
            .machine_kind
            .text()
            .map_err(attempt_column_corruption)?;
        let Some(admin_kind) = decode_registered_admin_kind(stored_kind_tag, registry)? else {
            return Ok(Err(AttemptRefusal::NotFound));
        };
        let stored_kind_tag = stored_kind_tag.to_owned();
        let subject_key = projection
            .subject_key
            .text()
            .map_err(attempt_column_corruption)?
            .to_owned();
        match projection.into_record_projection().decode_attempt_record() {
            Ok(record) if matches!(record.state, AttemptState::Retired { .. }) => {
                Ok(Ok((admin_kind, stored_kind_tag, InstanceKey(subject_key))))
            }
            Ok(_) | Err(_) => Ok(Err(AttemptRefusal::NotRetired)),
        }
    }

    /// Retry is the guarded delete in the caller's batch, which also re-arms.
    pub async fn plan_retired_attempt_retry<AdminMachineKind: Copy + Eq + Send + Sync>(
        &self,
        registry: &AdminMachineKindRegistry<'_, AdminMachineKind>,
        attempt_id: uuid::Uuid,
    ) -> Result<Result<GuardedStatement, AttemptRefusal>, StoreFailure> {
        Ok(self
            .read_retired_attempt_identity(registry, attempt_id)
            .await?
            .map(|_| plan_retired_attempt_delete(attempt_id)))
    }

    /// Resolves a retired attempt and runs its preparing host's registered finisher.
    ///
    /// A same-kind subject statements value cannot be substituted at the admin entry.
    ///
    /// ```compile_fail,E0061
    /// use central_engine_model::Machine;
    /// use fleet_engine::{DiscardSubjectStatements, FleetEngine, EngineHost};
    ///
    /// async fn replace_finisher<Host: EngineHost, SubjectMachine: Machine>(
    ///     engine: &FleetEngine<'_, Host>,
    ///     attempt_id: uuid::Uuid,
    ///     mut same_kind: DiscardSubjectStatements<SubjectMachine>,
    ///     event: SubjectMachine::Event,
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
        let (admin_kind, stored_machine_kind_tag, subject_key) = match self
            .read_retired_attempt_identity(self.0.admin_machine_kind_registry(), attempt_id)
            .await?
        {
            Ok(attempt) => attempt,
            Err(refusal) => return Ok(DiscardOutcome::Refused(refusal)),
        };
        self.0
            .finish_registered_discard(PreparedAttemptDiscard {
                preparing_host: self.0,
                attempt_id,
                admin_machine_kind: admin_kind,
                stored_machine_kind_tag,
                subject_key,
                admission_and_receipt,
            })
            .await
    }

    /// Discard reads only the finishing view: a corrupt payload does not stop it;
    /// only a lifecycle state that will not decode answers SubjectUnreadable,
    /// with nothing changed (Retry still works).
    async fn apply_discard_to_subject<SubjectMachine, SubjectStore>(
        &self,
        adapter: &SubjectStore,
        key: InstanceKey,
        event: SubjectMachine::Event,
        source: &DiscardEventSource,
    ) -> Result<DiscardOutcome, StoreFailure>
    where
        SubjectMachine: Machine,
        SubjectMachine::State: Send + Sync,
        SubjectMachine::Event: Send + Sync,
        SubjectMachine::Effect: Send + Sync,
        SubjectMachine::Error: Send + Sync,
        SubjectStore: super::machine_store_adapter::MachineStoreAdapter<SubjectMachine>,
    {
        match adapter.load_subject_state(&key).await {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(DiscardOutcome::Refused(AttemptRefusal::DiscardSuperseded)),
            Err(super::machine_store_adapter::SubjectLoadFailure::CorruptSubjectRow { .. }) => {
                return Ok(DiscardOutcome::Refused(AttemptRefusal::SubjectUnreadable));
            }
            Err(super::machine_store_adapter::SubjectLoadFailure::StorageAnswerLost) => {
                return Err(StoreFailure::AnswerLost);
            }
            Err(super::machine_store_adapter::SubjectLoadFailure::StoreReadFailed(failure)) => {
                return Err(failure);
            }
        }
        map_subject_discard_outcome(
            self.apply_machine_event::<SubjectMachine, SubjectStore, _>(
                adapter, key, event, source,
            )
            .await,
        )
    }
}

impl<Host: EngineHost> PreparedAttemptDiscard<'_, Host> {
    pub fn admin_machine_kind(&self) -> Host::AdminMachineKind {
        self.admin_machine_kind
    }

    /// Runs the typed finisher selected inside the host registration callback.
    pub async fn commit_subject_discard<SubjectMachine>(
        mut self,
        store: DiscardSubjectStatements<SubjectMachine>,
        event: SubjectMachine::Event,
    ) -> Result<DiscardOutcome, StoreFailure>
    where
        SubjectMachine: Machine,
        SubjectMachine::State: Send + Sync,
        SubjectMachine::Event: Send + Sync,
        SubjectMachine::Effect: Send + Sync,
        SubjectMachine::Error: Send + Sync,
    {
        if self.stored_machine_kind_tag != SubjectMachine::KIND.0 {
            return Ok(DiscardOutcome::Refused(AttemptRefusal::DiscardUnsupported));
        }
        self.admission_and_receipt
            .insert(0, plan_retired_attempt_delete(self.attempt_id));
        FleetEngine::new(self.preparing_host)
            .apply_discard_to_subject::<SubjectMachine, _>(
                &BoundDiscardStore {
                    preparing_host: self.preparing_host,
                    discard_subject_statements: store,
                },
                self.subject_key,
                event,
                &DiscardEventSource(self.admission_and_receipt),
            )
            .await
    }
}

fn map_subject_discard_outcome<State, Error>(
    outcome: MachineEventOutcome<State, Error>,
) -> Result<DiscardOutcome, StoreFailure> {
    match outcome {
        MachineEventOutcome::TransitionCommitted(_) => Ok(DiscardOutcome::Committed),
        // The subject moved, finished, vanished or is unreadable: nothing changed.
        MachineEventOutcome::Superseded
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

#[cfg(test)]
#[path = "attempt_administration_test.rs"]
mod tests;
