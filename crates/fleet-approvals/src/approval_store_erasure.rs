//! Approval retention and retired-marker cleanup on the applying loan.
use approval_request_protocol::{ApprovalRequestId, CentralTime, StopReceiptEnding};
use central_engine_model::{InstanceKey, next_five_minute_mark};
use central_storage::{
    GuardedStatement, RowExpectation, SafeInteger, SqlValue, Statement, StoreFailure,
};
use fleet_engine::{
    DueItemKey, DueKind, DueTableChange, checked_engine_statement, plan_due_item_attempt_clear,
    plan_due_table_change,
};
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};
use workspace_ids::RouterRunId;

use super::{ApprovalApplyFailure, ApprovalApplyingStore, ApprovalTransitionOutput};
use crate::{
    ApprovalEnrollmentStanding, ApprovalHolderScope, ApprovalReplyAddress, ApprovalRequestRowId,
    ApprovalState, ApprovalStoreTransaction, PendingApprovalReply,
    approval_lifecycle_effects::standing,
    approval_retention::cutoff_at,
    approval_store_reads::{
        ApprovalReadRecord, ApprovalRunRecord, read_approval_run, read_indexed_approval_due_subject,
    },
    approval_store_writes::{
        approval_due_key, approval_grid_mark, encode_stored_json, plan_approval_due_at,
    },
};

const RETIRED_RUN_PREFIX: &str = "retired-run:";
const ENDED_ENROLLMENT_PREFIX: &str = "ended-enrollment:";
const MAX_RETIRED_MARKERS_PER_BATCH: i64 = 64;
const MAX_ENDED_RUNS_PER_BATCH: i64 = 64;

enum ApprovalDueSubject {
    Request(ApprovalRequestRowId),
    RetiredRun(RouterRunId),
    EndedEnrollment(RouterRunId),
}

impl ApprovalDueSubject {
    fn from_key(key: &InstanceKey) -> Result<Self, StoreFailure> {
        if let Some(run) = key.0.strip_prefix(RETIRED_RUN_PREFIX) {
            return run
                .parse()
                .map(Self::RetiredRun)
                .map_err(|_| corrupt("approval_router_runs.router_run_id"));
        }
        if let Some(run) = key.0.strip_prefix(ENDED_ENROLLMENT_PREFIX) {
            return run
                .parse()
                .map(Self::EndedEnrollment)
                .map_err(|_| corrupt("approval_router_runs.enrollment_cleanup_ms"));
        }
        Ok(Self::Request(ApprovalRequestRowId::parse(&key.0)?))
    }
}

fn retired_run_key(run: &RouterRunId) -> DueItemKey {
    DueItemKey {
        kind: DueKind::ApprovalRequestExpiry,
        subject: InstanceKey(format!("{RETIRED_RUN_PREFIX}{}", run.as_str())),
    }
}

fn ended_enrollment_key(anchor: &str) -> DueItemKey {
    DueItemKey {
        kind: DueKind::ApprovalRequestExpiry,
        subject: InstanceKey(format!("{ENDED_ENROLLMENT_PREFIX}{anchor}")),
    }
}

struct EraseApproval {
    row_id: ApprovalRequestRowId,
    scope: ApprovalHolderScope,
    request_id: ApprovalRequestId,
    ending: StopReceiptEnding,
    cutoff: CentralTime,
}

impl EraseApproval {
    fn admit(
        record: ApprovalReadRecord,
        occurrence: CentralTime,
        at: CentralTime,
    ) -> Result<Option<Self>, StoreFailure> {
        let ApprovalState::Ended { saved, closed } = record.state else {
            return Ok(None);
        };
        let cutoff = cutoff_at(closed.ended_at)?;
        if occurrence != cutoff || at < cutoff {
            return Ok(None);
        }
        Ok(Some(Self {
            row_id: record.row_id,
            scope: record.scope,
            request_id: saved.key.request_id,
            ending: standing(&closed.ending.into_ending()),
            cutoff,
        }))
    }
}

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: crate::ApprovalSessionFacts<Transaction>,
{
    /// Match the indexed occurrence without decoding the potentially corrupt row.
    pub async fn approval_due_is_current(
        transaction: &mut Transaction,
        key: &InstanceKey,
        occurrence: CentralTime,
        now: CentralTime,
    ) -> Result<bool, StoreFailure> {
        if now < occurrence {
            return Ok(false);
        }
        let at = time_bind(occurrence)?;
        let query = checked_engine_statement!(
            query_file,
            [],
            "src/queries/read_approval_due_occurrence.sql",
            key = key.0.clone(),
            occurrence = at,
        );
        Ok(!transaction
            .read_approval_statement(query)
            .await?
            .rows
            .is_empty())
    }

    pub async fn apply_approval_due_work(
        &mut self,
        key: &InstanceKey,
        occurrence: CentralTime,
    ) -> Result<PendingApprovalReply<()>, ApprovalApplyFailure> {
        let at = self.sample_applying_time()?;
        match ApprovalDueSubject::from_key(key)? {
            ApprovalDueSubject::Request(row_id) => {
                if let Some(record) =
                    read_indexed_approval_due_subject(self.transaction, &row_id).await?
                {
                    if matches!(record.state, ApprovalState::Waiting { .. }) {
                        return self.apply_approval_deadline(key, occurrence).await;
                    }
                    if let Some(command) = EraseApproval::admit(record, occurrence, at)? {
                        self.erase_approval(command, at).await?;
                    }
                }
            }
            ApprovalDueSubject::RetiredRun(run) => {
                let record = read_approval_run(self.transaction, &run)
                    .await?
                    .filter(|owner| {
                        owner.scope.organization_id == *self.transaction.organization_id()
                            && owner.retired_at == Some(occurrence)
                    })
                    .ok_or_else(|| corrupt("approval_router_runs.retired_at"))?;
                self.clean_retired_markers(&record, occurrence, at).await?;
            }
            ApprovalDueSubject::EndedEnrollment(anchor) => {
                let record = read_approval_run(self.transaction, &anchor)
                    .await?
                    .filter(|owner| {
                        owner.scope.organization_id == *self.transaction.organization_id()
                            && owner.enrollment_cleanup_at == Some(occurrence)
                    })
                    .ok_or_else(|| corrupt("approval_router_runs.enrollment_cleanup_ms"))?;
                if self
                    .transaction
                    .approval_enrollment_standing(&record.scope.enrollment_scope())
                    .await?
                    == ApprovalEnrollmentStanding::Ended
                {
                    self.clean_ended_enrollment(&record.scope, occurrence, at)
                        .await?;
                }
            }
        }
        Ok(self.stage_reply(
            ApprovalReplyAddress::Maintenance,
            ApprovalTransitionOutput::with_reply(()),
        ))
    }

    async fn erase_approval(
        &mut self,
        command: EraseApproval,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        let row = command.row_id.as_str();
        let organization = command.scope.organization_id.as_str();
        let run_id = &command.scope.run.router_run;
        if let Some(run) = read_approval_run(self.transaction, run_id).await? {
            if run.scope != command.scope {
                return Err(corrupt("approval_requests.router_run_id").into());
            }
            if run.retired_at.is_none() {
                let marker = checked_engine_statement!(
                    query_file,
                    [],
                    "src/queries/insert_erased_approval.sql",
                    row_id = row.to_owned(),
                    organization = organization.to_owned(),
                    request_id = command.request_id.as_str().to_owned(),
                    run_id = run_id.as_str().to_owned(),
                    ending = encode_stored_json(
                        &command.ending,
                        "approval_erased_requests.ending_json"
                    )?,
                    at = format_utc_millis(at),
                );
                self.execute_guarded_write(guarded(marker, RowExpectation::Exactly(1)))
                    .await?;
            }
        }

        let destinations = checked_engine_statement!(
            query_file,
            [],
            "src/queries/delete_approval_destinations_for_erasure.sql",
            row_id = row.to_owned(),
        );
        let operations = checked_engine_statement!(
            query_file,
            [],
            "src/queries/delete_approval_operation_results.sql",
            row_id = row.to_owned(),
        );
        for statement in [destinations, operations] {
            self.execute_guarded_write(guarded(statement, RowExpectation::Any))
                .await?;
        }
        let request = checked_engine_statement!(
            query_file,
            [],
            "src/queries/delete_erased_approval_request.sql",
            organization = organization.to_owned(),
            row_id = row.to_owned(),
            cutoff = SafeInteger::try_from(command.cutoff.timestamp_millis())
                .map_err(|_| corrupt("approval_requests.erase_after_ms"))?,
        );
        self.execute_guarded_write(guarded(request, RowExpectation::Exactly(1)))
            .await?;
        self.remove_approval_due(approval_due_key(&command.row_id), at)
            .await
    }

    pub(super) async fn publish_retired_marker_cleanup(
        &mut self,
        run: &RouterRunId,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        if self.retired_markers_remain(run).await? {
            self.schedule_retired_markers(run, at, Some(approval_grid_mark(at)?), at)
                .await?;
        }
        Ok(())
    }

    async fn clean_retired_markers(
        &mut self,
        owner: &ApprovalRunRecord,
        occurrence: CentralTime,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        let run = &owner.scope.run.router_run;
        let deletion = checked_engine_statement!(
            query_file,
            [],
            "src/queries/delete_retired_run_markers.sql",
            run = run.as_str().to_owned(),
            maximum = SafeInteger::try_from(MAX_RETIRED_MARKERS_PER_BATCH)
                .map_err(|_| corrupt("approval_erased_requests.cleanup_limit"))?,
        );
        self.execute_guarded_write(guarded(deletion, RowExpectation::Any))
            .await?;
        if self.retired_markers_remain(run).await? {
            self.schedule_retired_markers(run, occurrence, Some(later_mark(at)?), at)
                .await?;
        } else {
            self.schedule_retired_markers(run, occurrence, None, at)
                .await?;
            self.wake_enrollment_after_markers(&owner.scope, at).await?;
        }
        Ok(())
    }

    async fn schedule_retired_markers(
        &mut self,
        run: &RouterRunId,
        occurrence: CentralTime,
        due: Option<CentralTime>,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        let stamp = checked_engine_statement!(
            query_file,
            [],
            "src/queries/set_retired_marker_due.sql",
            timestamp = due.map(time_bind).transpose()?,
            run = run.as_str().to_owned(),
        );
        self.execute_guarded_write(guarded(stamp, RowExpectation::Exactly(1)))
            .await?;
        self.schedule_cleanup(retired_run_key(run), occurrence, due, at)
            .await
    }

    async fn retired_markers_remain(&mut self, run: &RouterRunId) -> Result<bool, StoreFailure> {
        let query = checked_engine_statement!(
            query_file,
            [],
            "src/queries/read_retired_run_marker.sql",
            run = run.as_str().to_owned(),
        );
        Ok(!self
            .transaction
            .read_approval_statement(query)
            .await?
            .rows
            .is_empty())
    }

    pub(super) async fn publish_ended_enrollment_cleanup(
        &mut self,
        anchor: &ApprovalHolderScope,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        let due = approval_grid_mark(at)?;
        let mark = checked_engine_statement!(
            query_file,
            [],
            "src/queries/set_approval_enrollment_cleanup.sql",
            at = SafeInteger::try_from(at.timestamp_millis())
                .map_err(|_| corrupt("approval_router_runs.enrollment_cleanup_ms"))?,
            due = time_bind(due)?,
            run = anchor.run.router_run.as_str().to_owned(),
            organization = anchor.organization_id.as_str().to_owned(),
        );
        self.execute_guarded_write(guarded(mark, RowExpectation::Exactly(1)))
            .await?;
        self.schedule_cleanup(
            ended_enrollment_key(anchor.run.router_run.as_str()),
            at,
            Some(due),
            at,
        )
        .await
    }

    async fn clean_ended_enrollment(
        &mut self,
        anchor: &ApprovalHolderScope,
        occurrence: CentralTime,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        let run = &anchor.run.router_run;
        let organization = anchor.organization_id.as_str();
        let enrollment = anchor.run.enrollment.enrollment_id.as_str();
        if self.enrollment_has_markers(anchor).await? {
            // The final marker page republishes this subject. Until then it owns no lane slot.
            return self
                .set_enrollment_due(run.as_str(), occurrence, None, at)
                .await;
        }
        let delete = checked_engine_statement!(
            query_file,
            [],
            "src/queries/delete_approval_enrollment_runs.sql",
            organization = organization.to_owned(),
            enrollment = enrollment.to_owned(),
            anchor = run.as_str().to_owned(),
            maximum = SafeInteger::try_from(MAX_ENDED_RUNS_PER_BATCH - 1)
                .map_err(|_| corrupt("approval_router_runs.enrollment_cleanup_limit"))?,
        );
        self.execute_guarded_write(guarded(delete, RowExpectation::Any))
            .await?;
        let remaining = checked_engine_statement!(
            query_file,
            [],
            "src/queries/read_other_approval_enrollment_run.sql",
            organization = organization.to_owned(),
            enrollment = enrollment.to_owned(),
            anchor = run.as_str().to_owned(),
        );
        if !self
            .transaction
            .read_approval_statement(remaining)
            .await?
            .rows
            .is_empty()
        {
            return self
                .set_enrollment_due(run.as_str(), occurrence, Some(later_mark(at)?), at)
                .await;
        }
        let last = checked_engine_statement!(
            query_file,
            [],
            "src/queries/delete_last_approval_enrollment_run.sql",
            anchor = run.as_str().to_owned(),
            organization = organization.to_owned(),
            at = SafeInteger::try_from(occurrence.timestamp_millis())
                .map_err(|_| corrupt("approval_router_runs.enrollment_cleanup_ms"))?,
        );
        self.execute_guarded_write(guarded(last, RowExpectation::Exactly(1)))
            .await?;
        self.remove_approval_due(ended_enrollment_key(run.as_str()), at)
            .await
    }

    async fn enrollment_has_markers(
        &mut self,
        anchor: &ApprovalHolderScope,
    ) -> Result<bool, StoreFailure> {
        let query = checked_engine_statement!(
            query_file,
            [],
            "src/queries/read_blocking_enrollment_marker.sql",
            organization = anchor.organization_id.as_str().to_owned(),
            enrollment = anchor.run.enrollment.enrollment_id.as_str().to_owned(),
            machine = anchor.run.enrollment.machine_id.as_str().to_owned(),
        );
        Ok(!self
            .transaction
            .read_approval_statement(query)
            .await?
            .rows
            .is_empty())
    }

    async fn wake_enrollment_after_markers(
        &mut self,
        owner: &ApprovalHolderScope,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        let result = self
            .transaction
            .read_approval_statement(checked_engine_statement!(
                query_file,
                [],
                "src/queries/read_parked_enrollment_cleanup.sql",
                organization = owner.organization_id.as_str().to_owned(),
                enrollment = owner.run.enrollment.enrollment_id.as_str().to_owned(),
                machine = owner.run.enrollment.machine_id.as_str().to_owned(),
            ))
            .await?;
        let [cells] = result.rows.as_slice() else {
            return Ok(());
        };
        if self.enrollment_has_markers(owner).await? {
            return Ok(());
        }
        // This is a handoff of indexed work. The anchor's own attempt validates
        // the run; the healthy marker page owns only the scheduling fields.
        let run = fleet_runtime_ports::stored_values::string(&result, cells, "router_run_id")?;
        let SqlValue::Integer(occurrence) =
            fleet_runtime_ports::stored_values::column(&result, cells, "enrollment_cleanup_ms")?
        else {
            return Err(corrupt("approval_router_runs.enrollment_cleanup_ms").into());
        };
        let occurrence = CentralTime::from_timestamp_millis(occurrence.get())
            .ok_or_else(|| corrupt("approval_router_runs.enrollment_cleanup_ms"))?;
        self.set_enrollment_due(run, occurrence, Some(later_mark(at)?), at)
            .await
    }

    async fn set_enrollment_due(
        &mut self,
        anchor: &str,
        occurrence: CentralTime,
        due: Option<CentralTime>,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        let statement = checked_engine_statement!(
            query_file,
            [],
            "src/queries/set_enrollment_cleanup_due.sql",
            due = due.map(time_bind).transpose()?,
            anchor = anchor.to_owned(),
            occurrence = time_bind(occurrence)?,
        );
        self.execute_guarded_write(guarded(statement, RowExpectation::Exactly(1)))
            .await?;
        self.schedule_cleanup(ended_enrollment_key(anchor), occurrence, due, at)
            .await
    }

    async fn schedule_cleanup(
        &mut self,
        key: DueItemKey,
        occurrence: CentralTime,
        due: Option<CentralTime>,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        let Some(due) = due else {
            return self.remove_approval_due(key, at).await;
        };
        self.execute_guarded_write(plan_due_item_attempt_clear(&key))
            .await?;
        for write in plan_approval_due_at(key, occurrence, due, at)? {
            self.execute_guarded_write(write).await?;
        }
        Ok(())
    }

    async fn remove_approval_due(
        &mut self,
        key: DueItemKey,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        self.execute_guarded_write(plan_due_item_attempt_clear(&key))
            .await?;
        for write in plan_due_table_change(&DueTableChange::RemoveItem(key), at)? {
            self.execute_guarded_write(write).await?;
        }
        Ok(())
    }
}

fn guarded(statement: Statement, expect: RowExpectation) -> GuardedStatement {
    GuardedStatement { statement, expect }
}

fn time_bind(at: CentralTime) -> Result<SafeInteger, StoreFailure> {
    SafeInteger::try_from(at.timestamp_millis())
        .map_err(|_| corrupt("approval_requests.due_occurrence"))
}

fn later_mark(at: CentralTime) -> Result<CentralTime, StoreFailure> {
    next_five_minute_mark(at).map_err(|_| corrupt("approval_requests.due_mark"))
}
