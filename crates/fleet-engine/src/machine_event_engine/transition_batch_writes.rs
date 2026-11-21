use super::{
    attempt_statements::plan_attempt_delete,
    event_source_policy::ReceiptStatements,
    machine_store_adapter::{MachineStoreAdapter, VersionedSubjectState},
};
use central_engine_model::{InstanceKey, ItemFailureClass, Machine};
use central_storage::{AlarmUpdate, BatchOutcome, ConstraintKind, StatementBatch, StoreFailure};
use fleet_runtime_ports::CentralStore;
use std::future::Future;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BatchApplyFailure {
    Superseded,
    Rejected(ItemFailureClass),
    StorageAnswerLost,
    StorageFailed(StoreFailure),
}

pub(crate) fn classify_batch_application(
    result: Result<BatchOutcome, StoreFailure>,
) -> Result<(), BatchApplyFailure> {
    match result {
        Ok(BatchOutcome::Applied { .. }) => Ok(()),
        Ok(BatchOutcome::GuardFailed { .. }) => Err(BatchApplyFailure::Superseded),
        Ok(BatchOutcome::ConstraintViolated { kind, .. }) => Err(BatchApplyFailure::Rejected(
            ItemFailureClass::ConstraintViolated { constraint: kind },
        )),
        Ok(BatchOutcome::StatementRejected { .. }) => Err(BatchApplyFailure::Rejected(
            ItemFailureClass::StatementRejected,
        )),
        Err(StoreFailure::AnswerLost) => Err(BatchApplyFailure::StorageAnswerLost),
        Err(failure @ (StoreFailure::Unavailable | StoreFailure::AlarmUnpublishable { .. })) => {
            Err(BatchApplyFailure::StorageFailed(failure))
        }
        Err(StoreFailure::Corrupt { reason }) => Err(BatchApplyFailure::Rejected(
            super::machine_store_adapter::classify_corrupt_subject_row(reason),
        )),
    }
}

fn classify_batch_with_receipt_claim(
    result: Result<BatchOutcome, StoreFailure>,
    request_claim: bool,
) -> Result<(), BatchApplyFailure> {
    if request_claim
        && matches!(
            &result,
            Ok(BatchOutcome::ConstraintViolated {
                index: 1,
                kind: ConstraintKind::Unique
            })
        )
    {
        return Err(BatchApplyFailure::Superseded);
    }
    classify_batch_application(result)
}

/// The CentralStore supplies the platform transaction; the adapter supplies the
/// machine's statements. Both modern hosts use this same effect handler.
pub(crate) trait TransitionBatchWriter {
    fn apply_guarded_transition_batch<SubjectMachine, SubjectStore>(
        &self,
        adapter: &SubjectStore,
        key: &InstanceKey,
        prior: &VersionedSubjectState<SubjectMachine::State>,
        next: &SubjectMachine::State,
        effects: &[SubjectMachine::Effect],
        receipt: ReceiptStatements,
    ) -> impl Future<Output = Result<(), BatchApplyFailure>> + Send
    where
        SubjectMachine: Machine,
        SubjectMachine::State: Sync,
        SubjectMachine::Effect: Sync,
        SubjectStore: MachineStoreAdapter<SubjectMachine>;
}

pub(crate) struct CentralTransitionBatchWriter<'a, StoreBackend: CentralStore> {
    pub central_store: &'a StoreBackend,
    #[cfg(feature = "test-support")]
    pub test_pause_points: &'a fleet_runtime::PausePoints,
}

impl<StoreBackend: CentralStore> TransitionBatchWriter
    for CentralTransitionBatchWriter<'_, StoreBackend>
{
    async fn apply_guarded_transition_batch<SubjectMachine, SubjectStore>(
        &self,
        adapter: &SubjectStore,
        key: &InstanceKey,
        prior: &VersionedSubjectState<SubjectMachine::State>,
        next: &SubjectMachine::State,
        effects: &[SubjectMachine::Effect],
        receipt: ReceiptStatements,
    ) -> Result<(), BatchApplyFailure>
    where
        SubjectMachine: Machine,
        SubjectMachine::State: Sync,
        SubjectMachine::Effect: Sync,
        SubjectStore: MachineStoreAdapter<SubjectMachine>,
    {
        let request_claim = matches!(&receipt, ReceiptStatements::RequestReceiptWrites(_));
        // R7: a request that changes state re-publishes the alarm in its own
        // atomic batch, so rows, receipt and alarm commit together. SubjectStore due unit
        // keeps the alarm unchanged; its due batch publishes once at its end.
        let alarm = match &receipt {
            ReceiptStatements::None => AlarmUpdate::Unchanged,
            ReceiptStatements::RequestReceiptWrites(_)
            | ReceiptStatements::AdminReceiptWrites(_) => {
                AlarmUpdate::Engine(central_storage::EngineAlarmPlan::Recompute)
            }
        };
        let mut receipt = receipt.into_statements();
        let statements = (|| {
            receipt.push(adapter.plan_subject_state_write(key, prior, next)?);
            for effect in effects {
                receipt.extend(adapter.plan_machine_effect_writes(key, effect)?);
            }
            receipt.push(plan_attempt_delete(&adapter.failure_attempt_subject(key)));
            Ok::<_, StoreFailure>(receipt)
        })()
        .map_err(|failure| match failure {
            StoreFailure::Corrupt { reason } => BatchApplyFailure::Rejected(
                super::machine_store_adapter::classify_corrupt_subject_row(reason),
            ),
            StoreFailure::AnswerLost => BatchApplyFailure::StorageAnswerLost,
            failure @ (StoreFailure::Unavailable | StoreFailure::AlarmUnpublishable { .. }) => {
                BatchApplyFailure::StorageFailed(failure)
            }
        })?;
        #[cfg(feature = "test-support")]
        self.test_pause_points
            .reached_central(fleet_runtime::PausePoint::BeforeTimerCommit)
            .await;
        classify_batch_with_receipt_claim(
            self.central_store
                .run_batch(StatementBatch { statements, alarm })
                .await,
            request_claim,
        )?;
        #[cfg(feature = "test-support")]
        self.test_pause_points
            .reached_central(fleet_runtime::PausePoint::AfterTimerCommit)
            .await;
        Ok(())
    }
}

impl<StoreBackend: CentralStore> CentralTransitionBatchWriter<'_, StoreBackend> {
    pub(crate) async fn apply_receipt_only_batch(
        &self,
        receipt: ReceiptStatements,
    ) -> Result<(), BatchApplyFailure> {
        let request_claim = matches!(&receipt, ReceiptStatements::RequestReceiptWrites(_));
        classify_batch_with_receipt_claim(
            self.central_store
                .run_batch(StatementBatch {
                    statements: receipt.into_statements(),
                    alarm: AlarmUpdate::Unchanged,
                })
                .await,
            request_claim,
        )
    }
}
