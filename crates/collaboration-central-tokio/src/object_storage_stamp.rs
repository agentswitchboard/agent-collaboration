//! Stock SQLite binding for the sign-in stamp's owning transaction.
use super::{
    AlarmPublicationFailure, BatchOutcome, NativeTransaction, ObjectStorage, StoreFailure,
};
use fleet_signin::{
    organization_authority::StoredStampRead,
    stamp_store::{StampStore, StampWrite, StampWriteStop, read_stamp},
};
use futures_util::future::BoxFuture;
use std::ops::ControlFlow;

impl StampStore for ObjectStorage {
    fn read_stamp(&self) -> BoxFuture<'_, Result<StoredStampRead, StoreFailure>> {
        Box::pin(async move {
            self.opened()
                .await?
                .read_checked(async |connection| read_stamp(connection).await)
                .await
        })
    }

    fn write_stamp(
        &self,
        stamp_write: StampWrite,
    ) -> BoxFuture<'_, Result<BatchOutcome, StoreFailure>> {
        Box::pin(async move {
            let _order = self.lock_batch_order().await;
            let result = self.write_stamp_in_order(stamp_write).await;
            self.record_publication_status(true, &result);
            result
        })
    }
}

impl ObjectStorage {
    async fn write_stamp_in_order(
        &self,
        stamp_write: StampWrite,
    ) -> Result<BatchOutcome, StoreFailure> {
        if self.engine_publication.is_none() {
            return Err(StoreFailure::AlarmUnpublishable {
                reason: AlarmPublicationFailure::ClockUnavailable,
            });
        }
        match self
            .opened()
            .await?
            .run_transaction(async move |connection| {
                let affected_rows = match stamp_write
                    .apply_stamp_and_audit(&mut NativeTransaction(connection))
                    .await
                {
                    Ok(affected_rows) => affected_rows,
                    Err(stop) => return Ok(ControlFlow::Break(stop)),
                };
                let alarm_inputs = Self::read_engine_inputs(connection).await?;
                Ok(ControlFlow::Continue((affected_rows, alarm_inputs)))
            })
            .await?
        {
            ControlFlow::Continue((affected_rows, alarm_inputs)) => {
                #[cfg(feature = "test-support")]
                self.central_publication_pauses
                    .reached_central(fleet_runtime::PausePoint::AfterStoreApplied)
                    .await;
                let publication = self.publish_engine_alarm(
                    alarm_inputs.earliest_on_grid_due_unix_millis,
                    alarm_inputs.earliest_legacy_due_unix_millis,
                );
                #[cfg(feature = "test-support")]
                if let central_storage::AlarmPublication::Failed { reason } = &publication {
                    self.publication_failures
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(*reason);
                }
                Ok(BatchOutcome::Applied {
                    rows_affected: affected_rows.into(),
                    publication,
                })
            }
            ControlFlow::Break(StampWriteStop::Guard {
                index,
                rows_affected,
            }) => Ok(BatchOutcome::GuardFailed {
                index,
                rows_affected,
            }),
            ControlFlow::Break(StampWriteStop::Sql { index, source }) => {
                fleet_store_sqlx::batch_statement_failure(&source, index)
            }
            ControlFlow::Break(StampWriteStop::Store(failure)) => Err(failure),
        }
    }
}
