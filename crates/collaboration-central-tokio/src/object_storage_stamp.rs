//! Stock SQLite binding for the sign-in stamp's owning transaction.
use super::{AlarmPublicationFailure, BatchOutcome, ObjectStorage, StoreFailure};
use fleet_signin::{
    organization_authority::StoredStampRead,
    stamp_store::{StampStore, StampTransaction, StampWrite, StampWriteStop, read_stamp},
};
use futures_util::future::BoxFuture;
use sqlx::{Executor, Sqlite, SqliteConnection, sqlite::SqliteQueryResult};
use std::ops::ControlFlow;

struct NativeStampTransaction<'connection>(&'connection mut SqliteConnection);

impl StampTransaction for NativeStampTransaction<'_> {
    type Database = Sqlite;
    fn executor(&mut self) -> impl Executor<'_, Database = Sqlite> {
        &mut *self.0
    }
    fn rows_affected(result: SqliteQueryResult) -> u64 {
        result.rows_affected()
    }
}

impl StampStore for ObjectStorage {
    fn read_stamp(&self) -> BoxFuture<'_, Result<StoredStampRead, StoreFailure>> {
        Box::pin(async move {
            self.opened()
                .await?
                .read_checked(async |connection| read_stamp(connection).await)
                .await
        })
    }

    fn write_stamp(&self, write: StampWrite) -> BoxFuture<'_, Result<BatchOutcome, StoreFailure>> {
        Box::pin(async move {
            let _order = self.lock_batch_order().await;
            let result = self.write_stamp_in_order(write).await;
            self.record_publication_status(true, &result);
            result
        })
    }
}

impl ObjectStorage {
    async fn write_stamp_in_order(&self, write: StampWrite) -> Result<BatchOutcome, StoreFailure> {
        if self.engine_publication.is_none() {
            return Err(StoreFailure::AlarmUnpublishable {
                reason: AlarmPublicationFailure::ClockUnavailable,
            });
        }
        match self
            .opened()
            .await?
            .run_transaction(async move |connection| {
                let rows = match write.apply(&mut NativeStampTransaction(connection)).await {
                    Ok(rows) => rows,
                    Err(stop) => return Ok(ControlFlow::Break(stop)),
                };
                let times = Self::read_engine_inputs(connection).await?;
                Ok(ControlFlow::Continue((rows, times)))
            })
            .await?
        {
            ControlFlow::Continue((rows, times)) => {
                #[cfg(feature = "test-support")]
                self.central_publication_pauses
                    .reached_central(fleet_runtime::PausePoint::AfterStoreApplied)
                    .await;
                let publication = self.publish_engine_alarm(
                    times.earliest_on_grid_due_unix_millis,
                    times.earliest_legacy_due_unix_millis,
                );
                #[cfg(feature = "test-support")]
                if let central_storage::AlarmPublication::Failed { reason } = &publication {
                    self.publication_failures
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(*reason);
                }
                Ok(BatchOutcome::Applied {
                    rows_affected: rows.into(),
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
