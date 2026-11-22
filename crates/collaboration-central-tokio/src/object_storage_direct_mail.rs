//! Lend the stock SQLite transaction to the portable direct-message owner.
use super::{
    AlarmPublication, AlarmPublicationFailure, AlarmPublicationStatus, FleetTime,
    NativeTransaction, ObjectStorage, StoreFailure,
};
use fleet_engine::{CheckedDueFailure, FailureRecord};
use fleet_mail::{
    AdmittedDirectSend, AdmittedReceipt, ApplyMark, ApplyReceipt, DirectMark, DirectMessageStore,
    DirectOffer, DirectOfferLookup, DirectReadFailure, DirectSendLookup, DirectSendResult,
    DirectWriteFailure, OfferPageLookup, OwnedDirectStatus, RejectedDirectMark,
    RetainedDirectSendRead, direct_mark_apply, direct_store_reads,
    direct_store_writes::{self, DirectApplyFailure, DirectWriteOutcome},
};
use futures_util::future::BoxFuture;
use std::ops::ControlFlow;
use workspace_model::central_mail::MessageStatus;

macro_rules! native_read {
    ($name:ident, $command:ty, $answer:ty) => {
        fn $name(&self, command: $command) -> BoxFuture<'_, Result<$answer, DirectReadFailure>> {
            Box::pin(async move {
                let mut connection = self
                    .workspace_pool()
                    .await
                    .map_err(DirectReadFailure::from_store)?
                    .acquire()
                    .await
                    .map_err(|_| DirectReadFailure::Unavailable)?;
                direct_store_reads::$name(&mut NativeTransaction(&mut connection), command).await
            })
        }
    };
}

macro_rules! native_write {
    ($owner:ident, $name:ident, $command:ty, $answer:ty) => {
        fn $name(&self, command: $command) -> BoxFuture<'_, Result<$answer, DirectWriteFailure>> {
            Box::pin(async move {
                self.write_direct_with_engine_alarm(async move |connection| {
                    $owner::$name(&mut NativeTransaction(connection), command).await
                })
                .await
            })
        }
    };
}

impl DirectMessageStore for ObjectStorage {
    native_read!(read_retained_send, DirectSendLookup, RetainedDirectSendRead);
    native_read!(read_status, OwnedDirectStatus, MessageStatus);
    native_read!(
        read_offer_page,
        OfferPageLookup,
        Vec<fleet_contracts::mail_link::WindowedDelivery>
    );
    native_read!(read_offer, DirectOfferLookup, Option<DirectOffer>);

    native_write!(
        direct_store_writes,
        apply_direct_send,
        AdmittedDirectSend,
        DirectSendResult
    );
    native_write!(
        direct_store_writes,
        apply_receipt,
        AdmittedReceipt,
        ApplyReceipt
    );
    fn apply_mark(
        &self,
        command: DirectMark,
    ) -> BoxFuture<'_, Result<ApplyMark, CheckedDueFailure>> {
        Box::pin(async move {
            #[cfg(feature = "test-support")]
            let lose_answer_after_commit = match self
                .faults
                .take_direct_mark_answer_loss(&command.message_id)
            {
                Some(fleet_runtime::DirectMarkAnswerLoss::BeforeApplying) => {
                    return Err(CheckedDueFailure::AnswerLost);
                }
                Some(fleet_runtime::DirectMarkAnswerLoss::AfterCommit) => true,
                None => false,
            };
            match self
                .write_checked_due(async move |connection| {
                    direct_mark_apply::apply_mark(&mut NativeTransaction(connection), command).await
                })
                .await?
            {
                ControlFlow::Continue(applied) => {
                    #[cfg(feature = "test-support")]
                    if lose_answer_after_commit {
                        return Err(CheckedDueFailure::AnswerLost);
                    }
                    Ok(applied)
                }
                ControlFlow::Break(rollback) => rollback.after_rollback(),
            }
        })
    }

    fn charge_rejected_mark(
        &self,
        rejection: Box<RejectedDirectMark>,
        now: workspace_model::Timestamp,
    ) -> BoxFuture<'_, Result<Option<FailureRecord>, CheckedDueFailure>> {
        Box::pin(async move {
            match self
                .write_checked_due(async move |connection| {
                    direct_mark_apply::charge_rejected_mark(
                        &mut NativeTransaction(connection),
                        *rejection,
                        now,
                    )
                    .await
                })
                .await?
            {
                ControlFlow::Continue(charged) => Ok(charged),
                ControlFlow::Break(failure) => Err(failure),
            }
        })
    }
}

impl ObjectStorage {
    async fn write_checked_due<Answer, Rejected>(
        &self,
        apply: impl AsyncFnOnce(&mut sqlx::SqliteConnection) -> Result<Answer, Rejected>,
    ) -> Result<ControlFlow<Rejected, Answer>, CheckedDueFailure> {
        let _order = self.lock_batch_order().await;
        self.opened()
            .await
            .map_err(CheckedDueFailure::from_store)?
            .run_transaction(async move |connection| {
                Ok(match apply(connection).await {
                    Ok(answer) => ControlFlow::Continue(answer),
                    Err(rollback) => ControlFlow::Break(rollback),
                })
            })
            .await
            .map_err(CheckedDueFailure::from_store)
    }

    async fn write_direct_with_engine_alarm<Answer>(
        &self,
        apply: impl AsyncFnOnce(
            &mut sqlx::SqliteConnection,
        ) -> Result<DirectWriteOutcome<Answer>, DirectApplyFailure>,
    ) -> Result<Answer, DirectWriteFailure> {
        let _order = self.lock_batch_order().await;
        let store = self
            .opened()
            .await
            .map_err(DirectWriteFailure::from_store)?;
        let settled = store
            .run_transaction(async move |connection| {
                let applied = match apply(connection).await {
                    Ok(applied) => applied,
                    Err(failure) => return Ok(ControlFlow::Break(failure)),
                };
                let alarm = if applied.alarm.is_some() {
                    if self.engine_publication.is_none() {
                        return Err(StoreFailure::AlarmUnpublishable {
                            reason: AlarmPublicationFailure::ClockUnavailable,
                        });
                    }
                    Some(Self::read_engine_inputs(connection).await?)
                } else {
                    None
                };
                Ok(ControlFlow::Continue((applied.outcome, alarm)))
            })
            .await
            .map_err(DirectWriteFailure::from_store)?;

        let (outcome, alarm) = match settled {
            ControlFlow::Break(error) => return Err(error.into_write_failure()),
            ControlFlow::Continue(committed) => committed,
        };
        if let (Some(alarm), Some(engine)) = (alarm, self.engine_publication.as_ref()) {
            #[cfg(feature = "test-support")]
            self.central_publication_pauses
                .reached_central(fleet_runtime::PausePoint::AfterStoreApplied)
                .await;
            let published = self.publish_engine_alarm(
                alarm.earliest_on_grid_due_unix_millis,
                alarm.earliest_legacy_due_unix_millis,
            );
            let status = match published {
                AlarmPublication::Set { .. } | AlarmPublication::Cleared => {
                    AlarmPublicationStatus::Published {
                        deadline: self
                            .alarm
                            .borrow()
                            .map(|at| FleetTime::from_unix_millis(at.unix_millis())),
                    }
                }
                AlarmPublication::Failed { .. } | AlarmPublication::Unchanged => {
                    AlarmPublicationStatus::Failed {
                        at: engine.clock.now(),
                    }
                }
            };
            self.publication_status.send_replace(status);
        }
        Ok(outcome)
    }
}
