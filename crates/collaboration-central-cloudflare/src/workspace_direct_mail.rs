//! Lend this Central object's transaction to the portable direct-message owner.
use super::{AlarmPolicy, CentralStatements, SendFuture, statement_store};
use fleet_engine::{CheckedDueFailure, FailureRecord};
use fleet_mail::{
    AdmittedDirectSend, AdmittedReceipt, ApplyMark, ApplyReceipt, DirectMark, DirectMessageStore,
    DirectOffer, DirectOfferLookup, DirectReadFailure, DirectSendLookup, DirectSendResult,
    DirectWriteFailure, OfferPageLookup, OwnedDirectStatus, RejectedDirectMark,
    RetainedDirectSendRead, direct_mark_apply, direct_store_reads, direct_store_writes,
};
use futures_util::future::BoxFuture;
use std::ops::ControlFlow;
use workspace_model::central_mail::MessageStatus;

macro_rules! worker_read {
    ($name:ident, $command:ty, $answer:ty) => {
        fn $name(&self, command: $command) -> BoxFuture<'_, Result<$answer, DirectReadFailure>> {
            Box::pin(SendFuture::new(async move {
                direct_store_reads::$name(
                    &mut statement_store::WorkerTransaction::new(&*self.connection),
                    command,
                )
                .await
            }))
        }
    };
}
macro_rules! worker_write {
    ($owner:ident, $name:ident, $command:ty, $answer:ty) => {
        fn $name(&self, command: $command) -> BoxFuture<'_, Result<$answer, DirectWriteFailure>> {
            Box::pin(SendFuture::new(async move {
                statement_store::write_direct_with_engine_alarm(
                    &self.connection,
                    AlarmPolicy::Engine(
                        self.state.storage(),
                        self.clock.clone(),
                        self.direct_write_decider(),
                    ),
                    async move |transaction| {
                        $owner::$name(
                            &mut statement_store::WorkerTransaction::new(transaction),
                            command,
                        )
                        .await
                    },
                )
                .await
            }))
        }
    };
}
impl CentralStatements {
    fn direct_write_decider(&self) -> central_storage::EngineAlarmDecider {
        let decider = self.decide_alarm.clone();
        #[cfg(feature = "test-support")]
        {
            let zero_next = self.zero_next_write_alarm.clone();
            std::sync::Arc::new(move |times, now, publish| {
                let value = decider(times, now, publish)?;
                if zero_next.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    Ok(Some(0))
                } else {
                    Ok(value)
                }
            })
        }
        #[cfg(not(feature = "test-support"))]
        decider
    }
}

impl DirectMessageStore for CentralStatements {
    worker_read!(read_retained_send, DirectSendLookup, RetainedDirectSendRead);
    worker_read!(read_status, OwnedDirectStatus, MessageStatus);
    worker_read!(
        read_offer_page,
        OfferPageLookup,
        Vec<fleet_contracts::mail_link::WindowedDelivery>
    );
    worker_read!(read_offer, DirectOfferLookup, Option<DirectOffer>);
    worker_write!(
        direct_store_writes,
        apply_direct_send,
        AdmittedDirectSend,
        DirectSendResult
    );
    worker_write!(
        direct_store_writes,
        apply_receipt,
        AdmittedReceipt,
        ApplyReceipt
    );
    fn apply_mark(
        &self,
        command: DirectMark,
    ) -> BoxFuture<'_, Result<ApplyMark, CheckedDueFailure>> {
        Box::pin(SendFuture::new(async move {
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
            match statement_store::write_checked_due(&self.connection, async move |transaction| {
                direct_mark_apply::apply_mark(
                    &mut statement_store::WorkerTransaction::new(transaction),
                    command,
                )
                .await
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
        }))
    }

    fn charge_rejected_mark(
        &self,
        rejection: Box<RejectedDirectMark>,
        now: workspace_model::Timestamp,
    ) -> BoxFuture<'_, Result<Option<FailureRecord>, CheckedDueFailure>> {
        Box::pin(SendFuture::new(async move {
            match statement_store::write_checked_due(&self.connection, async move |transaction| {
                direct_mark_apply::charge_rejected_mark(
                    &mut statement_store::WorkerTransaction::new(transaction),
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
        }))
    }
}
