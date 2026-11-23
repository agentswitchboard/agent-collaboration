//! The storage the central link driver keeps its outbox in. Production is
//! machine_local.sqlite through MacOutbox. Contract cases exercise the storage
//! API independently of link scheduling.
#[cfg(test)]
use crate::outbox_store::StoredOutboxWrite;
use crate::outbox_store::{ClaimedBatch, MacOutbox, NewOutboxItem, OutboxInsert, OutboxStoreError};
use crate::{
    EndedWrite, OutboxRequestStatus, OutboxWriteOutcome, OutgoingWrite, PendingWrite,
    StoredSenderOutcome,
};
use crate::{MachineLocalStore, OutboxRequestKey};
use std::{future::Future, pin::Pin};
use workspace_ids::RequestId;
use workspace_model::Caller;

pub type OutboxFuture<'call, T> =
    Pin<Box<dyn Future<Output = Result<T, OutboxStoreError>> + Send + 'call>>;

/// Every outbox operation the driver makes, with MacOutbox's meaning.
pub trait OutboxStorage: Send {
    fn insert<'call>(
        &'call mut self,
        item: &'call NewOutboxItem,
        now: i64,
    ) -> OutboxFuture<'call, OutboxInsert>;
    fn request_status<'call>(
        &'call mut self,
        caller: &'call Caller,
        request: &'call RequestId,
    ) -> OutboxFuture<'call, OutboxRequestStatus>;
    fn scoped_request_status<'call>(
        &'call mut self,
        caller: &'call Caller,
        key: &'call OutboxRequestKey,
    ) -> OutboxFuture<'call, OutboxRequestStatus>;
    fn pending_for<'call>(
        &'call mut self,
        caller: &'call Caller,
    ) -> OutboxFuture<'call, Vec<PendingWrite>>;
    fn outgoing_for<'call>(
        &'call mut self,
        caller: &'call Caller,
        limit: u16,
    ) -> OutboxFuture<'call, Vec<OutgoingWrite>>;
    fn delete_ended_before_retention(&mut self, now: i64) -> OutboxFuture<'_, u64>;
    fn waiting(&mut self) -> OutboxFuture<'_, Vec<(i64, RequestId)>>;
    /// The earliest receipt mark among waiting writes.
    fn earliest_waiting(&mut self) -> OutboxFuture<'_, Option<i64>>;
    /// The earliest expiry among waiting writes, even when the link is down.
    fn earliest_deadline(&mut self) -> OutboxFuture<'_, Option<i64>>;
    /// Up to `limit` writes due by `mark` and before their deadline, oldest
    /// first by (created_at, request_id), moved to sent in one transaction
    /// that first ends every overdue waiting write (outbox_deadline).
    fn claim_next_batch(
        &mut self,
        mark: i64,
        limit: u16,
        now: i64,
    ) -> OutboxFuture<'_, ClaimedBatch>;
    /// Ends every waiting write past its deadline.
    fn end_overdue(&mut self, now: i64) -> OutboxFuture<'_, Vec<EndedWrite>>;
    /// Recover the original acceptance and retain the typed late delivery fact.
    /// True means the caller's pending inbox handoff can notify its sink.
    fn record_sender_delivery<'call>(
        &'call mut self,
        caller: &'call Caller,
        notice: &'call StoredSenderOutcome,
        now: i64,
    ) -> OutboxFuture<'call, bool>;
    fn sender_outcome<'call>(
        &'call mut self,
        caller: &'call Caller,
        request: &'call RequestId,
    ) -> OutboxFuture<'call, Option<StoredSenderOutcome>>;
    fn sent_without_answer(&mut self) -> OutboxFuture<'_, Vec<RequestId>>;
    #[cfg(test)]
    fn mark_sent<'call>(
        &'call mut self,
        requests: &'call [RequestId],
        now: i64,
    ) -> OutboxFuture<'call, Vec<StoredOutboxWrite>>;
    fn record_outcome<'call>(
        &'call mut self,
        request: &'call RequestId,
        outcome: &'call OutboxWriteOutcome,
        now: i64,
    ) -> OutboxFuture<'call, ()>;
    fn return_not_transmitted<'call>(
        &'call mut self,
        request: &'call RequestId,
        now: i64,
    ) -> OutboxFuture<'call, ()>;
    /// Sent writes back to waiting; one past its deadline ends instead.
    fn return_unanswered<'call>(
        &'call mut self,
        requests: &'call [RequestId],
        now: i64,
    ) -> OutboxFuture<'call, Vec<EndedWrite>>;
}

/// The production outbox: machine_local.sqlite, through MacOutbox. A separate
/// owner type so machine_local_store does not depend on outbox_store.
pub struct MachineLocalOutbox {
    store: MachineLocalStore,
}

impl MachineLocalOutbox {
    #[must_use]
    pub fn new(store: MachineLocalStore) -> Self {
        Self { store }
    }
}

impl OutboxStorage for MachineLocalOutbox {
    fn scoped_request_status<'call>(
        &'call mut self,
        caller: &'call Caller,
        key: &'call OutboxRequestKey,
    ) -> OutboxFuture<'call, OutboxRequestStatus> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .scoped_request_status(caller, key)
                .await
        })
    }
    fn insert<'call>(
        &'call mut self,
        item: &'call NewOutboxItem,
        now: i64,
    ) -> OutboxFuture<'call, OutboxInsert> {
        Box::pin(async move { MacOutbox::new(&mut self.store).insert(item, now).await })
    }
    fn request_status<'call>(
        &'call mut self,
        caller: &'call Caller,
        request: &'call RequestId,
    ) -> OutboxFuture<'call, OutboxRequestStatus> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .request_status(caller, request)
                .await
        })
    }
    fn pending_for<'call>(
        &'call mut self,
        caller: &'call Caller,
    ) -> OutboxFuture<'call, Vec<PendingWrite>> {
        Box::pin(async move { MacOutbox::new(&mut self.store).pending_for(caller).await })
    }
    fn outgoing_for<'call>(
        &'call mut self,
        caller: &'call Caller,
        limit: u16,
    ) -> OutboxFuture<'call, Vec<OutgoingWrite>> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .outgoing_for(caller, limit)
                .await
        })
    }
    fn delete_ended_before_retention(&mut self, now: i64) -> OutboxFuture<'_, u64> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .delete_ended_before_retention(now)
                .await
        })
    }
    fn waiting(&mut self) -> OutboxFuture<'_, Vec<(i64, RequestId)>> {
        Box::pin(async move { MacOutbox::new(&mut self.store).waiting().await })
    }
    fn earliest_waiting(&mut self) -> OutboxFuture<'_, Option<i64>> {
        Box::pin(async move { MacOutbox::new(&mut self.store).earliest_waiting().await })
    }
    fn earliest_deadline(&mut self) -> OutboxFuture<'_, Option<i64>> {
        Box::pin(async move { MacOutbox::new(&mut self.store).earliest_deadline().await })
    }
    fn claim_next_batch(
        &mut self,
        mark: i64,
        limit: u16,
        now: i64,
    ) -> OutboxFuture<'_, ClaimedBatch> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .claim_next_batch(mark, limit, now)
                .await
        })
    }
    fn end_overdue(&mut self, now: i64) -> OutboxFuture<'_, Vec<EndedWrite>> {
        Box::pin(async move { MacOutbox::new(&mut self.store).end_overdue(now).await })
    }
    fn record_sender_delivery<'call>(
        &'call mut self,
        caller: &'call Caller,
        notice: &'call StoredSenderOutcome,
        now: i64,
    ) -> OutboxFuture<'call, bool> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .record_sender_delivery(caller, notice, now)
                .await
        })
    }
    fn sender_outcome<'call>(
        &'call mut self,
        caller: &'call Caller,
        request: &'call RequestId,
    ) -> OutboxFuture<'call, Option<StoredSenderOutcome>> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .sender_outcome(caller, request)
                .await
        })
    }
    fn sent_without_answer(&mut self) -> OutboxFuture<'_, Vec<RequestId>> {
        Box::pin(async move { MacOutbox::new(&mut self.store).sent_without_answer().await })
    }
    #[cfg(test)]
    fn mark_sent<'call>(
        &'call mut self,
        requests: &'call [RequestId],
        now: i64,
    ) -> OutboxFuture<'call, Vec<StoredOutboxWrite>> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .mark_sent(requests, now)
                .await
        })
    }
    fn record_outcome<'call>(
        &'call mut self,
        request: &'call RequestId,
        outcome: &'call OutboxWriteOutcome,
        now: i64,
    ) -> OutboxFuture<'call, ()> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .record_outcome(request, outcome, now)
                .await
        })
    }
    fn return_not_transmitted<'call>(
        &'call mut self,
        request: &'call RequestId,
        now: i64,
    ) -> OutboxFuture<'call, ()> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .return_not_transmitted(request, now)
                .await
        })
    }
    fn return_unanswered<'call>(
        &'call mut self,
        requests: &'call [RequestId],
        now: i64,
    ) -> OutboxFuture<'call, Vec<EndedWrite>> {
        Box::pin(async move {
            MacOutbox::new(&mut self.store)
                .return_unanswered(requests, now)
                .await
        })
    }
}

#[cfg(test)]
#[path = "outbox_storage_contract_test.rs"]
mod contract_tests;
