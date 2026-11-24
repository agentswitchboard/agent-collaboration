//! The PR 307 one-shot store faults, applied through the current outbox port.

#![expect(
    clippy::unwrap_used,
    reason = "A poisoned test fault journal invalidates the proof."
)]

use std::{
    collections::BTreeSet,
    io,
    sync::{Arc, Mutex},
};
use switchboard_machine_core_store::{
    ClaimedBatch, EndedWrite, NewOutboxItem, OutboxFuture, OutboxInsert, OutboxRequestKey,
    OutboxRequestStatus, OutboxStorage, OutboxStoreError, OutboxWriteOutcome, OutgoingWrite,
    PendingWrite, StoreScan, StoredSenderOutcome, UnansweredOutboxClaim,
};
use tokio::sync::{Notify, oneshot};
use workspace_ids::RequestId;
use workspace_model::Caller;

/// The campaign chooses which outbox store call to fail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FaultSite {
    Insert,
    ScopedStatus,
    EarliestWaiting,
    SentWithoutAnswer,
    EndOverdue,
    RetentionPrune,
    ClaimBatch,
    RecordOutcome,
    RecordSenderNotice,
    ReturnNotTransmitted,
    ReturnUnanswered,
}

/// The campaign chooses when the store call loses its answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FaultStage {
    BeforeCall,
    AfterSuccess,
}

#[derive(Default)]
struct FaultJournal {
    planned: Vec<(FaultSite, FaultStage)>,
    fired: Vec<(FaultSite, FaultStage)>,
    insert_entry: Option<InsertEntry>,
    committed_insert: Option<Arc<StoreCommitGate>>,
    committed_claim: Option<Arc<StoreCommitGate>>,
    committed_claim_return: Option<Arc<StoreCommitGate>>,
    scoped_read_gate: Option<Arc<ScopedReadGate>>,
    scoped_reads_unavailable: bool,
}

struct InsertEntry {
    entered: Arc<Notify>,
    release: oneshot::Receiver<InsertRelease>,
}

#[derive(Debug)]
pub(super) enum InsertRelease {
    Continue,
    Panic,
}

/// A test holds the worker after a SQLite commit and before the store returns.
#[derive(Default)]
pub(super) struct StoreCommitGate {
    pub(super) committed: Notify,
    pub(super) release: Notify,
}

impl StoreCommitGate {
    fn hold_after<'call, ResultType: Send + 'call>(
        gate: Option<Arc<Self>>,
        operation: OutboxFuture<'call, ResultType>,
    ) -> OutboxFuture<'call, ResultType> {
        let Some(gate) = gate else {
            return operation;
        };
        Box::pin(async move {
            let committed = operation.await?;
            gate.committed.notify_one();
            gate.release.notified().await;
            Ok(committed)
        })
    }
}

/// A test stops custody inside one local status lookup until it publishes the competing event.
#[derive(Default)]
pub(super) struct ScopedReadGate {
    pub(super) entered: Notify,
    pub(super) release: Notify,
}

/// The campaign uses a plan to arm and observe faults at the store boundary.
#[derive(Clone, Default)]
pub(super) struct StoreFaultPlan(Arc<Mutex<FaultJournal>>);

impl StoreFaultPlan {
    pub(super) fn arm(&self, site: FaultSite, stage: FaultStage) {
        self.0.lock().unwrap().planned.push((site, stage));
    }

    pub(super) fn fired(&self) -> Vec<(FaultSite, FaultStage)> {
        self.0.lock().unwrap().fired.clone()
    }

    pub(super) fn is_drained(&self) -> bool {
        self.0.lock().unwrap().planned.is_empty()
    }

    pub(super) fn hold_next_insert_commit(&self) -> Arc<StoreCommitGate> {
        let gate = Arc::new(StoreCommitGate::default());
        self.0.lock().unwrap().committed_insert = Some(gate.clone());
        gate
    }

    pub(super) fn hold_next_insert_entry(&self) -> (Arc<Notify>, oneshot::Sender<InsertRelease>) {
        let entered = Arc::new(Notify::new());
        let (release, action) = oneshot::channel();
        self.0.lock().unwrap().insert_entry = Some(InsertEntry {
            entered: entered.clone(),
            release: action,
        });
        (entered, release)
    }

    pub(super) fn hold_next_claim_commit(&self) -> Arc<StoreCommitGate> {
        let gate = Arc::new(StoreCommitGate::default());
        self.0.lock().unwrap().committed_claim = Some(gate.clone());
        gate
    }

    pub(super) fn hold_next_claim_return_commit(&self) -> Arc<StoreCommitGate> {
        let gate = Arc::new(StoreCommitGate::default());
        self.0.lock().unwrap().committed_claim_return = Some(gate.clone());
        gate
    }

    pub(super) fn hold_next_scoped_read(&self) -> Arc<ScopedReadGate> {
        let gate = Arc::new(ScopedReadGate::default());
        self.0.lock().unwrap().scoped_read_gate = Some(gate.clone());
        gate
    }

    pub(super) fn fail_scoped_reads(&self, unavailable: bool) {
        self.0.lock().unwrap().scoped_reads_unavailable = unavailable;
    }

    fn wrap<'call, ResultType: Send + 'call>(
        &self,
        site: FaultSite,
        operation: OutboxFuture<'call, ResultType>,
    ) -> OutboxFuture<'call, ResultType> {
        let plan = self.clone();
        Box::pin(async move {
            let stage = {
                let mut journal = plan.0.lock().unwrap();
                journal
                    .planned
                    .iter()
                    .position(|(armed, _)| *armed == site)
                    .map(|index| journal.planned.remove(index).1)
            };
            match stage {
                None => operation.await,
                Some(stage) => {
                    // Record after-call faults only after observing success of
                    // the delegated transaction or read.
                    if stage == FaultStage::AfterSuccess {
                        operation.await?;
                    }
                    plan.0.lock().unwrap().fired.push((site, stage));
                    let source = io::Error::other(format!("scripted {site:?} {stage:?}")).into();
                    if site == FaultSite::Insert && stage == FaultStage::BeforeCall {
                        Err(OutboxStoreError::InsertNotStarted(source))
                    } else {
                        Err(OutboxStoreError::Database(source))
                    }
                }
            }
        })
    }
}

/// The fixture wraps a real store and injects the faults the campaign arms.
pub(super) struct FaultyOutbox {
    pub(super) inner: Box<dyn OutboxStorage>,
    pub(super) plan: StoreFaultPlan,
}

impl OutboxStorage for FaultyOutbox {
    fn insert<'call>(
        &'call mut self,
        item: &'call NewOutboxItem,
        now: i64,
    ) -> OutboxFuture<'call, OutboxInsert> {
        let (entry, commit) = {
            let mut plan = self.plan.0.lock().unwrap();
            (plan.insert_entry.take(), plan.committed_insert.take())
        };
        let insert = self
            .plan
            .wrap(FaultSite::Insert, self.inner.insert(item, now));
        let insert = StoreCommitGate::hold_after(commit, insert);
        let Some(entry) = entry else {
            return insert;
        };
        Box::pin(async move {
            entry.entered.notify_one();
            assert!(
                matches!(entry.release.await.unwrap(), InsertRelease::Continue),
                "scripted worker panic before the outbox insert"
            );
            insert.await
        })
    }

    fn request_status<'call>(
        &'call mut self,
        caller: &'call Caller,
        request: &'call RequestId,
    ) -> OutboxFuture<'call, OutboxRequestStatus> {
        self.inner.request_status(caller, request)
    }

    fn scoped_request_status<'call>(
        &'call mut self,
        caller: &'call Caller,
        key: &'call OutboxRequestKey,
    ) -> OutboxFuture<'call, OutboxRequestStatus> {
        let (gate, unavailable) = {
            let mut journal = self.plan.0.lock().unwrap();
            (
                journal.scoped_read_gate.take(),
                journal.scoped_reads_unavailable,
            )
        };
        let reading = self.plan.wrap(
            FaultSite::ScopedStatus,
            self.inner.scoped_request_status(caller, key),
        );
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            if unavailable {
                return Err(OutboxStoreError::Database(
                    io::Error::other("scripted persistent scoped read failure").into(),
                ));
            }
            reading.await
        })
    }

    fn pending_for<'call>(
        &'call mut self,
        caller: &'call Caller,
    ) -> OutboxFuture<'call, Vec<PendingWrite>> {
        self.inner.pending_for(caller)
    }

    fn outgoing_for<'call>(
        &'call mut self,
        caller: &'call Caller,
        limit: u16,
    ) -> OutboxFuture<'call, Vec<OutgoingWrite>> {
        self.inner.outgoing_for(caller, limit)
    }

    fn delete_ended_before_retention(&mut self, now: i64) -> OutboxFuture<'_, StoreScan<u64>> {
        self.plan.wrap(
            FaultSite::RetentionPrune,
            self.inner.delete_ended_before_retention(now),
        )
    }

    fn earliest_waiting(&mut self) -> OutboxFuture<'_, StoreScan<Option<i64>>> {
        self.plan
            .wrap(FaultSite::EarliestWaiting, self.inner.earliest_waiting())
    }

    fn earliest_deadline(&mut self) -> OutboxFuture<'_, StoreScan<Option<i64>>> {
        self.inner.earliest_deadline()
    }

    fn claim_next_batch<'call>(
        &'call mut self,
        mark_ms: i64,
        limit: u16,
        now_ms: i64,
        required_deadline_after_ms: i64,
        excluded_request_ids: &'call BTreeSet<RequestId>,
    ) -> OutboxFuture<'call, StoreScan<ClaimedBatch>> {
        let gate = self.plan.0.lock().unwrap().committed_claim.take();
        let claiming = self.plan.wrap(
            FaultSite::ClaimBatch,
            self.inner.claim_next_batch(
                mark_ms,
                limit,
                now_ms,
                required_deadline_after_ms,
                excluded_request_ids,
            ),
        );
        StoreCommitGate::hold_after(gate, claiming)
    }

    fn end_overdue(&mut self, now: i64) -> OutboxFuture<'_, StoreScan<Vec<EndedWrite>>> {
        self.plan
            .wrap(FaultSite::EndOverdue, self.inner.end_overdue(now))
    }

    fn record_sender_delivery<'call>(
        &'call mut self,
        caller: &'call Caller,
        notice: &'call StoredSenderOutcome,
        now: i64,
    ) -> OutboxFuture<'call, bool> {
        self.plan.wrap(
            FaultSite::RecordSenderNotice,
            self.inner.record_sender_delivery(caller, notice, now),
        )
    }

    fn sender_outcome<'call>(
        &'call mut self,
        caller: &'call Caller,
        request: &'call RequestId,
    ) -> OutboxFuture<'call, Option<StoredSenderOutcome>> {
        self.inner.sender_outcome(caller, request)
    }

    fn sent_without_answer(&mut self) -> OutboxFuture<'_, StoreScan<Vec<UnansweredOutboxClaim>>> {
        self.plan.wrap(
            FaultSite::SentWithoutAnswer,
            self.inner.sent_without_answer(),
        )
    }

    fn end_overdue_orphans<'call>(
        &'call mut self,
        now_ms: i64,
        live_request_ids: &'call BTreeSet<RequestId>,
    ) -> OutboxFuture<'call, StoreScan<Vec<EndedWrite>>> {
        self.inner.end_overdue_orphans(now_ms, live_request_ids)
    }

    fn record_outcome<'call>(
        &'call mut self,
        request: &'call RequestId,
        outcome: &'call OutboxWriteOutcome,
        now: i64,
    ) -> OutboxFuture<'call, ()> {
        self.plan.wrap(
            FaultSite::RecordOutcome,
            self.inner.record_outcome(request, outcome, now),
        )
    }

    fn record_late_outcome<'call>(
        &'call mut self,
        request: &'call RequestId,
        outcome: &'call OutboxWriteOutcome,
    ) -> OutboxFuture<'call, ()> {
        self.inner.record_late_outcome(request, outcome)
    }

    fn return_not_transmitted<'call>(
        &'call mut self,
        request: &'call RequestId,
        now: i64,
    ) -> OutboxFuture<'call, ()> {
        let gate = self.plan.0.lock().unwrap().committed_claim_return.take();
        let returning = self.plan.wrap(
            FaultSite::ReturnNotTransmitted,
            self.inner.return_not_transmitted(request, now),
        );
        StoreCommitGate::hold_after(gate, returning)
    }

    fn return_unanswered<'call>(
        &'call mut self,
        requests: &'call [RequestId],
        now: i64,
    ) -> OutboxFuture<'call, StoreScan<Vec<EndedWrite>>> {
        let gate = self.plan.0.lock().unwrap().committed_claim_return.take();
        let returning = self.plan.wrap(
            FaultSite::ReturnUnanswered,
            self.inner.return_unanswered(requests, now),
        );
        StoreCommitGate::hold_after(gate, returning)
    }
}
