//! Durable work selected by the minute scheduler and the separate expiry wake.

use central_link_client::{LINK_LANE_SETTLEMENT_LIMIT, WelcomeClockAnchor};
use central_link_protocol::MAX_LINK_REQUEST_BATCH_SIZE;
use router_clock::{ClockInstant, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    ClaimedBatch, EndedWrite, OutboxStorage, OutboxStoreError, UnansweredOutboxClaim,
};
use workspace_ids::RequestId;
use workspace_model::Timestamp;

use crate::{
    custody_operator::{CustodyOperatorEvent, outbox_scan},
    flush_clock::shared_time_at,
    flush_marks::mark_in_window,
};

const ORIGINAL_WRITE_WINDOW_MS: i64 = 15 * 60 * 1_000;

/// The result of asking the outbox to claim a batch on the selected shared mark.
#[derive(Debug)]
pub enum FlushSelection {
    OutsideWindow,
    Claimed(ClaimedBatch),
}

/// A failure reported while reading or changing durable outbox work.
#[derive(Debug, thiserror::Error)]
pub enum OutboxWorkError {
    #[error("outbox work could not be read or updated")]
    Store(#[from] OutboxStoreError),
    #[error("link batch limit does not fit the outbox store")]
    InvalidBatchLimit,
    #[error("the claim's required settlement deadline is outside the local clock range")]
    InvalidSettlementHorizon,
}

/// The custody task owns this store adapter and gives it the shared injected clock.
pub struct OutboxWork {
    store: Box<dyn OutboxStorage>,
    clock: Arc<dyn RuntimeClock>,
    local_epoch: LocalOutboxEpoch,
    operator_events: Vec<CustodyOperatorEvent>,
}

// The local durable timestamps and the server's minute grid are separate
// projections of the same elapsed clock. The local projection survives a
// wall-clock jump within this process and samples UTC again on reopen.
struct LocalOutboxEpoch {
    unix_millis: i64,
    sampled_at: ClockInstant,
}

impl OutboxWork {
    #[must_use]
    pub fn new(store: Box<dyn OutboxStorage>, clock: Arc<dyn RuntimeClock>) -> Self {
        let local_epoch = LocalOutboxEpoch {
            unix_millis: clock.now().unix_millis(),
            sampled_at: clock.monotonic_now(),
        };
        Self {
            store,
            clock,
            local_epoch,
            operator_events: Vec::new(),
        }
    }

    /// Current time in the durable store's local Unix-millisecond time domain.
    #[must_use]
    pub fn local_now_ms(&self) -> i64 {
        self.local_time_at(self.clock.monotonic_now())
    }

    pub(crate) fn local_time_at(&self, observed_at: ClockInstant) -> i64 {
        let elapsed = observed_at.saturating_duration_since(self.local_epoch.sampled_at);
        self.local_epoch
            .unix_millis
            .saturating_add(i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
    }

    pub(crate) fn outbox_storage(&mut self) -> &mut dyn OutboxStorage {
        self.store.as_mut()
    }

    /// Return accumulated row diagnostics to the custody task's operator output.
    pub fn take_operator_events(&mut self) -> Vec<CustodyOperatorEvent> {
        std::mem::take(&mut self.operator_events)
    }

    pub(crate) fn report_operator_event(&mut self, event: CustodyOperatorEvent) {
        self.operator_events.push(event);
    }

    pub(crate) fn report_point_read_error(&mut self, error: &OutboxStoreError) {
        if let OutboxStoreError::CorruptItem { request_id, field } = error {
            self.report_operator_event(CustodyOperatorEvent::CorruptOutboxPoint {
                request_id: request_id.clone(),
                field,
            });
        }
    }

    pub(crate) fn before_elapsed_fence(&self, fence: ClockInstant) -> bool {
        self.clock.monotonic_now() < fence
    }

    pub(crate) fn has_settlement_time(&self, deadline_ms: i64) -> bool {
        self.clock.monotonic_now().after(LINK_LANE_SETTLEMENT_LIMIT)
            < self.local_wake_at(deadline_ms)
    }

    /// Read the first waiting receipt mark for the scheduler, including unanswered replays.
    pub async fn earliest_waiting_mark(&mut self) -> Result<Option<SyncTime>, OutboxWorkError> {
        let scanned = self.store.earliest_waiting().await?;
        Ok(outbox_scan(scanned, &mut self.operator_events).map(SyncTime::from_unix_millis))
    }

    pub(crate) async fn orphan_claims(
        &mut self,
    ) -> Result<Vec<UnansweredOutboxClaim>, OutboxWorkError> {
        let scan = self.store.sent_without_answer().await?;
        Ok(outbox_scan(scan, &mut self.operator_events))
    }

    /// The exact local expiry wake, independent of lane admission and minute jitter.
    pub async fn earliest_deadline(&mut self) -> Result<Option<ClockInstant>, OutboxWorkError> {
        let scanned = self.store.earliest_deadline().await?;
        let deadline = outbox_scan(scanned, &mut self.operator_events);
        Ok(deadline.map(|deadline_ms| self.local_wake_at(deadline_ms)))
    }

    pub(crate) fn write_send_fence(&self, original_send: Timestamp) -> ClockInstant {
        let deadline = self.write_deadline(original_send);
        ClockInstant::ZERO.after(
            deadline
                .saturating_duration_since(ClockInstant::ZERO)
                .saturating_sub(LINK_LANE_SETTLEMENT_LIMIT),
        )
    }

    pub(crate) fn write_deadline(&self, original_send: Timestamp) -> ClockInstant {
        self.local_wake_at(
            original_send
                .timestamp_millis()
                .saturating_add(ORIGINAL_WRITE_WINDOW_MS),
        )
    }

    pub(crate) fn local_wake_at(&self, deadline_ms: i64) -> ClockInstant {
        let elapsed_ms = deadline_ms.saturating_sub(self.local_epoch.unix_millis);
        self.local_epoch.sampled_at.after(Duration::from_millis(
            u64::try_from(elapsed_ms).unwrap_or(0),
        ))
    }

    /// End overdue waiting rows and return the typed notices for their senders.
    pub async fn end_due_writes(&mut self) -> Result<Vec<EndedWrite>, OutboxWorkError> {
        let now_ms = self.local_now_ms();
        let scanned = self.store.end_overdue(now_ms).await?;
        Ok(outbox_scan(scanned, &mut self.operator_events))
    }

    /// End only due genuine orphans, preserving claims with evidence still held by the worker.
    pub async fn end_due_orphans(
        &mut self,
        protected_ids: &BTreeSet<RequestId>,
    ) -> Result<Vec<EndedWrite>, OutboxWorkError> {
        let now_ms = self.local_now_ms();
        let scanned = self
            .store
            .end_overdue_orphans(now_ms, protected_ids)
            .await?;
        Ok(outbox_scan(scanned, &mut self.operator_events))
    }

    pub(crate) async fn end_selected_claim(
        &mut self,
        request: &RequestId,
    ) -> Result<Vec<EndedWrite>, OutboxStoreError> {
        let scan = self.store.sent_without_answer().await?;
        let mut excluded = outbox_scan(scan, &mut self.operator_events)
            .into_iter()
            .map(|claim| claim.key.request_id)
            .collect::<BTreeSet<_>>();
        if !excluded.remove(request) {
            return Ok(Vec::new());
        }
        let now_ms = self.local_now_ms();
        let ended = self.store.end_overdue_orphans(now_ms, &excluded).await?;
        Ok(outbox_scan(ended, &mut self.operator_events))
    }

    pub(crate) async fn return_unanswered(
        &mut self,
        requests: &[RequestId],
    ) -> Result<Vec<EndedWrite>, OutboxStoreError> {
        let now_ms = self.local_now_ms();
        let scanned = self.store.return_unanswered(requests, now_ms).await?;
        Ok(outbox_scan(scanned, &mut self.operator_events))
    }

    /// Claim up to the protocol limit on a selected mark of the admitted link's clock.
    pub async fn claim_at_mark(
        &mut self,
        anchor: WelcomeClockAnchor,
        mark: SyncTime,
    ) -> Result<FlushSelection, OutboxWorkError> {
        self.claim_due_through(anchor, mark, mark, &BTreeSet::new())
            .await
    }

    pub(crate) async fn claim_due_through(
        &mut self,
        anchor: WelcomeClockAnchor,
        mark: SyncTime,
        eligible_through: SyncTime,
        excluded_request_ids: &BTreeSet<RequestId>,
    ) -> Result<FlushSelection, OutboxWorkError> {
        if !self.mark_is_open(anchor, mark) {
            return Ok(FlushSelection::OutsideWindow);
        }
        let limit = u16::try_from(MAX_LINK_REQUEST_BATCH_SIZE)
            .map_err(|_| OutboxWorkError::InvalidBatchLimit)?;
        let now_ms = self.local_now_ms();
        let settlement_budget_ms = i64::try_from(LINK_LANE_SETTLEMENT_LIMIT.as_millis())
            .map_err(|_| OutboxWorkError::InvalidSettlementHorizon)?;
        let required_deadline_after_ms = now_ms
            .checked_add(settlement_budget_ms)
            .ok_or(OutboxWorkError::InvalidSettlementHorizon)?;
        let scanned = self
            .store
            .claim_next_batch(
                eligible_through.unix_millis(),
                limit,
                now_ms,
                required_deadline_after_ms,
                excluded_request_ids,
            )
            .await?;
        Ok(FlushSelection::Claimed(outbox_scan(
            scanned,
            &mut self.operator_events,
        )))
    }

    /// Recheck after storage I/O and before handing a command to the driver.
    #[must_use]
    pub fn mark_is_open(&self, anchor: WelcomeClockAnchor, mark: SyncTime) -> bool {
        mark_in_window(shared_time_at(anchor, self.clock.monotonic_now())) == Some(mark)
    }

    /// The periodic retention pass is separate from selecting and confirming a batch.
    pub async fn prune_ended_writes(&mut self) -> Result<u64, OutboxWorkError> {
        let now_ms = self.local_now_ms();
        let scanned = self.store.delete_ended_before_retention(now_ms).await?;
        Ok(outbox_scan(scanned, &mut self.operator_events))
    }
}
