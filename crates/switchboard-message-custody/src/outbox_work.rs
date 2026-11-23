//! Durable work selected by the minute scheduler and the separate expiry wake.

use central_link_client::WelcomeClockAnchor;
use central_link_protocol::MAX_LINK_REQUEST_BATCH_SIZE;
use router_clock::{ClockInstant, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{sync::Arc, time::Duration};
use switchboard_machine_core_store::{ClaimedBatch, EndedWrite, OutboxStorage, OutboxStoreError};
use workspace_ids::RequestId;

use crate::{
    custody_operator::{CustodyOperatorEvent, outbox_scan},
    flush_clock::shared_time_at,
    flush_marks::mark_in_window,
};

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
        let elapsed = self
            .clock
            .monotonic_now()
            .saturating_duration_since(self.local_epoch.sampled_at);
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

    /// Read the first waiting receipt mark for the scheduler, including unanswered replays.
    pub async fn earliest_waiting_mark(&mut self) -> Result<Option<SyncTime>, OutboxWorkError> {
        let scanned = self.store.earliest_waiting().await?;
        Ok(outbox_scan(scanned, &mut self.operator_events).map(SyncTime::from_unix_millis))
    }

    /// The exact local expiry wake, independent of lane admission and minute jitter.
    pub async fn earliest_deadline(&mut self) -> Result<Option<ClockInstant>, OutboxWorkError> {
        let scanned = self.store.earliest_deadline().await?;
        Ok(
            outbox_scan(scanned, &mut self.operator_events).map(|deadline_ms| {
                let elapsed_ms = deadline_ms.saturating_sub(self.local_epoch.unix_millis);
                self.local_epoch.sampled_at.after(Duration::from_millis(
                    u64::try_from(elapsed_ms).unwrap_or(0),
                ))
            }),
        )
    }

    /// End overdue waiting rows and return the typed notices for their senders.
    pub async fn end_due_writes(&mut self) -> Result<Vec<EndedWrite>, OutboxWorkError> {
        let now_ms = self.local_now_ms();
        let scanned = self.store.end_overdue(now_ms).await?;
        Ok(outbox_scan(scanned, &mut self.operator_events))
    }

    /// Recover orphaned claims when no batch owned by this task is in flight.
    /// The store retains the original request id and deadline across the return.
    pub async fn recover_orphaned_claims(&mut self) -> Result<Vec<EndedWrite>, OutboxWorkError> {
        let scanned = self.store.sent_without_answer().await?;
        let unanswered = outbox_scan(scanned, &mut self.operator_events);
        if unanswered.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.return_unanswered(&unanswered).await?)
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
        if !self.mark_is_open(anchor, mark) {
            return Ok(FlushSelection::OutsideWindow);
        }
        let limit = u16::try_from(MAX_LINK_REQUEST_BATCH_SIZE)
            .map_err(|_| OutboxWorkError::InvalidBatchLimit)?;
        let now_ms = self.local_now_ms();
        let scanned = self
            .store
            .claim_next_batch(mark.unix_millis(), limit, now_ms)
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
