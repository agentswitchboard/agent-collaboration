use central_engine_model::{EngineTime, next_five_minute_mark, supported_time};
use central_storage::{AlarmPublicationFailure, StoreFailure};
use std::sync::{
    Mutex, MutexGuard, PoisonError,
    atomic::{AtomicI64, Ordering},
};
use tokio::sync::Notify;

const EMPTY: i64 = i64::MIN;
const LEFTOVERS: i64 = 1;
const PROTECTED: i64 = 2;
const FLAG_COUNT: i64 = 4;

#[derive(Clone, Copy)]
pub enum DueBatchTrigger {
    Request,
    Alarm,
}

enum ClaimOutcome {
    Started { previous_start: Option<EngineTime> },
    WaitForEarlierClaim,
    Deferred,
}

/// Keeps another batch from discovering the same rows before the cap is known.
/// Only capped batches keep this protection while applying their selected rows.
pub struct DueBatchClaim<'state> {
    state: &'state DueBatchState,
    started_at: EngineTime,
    previous_start: Option<EngineTime>,
}

impl DueBatchClaim<'_> {
    pub fn started_at(&self) -> EngineTime {
        self.started_at
    }

    pub fn previous_start(&self) -> Option<EngineTime> {
        self.previous_start
    }

    pub fn record_selection(&self, has_leftovers: bool) {
        self.state.update_claim(
            self.started_at.timestamp_millis(),
            Some(has_leftovers),
            has_leftovers,
        );
    }
}

impl Drop for DueBatchClaim<'_> {
    fn drop(&mut self) {
        self.state
            .update_claim(self.started_at.timestamp_millis(), None, false);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DueBatchSnapshot {
    pub last_start_unix_millis: Option<i64>,
    pub leftovers_pending: bool,
}

/// Test-support receipt of one unit a due batch started, in start order
/// (acceptance R2/R3 visit order). Never built without `test-support`.
#[cfg(feature = "test-support")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DueUnitStart {
    /// The start time of the due batch that started this unit.
    pub batch_start_unix_millis: i64,
    pub machine_kind: String,
    pub subject_key: String,
}

/// The start and flags share one word, so a publisher reads one coherent snapshot.
///
/// After a cold reopen between two marks, leftover capped work may be taken by
/// the next request before the next mark. The alarm itself is never set off the mark.
pub struct DueBatchState {
    encoded: AtomicI64,
    publication_order: Mutex<()>,
    claim_released: Notify,
    #[cfg(feature = "test-support")]
    claim_waiting: tokio::sync::watch::Sender<usize>,
    #[cfg(feature = "test-support")]
    unit_starts: Mutex<Vec<DueUnitStart>>,
    /// One entry per claimed batch, so two batches at one millisecond stay two.
    #[cfg(feature = "test-support")]
    claimed_starts: Mutex<Vec<i64>>,
}

impl Default for DueBatchState {
    fn default() -> Self {
        Self {
            encoded: AtomicI64::new(EMPTY),
            publication_order: Mutex::new(()),
            claim_released: Notify::new(),
            #[cfg(feature = "test-support")]
            claim_waiting: tokio::sync::watch::channel(0).0,
            #[cfg(feature = "test-support")]
            unit_starts: Mutex::new(Vec::new()),
            #[cfg(feature = "test-support")]
            claimed_starts: Mutex::new(Vec::new()),
        }
    }
}

fn snapshot(encoded: i64) -> DueBatchSnapshot {
    if encoded == EMPTY {
        DueBatchSnapshot {
            last_start_unix_millis: None,
            leftovers_pending: false,
        }
    } else {
        DueBatchSnapshot {
            last_start_unix_millis: Some(encoded.div_euclid(FLAG_COUNT)),
            leftovers_pending: encoded & LEFTOVERS != 0,
        }
    }
}

fn encode(start: i64, leftovers: bool, protected: bool) -> Option<i64> {
    let flags = i64::from(leftovers) * LEFTOVERS + i64::from(protected) * PROTECTED;
    start.checked_mul(FLAG_COUNT)?.checked_add(flags)
}

impl DueBatchState {
    pub fn snapshot(&self) -> DueBatchSnapshot {
        snapshot(self.encoded.load(Ordering::Acquire))
    }

    pub async fn begin_batch(
        &self,
        sample_clock: impl Fn() -> Result<EngineTime, StoreFailure>,
        spacing: chrono::Duration,
        trigger: DueBatchTrigger,
    ) -> Result<Option<DueBatchClaim<'_>>, StoreFailure> {
        loop {
            let mut released = std::pin::pin!(self.claim_released.notified());
            if matches!(trigger, DueBatchTrigger::Alarm) {
                // Subscribe before checking the claim so its release cannot be missed.
                released.as_mut().enable();
            }
            let (started_at, outcome) = {
                let _publication = self.claim_lock();
                let sampled = sample_clock()?;
                let outcome = self
                    .claim_at(sampled, spacing, trigger, true)
                    .map_err(|reason| StoreFailure::AlarmUnpublishable { reason })?;
                (sampled, outcome)
            };
            match outcome {
                ClaimOutcome::Started { previous_start } => {
                    return Ok(Some(DueBatchClaim {
                        state: self,
                        started_at,
                        previous_start,
                    }));
                }
                ClaimOutcome::WaitForEarlierClaim => released.await,
                ClaimOutcome::Deferred => return Ok(None),
            }
        }
    }

    /// An accepted wake can mature while its request is applying. Give any
    /// earlier discovery a chance to establish its cap before admitting the wake.
    pub async fn admit_overdue_wake(
        &self,
        due_at: EngineTime,
        sample_clock: impl Fn() -> Result<EngineTime, StoreFailure>,
    ) -> Result<bool, StoreFailure> {
        loop {
            let mut released = std::pin::pin!(self.claim_released.notified());
            released.as_mut().enable();
            {
                let _publication = self.claim_lock();
                let now = sample_clock()?;
                let prior = self.encoded.load(Ordering::Acquire);
                let current = snapshot(prior);
                if now < due_at
                    || current.leftovers_pending
                        && current
                            .last_start_unix_millis
                            .and_then(EngineTime::from_timestamp_millis)
                            .and_then(|start| next_five_minute_mark(start).ok())
                            .is_none_or(|mark| now < mark)
                {
                    return Ok(false);
                }
                if prior & PROTECTED == 0 {
                    return Ok(true);
                }
            }
            released.await;
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn try_claim(
        &self,
        now_unix_millis: i64,
        spacing: chrono::Duration,
    ) -> Result<bool, AlarmPublicationFailure> {
        let now = EngineTime::from_timestamp_millis(now_unix_millis)
            .ok_or(AlarmPublicationFailure::NowOutOfRange)?;
        let _publication = self.claim_lock();
        Ok(matches!(
            self.claim_at(now, spacing, DueBatchTrigger::Request, false)?,
            ClaimOutcome::Started { .. }
        ))
    }

    fn claim_at(
        &self,
        now: EngineTime,
        spacing: chrono::Duration,
        trigger: DueBatchTrigger,
        protect: bool,
    ) -> Result<ClaimOutcome, AlarmPublicationFailure> {
        if !supported_time(now) {
            return Err(AlarmPublicationFailure::NowOutOfRange);
        }
        let prior = self.encoded.load(Ordering::Acquire);
        let current = snapshot(prior);
        let previous_start = current
            .last_start_unix_millis
            .map(|start| {
                EngineTime::from_timestamp_millis(start)
                    .ok_or(AlarmPublicationFailure::NowOutOfRange)
            })
            .transpose()?;
        if let Some(start) = previous_start {
            let reached_next_mark = next_five_minute_mark(start).is_ok_and(|mark| now >= mark);
            if prior & PROTECTED != 0 {
                return Ok(
                    if matches!(trigger, DueBatchTrigger::Alarm) && reached_next_mark {
                        ClaimOutcome::WaitForEarlierClaim
                    } else {
                        ClaimOutcome::Deferred
                    },
                );
            }
            let admitted = match (current.leftovers_pending, trigger) {
                (true, DueBatchTrigger::Request | DueBatchTrigger::Alarm) => reached_next_mark,
                (false, DueBatchTrigger::Alarm) => reached_next_mark || now - start >= spacing,
                (false, DueBatchTrigger::Request) => now - start >= spacing,
            };
            if !admitted {
                return Ok(ClaimOutcome::Deferred);
            }
        }
        let now_unix_millis = now.timestamp_millis();
        let next = encode(now_unix_millis, current.leftovers_pending, protect)
            .ok_or(AlarmPublicationFailure::NowOutOfRange)?;
        self.encoded.store(next, Ordering::Release);
        #[cfg(feature = "test-support")]
        self.claimed_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(now_unix_millis);
        Ok(ClaimOutcome::Started { previous_start })
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn finish(&self, started_at_unix_millis: i64, leftovers_pending: bool) -> bool {
        self.update_claim(started_at_unix_millis, Some(leftovers_pending), false)
    }

    /// An old batch cannot change a newer batch's selection or release its claim.
    fn update_claim(
        &self,
        started_at_unix_millis: i64,
        leftovers: Option<bool>,
        protect: bool,
    ) -> bool {
        let publication = self
            .publication_order
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let prior = self.encoded.load(Ordering::Acquire);
        let current = snapshot(prior);
        if current.last_start_unix_millis != Some(started_at_unix_millis) {
            return false;
        }
        let Some(next) = encode(
            started_at_unix_millis,
            leftovers.unwrap_or(current.leftovers_pending),
            protect,
        ) else {
            return false;
        };
        self.encoded.store(next, Ordering::Release);
        drop(publication);
        if prior & PROTECTED != 0 && !protect {
            self.claim_released.notify_waiters();
        }
        true
    }

    pub(super) fn with_publication<R>(&self, publish: impl FnOnce(DueBatchSnapshot) -> R) -> R {
        let _publication = self
            .publication_order
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        publish(self.snapshot())
    }

    fn claim_lock(&self) -> MutexGuard<'_, ()> {
        #[cfg(not(feature = "test-support"))]
        {
            self.publication_order
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        }
        #[cfg(feature = "test-support")]
        {
            match self.publication_order.try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    self.claim_waiting.send_modify(|count| *count += 1);
                    let guard = self
                        .publication_order
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    self.claim_waiting.send_modify(|count| *count -= 1);
                    guard
                }
            }
        }
    }

    #[cfg(feature = "test-support")]
    pub async fn until_claim_waiting(&self) {
        let mut waiting = self.claim_waiting.subscribe();
        let _ = waiting.wait_for(|count| *count > 0).await;
    }

    /// Called by the due batch as each unit starts, before its first await.
    #[cfg(feature = "test-support")]
    pub fn record_unit_start(&self, start: DueUnitStart) {
        self.unit_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(start);
    }

    /// The start time of every due batch this object claimed, in claim order.
    #[cfg(feature = "test-support")]
    pub fn claimed_starts(&self) -> Vec<i64> {
        self.claimed_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Every unit started by this object's due batches, in start order.
    #[cfg(feature = "test-support")]
    pub fn unit_starts(&self) -> Vec<DueUnitStart> {
        self.unit_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[cfg(test)]
#[path = "due_batch_state_test.rs"]
mod tests;
