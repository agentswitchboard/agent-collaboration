use central_engine_model::{EngineTime, next_five_minute_mark, supported_time};
use central_storage::AlarmPublicationFailure;
use std::sync::{
    Mutex, MutexGuard, PoisonError,
    atomic::{AtomicI64, Ordering},
};

const EMPTY: i64 = i64::MIN;
const LEFTOVERS: i64 = 1;
const PROTECTED: i64 = 2;
const FLAG_COUNT: i64 = 4;

#[derive(Clone, Copy)]
pub enum DueBatchTrigger {
    Request,
    Alarm,
}

/// Keeps another batch from discovering the same rows before the cap is known.
/// Only capped batches keep this protection while applying their selected rows.
pub struct DueBatchClaim<'state> {
    state: &'state DueBatchState,
    start: i64,
}

impl DueBatchClaim<'_> {
    pub fn record_selection(&self, has_leftovers: bool) {
        self.state
            .update_claim(self.start, Some(has_leftovers), has_leftovers);
    }
}

impl Drop for DueBatchClaim<'_> {
    fn drop(&mut self) {
        self.state.update_claim(self.start, None, false);
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
pub struct DueBatchState {
    encoded: AtomicI64,
    publication_order: Mutex<()>,
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

    pub fn begin_batch(
        &self,
        now: EngineTime,
        spacing: chrono::Duration,
        trigger: DueBatchTrigger,
    ) -> Result<Option<DueBatchClaim<'_>>, AlarmPublicationFailure> {
        let start = now.timestamp_millis();
        Ok(self
            .claim(now, spacing, trigger, true)?
            .then(|| DueBatchClaim { state: self, start }))
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn try_claim(
        &self,
        now_unix_millis: i64,
        spacing: chrono::Duration,
    ) -> Result<bool, AlarmPublicationFailure> {
        let now = EngineTime::from_timestamp_millis(now_unix_millis)
            .ok_or(AlarmPublicationFailure::NowOutOfRange)?;
        self.claim(now, spacing, DueBatchTrigger::Request, false)
    }

    fn claim(
        &self,
        now: EngineTime,
        spacing: chrono::Duration,
        trigger: DueBatchTrigger,
        protect: bool,
    ) -> Result<bool, AlarmPublicationFailure> {
        if !supported_time(now) {
            return Err(AlarmPublicationFailure::NowOutOfRange);
        }
        let _publication = self.claim_lock();
        let prior = self.encoded.load(Ordering::Acquire);
        let current = snapshot(prior);
        if prior & PROTECTED != 0 {
            return Ok(false);
        }
        if let Some(start) = current.last_start_unix_millis {
            let start = EngineTime::from_timestamp_millis(start)
                .ok_or(AlarmPublicationFailure::NowOutOfRange)?;
            let reached_next_mark = next_five_minute_mark(start).is_ok_and(|mark| now >= mark);
            let admitted = match (current.leftovers_pending, trigger) {
                (true, DueBatchTrigger::Request | DueBatchTrigger::Alarm) => reached_next_mark,
                (false, DueBatchTrigger::Alarm) => reached_next_mark || now - start >= spacing,
                (false, DueBatchTrigger::Request) => now - start >= spacing,
            };
            if !admitted {
                return Ok(false);
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
        Ok(true)
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
        let _publication = self
            .publication_order
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let current = self.snapshot();
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
