//! The engine's Hegel stateful (model-based) test on the native host. Random
//! sequences of requests, alarm firings, clock advances across 5-minute marks,
//! injected faults, admin repair and restarts drive the real engine; the
//! invariants are checked after every step.
//!
//! hegeltest 0.52.6 `src/stateful.rs`: `#[hegel::state_machine]` turns `#[rule]`
//! methods into actions and `#[invariant(always_run)]` methods into checks run
//! after every rule; `hegel::stateful::machine(model).steps(n).run(tc)` runs it.
//! Rules are synchronous `fn(&mut self, TestCase)`, so the model owns one
//! current-thread Tokio runtime and blocks on it; the host's alarm driver is a
//! task on that runtime and only the injected `ManualClock` ever moves time.
//!
//! The model predicts, independently of the store, what every due-batch unit
//! did to each subject (fired, failed, not applied, stale) and the failure
//! record spec R4 then requires; the invariants compare the store exactly.
// Test-only module: clippy's test allowances (clippy.toml) apply to it.
#![cfg(test)]
use super::engine_admin_support::*;
use super::engine_stateful_attempts::{AttemptOutcome, ExpectedRecord};
use super::support::*;
use central_storage::SqlValue;
use collaboration_central_core::engine_probe::{
    EngineReceipt, EngineRequestOutcome, InstanceKey, ProbeEvent, ProbeRefusal, ProbeRequest,
    ProbeState, SeedProbe,
};
use fleet_runtime::{EngineCommitFault, EngineCommitFaultKind};
use hegel::{TestCase, generators as gs};
use router_clock::RuntimeClock;
use workspace_model::central_mail::{CentralMailAnswer, CentralMailOperation, CentralMailResult};

const SPACING_MS: i64 = 30_000;
const CADENCE_MS: i64 = 300_000;
/// Scheduler turns `settle` gives the alarm driver to make visible progress.
/// A live driver needs a handful per store round trip; a stalled or dead one
/// never progresses, so the budget turns a hang into a failing case without
/// reading any clock.
const PROGRESS_TURNS: u32 = 5_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Wake,
    Probe,
}
impl Kind {
    fn machine(self) -> &'static str {
        match self {
            Self::Wake => "wake_timer",
            Self::Probe => "engine_probe",
        }
    }
}

/// What the test did to a subject; the engine's answers are read back from the
/// store and compared with what these facts allow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fate {
    Live,
    /// Cancelled (wake) or stopped (probe).
    Finished,
    /// Discarded by admin while retired.
    Discarded,
}

/// A one-shot commit fault armed on one subject's next commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArmedFault {
    /// `DatabaseCode(19)`: the commit is rejected, a recorded failure.
    Rejected,
    AppliedAnswerLost,
    NotAppliedAnswerLost,
}

#[derive(Debug)]
struct Subject {
    kind: Kind,
    key: String,
    group: Option<String>,
    due: Time,
    /// When the subject last became due to the engine: its due time, the
    /// moment an admin retry handed it back (V13), or the batch whose commit
    /// answer was lost before it applied.
    eligible_at: Time,
    fate: Fate,
    /// The model's own record of whether the occurrence fired.
    fired: bool,
    /// The failure record R4 requires, predicted from the attempts.
    expected: ExpectedRecord,
    /// The original target while a corruption keeps the wake from decoding.
    corrupted_target: Option<SqlValue>,
}

struct EngineModel {
    runtime: tokio::runtime::Runtime,
    engine: Option<EngineOrg>,
    subjects: Vec<Subject>,
    /// Every due batch claimed in this process lifetime (the floor is in
    /// memory), one entry per batch even at the same millisecond.
    batch_starts: Vec<i64>,
    /// How many unit starts and batch claims of this runtime were observed.
    seen_units: usize,
    seen_batches: usize,
    /// The armed one-shot commit fault and its subject (arming replaces it).
    armed_fault: Option<(String, ArmedFault)>,
    /// Whether the armed fault was still pending when last observed.
    fault_pending: bool,
}

impl EngineModel {
    fn engine(&self) -> &EngineOrg {
        self.engine.as_ref().expect("engine open")
    }
    fn block<T>(&self, work: impl std::future::Future<Output = T>) -> T {
        self.runtime.block_on(work)
    }
    fn now_ms(&self) -> i64 {
        self.engine().now().timestamp_millis()
    }
    fn pick(&self, tc: &TestCase, eligible: impl Fn(&Subject) -> bool) -> Option<usize> {
        let indexes: Vec<usize> = (0..self.subjects.len())
            .filter(|index| eligible(&self.subjects[*index]))
            .collect();
        tc.assume(!indexes.is_empty());
        let choice = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(indexes.len() - 1),
        );
        indexes.get(choice).copied()
    }
    fn commit_fault_pending(&self) -> bool {
        self.block(async {
            self.engine()
                .runtime()
                .await
                .runtime()
                .statement_host()
                .engine_commit_fault_pending()
        })
    }
    fn arm(&mut self, key: String, fault: ArmedFault) {
        self.armed_fault = Some((key, fault));
        self.fault_pending = true;
    }
    /// A request on `key` used up the armed fault with its own commit.
    fn request_consumed_fault(&mut self, key: &str) {
        if self
            .armed_fault
            .as_ref()
            .is_some_and(|(armed, _)| armed == key)
            && !self.commit_fault_pending()
        {
            self.armed_fault = None;
            self.fault_pending = false;
        }
    }
    /// Lets the host's own driver finish everything due at the current time,
    /// then predicts what each new due-batch unit did.
    fn settle(&mut self) {
        let engine = self.engine();
        self.runtime.block_on(settle(engine));
        self.observe();
    }
    fn observe(&mut self) {
        let runtime = self.block(self.engine().runtime());
        let batches = runtime.runtime().due_batch_starts();
        self.batch_starts
            .extend_from_slice(&batches[self.seen_batches.min(batches.len())..]);
        self.seen_batches = batches.len();
        let units = runtime.runtime().due_unit_starts();
        let new_units = units[self.seen_units.min(units.len())..].to_vec();
        self.seen_units = units.len();
        let pending_now = runtime
            .runtime()
            .statement_host()
            .engine_commit_fault_pending();
        // The armed fault was used up during this step by one of its attempts.
        let mut consumed = self
            .armed_fault
            .clone()
            .filter(|_| self.fault_pending && !pending_now);
        for unit in new_units {
            let Some(index) = self.subjects.iter().position(|subject| {
                subject.kind.machine() == unit.machine_kind && subject.key == unit.subject_key
            }) else {
                continue;
            };
            let outcome = self.attempt_outcome(index, unit.batch_start_unix_millis, &mut consumed);
            let subject = &mut self.subjects[index];
            let earliest = subject
                .expected
                .earliest_batch_ms(&subject.key, subject.due.timestamp_millis());
            assert!(
                unit.batch_start_unix_millis >= earliest,
                "{} was worked by the batch at {} before {earliest} (due {})",
                subject.key,
                unit.batch_start_unix_millis,
                subject.due
            );
            let batch = unit.batch_start_unix_millis;
            match outcome {
                AttemptOutcome::Fired => {
                    subject.fired = true;
                    subject.expected = ExpectedRecord::None;
                }
                AttemptOutcome::Failed => {
                    subject.expected = subject.expected.after_failure(&subject.key, batch);
                }
                AttemptOutcome::NotApplied => {
                    subject.eligible_at = chrono::DateTime::from_timestamp_millis(batch).unwrap();
                }
                AttemptOutcome::Stale | AttemptOutcome::Skipped => {}
            }
        }
        if !pending_now {
            self.armed_fault = None;
        }
        self.fault_pending = pending_now && self.armed_fault.is_some();
    }
    fn attempt_outcome(
        &self,
        index: usize,
        batch_ms: i64,
        consumed: &mut Option<(String, ArmedFault)>,
    ) -> AttemptOutcome {
        let subject = &self.subjects[index];
        if subject.fate != Fate::Live || subject.fired {
            return AttemptOutcome::Stale;
        }
        // The gate runs before decoding or committing, so a skip uses no fault.
        if let ExpectedRecord::Failing { next_try_ms } = subject.expected
            && batch_ms < next_try_ms
        {
            return AttemptOutcome::Skipped;
        }
        // A corrupt row fails before its commit, so it never uses a fault.
        if subject.corrupted_target.is_some() {
            return AttemptOutcome::Failed;
        }
        match consumed.take_if(|(key, _)| *key == subject.key) {
            Some((_, ArmedFault::Rejected)) => AttemptOutcome::Failed,
            Some((_, ArmedFault::NotAppliedAnswerLost)) => AttemptOutcome::NotApplied,
            Some((_, ArmedFault::AppliedAnswerLost)) | None => AttemptOutcome::Fired,
        }
    }
    fn fired_in_store(&self, subject: &Subject) -> i64 {
        let engine = self.engine();
        match subject.kind {
            Kind::Wake => self.block(engine.wake_messages(&subject.key)),
            Kind::Probe => self.block(engine.count(
                "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
                vec![text(subject.group.as_deref().expect("probe group"))],
            )),
        }
    }
    /// A raw fixture statement on `key` would consume a commit fault armed on it.
    fn fault_armed_on(&self, key: &str) -> bool {
        self.armed_fault
            .as_ref()
            .is_some_and(|(armed, _)| armed == key)
            && self.commit_fault_pending()
    }
}

/// Polls `check` between scheduler turns; a driver that stops making progress
/// fails the case with `what` instead of hanging it.
async fn within_progress_budget<T>(what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    for _ in 0..PROGRESS_TURNS {
        if let Some(value) = check() {
            return value;
        }
        tokio::task::yield_now().await;
    }
    panic!("the alarm driver made no progress toward {what} in {PROGRESS_TURNS} scheduler turns");
}

/// Waits on the host's actual alarm driver: until it completes a due alarm, or
/// re-checks a future alarm at the current manual time. A failed, stalled or
/// dead driver fails the case.
async fn settle(engine: &EngineOrg) {
    let runtime = engine.runtime().await;
    assert!(
        runtime.runtime().alarm_failures().is_empty(),
        "the alarm driver failed: {:?}",
        runtime.runtime().alarm_failures()
    );
    for _ in 0..16 {
        let completed = runtime.runtime().alarm_completion_count();
        let failures = runtime.runtime().alarm_failures().len();
        let failed = || {
            panic!(
                "the alarm driver failed: {:?}; due batch {:?}",
                runtime.runtime().alarm_failures(),
                runtime.runtime().due_batch_snapshot()
            )
        };
        let now = engine.now().timestamp_millis();
        match runtime.runtime().statement_host().alarm() {
            Some(alarm) if alarm.unix_millis() <= now => {
                within_progress_budget("completing the due alarm", || {
                    if runtime.runtime().alarm_failures().len() > failures {
                        failed();
                    }
                    (runtime.runtime().alarm_completion_count() > completed).then_some(())
                })
                .await;
            }
            Some(alarm) => {
                let deadline = engine.f.clock.wall_deadline(alarm);
                within_progress_budget("re-checking the future alarm", || {
                    if runtime.runtime().alarm_failures().len() > failures {
                        failed();
                    }
                    engine.f.clock.has_not_due_check_now(deadline).then_some(())
                })
                .await;
                return;
            }
            None => return,
        }
    }
    panic!("the driver never settled: its alarm stays at or before now");
}

#[path = "engine_stateful/engine_stateful_rules.rs"]
mod engine_stateful_rules;

impl Drop for EngineModel {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.take() {
            if std::thread::panicking() {
                // A failed case may have a stalled driver that close would wait
                // on forever; dropping the runtime cancels its tasks instead.
                std::mem::forget(engine);
                return;
            }
            self.runtime.block_on(engine.close());
        }
    }
}

async fn request_probe_as_engine_admin(
    engine: &EngineOrg,
    runtime: &Runtime,
    request: ProbeRequest,
) -> EngineRequestOutcome<ProbeState, ProbeRefusal> {
    let caller = engine.f.machine_caller("engine-admin").await.unwrap();
    runtime.request_probe(&caller, request).await.unwrap()
}

#[tokio::test]
async fn probe_stop_uses_fresh_authority_after_the_fixture_clock_advances() {
    let engine = EngineOrg::new().await;
    let runtime = engine.runtime().await;
    let key = engine.fresh().parse().unwrap();
    let seeded = runtime
        .seed_probe(SeedProbe {
            key,
            group: engine.fresh().parse().unwrap(),
            due: None,
            claim: key.to_string(),
        })
        .await
        .unwrap();
    assert!(matches!(
        seeded,
        central_storage::BatchOutcome::Applied { .. }
    ));
    engine
        .set(engine.now() + chrono::Duration::minutes(10))
        .await;
    let outcome = request_probe_as_engine_admin(
        &engine,
        &runtime,
        ProbeRequest {
            request_id: engine.fresh().try_into().unwrap(),
            key: InstanceKey(key.to_string()),
            event: ProbeEvent::Stop,
        },
    )
    .await;
    assert!(
        matches!(
            outcome,
            EngineRequestOutcome::Answer {
                receipt: EngineReceipt::Applied { .. }
            }
        ),
        "{outcome:?}",
    );
    engine.f.close().await.unwrap();
}

#[hegel::test(test_cases = 50, suppress_health_check = [hegel::HealthCheck::TooSlow])]
fn the_engine_keeps_its_invariants_under_random_requests_faults_and_restarts(tc: TestCase) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let engine = runtime.block_on(EngineOrg::new());
    let model = EngineModel {
        runtime,
        engine: Some(engine),
        subjects: Vec::new(),
        batch_starts: Vec::new(),
        seen_units: 0,
        seen_batches: 0,
        armed_fault: None,
        fault_pending: false,
    };
    hegel::stateful::machine(model).steps(50).run(tc);
}
