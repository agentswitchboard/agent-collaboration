//! Owned test payloads and their disposal witnesses, independent of a provider.
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use switchboard_harness_contracts::{
    AcceptedRunSettings, LifecycleRefusal, LifecycleReply, ReservedReply, SettingAttempt,
    SettingFact, SetupReport,
};
use tokio::sync::oneshot;

use super::{OpeningAdapter, RestoreDecision, SendRestoreDecision};

pub(super) struct TestOpening;

impl OpeningAdapter for TestOpening {
    type OpenedSession = TestOpenedSession;
    type SettingPlan = ();
    type Capabilities = ();
    type Settings = ();
    type Skills = ();
    type RestoreSignal = TestRestoreSignal;

    fn accept_catalogue(
        _: &mut Self::OpenedSession,
        _: AcceptedRunSettings,
    ) -> SetupReport<Self::Capabilities, Self::Settings> {
        SetupReport {
            capabilities: (),
            settings: (),
            observed: Default::default(),
        }
    }

    fn plan_setting_put_back(
        _: String,
        _: SettingAttempt,
        _: Option<String>,
    ) -> Result<Self::SettingPlan, LifecycleRefusal> {
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct HeldTable {
    mutex: Mutex<()>,
    held: Arc<AtomicBool>,
}

impl HeldTable {
    pub(super) fn lock(&self) -> HeldTableGuard<'_> {
        let guard = self.mutex.lock().unwrap();
        assert!(!self.held.swap(true, Ordering::SeqCst));
        HeldTableGuard {
            _guard: guard,
            held: &self.held,
        }
    }
}

pub(super) struct HeldTableGuard<'table> {
    _guard: MutexGuard<'table, ()>,
    held: &'table AtomicBool,
}

impl Drop for HeldTableGuard<'_> {
    fn drop(&mut self) {
        assert!(self.held.swap(false, Ordering::SeqCst));
    }
}

#[derive(Default)]
struct Counts {
    inside: AtomicUsize,
    outside: AtomicUsize,
}

#[derive(Default)]
pub(super) struct DisposalProbe(Arc<Counts>);

impl DisposalProbe {
    pub(super) fn bind(&self, table: &HeldTable) -> DisposalToken {
        DisposalToken {
            held: Arc::clone(&table.held),
            counts: Arc::clone(&self.0),
        }
    }

    pub(super) fn assert_disposed_outside_table(&self) {
        assert_eq!(
            self.0.inside.load(Ordering::SeqCst),
            0,
            "owned opening resource was disposed while its table lock was held"
        );
        assert_eq!(
            self.0.outside.load(Ordering::SeqCst),
            1,
            "owned opening resource must be disposed exactly once outside its table lock"
        );
    }
}

pub(super) struct DisposalToken {
    held: Arc<AtomicBool>,
    counts: Arc<Counts>,
}

impl Drop for DisposalToken {
    fn drop(&mut self) {
        let counter = if self.held.load(Ordering::SeqCst) {
            &self.counts.inside
        } else {
            &self.counts.outside
        };
        counter.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) struct TestOpenedSession {
    _disposal: Option<DisposalToken>,
    order_drop: Option<(OrderWitness, &'static str)>,
}

impl TestOpenedSession {
    pub(super) fn new(disposal: DisposalToken) -> Self {
        Self {
            _disposal: Some(disposal),
            order_drop: None,
        }
    }

    pub(super) fn ordered(order: OrderWitness, event: &'static str) -> Self {
        Self {
            _disposal: None,
            order_drop: Some((order, event)),
        }
    }

    pub(super) fn provider_id(&self) -> &'static str {
        "matrix-session"
    }
}

impl Drop for TestOpenedSession {
    fn drop(&mut self) {
        if let Some((order, event)) = &self.order_drop {
            order.record(event);
        }
    }
}

pub(super) struct TestRestoreSignal {
    sender: oneshot::Sender<RestoreDecision>,
    _disposal: Option<DisposalToken>,
    order: Option<OrderWitness>,
}

impl TestRestoreSignal {
    pub(super) fn new(sender: oneshot::Sender<RestoreDecision>, disposal: DisposalToken) -> Self {
        Self {
            sender,
            _disposal: Some(disposal),
            order: None,
        }
    }

    pub(super) fn ordered(sender: oneshot::Sender<RestoreDecision>, order: OrderWitness) -> Self {
        Self {
            sender,
            _disposal: None,
            order: Some(order),
        }
    }
}

impl SendRestoreDecision for TestRestoreSignal {
    fn send(self, decision: RestoreDecision) {
        if let Some(order) = self.order {
            order.record("send decision");
        }
        let _worker_gone = self.sender.send(decision);
    }
}

pub(super) fn ignore_setting_reply(token: DisposalToken) -> LifecycleReply<SettingFact> {
    struct IgnoreSettingReply {
        _disposal: DisposalToken,
    }
    impl ReservedReply<SettingFact> for IgnoreSettingReply {
        fn send(self: Box<Self>, _: SettingFact) {}
    }
    Box::new(IgnoreSettingReply { _disposal: token })
}

#[derive(Clone, Default)]
pub(super) struct OrderWitness(Arc<Mutex<Vec<&'static str>>>);

impl OrderWitness {
    pub(super) fn record(&self, event: &'static str) {
        self.0.lock().unwrap().push(event);
    }

    pub(super) fn assert_order(&self, expected: &[&str]) {
        assert_eq!(*self.0.lock().unwrap(), expected);
    }
}
