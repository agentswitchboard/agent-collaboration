//! Causal suspension points for the real Host listener's preservation tests.

use std::future::{Future, poll_fn};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::Waker;
use tokio_util::sync::CancellationToken;

tokio::task_local! {
    static PROBE: Option<Arc<PreservationProbe>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreservationPhase {
    BeforeSetupPoll,
    DuringDirectoryNormalization,
    BeforeSetupCollection,
    BeforeRegistryHandoff,
    HostStopBeforeHandoff,
    HostStopAfterPromptDetach,
}

#[derive(Default)]
struct PromptPoll {
    last_waker: Option<Waker>,
    check_next: bool,
    completed: bool,
}

pub struct PreservationProbe {
    phase: PreservationPhase,
    armed: AtomicBool,
    pub reached: CancellationToken,
    pub release_handoff: CancellationToken,
    pub lazy_close_issued: CancellationToken,
    pub generation_disposed: CancellationToken,
    pub prompt_detached: CancellationToken,
    pub prompt_started: CancellationToken,
    pub prompt_checked: CancellationToken,
    prompt_poll: Mutex<PromptPoll>,
    pub listener_draining: CancellationToken,
    pub release_outer_abort: CancellationToken,
}

impl PreservationProbe {
    pub fn new(phase: PreservationPhase) -> Arc<Self> {
        Arc::new(Self {
            phase,
            armed: AtomicBool::new(false),
            reached: CancellationToken::new(),
            release_handoff: CancellationToken::new(),
            lazy_close_issued: CancellationToken::new(),
            generation_disposed: CancellationToken::new(),
            prompt_detached: CancellationToken::new(),
            prompt_started: CancellationToken::new(),
            prompt_checked: CancellationToken::new(),
            prompt_poll: Mutex::default(),
            listener_draining: CancellationToken::new(),
            release_outer_abort: CancellationToken::new(),
        })
    }

    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    pub fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }

    pub fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    pub fn phase(&self) -> PreservationPhase {
        self.phase
    }

    pub fn check_prompt_after_retirement(&self) -> bool {
        let mut poll = self
            .prompt_poll
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        poll.check_next = true;
        let Some(waker) = poll.last_waker.take() else {
            return false;
        };
        drop(poll);
        waker.wake();
        true
    }

    pub fn prompt_completed_after_retirement(&self) -> bool {
        self.prompt_poll
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .completed
    }
}

pub fn current() -> Option<Arc<PreservationProbe>> {
    PROBE.try_with(Clone::clone).ok().flatten()
}

pub async fn scope<T>(probe: Option<Arc<PreservationProbe>>, future: impl Future<Output = T>) -> T {
    PROBE.scope(probe, future).await
}

pub(crate) async fn watch_prompt<T>(future: impl Future<Output = T>) -> T {
    let Some(probe) =
        current().filter(|probe| probe.phase() == PreservationPhase::HostStopAfterPromptDetach)
    else {
        return future.await;
    };
    let mut future = std::pin::pin!(future);
    poll_fn(|context| {
        let result = future.as_mut().poll(context);
        let mut poll = probe
            .prompt_poll
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if poll.check_next {
            poll.check_next = false;
            poll.completed = result.is_ready();
            probe.prompt_checked.cancel();
        } else {
            poll.last_waker = Some(context.waker().clone());
        }
        result
    })
    .await
}

pub(crate) async fn hold_at(phase: PreservationPhase) {
    if let Some(probe) = current().filter(|probe| probe.phase() == phase) {
        probe.reached.cancel();
        probe.release_handoff.cancelled().await;
    }
}

pub(crate) fn observe_prompt_detachment() {
    if let Some(probe) = current() {
        probe.prompt_detached.cancel();
    }
}

pub(crate) fn observe_setup_completed() {
    if let Some(probe) =
        current().filter(|probe| probe.phase() == PreservationPhase::BeforeSetupCollection)
    {
        probe.reached.cancel();
    }
}

pub(crate) fn hold_collection() -> bool {
    current().is_some_and(|probe| probe.phase() == PreservationPhase::BeforeSetupCollection)
}

pub(crate) struct ObserveGenerationDisposal(pub(crate) Option<Arc<PreservationProbe>>);

impl Drop for ObserveGenerationDisposal {
    fn drop(&mut self) {
        if let Some(probe) = &self.0 {
            probe.generation_disposed.cancel();
        }
    }
}
