//! Retains the physical completion of both custody members.

use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::pin,
    sync::Arc,
    task::Poll,
};

use crate::handover_notice::NoticeAuthority;
use tokio::{
    sync::{Mutex, watch},
    task::{JoinHandle, JoinSet},
};

/// The task group ended without completing normally.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CustodyRunFailure {
    #[error("custody worker panicked")]
    WorkerPanicked,
    #[error("custody offer taker panicked")]
    OfferTakerPanicked,
    /// Both tasks and the remote waits have physically ended. This does not
    /// prove that a SQLite statement already issued on the SQLx connection
    /// thread has finished.
    #[error("custody run was cancelled")]
    Cancelled,
}

/// Owns the stop signal and retains the result of joining both members.
pub struct CustodyRunGroup {
    stop: CustodyStop,
    joins: Mutex<CustodyJoins>,
    remote_tools: Arc<Mutex<JoinSet<()>>>,
}

struct CustodyJoins {
    worker: Option<JoinHandle<()>>,
    offer_taker: Option<JoinHandle<()>>,
    result: Option<Result<(), CustodyRunFailure>>,
}

impl CustodyRunGroup {
    /// Observes revoked admission without collecting the group's owned work.
    /// A member exiting revokes admission before its join handle is ready.
    pub async fn admission_stopped(&self) {
        wait_for_stop(&mut self.stop.0.subscribe()).await;
    }

    pub fn request_stop(&self) {
        self.stop.request();
    }

    /// Wait for both members and their owned work to end. The result remains
    /// available if a caller drops this future or calls join again.
    pub async fn join(&self) -> Result<(), CustodyRunFailure> {
        let mut joins = self.joins.lock().await;
        if let Some(result) = joins.result {
            return result;
        }
        join_member(
            &mut joins.worker,
            &self.stop,
            CustodyRunFailure::WorkerPanicked,
        )
        .await;
        join_member(
            &mut joins.offer_taker,
            &self.stop,
            CustodyRunFailure::OfferTakerPanicked,
        )
        .await;
        // The set stays owned by the group if this shutdown wait is dropped.
        self.remote_tools.lock().await.shutdown().await;
        let result = self.stop.0.borrow().first_failure.map_or(Ok(()), Err);
        joins.result = Some(result);
        result
    }
}

impl Drop for CustodyRunGroup {
    fn drop(&mut self) {
        self.request_stop();
    }
}

#[derive(Clone, Default)]
pub(super) struct CustodyRunState {
    pub(super) stopped: bool,
    first_failure: Option<CustodyRunFailure>,
}

#[derive(Clone)]
pub(super) struct CustodyStop(
    pub(super) watch::Sender<CustodyRunState>,
    pub(super) Arc<NoticeAuthority>,
);

impl CustodyStop {
    pub(super) fn new() -> Self {
        Self(
            watch::Sender::new(CustodyRunState::default()),
            Arc::new(NoticeAuthority::new()),
        )
    }

    pub(super) fn request(&self) {
        self.stop_with(None);
    }

    fn stop_with(&self, failure: Option<CustodyRunFailure>) {
        self.1.close();
        self.0.send_if_modified(|state| {
            let changed = !state.stopped || (state.first_failure.is_none() && failure.is_some());
            state.stopped = true;
            state.first_failure = state.first_failure.or(failure);
            changed
        });
    }
}

pub(super) async fn wait_for_stop(stop: &mut watch::Receiver<CustodyRunState>) {
    let _ = stop.wait_for(|state| state.stopped).await;
}

/// Revokes admission at member exit, including unwinding, before the join
/// handle becomes ready. Each member supplies its own panic outcome.
pub(super) struct StopOnExit {
    pub(super) stop: CustodyStop,
    panic_failure: CustodyRunFailure,
    finished: bool,
}

impl StopOnExit {
    pub(super) fn new(stop: &CustodyStop, panic_failure: CustodyRunFailure) -> Self {
        Self {
            stop: stop.clone(),
            panic_failure,
            finished: false,
        }
    }

    pub(super) async fn run(&mut self, member: impl Future<Output = ()>) {
        let mut member = pin!(member);
        // Catch only panics from the member's poll. The caller retains the
        // exit guard outside this future, including during runtime shutdown.
        poll_fn(
            |context| match catch_unwind(AssertUnwindSafe(|| member.as_mut().poll(context))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(())) => {
                    self.finish(None);
                    Poll::Ready(())
                }
                Err(panic) => {
                    self.finish(Some(self.panic_failure));
                    resume_unwind(panic);
                }
            },
        )
        .await;
    }

    fn finish(&mut self, failure: Option<CustodyRunFailure>) {
        self.stop.stop_with(failure);
        self.finished = true;
    }
}

impl Drop for StopOnExit {
    fn drop(&mut self) {
        if !self.finished {
            self.stop.stop_with(Some(CustodyRunFailure::Cancelled));
        }
    }
}

pub(super) fn retain_group(
    stop: CustodyStop,
    worker: JoinHandle<()>,
    offer_taker: JoinHandle<()>,
    remote_tools: Arc<Mutex<JoinSet<()>>>,
) -> CustodyRunGroup {
    CustodyRunGroup {
        stop,
        joins: Mutex::new(CustodyJoins {
            worker: Some(worker),
            offer_taker: Some(offer_taker),
            result: None,
        }),
        remote_tools,
    }
}

async fn join_member(
    retained: &mut Option<JoinHandle<()>>,
    stop: &CustodyStop,
    panic_failure: CustodyRunFailure,
) {
    let Some(member) = retained.as_mut() else {
        return;
    };
    let failure = member.await.err().map(|error| {
        if error.is_panic() {
            panic_failure
        } else {
            CustodyRunFailure::Cancelled
        }
    });
    stop.stop_with(failure);
    *retained = None;
}

#[cfg(test)]
#[path = "custody_run_group_test.rs"]
mod tests;
