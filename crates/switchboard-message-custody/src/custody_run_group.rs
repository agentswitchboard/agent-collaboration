//! Retains the physical completion of both custody members.

use std::sync::Arc;

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
pub(super) struct CustodyStop(pub(super) watch::Sender<CustodyRunState>);

impl CustodyStop {
    pub(super) fn new() -> Self {
        Self(watch::Sender::new(CustodyRunState::default()))
    }

    fn request(&self) {
        self.stop_with(None);
    }

    fn stop_with(&self, failure: Option<CustodyRunFailure>) {
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
    completed: bool,
}

impl StopOnExit {
    pub(super) fn new(stop: &CustodyStop, panic_failure: CustodyRunFailure) -> Self {
        Self {
            stop: stop.clone(),
            panic_failure,
            completed: false,
        }
    }

    pub(super) fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for StopOnExit {
    fn drop(&mut self) {
        let failure = if std::thread::panicking() {
            Some(self.panic_failure)
        } else if self.completed {
            None
        } else {
            Some(CustodyRunFailure::Cancelled)
        };
        self.stop.stop_with(failure);
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
