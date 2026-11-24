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
    #[error("custody run was cancelled")]
    Cancelled,
}

/// Owns the stop signal and retains the result of joining both members.
pub struct CustodyRunGroup {
    stop: CustodyStop,
    result: watch::Receiver<Option<Result<(), CustodyRunFailure>>>,
}

impl CustodyRunGroup {
    pub fn request_stop(&self) {
        self.stop.request();
    }

    /// Wait for both members and their owned work to end. The result remains
    /// available if a caller drops this future or calls join again.
    pub async fn join(&self) -> Result<(), CustodyRunFailure> {
        let mut result = self.result.clone();
        result
            .wait_for(Option::is_some)
            .await
            .map_err(|_| CustodyRunFailure::Cancelled)?
            .unwrap_or(Err(CustodyRunFailure::Cancelled))
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
    pub(super) panic_failure: CustodyRunFailure,
}

impl Drop for StopOnExit {
    fn drop(&mut self) {
        self.stop
            .stop_with(std::thread::panicking().then_some(self.panic_failure));
    }
}

pub(super) fn retain_group(
    stop: CustodyStop,
    worker: JoinHandle<()>,
    offer_taker: JoinHandle<()>,
    remote_tools: Arc<Mutex<JoinSet<()>>>,
) -> CustodyRunGroup {
    let (joined, result) = watch::channel(None);
    let member_stop = stop.clone();
    tokio::spawn(async move {
        tokio::join!(
            async {
                join_member(worker, &member_stop, CustodyRunFailure::WorkerPanicked).await;
                // Remote tool waits are rebuild losses. Abort and reap them
                // even if the worker unwound while one was in flight.
                remote_tools.lock().await.shutdown().await;
            },
            join_member(
                offer_taker,
                &member_stop,
                CustodyRunFailure::OfferTakerPanicked
            )
        );
        let outcome = member_stop.0.borrow().first_failure.map_or(Ok(()), Err);
        joined.send_replace(Some(outcome));
    });
    CustodyRunGroup { stop, result }
}

async fn join_member(member: JoinHandle<()>, stop: &CustodyStop, panic_failure: CustodyRunFailure) {
    let failure = member.await.err().map(|error| {
        if error.is_panic() {
            panic_failure
        } else {
            CustodyRunFailure::Cancelled
        }
    });
    stop.stop_with(failure);
}

#[cfg(test)]
#[path = "custody_run_group_test.rs"]
mod tests;
