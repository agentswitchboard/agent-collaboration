//! Failure containment for the socket fixtures when every participant remains parked.
use super::FixtureError;
use std::{cell::RefCell, future::Future, sync::mpsc, time::Duration};

tokio::task_local! {
    static STAGE: RefCell<String>;
}

pub(super) fn proof_stage(stage: impl Into<String>) {
    STAGE.with(|current| *current.borrow_mut() = stage.into());
}

pub(super) async fn run_scenario(proof: impl Future<Output = Result<(), FixtureError>>) {
    STAGE
        .scope(
            RefCell::new("starting the listener fixture".into()),
            async {
                // The external I/O watchdog cannot advance a schedule or the product clock.
                let (finished, done) = mpsc::channel::<()>();
                let (expired, deadline) = tokio::sync::oneshot::channel();
                let watchdog = std::thread::spawn(move || {
                    if done.recv_timeout(Duration::from_secs(6))
                        == Err(mpsc::RecvTimeoutError::Timeout)
                    {
                        let _closed = expired.send(());
                    }
                });
                let result = tokio::select! {
                    biased;
                    finished = proof => finished,
                    _ = deadline => Err("external I/O watchdog expired".into()),
                };
                drop(finished);
                assert!(
                    watchdog.join().is_ok(),
                    "external I/O watchdog terminated cleanly"
                );
                let stage = STAGE.with(|name| name.borrow().clone());
                assert!(
                    result.is_ok(),
                    "preservation proof failed at {stage}: {result:?}"
                );
            },
        )
        .await;
}
