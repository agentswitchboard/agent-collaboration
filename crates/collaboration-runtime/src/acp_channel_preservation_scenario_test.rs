//! Failure containment for the socket fixtures when every participant remains parked.
use super::FixtureError;
use std::{cell::RefCell, future::Future, time::Duration};

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
                // The watchdog cannot advance a schedule or make it pass. It only owns
                // external I/O containment when neither participant reaches its causal exit.
                let result = tokio::time::timeout(Duration::from_secs(6), proof).await;
                let stage = STAGE.with(|name| name.borrow().clone());
                assert!(
                    matches!(result, Ok(Ok(()))),
                    "preservation proof failed at {stage}: {result:?}"
                );
            },
        )
        .await;
}
