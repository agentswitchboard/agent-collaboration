//! Collect one native turn/start failure across Host tasks for the diagnostic branch.

use collaboration_contracts::CommandFailure;
use session_event_model::InputId;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};

type Observations = Arc<Mutex<BTreeMap<&'static str, String>>>;

tokio::task_local! {
    static ACTIVE_TURN_START: Observations;
}

pub(crate) async fn observe<F: Future>(
    input: InputId,
    request: F,
    is_busy: impl FnOnce(&F::Output) -> bool,
) -> F::Output {
    let observations = Observations::default();
    let result = ACTIVE_TURN_START
        .scope(Arc::clone(&observations), request)
        .await;
    if is_busy(&result) {
        let fields = observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        eprintln!(
            "D48_NATIVE_TURN_START_BUSY input={input:?} producer={} admission_deadline={} state={fields:?}",
            fields
                .get("producer")
                .map_or("command_port", String::as_str),
            fields.contains_key("admission_deadline"),
        );
    }
    result
}

/// Capture the caller's observation before a retained operation spawns.
pub fn carry<F: Future>(work: F) -> impl Future<Output = F::Output> {
    let observations = ACTIVE_TURN_START.try_with(Arc::clone).ok();
    async move {
        match observations {
            Some(observations) => ACTIVE_TURN_START.scope(observations, work).await,
            None => work.await,
        }
    }
}

/// Save only typed state names and operation, input or turn IDs.
pub fn record(field: &'static str, value: impl FnOnce() -> String) {
    let _ = ACTIVE_TURN_START.try_with(|observations| {
        let value = value();
        observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(field, value);
    });
}

pub(crate) fn record_command_failure(failure: &CommandFailure, phase: &'static str) {
    let kind = match failure.classification().as_ref() {
        CommandFailure::Busy => "Busy",
        CommandFailure::NoActiveTurn => "NoActiveTurn",
        CommandFailure::SteerTurnMismatch { .. } => "SteerTurnMismatch",
        _ => return,
    };
    record("command_phase", || phase.into());
    record("command_failure_kind", || kind.into());
    if let Some(operation) = failure.operation_failure() {
        record("operation_failure", || {
            format!(
                "{:?}/{:?}/{:?}",
                operation.kind, operation.stage, operation.effect
            )
        });
        record("operation_id", || format!("{:?}", operation.operation_id));
    }
}
