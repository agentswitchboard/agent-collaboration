#![expect(
    clippy::unwrap_used,
    reason = "The integration test fixtures fail at the call site on invalid specimens or missing expected effects."
)]

mod core_link_loss;
mod core_model_traces;
mod fixtures;
