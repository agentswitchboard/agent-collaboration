#![expect(
    clippy::unwrap_used,
    reason = "The integration test fixtures fail at the call site on invalid specimens or missing expected effects."
)]

mod agent_withdrawal;
mod core_link_loss;
mod core_model_traces;
mod fixtures;
mod generated_history;
mod review_regressions;
