#![expect(
    clippy::unwrap_used,
    reason = "The integration test fixtures fail at the call site on invalid specimens or missing expected effects."
)]

mod agent_withdrawal;
mod capacity_refusals;
mod confirmation_regressions;
mod core_link_loss;
mod core_model_traces;
mod fixtures;
mod generated_history;
mod raise_exposure;
mod review_regressions;
mod unproven_call;
