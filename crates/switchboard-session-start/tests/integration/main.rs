#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "Test harnesses fail at the violated fixture or protocol expectation."
)]

mod central_contract;
mod choices;
mod executor;
mod fixtures;
mod histories;
mod model;
mod revision_4;
mod revision_5;
mod wrong_builds;
