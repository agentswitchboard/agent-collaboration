#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "Test harnesses fail at the violated fixture or protocol expectation."
)]

mod central_contract;
mod fixtures;
mod wrong_builds;
