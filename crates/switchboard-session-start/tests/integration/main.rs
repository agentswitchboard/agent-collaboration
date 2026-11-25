#![expect(
    clippy::unwrap_used,
    reason = "Test harnesses fail at the violated fixture or protocol expectation."
)]

mod native_make;
mod start_fixture;
mod start_histories;
mod start_table;
