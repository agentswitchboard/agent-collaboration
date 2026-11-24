#![expect(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "fixture setup and unexpected credential or retirement effects fail at the test site"
)]

mod lease_contract;
