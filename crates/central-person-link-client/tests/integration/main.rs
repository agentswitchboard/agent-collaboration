#![expect(
    clippy::unwrap_used,
    reason = "fixture setup and required test receipts"
)]

mod fixture_authority;
mod fixture_signin;
mod login_and_refresh;
mod workspace_reconnect;
