//! Loopback TLS and credentials for person-link integration tests.

#![expect(
    clippy::unwrap_used,
    reason = "fixture setup and required test receipts"
)]

pub mod fixture_authority;
pub mod fixture_signin;
