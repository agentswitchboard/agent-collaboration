//! Shared test fixture boundaries for the owning-host trust traces.
use super::{IssuerOrderHost, SignInKeys};
use crate::ConformanceResult;
use serde_json::Value;
use std::future::Future;

pub trait TrustTraceHost: IssuerOrderHost {
    fn revision(&self) -> impl Future<Output = ConformanceResult<(u16, Value)>>;
    fn reopen_owner(&mut self) -> impl Future<Output = ConformanceResult<()>>;
}

pub(super) fn expect_reply(
    issuer: &SignInKeys,
    label: impl std::fmt::Display,
    before: usize,
    expected_fetches: usize,
    status: u16,
    expected_status: u16,
) -> ConformanceResult<()> {
    let fetched = issuer.fetches() - before;
    if (status, fetched) == (expected_status, expected_fetches) {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected status {expected_status} and {expected_fetches} fetches, got status {status} and {fetched} fetches"
        )
        .into())
    }
}
