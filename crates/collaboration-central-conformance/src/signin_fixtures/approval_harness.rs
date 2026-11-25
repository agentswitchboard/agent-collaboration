//! Shared harness for driving the signed approval routes and reading their owning rows.
use std::future::Future;

use central_storage::SqlValue;
use serde_json::Value;

use crate::ConformanceResult;

pub trait PersonApprovalHarness {
    #[cfg(feature = "session-link-test-support")]
    fn person_socket(&self) -> Option<super::approval_socket::PersonApprovalEndpoint<'_>> {
        None
    }

    fn post(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
    ) -> impl Future<Output = ConformanceResult<(u16, Vec<u8>)>>;

    /// Advance the injected clock between transport admission and the applying sample.
    fn advance_at_approval_sample<Reply>(
        &self,
        send: impl Future<Output = ConformanceResult<Reply>>,
        advance_ms: u64,
    ) -> impl Future<Output = ConformanceResult<Reply>>;

    fn post_after_verification(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
        advance_ms: u64,
    ) -> impl Future<Output = ConformanceResult<(u16, Vec<u8>)>> {
        self.advance_at_approval_sample(self.post(path, body, headers), advance_ms)
    }

    fn write(
        &self,
        statement: &'static str,
        args: Vec<SqlValue>,
    ) -> impl Future<Output = ConformanceResult<()>>;

    fn lose_reply_after_apply(
        &self,
        operation: &str,
    ) -> impl Future<Output = ConformanceResult<()>>;

    fn read(
        &self,
        statement: &'static str,
    ) -> impl Future<Output = ConformanceResult<Vec<Vec<SqlValue>>>> {
        self.read_with_args(statement, Vec::new())
    }

    fn read_with_args(
        &self,
        statement: &'static str,
        args: Vec<SqlValue>,
    ) -> impl Future<Output = ConformanceResult<Vec<Vec<SqlValue>>>>;
}
