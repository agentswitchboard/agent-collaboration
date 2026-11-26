//! Shared harness for driving the person approval socket and reading their owning rows.
use std::future::Future;

use central_storage::SqlValue;
use serde_json::Value;

use crate::ConformanceResult;

pub trait PersonApprovalHarness {
    fn person_socket(&self) -> super::approval_socket::PersonApprovalEndpoint<'_>;

    fn post_revision(
        &self,
        body: &Value,
    ) -> impl Future<Output = ConformanceResult<(u16, Vec<u8>)>>;

    /// Advance the injected clock between transport admission and the applying sample.
    fn advance_at_approval_sample<Reply>(
        &self,
        send: impl Future<Output = ConformanceResult<Reply>>,
        advance_ms: u64,
    ) -> impl Future<Output = ConformanceResult<Reply>>;

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
