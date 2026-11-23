//! Shared harness for driving the signed approval routes and reading their owning rows.
use std::future::Future;

use central_storage::SqlValue;
use serde_json::Value;

use crate::ConformanceResult;

pub trait PersonApprovalHarness {
    fn post(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
    ) -> impl Future<Output = ConformanceResult<(u16, Vec<u8>)>>;

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
    ) -> impl Future<Output = ConformanceResult<Vec<Vec<SqlValue>>>>;
}
