//! The caller's reference to a request in one owning store.
use crate::link_request::{ReplyPayload, RequestScope};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use workspace_ids::RequestId;
use workspace_model::{RefusalReason, central_mail::CentralMailRefusal};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestStatusQuery {
    pub request_id: RequestId,
    pub scope: RequestScope,
}

/// The stored decision, scoped to the original caller and owning store.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum RequestStatusAnswer {
    Answered(ReplyPayload),
    Refused(RequestRefusal),
    NotFound,
    /// The query is inconclusive. The retained decision, if any, was not changed.
    CentralUnavailable {},
}

/// A domain refusal recorded in the request log.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    content = "reason",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum RequestRefusal {
    Workspace(RefusalReason),
    Mail(CentralMailRefusal),
}

#[cfg(test)]
#[path = "request_status_test.rs"]
mod tests;

#[cfg(all(test, feature = "wire-export", not(target_arch = "wasm32")))]
#[path = "request_status_typescript_test.rs"]
mod typescript_tests;
