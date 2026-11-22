//! Private payloads for starting and querying a session load.

use serde::{Deserialize, Serialize};
use workspace_ids::{SessionOperationId, SessionRef};

/// The caller's request to start a session load under an operation ID it already holds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadRequest {
    pub target: SessionRef,
    pub operation_id: SessionOperationId,
}

/// The caller's read-only query for a load identified by its original target and operation ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadQuery {
    pub target: SessionRef,
    pub operation_id: SessionOperationId,
}
