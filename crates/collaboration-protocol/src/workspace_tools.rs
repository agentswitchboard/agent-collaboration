//! Arguments of the workspace tools that read from or write to Central.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
pub use workspace_ids::SessionId;
pub use workspace_model::{ThreadId, WorkspaceId};

use crate::link_tool_result::{LinkToolArguments, RemovedField};

pub const THREAD_SHOW_DESCRIPTION: &str = "Read one thread from Central by workspaceId and threadId. Report your current callerSessionId. This reads the thread itself, not its posts.";
pub const THREAD_CLI_HELP: &str = "  thread show --workspace-id UUID THREAD_ID [--json]\n";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadShowArguments {
    pub caller_session_id: SessionId,
    pub workspace_id: WorkspaceId,
    pub thread_id: ThreadId,
}

impl LinkToolArguments for ThreadShowArguments {
    const REMOVED_INPUTS: &'static [(&'static str, RemovedField)] = &[
        ("includeRedacted", RemovedField::IncludeRedacted),
        ("protocolVersion", RemovedField::ProtocolVersion),
        ("projectId", RemovedField::ProjectQualifier),
        ("project", RemovedField::ProjectQualifier),
    ];
}
