//! Workspace reads through the same local session admission and live link as mail.

use collaboration_protocol::{
    message_tools::MessageToolRefusal,
    workspace_tools::{ThreadShowArguments, WorkspaceReply},
};

use super::message_operations::MessageOperations;

pub struct WorkspaceOperations<'app> {
    link_operations: &'app MessageOperations,
}

impl<'app> WorkspaceOperations<'app> {
    pub(super) fn new(link_operations: &'app MessageOperations) -> Self {
        Self { link_operations }
    }

    pub async fn show_thread(
        &self,
        input: ThreadShowArguments,
    ) -> Result<WorkspaceReply, MessageToolRefusal> {
        let caller = self
            .link_operations
            .admit_caller(Some(input.caller_session_id))
            .await?;
        self.link_operations
            .unit()?
            .show_thread(&caller, input.workspace_id, input.thread_id)
            .await
    }
}
