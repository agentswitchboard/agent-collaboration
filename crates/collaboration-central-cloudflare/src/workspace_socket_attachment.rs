//! Authority and service registration kept with the physical hibernating socket.

use fleet_contracts::MachineConnectionAttachment;
use serde::{Deserialize, Serialize};
use worker::{Result, WebSocket};
use workspace_ids::RouterRunId;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SocketAttachment {
    pub(super) authority: MachineConnectionAttachment,
    pub(super) registered_run: Option<RouterRunId>,
}

impl SocketAttachment {
    pub(super) fn new(authority: MachineConnectionAttachment) -> Self {
        Self {
            authority,
            registered_run: None,
        }
    }

    pub(super) fn read(socket: &WebSocket) -> Result<Option<Self>> {
        socket.deserialize_attachment()
    }

    pub(super) fn save(&self, socket: &WebSocket) -> Result<()> {
        socket.serialize_attachment(self)
    }
}
