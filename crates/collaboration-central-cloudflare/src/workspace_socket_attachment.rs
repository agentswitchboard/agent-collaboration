//! Authority and service registration kept with the physical hibernating socket.

use central_link_protocol::SocketMacCallCapabilities;
use fleet_contracts::MachineConnectionAttachment;
use serde::{Deserialize, Serialize};
use worker::{Result, WebSocket};
use workspace_ids::RouterRunId;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SocketAttachment {
    pub(super) authority: MachineConnectionAttachment,
    pub(super) registered_run: Option<RouterRunId>,
    /// This physical socket owns the set across hibernation. It has no SQL or
    /// per-machine record, and a later connection starts with no capabilities.
    #[serde(default)]
    pub(super) mac_capabilities: SocketMacCallCapabilities,
}

impl SocketAttachment {
    pub(super) fn new(authority: MachineConnectionAttachment) -> Self {
        Self {
            authority,
            registered_run: None,
            mac_capabilities: SocketMacCallCapabilities::default(),
        }
    }

    pub(super) fn read(socket: &WebSocket) -> Result<Option<Self>> {
        socket.deserialize_attachment()
    }

    pub(super) fn save(&self, socket: &WebSocket) -> Result<()> {
        socket.serialize_attachment(self)
    }
}
