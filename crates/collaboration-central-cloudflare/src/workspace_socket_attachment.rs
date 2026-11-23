//! Authority and service registration kept with the physical hibernating socket.

use central_link_protocol::SocketMacCallState;
use fleet_contracts::MachineConnectionAttachment;
use serde::{Deserialize, Serialize};
use worker::{Result, WebSocket};
use workspace_ids::RouterRunId;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SocketAttachment {
    pub(super) authority: MachineConnectionAttachment,
    pub(super) registered_run: Option<RouterRunId>,
    /// The announcement and bounded delivery ledger belong to this physical
    /// socket across hibernation. A reconnect inherits neither.
    #[serde(default)]
    pub(super) mac_calls: SocketMacCallState,
}

impl SocketAttachment {
    pub(super) fn new(authority: MachineConnectionAttachment) -> Self {
        Self {
            authority,
            registered_run: None,
            mac_calls: SocketMacCallState::default(),
        }
    }

    pub(super) fn read(socket: &WebSocket) -> Result<Option<Self>> {
        socket.deserialize_attachment()
    }

    pub(super) fn save(&self, socket: &WebSocket) -> Result<()> {
        socket.serialize_attachment(self)
    }
}
