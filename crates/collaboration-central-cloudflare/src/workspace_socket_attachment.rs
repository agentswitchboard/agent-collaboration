//! Authority and service registration kept with the physical hibernating socket.

use central_link_protocol::{SocketDirectMailOffers, SocketMacCallState};
use fleet_contracts::MachineConnectionAttachment;
use serde::{Deserialize, Serialize};
use worker::{Result, State, WebSocket};
use workspace_ids::{MachineId, RouterRunId};

pub(super) const SOCKET_TAG_PREFIX: &str = "typed-link:";

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SocketAttachment {
    pub(super) authority: MachineConnectionAttachment,
    pub(super) registered_run: Option<RouterRunId>,
    /// The announcement and bounded delivery ledger belong to this physical
    /// socket across hibernation. A reconnect inherits neither.
    #[serde(default)]
    pub(super) mac_calls: SocketMacCallState,
    #[serde(default)]
    pub(super) mail_offers: SocketDirectMailOffers,
}

impl SocketAttachment {
    pub(super) fn new(authority: MachineConnectionAttachment) -> Self {
        Self {
            authority,
            registered_run: None,
            mac_calls: SocketMacCallState::default(),
            mail_offers: SocketDirectMailOffers::default(),
        }
    }

    pub(super) fn read(socket: &WebSocket) -> Result<Option<Self>> {
        socket.deserialize_attachment()
    }

    pub(super) fn save(&self, socket: &WebSocket) -> Result<()> {
        socket.serialize_attachment(self)
    }
}

pub(super) fn is_current_socket(state: &State, socket: &WebSocket, machine: &MachineId) -> bool {
    let Some(physical_tag) = socket_tag(state, socket) else {
        return false;
    };
    state
        .get_websockets_with_tag(machine.as_str())
        .into_iter()
        .find(socket_open)
        .and_then(|current| socket_tag(state, &current))
        .is_some_and(|tag| tag == physical_tag)
}

pub(super) fn socket_tag(state: &State, socket: &WebSocket) -> Option<String> {
    state
        .as_raw()
        .get_tags(socket.as_ref())
        .ok()?
        .into_iter()
        .find(|tag| tag.starts_with(SOCKET_TAG_PREFIX))
}

pub(super) fn socket_open(socket: &WebSocket) -> bool {
    socket.as_ref().ready_state() == web_sys::WebSocket::OPEN
}
