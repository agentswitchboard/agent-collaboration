//! The direct-mail claims owned by a physical socket.

use std::collections::BTreeSet;
use workspace_ids::MessageId;

/// Each key is either unoffered (absent) or claimed on this socket.
#[derive(Default)]
pub struct SocketDirectMailOffers {
    claimed: BTreeSet<MessageId>,
}

impl SocketDirectMailOffers {
    #[must_use]
    pub fn claim(&mut self, message: &MessageId) -> bool {
        self.claimed.insert(message.clone())
    }
}
