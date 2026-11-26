//! Determine whether an SDK error arrived from a connection known to be ending.

use agent_client_protocol::{Agent, ConnectionTo, Error};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(super) struct WriterReplySignals {
    pub(super) shutdown: CancellationToken,
    transport_ending: CancellationToken,
}

impl WriterReplySignals {
    pub(super) const fn new(
        shutdown: CancellationToken,
        transport_ending: CancellationToken,
    ) -> Self {
        Self {
            shutdown,
            transport_ending,
        }
    }

    pub(super) fn transport_ended(&self, connection: &ConnectionTo<Agent>, error: &Error) -> bool {
        self.transport_ending.is_cancelled()
            || connection.is_incoming_closed()
            || agent_client_protocol::is_incoming_transport_closed(error)
    }

    pub(super) fn reply_lost(&self, connection: &ConnectionTo<Agent>, error: &Error) -> bool {
        error_reply_lost(&self.shutdown, error, |error| {
            self.transport_ended(connection, error)
        })
    }
}

pub(super) fn error_reply_lost(
    shutdown: &CancellationToken,
    error: &Error,
    transport_ended: impl FnOnce(&Error) -> bool,
) -> bool {
    shutdown.is_cancelled() || transport_ended(error)
}
