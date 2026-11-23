//! Bounded direct-mail claims owned by one physical socket.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use workspace_ids::MessageId;
use workspace_model::Timestamp;

/// Allow room for the typed-call ledger and authority in a hibernating attachment.
pub const SOCKET_DIRECT_MAIL_LIMIT: usize = 64;

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SocketDirectMailOffers {
    #[serde(rename = "c")]
    claimed: BTreeMap<MessageId, Timestamp>,
    #[serde(rename = "d")]
    deferred: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NotOffered {
    NoLongerQueued,
    AlreadyOffered,
    Deferred,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveOfferOutcome {
    Offered,
    DefinitivelyNotOffered(NotOffered),
    Ambiguous,
}

impl LiveOfferOutcome {
    /// Once a physical write has begun, failure cannot prove the peer saw nothing.
    #[must_use]
    pub fn from_write(written: bool) -> Self {
        if written {
            Self::Offered
        } else {
            Self::Ambiguous
        }
    }
}

impl SocketDirectMailOffers {
    /// Past-cutoff claims are candidates for a durable retirement check. A
    /// wall-clock sample alone cannot prove that an offer stayed ineligible.
    pub fn elapsed_claims(&self, now: Timestamp) -> impl Iterator<Item = (&MessageId, Timestamp)> {
        self.claimed
            .iter()
            .filter_map(move |(message, cutoff)| (*cutoff <= now).then_some((message, *cutoff)))
    }

    pub fn has_deferred(&self) -> bool {
        self.deferred
    }

    /// Called after fallible preparation, in the same turn as the physical write.
    pub fn claim(&mut self, message: &MessageId, cutoff: Timestamp) -> Result<(), NotOffered> {
        if self.claimed.contains_key(message) {
            return Err(NotOffered::AlreadyOffered);
        }
        if self.claimed.len() >= SOCKET_DIRECT_MAIL_LIMIT {
            self.deferred = true;
            return Err(NotOffered::Deferred);
        }
        self.claimed.insert(message.clone(), cutoff);
        Ok(())
    }

    pub fn release(&mut self, message: &MessageId) {
        self.claimed.remove(message);
    }

    /// The caller rescans the durable direct queue. A full scan clears the
    /// signal, and hitting the limit during that scan sets it again.
    pub fn take_deferred(&mut self) -> bool {
        if self.claimed.len() >= SOCKET_DIRECT_MAIL_LIMIT {
            return false;
        }
        std::mem::take(&mut self.deferred)
    }
}

#[cfg(test)]
#[path = "socket_direct_mail_test.rs"]
mod tests;
