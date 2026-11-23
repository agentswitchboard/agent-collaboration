//! One admission and attempt ledger for the calls owned by a physical socket.

use crate::{HandledMacCalls, MacCall, MacCallKind, MacCapabilities};
use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, CentralSessionId, RouterRunId};

/// Two UUIDv7 strings and compact tags per entry fit alongside the existing
/// authority and run comfortably within Workerd's 16 KiB attachment.
pub const MAX_SOCKET_MAC_CALL_ITEMS: usize = 96;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SocketMacCallState {
    #[serde(rename = "a")]
    announcement: Option<HandledMacCalls>,
    #[serde(rename = "i")]
    items: Vec<(MacCallDeliveryKey, MacCallAttemptPhase)>,
    #[serde(rename = "s")]
    saturation: MacCallSaturation,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "k", content = "v")]
pub enum MacCallDeliveryKey {
    #[serde(rename = "h")]
    Ending(ApprovalRequestId, RouterRunId),
    #[serde(rename = "n")]
    Notice(ApprovalRequestId, CentralSessionId),
}

impl MacCallDeliveryKey {
    pub const fn kind(&self) -> MacCallKind {
        match self {
            Self::Ending(..) => MacCallKind::DeliverEnding,
            Self::Notice(..) => MacCallKind::ApprovalNotice,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
enum MacCallAttemptPhase {
    #[serde(rename = "i")]
    InFlight,
    #[serde(rename = "f")]
    Finished,
    #[serde(rename = "s")]
    SettledInFlight,
}

#[derive(Debug, Default, Deserialize, Serialize)]
enum MacCallSaturation {
    #[default]
    #[serde(rename = "n")]
    Unobserved,
    #[serde(rename = "r")]
    ReportedIdle,
    #[serde(rename = "d")]
    Deferred,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the socket already announced Mac call capabilities")]
pub struct DuplicateMacCapabilities;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MacCallSkip {
    KindNotAdvertised,
    AlreadyOffered,
    FirstSaturation,
    Saturated,
}

impl SocketMacCallState {
    /// Empty and absent both handle no calls. Only absence allows an announcement.
    pub fn announce(
        &mut self,
        announcement: MacCapabilities,
    ) -> Result<(), DuplicateMacCapabilities> {
        if self.announcement.is_some() {
            return Err(DuplicateMacCapabilities);
        }
        self.announcement = Some(announcement.handled_calls);
        Ok(())
    }

    #[must_use]
    pub fn handles(&self, kind: MacCallKind) -> bool {
        self.announcement
            .as_ref()
            .is_some_and(|handled| handled.contains(kind))
    }

    /// The ordinary absence of an advertised call kind cannot start transport.
    pub fn check_kind(&self, kind: MacCallKind) -> Result<(), MacCallSkip> {
        if self.handles(kind) {
            Ok(())
        } else {
            Err(MacCallSkip::KindNotAdvertised)
        }
    }

    /// The generic, fixture or probe route cannot bypass capability admission.
    pub fn check_request(&self, request: &MacCall) -> Result<(), MacCallSkip> {
        match request {
            MacCall::DeliverEnding(_) => self.check_kind(MacCallKind::DeliverEnding),
            MacCall::ApprovalNotice(_) => self.check_kind(MacCallKind::ApprovalNotice),
            #[cfg(feature = "test-support")]
            MacCall::MacProbe(_) => Ok(()),
            #[cfg(all(test, not(feature = "test-support")))]
            MacCall::MacProbe(_) => Err(MacCallSkip::KindNotAdvertised),
        }
    }

    /// Claim the only attempt for this debt on this socket. An exhausted ledger
    /// keeps new work owed and records whether its saturation was already seen.
    pub fn begin(&mut self, item: MacCallDeliveryKey) -> Result<(), MacCallSkip> {
        self.check_kind(item.kind())?;
        if self.items.iter().any(|(existing, _)| existing == &item) {
            return Err(MacCallSkip::AlreadyOffered);
        }
        if self.items.len() >= MAX_SOCKET_MAC_CALL_ITEMS {
            let first = matches!(self.saturation, MacCallSaturation::Unobserved);
            self.saturation = MacCallSaturation::Deferred;
            return Err(if first {
                MacCallSkip::FirstSaturation
            } else {
                MacCallSkip::Saturated
            });
        }
        self.items.push((item, MacCallAttemptPhase::InFlight));
        Ok(())
    }

    /// An unanswered attempt is finished on the socket and remains owed in storage.
    pub fn finish(&mut self, item: &MacCallDeliveryKey) -> bool {
        self.items.retain_mut(|(existing, phase)| {
            if existing != item {
                return true;
            }
            match phase {
                MacCallAttemptPhase::SettledInFlight => false,
                MacCallAttemptPhase::InFlight | MacCallAttemptPhase::Finished => {
                    *phase = MacCallAttemptPhase::Finished;
                    true
                }
            }
        });
        self.resume_if_capacity()
    }

    /// A committed debt observation is the only authorization to forget a key.
    /// While its attempt still owns a waiter, retain the slot until it finishes.
    pub fn reclaim_settled(&mut self, item: &MacCallDeliveryKey) -> bool {
        self.items.retain_mut(|(existing, phase)| {
            if existing != item {
                return true;
            }
            match phase {
                MacCallAttemptPhase::Finished => false,
                MacCallAttemptPhase::InFlight | MacCallAttemptPhase::SettledInFlight => {
                    *phase = MacCallAttemptPhase::SettledInFlight;
                    true
                }
            }
        });
        self.resume_if_capacity()
    }

    fn resume_if_capacity(&mut self) -> bool {
        if self.items.len() < MAX_SOCKET_MAC_CALL_ITEMS
            && matches!(self.saturation, MacCallSaturation::Deferred)
        {
            self.saturation = MacCallSaturation::ReportedIdle;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
#[path = "socket_mac_call_capabilities_test.rs"]
mod tests;
