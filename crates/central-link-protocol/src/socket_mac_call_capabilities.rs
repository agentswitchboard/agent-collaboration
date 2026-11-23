//! The capability announcement can be accepted only once on each physical link.

use crate::{HandledMacCalls, MacCall, MacCallKind, MacCapabilities};
use fleet_rpc::CallFailure;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub struct SocketMacCallCapabilities(Option<HandledMacCalls>);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the socket already announced Mac call capabilities")]
pub struct DuplicateMacCapabilities;

impl SocketMacCallCapabilities {
    /// Empty and absent both handle no calls. Only absence allows an announcement.
    pub fn announce(
        &mut self,
        announcement: MacCapabilities,
    ) -> Result<(), DuplicateMacCapabilities> {
        if self.0.is_some() {
            return Err(DuplicateMacCapabilities);
        }
        self.0 = Some(announcement.handled_calls);
        Ok(())
    }

    #[must_use]
    pub fn handles(&self, call_kind: MacCallKind) -> bool {
        self.0
            .as_ref()
            .is_some_and(|handled| handled.contains(call_kind))
    }

    /// Keep an unsupported call equivalent to a disconnected socket.
    pub fn admit_outbound(&self, request: &MacCall) -> Result<(), CallFailure> {
        let call_kind = match request {
            MacCall::DeliverEnding(_) => MacCallKind::DeliverEnding,
            #[cfg(feature = "test-support")]
            MacCall::MacProbe(_) => return Ok(()),
            #[cfg(all(test, not(feature = "test-support")))]
            MacCall::MacProbe(_) => return Err(CallFailure::NeverReached),
        };
        if self.handles(call_kind) {
            Ok(())
        } else {
            Err(CallFailure::NeverReached)
        }
    }
}
