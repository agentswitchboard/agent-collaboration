//! The call kinds a Mac can advertise on its own machine link.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeSet;

/// A Central-to-Mac call kind that a production Mac can advertise.
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum MacCallKind {
    DeliverEnding,
    ApprovalNotice,
}

/// A set serialized as an array. An empty set is a normal announcement.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(transparent)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct HandledMacCalls(BTreeSet<MacCallKind>);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("duplicate supported Mac call kind: {call_kind:?}")]
pub struct DuplicateMacCallKind {
    pub call_kind: MacCallKind,
}

impl HandledMacCalls {
    #[must_use]
    pub fn contains(&self, call_kind: MacCallKind) -> bool {
        self.0.contains(&call_kind)
    }
}

impl TryFrom<Vec<MacCallKind>> for HandledMacCalls {
    type Error = DuplicateMacCallKind;

    fn try_from(call_kinds: Vec<MacCallKind>) -> Result<Self, Self::Error> {
        let mut handled_calls = BTreeSet::new();
        for call_kind in call_kinds {
            if !handled_calls.insert(call_kind) {
                return Err(DuplicateMacCallKind { call_kind });
            }
        }
        Ok(Self(handled_calls))
    }
}

impl<'de> Deserialize<'de> for HandledMacCalls {
    fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
        let call_kinds = Vec::<MacCallKind>::deserialize(input)?;
        Self::try_from(call_kinds).map_err(serde::de::Error::custom)
    }
}

/// The Mac announces its capabilities once after receiving Welcome.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct MacCapabilities {
    pub handled_calls: HandledMacCalls,
}

#[cfg(test)]
#[path = "mac_call_capabilities_test.rs"]
mod tests;

#[cfg(all(test, feature = "wire-export", not(target_arch = "wasm32")))]
#[path = "mac_call_capabilities_typescript_test.rs"]
mod typescript_tests;
