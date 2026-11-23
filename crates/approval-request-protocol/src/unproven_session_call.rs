//! The held session operation crosses the link only as a summary and digest.
//!
//! The Router fingerprints a start's operation ID, agent program, canonical
//! working folder, requested mode, model and effort when given, ordered work
//! folders, session kind, entry point, and machine ID. The scratch folder and
//! other values derived by the Router are outside that fingerprint. An end
//! binds its operation ID, Central session ID, and end-for-good cause.
//!
//! The receiver of an allowed ending checks the request ID and contents digest
//! against the held request before it executes its own copy.
use serde::{Deserialize, Serialize};
use workspace_ids::{CentralSessionId, RouterRunId};

use crate::SessionCallWorkFolder;
use crate::approval_wire_codec::{approval_wire_record, approval_wire_union};

macro_rules! wire_string {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
        #[serde(rename_all = "camelCase")]
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
        pub enum $name { $($variant),+ }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
                match String::deserialize(decoder)?.as_str() {
                    $($value => Ok(Self::$variant),)+
                    invalid => Err(serde::de::Error::unknown_variant(invalid, &[$($value),+])),
                }
            }
        }
    };
}

const MAX_SESSION_CALL_WORK_FOLDERS: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    try_from = "Vec<SessionCallWorkFolder>",
    into = "Vec<SessionCallWorkFolder>"
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct SessionCallWorkFolders(Vec<SessionCallWorkFolder>);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("a session call can show at most {MAX_SESSION_CALL_WORK_FOLDERS} work folders")]
pub struct TooManySessionCallWorkFolders;

impl TryFrom<Vec<SessionCallWorkFolder>> for SessionCallWorkFolders {
    type Error = TooManySessionCallWorkFolders;

    fn try_from(folders: Vec<SessionCallWorkFolder>) -> Result<Self, Self::Error> {
        if folders.len() > MAX_SESSION_CALL_WORK_FOLDERS {
            return Err(TooManySessionCallWorkFolders);
        }
        Ok(Self(folders))
    }
}

impl From<SessionCallWorkFolders> for Vec<SessionCallWorkFolder> {
    fn from(folders: SessionCallWorkFolders) -> Self {
        folders.0
    }
}

impl SessionCallWorkFolders {
    #[must_use]
    pub fn as_slice(&self) -> &[SessionCallWorkFolder] {
        &self.0
    }
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct ParkedCallHolder {
    pub router_run: RouterRunId,
}
}

wire_string!(SessionCallProgram { Claude => "claude", Cursor => "cursor" });

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum SessionCallOperation {
    StartSession {
        agent_program: SessionCallProgram,
        working_folder: crate::SessionCallFolder,
        mode: crate::SessionCallMode,
        work_folders: SessionCallWorkFolders,
    },
    EndSessionForGood { session_id: CentralSessionId },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
/// Offers the fixed choices Allow and Decline for this request only.
pub struct CallOffer {
    pub operation: SessionCallOperation,
}
}

wire_string!(SessionCallDecision { Allow => "allow", Decline => "decline" });

wire_string!(CallStopCause { CallWithdrawn => "callWithdrawn" });
