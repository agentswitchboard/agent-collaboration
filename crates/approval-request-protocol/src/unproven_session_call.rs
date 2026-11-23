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

use crate::approval_wire_codec::{approval_wire_record, approval_wire_union};

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct ParkedCallHolder {
    pub router_run: RouterRunId,
}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum SessionCallProgram {
    Claude,
    Cursor,
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum SessionCallOperation {
    StartSession {
        agent_program: SessionCallProgram,
        working_folder: crate::SessionCallFolder,
        mode: crate::SessionCallMode,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum SessionCallDecision {
    Allow,
    Decline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum CallStopCause {
    CallWithdrawn,
}
