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
use workspace_ids::{CentralSessionId, PersonId, RouterRunId};

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

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum CallCancellation {
    Stopped { cause: CallStopCause },
    RequesterGone {},
    EnrollmentEnded {},
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ParkedCallEnding {
    Decided {
        decision: SessionCallDecision,
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
    },
    Cancelled { reason: CallCancellation },
    TimedOut {},
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ParkedCallEndingView {
    Decided {
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
    },
    Giver {
        decision: SessionCallDecision,
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
    },
    Cancelled { reason: CallCancellation },
    TimedOut {},
}
}

impl ParkedCallEndingView {
    pub(crate) fn for_reader(ending: ParkedCallEnding, actor: &crate::DecidedBy) -> Self {
        match ending {
            ParkedCallEnding::Decided {
                decision,
                person_id,
            } if actor
                == &(crate::DecidedBy::Person {
                    person_id: person_id.clone(),
                }) =>
            {
                Self::Giver {
                    decision,
                    person_id,
                }
            }
            ParkedCallEnding::Decided { person_id, .. } => Self::Decided { person_id },
            ParkedCallEnding::Cancelled { reason } => Self::Cancelled { reason },
            ParkedCallEnding::TimedOut {} => Self::TimedOut {},
        }
    }
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ParkedCallStateView {
    Waiting {},
    Ended {
        ending: ParkedCallEndingView,
        ended_at: crate::CentralTime,
        delivery: crate::Delivery,
    },
}
}
