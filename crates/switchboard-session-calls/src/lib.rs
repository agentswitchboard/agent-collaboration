//! Private call and position types for the session watch protocol.

mod frame_ordinal;
mod position_number;

pub use frame_ordinal::{FrameOrdinal, FrameOrdinalError, FrameOrdinalExhausted};
pub use position_number::{DecimalPositionError, HistoryEpoch, IssuedPosition};

use serde::{Deserialize, Serialize};
use workspace_ids::{RouterRunId, SessionRef};

/// An exact position made by the issuing Router for a session, Router run and history epoch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Continuation {
    pub target: SessionRef,
    pub router_run_id: RouterRunId,
    pub history_epoch: HistoryEpoch,
    pub after: IssuedPosition,
}

/// The caller's choice to start a new watch or resume at a position obtained from the Router.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum WatchStart {
    Fresh { target: SessionRef },
    Resume { position: Continuation },
}
