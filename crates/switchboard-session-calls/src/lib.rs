//! Private call and position types for the session watch protocol.

mod frame_ordinal;
mod object_reader;
mod position_number;

pub use frame_ordinal::{FrameOrdinal, FrameOrdinalError, FrameOrdinalExhausted};
pub use position_number::{DecimalPositionError, HistoryEpoch, IssuedPosition};

use object_reader::deserialize_object;
use serde::{Deserialize, Deserializer, Serialize};
use workspace_ids::{RouterRunId, SessionRef};

/// An exact position made by the issuing Router for a session, Router run and history epoch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Continuation {
    pub target: SessionRef,
    pub router_run_id: RouterRunId,
    pub history_epoch: HistoryEpoch,
    pub after: IssuedPosition,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContinuationFields {
    #[serde(deserialize_with = "deserialize_object")]
    target: SessionRef,
    router_run_id: RouterRunId,
    history_epoch: HistoryEpoch,
    after: IssuedPosition,
}

impl<'de> Deserialize<'de> for Continuation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let ContinuationFields {
            target,
            router_run_id,
            history_epoch,
            after,
        } = deserialize_object(deserializer)?;
        Ok(Self {
            target,
            router_run_id,
            history_epoch,
            after,
        })
    }
}

/// The caller's choice to start a new watch or resume at a position obtained from the Router.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum WatchStart {
    Fresh {
        #[serde(deserialize_with = "deserialize_object")]
        target: SessionRef,
    },
    Resume {
        position: Continuation,
    },
}
