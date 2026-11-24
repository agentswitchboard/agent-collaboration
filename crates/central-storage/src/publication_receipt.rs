use crate::AlarmPublicationFailure;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineAlarmTimes {
    pub earliest_on_grid_due_unix_millis: Option<i64>,
    pub earliest_legacy_due_unix_millis: Option<i64>,
}

/// Called synchronously at publication with the host's freshly sampled clock.
/// The callback uses the durable due times and performs no I/O. Native hosts
/// provide the synchronous alarm write under their storage ordering guard.
/// Cloudflare sets the alarm inside its storage transaction.
pub type EngineAlarmDecider = std::sync::Arc<
    dyn Fn(
            EngineAlarmTimes,
            i64,
            &mut dyn FnMut(Option<i64>),
        ) -> Result<Option<i64>, AlarmPublicationFailure>
        + Send
        + Sync,
>;

/// The physical alarm outcome, separate from whether database writes committed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum AlarmPublication {
    Unchanged,
    Set { unix_millis: i64 },
    Cleared,
    Failed { reason: AlarmPublicationFailure },
}
