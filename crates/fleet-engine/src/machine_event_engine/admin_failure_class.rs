use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The closed failure classes of engine_work_attempts.last_failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum EngineFailureClass {
    CorruptRow,
    ConstraintViolated,
    StatementRejected,
    Refused,
    OtherStoreUnavailable,
}
