use crate::{EngineTime, supported_time};
use central_storage::SafeInteger;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DueRow {
    pub key: crate::InstanceKey,
    pub occurrence_ms: i64,
    pub effective_due_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DueQueryError {
    #[error("the due horizon is outside the portable time range")]
    HorizonOutOfRange,
    #[error("the due cap is outside the portable integer range")]
    CapOutOfRange,
}

/// A checked owner SELECT with the two portable integer bindings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DueListQuery {
    pub sql: &'static str,
    pub horizon: SafeInteger,
    pub cap: SafeInteger,
}

/// Registry-owned static SQL binds the horizon and per-kind cap as ?1 and ?2.
pub fn due_list_sql(
    sql: &'static str,
    horizon: EngineTime,
    per_kind_cap: usize,
) -> Result<DueListQuery, DueQueryError> {
    if !supported_time(horizon) {
        return Err(DueQueryError::HorizonOutOfRange);
    }
    let horizon = SafeInteger::new(horizon.timestamp_millis())
        .map_err(|_| DueQueryError::HorizonOutOfRange)?;
    let cap = i64::try_from(per_kind_cap)
        .ok()
        .and_then(|cap| SafeInteger::new(cap).ok())
        .ok_or(DueQueryError::CapOutOfRange)?;
    Ok(DueListQuery { sql, horizon, cap })
}
