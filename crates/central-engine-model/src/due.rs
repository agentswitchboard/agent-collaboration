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

/// The checked SELECT belonging to each timed owner.
#[cfg_attr(
    not(feature = "test-support"),
    doc = "The fixture probe cannot be requested in a production build.

```compile_fail,E0599
use central_engine_model::DueListKind;
let _ = DueListKind::Probes;
```"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DueListKind {
    Wakes,
    Schedules,
    DirectMessages,
    SystemExpiries,
    ApprovalExpiries,
    #[cfg(feature = "test-support")]
    Probes,
}

impl DueListKind {
    pub const fn machine_kind(self) -> &'static str {
        match self {
            Self::Wakes => "wake_timer",
            Self::Schedules => "automation_timer",
            Self::DirectMessages => "direct_message",
            Self::SystemExpiries => "system_message_expiry",
            Self::ApprovalExpiries => "approval_request",
            #[cfg(feature = "test-support")]
            Self::Probes => "engine_probe",
        }
    }
}

/// An owner SELECT and its portable integer bindings.
///
/// Runtime callers cannot supply alternate SQL to a checked list.
/// ```compile_fail,E0308
/// use central_engine_model::{EngineTime, due_list_query};
/// let _ = due_list_query("SELECT * FROM nonexistent_table", EngineTime::UNIX_EPOCH, 250);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DueListQuery {
    kind: DueListKind,
    horizon: SafeInteger,
    retry_cutoff: SafeInteger,
    cap: SafeInteger,
}

impl DueListQuery {
    pub const fn kind(self) -> DueListKind {
        self.kind
    }

    pub const fn retry_cutoff(self) -> i64 {
        self.retry_cutoff.get()
    }

    /// The horizon and per-kind cap bind in this order to ?1 and ?2.
    pub const fn bindings(self) -> (i64, i64) {
        (self.horizon.get(), self.cap.get())
    }
}

pub fn due_list_query(
    kind: DueListKind,
    horizon: EngineTime,
    per_kind_cap: usize,
) -> Result<DueListQuery, DueQueryError> {
    due_list_query_with_retry_cutoff(kind, horizon, horizon, per_kind_cap)
}

pub fn due_list_query_with_retry_cutoff(
    kind: DueListKind,
    horizon: EngineTime,
    retry_cutoff: EngineTime,
    per_kind_cap: usize,
) -> Result<DueListQuery, DueQueryError> {
    if !supported_time(horizon) || !supported_time(retry_cutoff) {
        return Err(DueQueryError::HorizonOutOfRange);
    }
    let horizon = SafeInteger::new(horizon.timestamp_millis())
        .map_err(|_| DueQueryError::HorizonOutOfRange)?;
    let retry_cutoff = SafeInteger::new(retry_cutoff.timestamp_millis())
        .map_err(|_| DueQueryError::HorizonOutOfRange)?;
    let cap = i64::try_from(per_kind_cap)
        .ok()
        .and_then(|cap| SafeInteger::new(cap).ok())
        .ok_or(DueQueryError::CapOutOfRange)?;
    Ok(DueListQuery {
        kind,
        horizon,
        retry_cutoff,
        cap,
    })
}
