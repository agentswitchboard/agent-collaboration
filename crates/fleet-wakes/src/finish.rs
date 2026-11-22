use crate::{
    stored_rows::{StoredWake, WakeVersion, plan_wake_read},
    timed_store::{plan_wake_attempt, plan_wake_transition},
};
use central_engine_model::{EngineTime, ItemFailureClass, MachineKind, StateTag};
use central_storage::ReadOutcome;
use fleet_engine::{DiscardSubjectStatements, SubjectLoadFailure, VersionedSubjectState};
use fleet_timer_policy::{FinishKind, Lifecycle, LifecycleFinish};

pub struct WakeKind;
impl FinishKind for WakeKind {
    const KIND: MachineKind = MachineKind("wake_timer");
}
pub type WakeFinish = LifecycleFinish<WakeKind>;

/// Only the lifecycle column is decoded; an unknown value is the one case the
/// finishing view cannot act on, and it is never coerced.
fn decode_lifecycle(
    rows: &ReadOutcome,
) -> Result<Option<VersionedSubjectState<Lifecycle, WakeVersion>>, SubjectLoadFailure<WakeVersion>>
{
    let Some((stored, version)) = StoredWake::observe(rows)? else {
        return Ok(None);
    };
    let state = stored
        .lifecycle()
        .map_err(|_| SubjectLoadFailure::CorruptSubjectRow {
            failure_class: ItemFailureClass::CorruptRow {
                column: "automation_wakes.state".into(),
                raw_value: None,
            },
            observed_subject_version: version.clone(),
        })?;
    Ok(Some(VersionedSubjectState {
        decoded_state: state,
        observed_subject_version: version,
    }))
}

pub fn wake_finish_store() -> DiscardSubjectStatements<WakeFinish, WakeVersion> {
    DiscardSubjectStatements {
        plan_discard_subject_read: plan_wake_read,
        decode_discard_subject: decode_lifecycle,
        plan_discard_subject_write: |prior, next, now: EngineTime| {
            plan_wake_transition(&prior.observed_subject_version, next.state_tag(), now)
        },
        plan_discard_effect_writes: |effect| match *effect {},
        discard_attempt_failure_writer: plan_wake_attempt,
    }
}

#[cfg(test)]
#[path = "finish_test.rs"]
mod tests;
