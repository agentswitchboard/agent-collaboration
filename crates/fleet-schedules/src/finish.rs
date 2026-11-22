use crate::{
    stored_rows::{StoredSchedule, plan_schedule_read},
    timed_store::{plan_schedule_attempt, plan_schedule_finish},
};
use central_engine_model::{EngineTime, ItemFailureClass, MachineKind, StateTag};
use central_storage::ReadOutcome;
use fleet_engine::{DiscardSubjectStatements, SubjectLoadFailure, VersionedSubjectState};
use fleet_timer_policy::{FinishKind, Lifecycle, LifecycleFinish};

pub struct AutomationKind;
impl FinishKind for AutomationKind {
    const KIND: MachineKind = MachineKind("automation_timer");
}
pub type AutomationFinish = LifecycleFinish<AutomationKind>;

/// Only the lifecycle column is decoded; an unknown value is the one case the
/// finishing view cannot act on, and it is never coerced.
fn decode_lifecycle(
    rows: &ReadOutcome,
) -> Result<Option<VersionedSubjectState<Lifecycle>>, SubjectLoadFailure> {
    let Some((stored, version)) = StoredSchedule::observe(rows)? else {
        return Ok(None);
    };
    let state = stored
        .lifecycle()
        .map_err(|_| SubjectLoadFailure::CorruptSubjectRow {
            failure_class: ItemFailureClass::CorruptRow {
                column: "automation_schedules.state".into(),
                raw_value: None,
            },
            observed_subject_version: version.clone(),
        })?;
    Ok(Some(VersionedSubjectState {
        decoded_state: state,
        observed_subject_version: version,
    }))
}

pub fn automation_finish_store() -> DiscardSubjectStatements<AutomationFinish> {
    DiscardSubjectStatements {
        plan_discard_subject_read: plan_schedule_read,
        decode_discard_subject: decode_lifecycle,
        plan_discard_subject_write: |prior, next, now: EngineTime| {
            plan_schedule_finish(&prior.observed_subject_version, next.state_tag(), now)
        },
        plan_discard_effect_writes: |effect| match *effect {},
        discard_attempt_failure_writer: plan_schedule_attempt,
    }
}
