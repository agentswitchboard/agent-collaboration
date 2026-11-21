use crate::stored_rows::WAKE_FAILURE;
use central_engine_model::{EngineTime, ItemFailureClass, MachineKind};
use central_storage::ReadOutcome;
use fleet_engine::{
    DiscardSubjectStatements, SubjectLoadFailure, VersionedSubjectState,
    read_observed_subject_version,
};
use fleet_runtime_ports::stored_values::format_utc_millis;
use fleet_runtime_ports::{
    guarded_statements::{exact, text},
    stored_values::string,
};
use fleet_timer_policy::{FinishKind, Lifecycle, LifecycleFinish};

pub struct WakeKind;
impl FinishKind for WakeKind {
    const KIND: MachineKind = MachineKind("wake_timer");
}
pub type WakeFinish = LifecycleFinish<WakeKind>;

/// Only the lifecycle column is decoded; an unknown value is the one case the
/// finishing view cannot act on, and it is never coerced.
fn lifecycle(rows: &ReadOutcome, column: &'static str) -> Result<Lifecycle, ItemFailureClass> {
    let corrupt = || ItemFailureClass::CorruptRow {
        column: column.to_owned(),
        raw_value: None,
    };
    let row = rows.rows.first().ok_or_else(corrupt)?;
    Lifecycle::from_stored_tag(string(rows, row, "state").map_err(|_| corrupt())?)
        .ok_or_else(corrupt)
}

fn decode_lifecycle(
    rows: &ReadOutcome,
    state_column: &'static str,
) -> Result<Option<VersionedSubjectState<Lifecycle>>, SubjectLoadFailure> {
    let Some(version) = read_observed_subject_version(rows)? else {
        return Ok(None);
    };
    match lifecycle(rows, state_column) {
        Ok(state) => Ok(Some(VersionedSubjectState {
            decoded_state: state,
            observed_subject_version: version,
        })),
        Err(class) => Err(SubjectLoadFailure::CorruptSubjectRow {
            failure_class: class,
            observed_subject_version: version,
        }),
    }
}

pub fn wake_finish_store() -> DiscardSubjectStatements<WakeFinish> {
    DiscardSubjectStatements {
        read_discard_subject_sql: "SELECT wake_id,target,due_at,text,state,created_at,updated_at FROM automation_wakes WHERE wake_id=?",
        decode_discard_subject: |rows| decode_lifecycle(rows, "automation_wakes.state"),
        plan_discard_subject_write: |prior, next, now: EngineTime| {
            let mut args = vec![
                text(central_engine_model::StateTag::state_tag(next)),
                text(format_utc_millis(now)),
            ];
            args.extend(prior.observed_subject_version.0.clone());
            Ok(exact(
                "UPDATE automation_wakes SET state=?1,updated_at=?2 WHERE wake_id IS ?3 AND target IS ?4 AND due_at IS ?5 AND text IS ?6 AND state IS ?7 AND created_at IS ?8 AND updated_at IS ?9",
                args,
            ))
        },
        plan_discard_effect_writes: |effect| match *effect {},
        write_discard_attempt_failure_sql: WAKE_FAILURE,
    }
}

#[cfg(test)]
#[path = "finish_test.rs"]
mod tests;
