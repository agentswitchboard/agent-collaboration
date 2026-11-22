use central_engine_model::InstanceKey;
use central_storage::{Statement, StoreFailure};
use fleet_engine::{checked_engine_statement, checked_subject_projection};
use fleet_runtime_ports::{stored_values::json, workspace_decode_error::corrupt};
use workspace_ids::SessionRef;
use workspace_model::{PostText, Timestamp, central_mail as mail};

use workspace_ids::WakeId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WakeRow {
    pub(crate) id: WakeId,
    pub(crate) target: SessionRef,
    pub(crate) due: Timestamp,
    pub(crate) text: PostText,
    pub(crate) state: mail::WakeState,
}
fn parse_time(value: &str, field: &str) -> Result<Timestamp, StoreFailure> {
    let time: Timestamp = parse(value, field)?;
    if !central_engine_model::supported_time(time) {
        return Err(corrupt(field));
    }
    Ok(time)
}
fn parse<T: std::str::FromStr>(value: &str, column: &str) -> Result<T, StoreFailure> {
    value.parse().map_err(|_| corrupt(column))
}

checked_subject_projection!(pub(super) StoredWake {
    wake_id: Option<String> => WakeId,
    target: String => Target,
    due_at: String => DueAt,
    text: String => Text,
    state: String => State,
    created_at: String => CreatedAt,
    updated_at: String => UpdatedAt,
}; WakeVersion, wake_columns);

#[cfg(test)]
type CheckedWake = StoredWake<fleet_engine::owner_sql::CheckedStorage>;

pub(super) fn plan_wake_read(key: &InstanceKey) -> Statement {
    checked_engine_statement!(
        query_file_as,
        [CheckedWake],
        "src/queries/read_wake.sql",
        wake_id = key.0.clone(),
    )
}

impl StoredWake {
    pub(super) fn decode_wake(&self) -> Result<WakeRow, StoreFailure> {
        let id = parse(self.wake_id.stored_text()?, "automation_wakes.wake_id")?;
        let target = json(self.target.stored_text()?, "automation_wakes.target")?;
        let due = parse_time(self.due_at.stored_text()?, "automation_wakes.due_at")?;
        let created_at = parse_time(
            self.created_at.stored_text()?,
            "automation_wakes.created_at",
        )?;
        let expiry = fleet_timer_policy::lifetime_end(created_at)
            .ok_or_else(|| corrupt("automation_wakes.created_at"))?;
        if !central_engine_model::on_five_minute_grid(due) || due <= created_at || due > expiry {
            return Err(corrupt("automation_wakes.due_at"));
        }
        Ok(WakeRow {
            id,
            target,
            due,
            text: self
                .text
                .stored_text()?
                .to_owned()
                .try_into()
                .map_err(|_| corrupt("automation_wakes.text"))?,
            state: serde_json::from_value(serde_json::Value::String(
                self.state.stored_text()?.to_owned(),
            ))
            .map_err(|_| corrupt("automation_wakes.state"))?,
        })
    }

    pub(super) fn lifecycle(&self) -> Result<fleet_timer_policy::Lifecycle, StoreFailure> {
        fleet_timer_policy::Lifecycle::from_stored_tag(self.state.stored_text()?)
            .ok_or_else(|| corrupt("automation_wakes.state"))
    }
}
