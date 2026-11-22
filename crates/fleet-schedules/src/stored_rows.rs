use central_engine_model::InstanceKey;
use central_storage::{ReadOutcome, Statement, StoreFailure};
use fleet_engine::{checked_engine_statement, checked_subject_projection};
use fleet_runtime_ports::{stored_values::json, workspace_decode_error::corrupt};
use workspace_ids::SessionRef;
use workspace_model::{PostText, Timestamp, central_mail as mail};

use agent_automation::TimingRule;
use fleet_timer_policy::LIFETIME_SECONDS;
use workspace_ids::AutomationId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomationRow {
    pub(crate) id: AutomationId,
    pub(crate) target: SessionRef,
    pub(crate) timing: TimingRule,
    pub(crate) anchor: Timestamp,
    pub(crate) expires_at: Timestamp,
    pub(crate) evaluated: Option<Timestamp>,
    pub(crate) due: Option<Timestamp>,
    pub(crate) text: PostText,
    pub(crate) state: mail::AutomationState,
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

macro_rules! declare_schedule_projections {
    ($($field:ident : $column:ty => $identity:ident),+ $(,)?) => {
        checked_subject_projection!(ScheduleFields { $($field: $column,)+ });
        checked_subject_projection!(pub(super) StoredSchedule {
            $($field: $column => $identity,)+
            updated_at: String => UpdatedAt,
        }; ScheduleVersion, schedule_columns);

        impl StoredSchedule {
            fn into_schedule_fields(self) -> ScheduleFields {
                ScheduleFields { $($field: self.$field,)+ }
            }

        }
    };
}

declare_schedule_projections!(
    automation_id: Option<String> => AutomationId,
    target: String => Target,
    timing_rule: String => TimingRule,
    anchor_at: String => AnchorAt,
    expires_at: String => ExpiresAt,
    evaluated_through_at: Option<String> => EvaluatedThroughAt,
    text: String => Text,
    next_due_at: Option<String> => NextDueAt,
    state: String => State,
    created_at: String => CreatedAt,
);

#[cfg(test)]
type CheckedSchedule = StoredSchedule<fleet_engine::owner_sql::CheckedStorage>;

pub(super) fn plan_schedule_read(key: &InstanceKey) -> Statement {
    checked_engine_statement!(
        query_file_as,
        [CheckedSchedule],
        "src/queries/read_schedule.sql",
        schedule_id = key.0.clone(),
    )
}

impl StoredSchedule {
    pub(super) fn lifecycle(&self) -> Result<fleet_timer_policy::Lifecycle, StoreFailure> {
        fleet_timer_policy::Lifecycle::from_stored_tag(self.state.stored_text()?)
            .ok_or_else(|| corrupt("automation_schedules.state"))
    }

    pub(super) fn decode_schedule(self) -> Result<AutomationRow, StoreFailure> {
        self.into_schedule_fields().decode_schedule()
    }
}

pub(crate) fn decode_automations(rows: &ReadOutcome) -> Result<Vec<AutomationRow>, StoreFailure> {
    rows.rows
        .iter()
        .map(|row| ScheduleFields::read_named(rows, row)?.decode_schedule())
        .collect()
}

impl ScheduleFields {
    fn decode_schedule(&self) -> Result<AutomationRow, StoreFailure> {
        let state: mail::AutomationState = serde_json::from_value(serde_json::Value::String(
            self.state.stored_text()?.to_owned(),
        ))
        .map_err(|_| corrupt("automation_schedules.state"))?;
        let due = self
            .next_due_at
            .stored_optional_text()?
            .map(|time| parse_time(time, "next_due_at"))
            .transpose()?;
        let evaluated = self
            .evaluated_through_at
            .stored_optional_text()?
            .map(|time| parse_time(time, "evaluated_through_at"))
            .transpose()?;
        if state == mail::AutomationState::Active && due.is_none() {
            return Err(corrupt("automation_schedules.next_due_at"));
        }
        if state == mail::AutomationState::Done && due.is_some() {
            return Err(corrupt("automation_schedules.next_due_at"));
        }
        if due
            .zip(evaluated)
            .is_some_and(|(due, evaluated)| due <= evaluated)
        {
            return Err(corrupt("automation_schedules.next_due_at"));
        }
        let timing: TimingRule = json(
            self.timing_rule.stored_text()?,
            "automation_schedules.timing_rule",
        )?;
        let anchor = parse_time(
            self.anchor_at.stored_text()?,
            "automation_schedules.anchor_at",
        )?;
        let expires_at = parse_time(
            self.expires_at.stored_text()?,
            "automation_schedules.expires_at",
        )?;
        let admission = parse_time(
            self.created_at.stored_text()?,
            "automation_schedules.created_at",
        )?;
        let expected_expiry = fleet_timer_policy::lifetime_end(admission)
            .ok_or_else(|| corrupt("automation_schedules.created_at"))?;
        if expires_at != expected_expiry {
            return Err(corrupt("automation_schedules.expires_at"));
        }
        if central_engine_model::aligned_at_or_after(admission, 300) != Some(anchor) {
            return Err(corrupt("automation_schedules.anchor_at"));
        }
        if due.is_some_and(|due| {
            !central_engine_model::on_five_minute_grid(due) || due > expires_at || due < anchor
        }) {
            return Err(corrupt("automation_schedules.next_due_at"));
        }
        if state == mail::AutomationState::Active {
            let due = due.ok_or_else(|| corrupt("automation_schedules.next_due_at"))?;
            let matches_rule = match &timing {
                TimingRule::At { at } => *at > admission && due == *at,
                TimingRule::After { seconds } => {
                    *seconds >= 300
                        && i64::from(*seconds) <= LIFETIME_SECONDS
                        && *seconds % 300 == 0
                        && crate::timing::first_due(&timing, admission)
                            .map_err(|_| corrupt("automation_schedules.timing_rule"))?
                            == Some(due)
                }
                TimingRule::Interval { seconds } => {
                    *seconds >= 300
                        && i64::from(*seconds) <= LIFETIME_SECONDS
                        && *seconds % 300 == 0
                        && due.signed_duration_since(anchor).num_seconds() % i64::from(*seconds)
                            == 0
                }
                TimingRule::Cron { .. } => {
                    let before = due
                        .checked_sub_signed(chrono::Duration::seconds(1))
                        .ok_or_else(|| corrupt("automation_schedules.next_due_at"))?;
                    timing
                        .next_due(admission, Some(before))
                        .map_err(|_| corrupt("automation_schedules.timing_rule"))?
                        == Some(due)
                }
            };
            if !matches_rule {
                return Err(corrupt("automation_schedules.next_due_at"));
            }
        }
        Ok(AutomationRow {
            id: parse(
                self.automation_id.stored_text()?,
                "automation_schedules.automation_id",
            )?,
            target: json(self.target.stored_text()?, "automation_schedules.target")?,
            timing,
            anchor,
            expires_at,
            evaluated,
            due,
            text: self
                .text
                .stored_text()?
                .to_owned()
                .try_into()
                .map_err(|_| corrupt("automation_schedules.text"))?,
            state,
        })
    }
}
