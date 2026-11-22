use central_engine_model::{
    AttemptState, EngineTime, FailureRecord, ItemFailureClass, supported_time,
};
use central_storage::{ConstraintKind, ReadOutcome, StoreFailure};
use fleet_contracts::AttemptGuard;
use fleet_runtime_ports::workspace_decode_error::corrupt;

use super::attempt_projection::{AttemptCorruption, ObservedColumn, attempt_projection};
use std::num::NonZeroU32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredAttempt {
    pub observed_attempt_guard: AttemptGuard,
    pub decoded_failure_record: Result<FailureRecord, AttemptCorruption>,
}

macro_rules! declare_attempt_projections {
    ($($field:ident : $column:ty),+ $(,)?) => {
        attempt_projection!(AttemptRecordProjection { $($field: $column,)+ });
        attempt_projection!(FullAttemptProjection {
            machine_kind: String,
            subject_key: String,
            $($field: $column,)+
        });

        impl FullAttemptProjection {
            pub(super) fn into_record_projection(self) -> AttemptRecordProjection {
                AttemptRecordProjection {
                    $($field: self.$field,)+
                }
            }
        }
    };
}

declare_attempt_projections!(
    attempt_id: Option<String>,
    state: String,
    failure_count: i64,
    next_try_at: Option<String>,
    last_failure: String,
    last_failure_detail: Option<String>,
    first_failed_at: String,
    last_failed_at: String,
    retired_at: Option<String>,
    created_at: String,
    updated_at: String,
);

#[cfg(test)]
pub(super) type CheckedFullAttemptProjection =
    FullAttemptProjection<super::attempt_projection::CheckedStorage>;

fn parse_engine_time(
    text: &str,
    bad: impl FnOnce() -> AttemptCorruption,
) -> Result<EngineTime, AttemptCorruption> {
    text.parse::<EngineTime>()
        .ok()
        .filter(|time| supported_time(*time))
        .ok_or_else(bad)
}

fn read_engine_time(column: &ObservedColumn<String>) -> Result<EngineTime, AttemptCorruption> {
    parse_engine_time(column.text()?, || column.corrupt())
}

fn read_nullable_engine_time(
    column: &ObservedColumn<Option<String>>,
) -> Result<Option<EngineTime>, AttemptCorruption> {
    column
        .nullable_text()?
        .map(|text| parse_engine_time(text, || column.corrupt()))
        .transpose()
}

impl AttemptRecordProjection {
    fn decode_stored_failure_class(&self) -> Result<ItemFailureClass, AttemptCorruption> {
        let detail = self.last_failure_detail.nullable_text()?;
        let bad_detail = || self.last_failure_detail.corrupt();
        match self.last_failure.text()? {
            "corrupt_row" => {
                let detail = detail
                    .filter(|value| !value.is_empty())
                    .ok_or_else(bad_detail)?;
                let (column, raw_value) = detail
                    .split_once('\n')
                    .map_or((detail, None), |(column, raw)| {
                        (column, Some(raw.to_owned()))
                    });
                if column.is_empty() {
                    return Err(bad_detail());
                }
                Ok(ItemFailureClass::CorruptRow {
                    column: column.to_owned(),
                    raw_value,
                })
            }
            "constraint_violated" => {
                let constraint = match detail {
                    Some("unique") => ConstraintKind::Unique,
                    Some("foreign_key") => ConstraintKind::ForeignKey,
                    Some("not_null") => ConstraintKind::NotNull,
                    Some("check_boolean") => ConstraintKind::CheckBoolean,
                    _ => return Err(bad_detail()),
                };
                Ok(ItemFailureClass::ConstraintViolated { constraint })
            }
            "statement_rejected" if detail.is_none() => Ok(ItemFailureClass::StatementRejected),
            "other_store_unavailable" if detail.is_none() => {
                Ok(ItemFailureClass::OtherStoreUnavailable)
            }
            "refused" => Ok(ItemFailureClass::Refused {
                refusal: detail
                    .filter(|value| !value.is_empty())
                    .ok_or_else(bad_detail)?
                    .to_owned(),
            }),
            "statement_rejected" | "other_store_unavailable" => Err(bad_detail()),
            _ => Err(self.last_failure.corrupt()),
        }
    }
    pub(super) fn decode_attempt_record(&self) -> Result<FailureRecord, AttemptCorruption> {
        let attempt_id = self
            .attempt_id
            .text()?
            .parse::<uuid::Uuid>()
            .ok()
            .filter(|id| id.get_version_num() == 7)
            .ok_or_else(|| self.attempt_id.corrupt())?;
        let failure_count = u32::try_from(self.failure_count.integer()?)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| self.failure_count.corrupt())?;
        let next_try = read_nullable_engine_time(&self.next_try_at)?;
        let retired = read_nullable_engine_time(&self.retired_at)?;
        let state = match self.state.text()? {
            "failing" => {
                if retired.is_some() {
                    return Err(self.retired_at.corrupt());
                }
                AttemptState::Failing {
                    next_try_at: next_try.ok_or_else(|| self.next_try_at.corrupt())?,
                }
            }
            "retired" => {
                if next_try.is_some() {
                    return Err(self.next_try_at.corrupt());
                }
                AttemptState::Retired {
                    retired_at: retired.ok_or_else(|| self.retired_at.corrupt())?,
                }
            }
            _ => return Err(self.state.corrupt()),
        };
        Ok(FailureRecord {
            attempt_id,
            failure_count,
            state,
            last_failure: self.decode_stored_failure_class()?,
            first_failed_at: read_engine_time(&self.first_failed_at)?,
            last_failed_at: read_engine_time(&self.last_failed_at)?,
            created_at: read_engine_time(&self.created_at)?,
            updated_at: read_engine_time(&self.updated_at)?,
        })
    }

    fn into_stored_attempt(self) -> StoredAttempt {
        let decoded_failure_record = self.decode_attempt_record();
        let observed_attempt_guard = AttemptGuard {
            attempt_id: self.attempt_id.into_raw(),
            state: self.state.into_raw(),
            failure_count: self.failure_count.into_raw(),
            last_failure: self.last_failure.into_raw(),
            updated_at: self.updated_at.into_raw(),
        };
        StoredAttempt {
            observed_attempt_guard,
            decoded_failure_record,
        }
    }
}

pub(crate) fn decode_attempt_with_guard(
    query_result: &ReadOutcome,
) -> Result<Option<StoredAttempt>, StoreFailure> {
    let column_values = match query_result.rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        _ => return Err(corrupt("engine_work_attempts.subject")),
    };
    Ok(Some(
        FullAttemptProjection::read_named(query_result, column_values)?
            .into_record_projection()
            .into_stored_attempt(),
    ))
}

/// The already-checked direct read has no kind or subject columns and needs no raw guard.
pub(super) fn decode_checked_attempt_record(
    query_result: &ReadOutcome,
) -> Result<Result<FailureRecord, ItemFailureClass>, StoreFailure> {
    let [row] = query_result.rows.as_slice() else {
        return Err(corrupt("engine_work_attempts.subject"));
    };
    Ok(AttemptRecordProjection::read_named(query_result, row)?
        .decode_attempt_record()
        .map_err(|corruption| corruption.to_corrupt_row_failure()))
}

pub fn encode_attempt_failure_detail(failure: &ItemFailureClass) -> Option<String> {
    match failure {
        ItemFailureClass::CorruptRow { column, raw_value } => Some(
            raw_value
                .as_ref()
                .map_or_else(|| column.clone(), |raw| format!("{column}\n{raw}")),
        ),
        ItemFailureClass::ConstraintViolated { constraint } => Some(
            match constraint {
                ConstraintKind::Unique => "unique",
                ConstraintKind::ForeignKey => "foreign_key",
                ConstraintKind::NotNull => "not_null",
                ConstraintKind::CheckBoolean => "check_boolean",
            }
            .into(),
        ),
        ItemFailureClass::Refused { refusal } => Some(refusal.clone()),
        ItemFailureClass::StatementRejected | ItemFailureClass::OtherStoreUnavailable => None,
    }
}

#[cfg(test)]
#[path = "attempt_records_test.rs"]
mod tests;
