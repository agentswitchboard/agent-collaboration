//! Shared statements and observed cells for Engine storage owners.
use central_storage::StoreFailure;
use fleet_contracts::SubjectVersion;
use fleet_runtime_ports::workspace_decode_error::corrupt;

pub use crate::machine_event_engine::attempt_bindings::{AttemptBind, ObservedBind};
pub use crate::machine_event_engine::attempt_projection::{
    CheckedStorage, ObservedColumn, ObservedStorage, ProjectionStorage,
};
pub use crate::machine_event_engine::attempt_records::encode_attempt_failure_detail;
pub use crate::machine_event_engine::attempt_statements::encode_attempt_integer_bind;
#[cfg(feature = "test-support")]
pub use crate::machine_event_engine::attempt_statements::plan_guarded_attempt_upsert;
pub use central_engine_model::AttemptState;
pub use central_storage::SqlValue;
pub use fleet_contracts::AttemptGuard;
pub use fleet_runtime_ports::stored_values::format_utc_millis;

/// Reads named cells while sharing the exact fields and types with SQLx.
#[doc(hidden)]
#[macro_export]
macro_rules! checked_subject_projection {
    ($visibility:vis $projection:ident { $($field:ident : $column:ty),+ $(,)? }; observed) => {
        $crate::checked_subject_projection!($visibility $projection { $($field: $column,)+ });

        impl $projection {
            $visibility fn observe(
                rows: &central_storage::ReadOutcome
            ) -> Result<Option<(Self, fleet_contracts::SubjectVersion)>, $crate::SubjectLoadFailure> {
                let row = match rows.rows.as_slice() {
                    [] => return Ok(None),
                    [row] => row,
                    _ => return Err($crate::classify_subject_read_failure(
                        fleet_runtime_ports::workspace_decode_error::corrupt("engine.subject_key"),
                    )),
                };
                let projection = Self::read_named(rows, row)
                    .map_err($crate::classify_subject_read_failure)?;
                let version = fleet_contracts::SubjectVersion(vec![$(projection.$field.raw().clone(),)+]);
                Ok(Some((projection, version)))
            }
        }
    };
    ($visibility:vis $projection:ident { $($field:ident : $column:ty),+ $(,)? }) => {
        $visibility struct $projection<Storage: $crate::owner_sql::ProjectionStorage = $crate::owner_sql::ObservedStorage> {
            $($field: Storage::Column<$column>,)+
        }

        impl $projection {
            fn read_named(
                outcome: &central_storage::ReadOutcome,
                row: &[central_storage::SqlValue],
            ) -> Result<Self, central_storage::StoreFailure> {
                Ok(Self {
                    $($field: $crate::owner_sql::ObservedColumn::new(
                        stringify!($field),
                        fleet_runtime_ports::stored_values::column(outcome, row, stringify!($field))?.clone(),
                    ),)+
                })
            }
        }
    };
}

pub fn observed_text_binds<const COUNT: usize>(
    version: &SubjectVersion,
) -> Result<[ObservedBind<String>; COUNT], StoreFailure> {
    let cells: &[SqlValue; COUNT] = version
        .0
        .as_slice()
        .try_into()
        .map_err(|_| corrupt("engine.subject_key"))?;
    Ok((*cells).clone().map(ObservedBind::new))
}

#[doc(hidden)]
#[macro_export]
macro_rules! checked_subject_attempt {
    ($file:literal, $subject:ident, $record:ident, $prior:ident, $($name:ident = $value:expr),+ $(,)?) => {{
        use $crate::checked_owner_attempt_statement;
        central_storage::GuardedStatement {
            statement: $crate::engine_attempt_upsert_bindings!(
                checked_owner_attempt_statement, $subject, $record, $prior; [$file,] [$($name = $value,)*]
            ),
            expect: central_storage::RowExpectation::Exactly(1),
        }
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! checked_owner_attempt_statement {
    ($file:literal, $($binding:tt)*) => {
        $crate::checked_engine_statement!(query_file, [], $file, $($binding)*)
    };
}
