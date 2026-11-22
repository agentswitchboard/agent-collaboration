//! Shared column declarations for SQLx and the two hosts' named reads.
use central_storage::SqlValue;
use std::marker::PhantomData;

use central_engine_model::ItemFailureClass;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("central attempt holds an invalid value in {column_path}")]
pub(crate) struct AttemptCorruption {
    pub column_path: String,
    pub raw_value: String,
}

impl AttemptCorruption {
    pub fn to_corrupt_row_failure(&self) -> ItemFailureClass {
        ItemFailureClass::CorruptRow {
            column: self.column_path.clone(),
            raw_value: Some(self.raw_value.clone()),
        }
    }
}

fn corrupt_attempt_field(name: &str, raw: &SqlValue) -> AttemptCorruption {
    let raw_value = match raw {
        SqlValue::Text(value) => value.clone(),
        value => format!("{value:?}"),
    };
    AttemptCorruption {
        column_path: format!("engine_work_attempts.{name}"),
        raw_value,
    }
}

pub trait ProjectionStorage {
    type Column<Value>;
}

pub enum ObservedStorage {}

impl ProjectionStorage for ObservedStorage {
    type Column<Value> = ObservedColumn<Value>;
}

pub enum CheckedStorage {}
impl ProjectionStorage for CheckedStorage {
    type Column<Value> = Value;
}

/// A host cell retains the original SQL value even when decoding it fails.
pub struct ObservedColumn<Value> {
    name: &'static str,
    raw: SqlValue,
    expected: PhantomData<Value>,
}

impl<Value> ObservedColumn<Value> {
    pub fn new(name: &'static str, raw: SqlValue) -> Self {
        Self {
            name,
            raw,
            expected: PhantomData,
        }
    }

    pub(super) fn corrupt(&self) -> AttemptCorruption {
        corrupt_attempt_field(self.name, &self.raw)
    }

    pub fn into_raw(self) -> SqlValue {
        self.raw
    }

    pub fn raw(&self) -> &SqlValue {
        &self.raw
    }

    fn require_text(&self) -> Result<&str, AttemptCorruption> {
        match &self.raw {
            SqlValue::Text(value) => Ok(value),
            _ => Err(self.corrupt()),
        }
    }

    fn require_integer(&self) -> Result<i64, AttemptCorruption> {
        match &self.raw {
            SqlValue::Integer(value) => Ok(value.get()),
            _ => Err(self.corrupt()),
        }
    }
}

impl ObservedColumn<String> {
    pub fn stored_text(&self) -> Result<&str, central_storage::StoreFailure> {
        self.text()
            .map_err(|_| fleet_runtime_ports::workspace_decode_error::corrupt(self.name))
    }

    pub(super) fn text(&self) -> Result<&str, AttemptCorruption> {
        self.require_text()
    }
}

impl ObservedColumn<Option<String>> {
    pub fn stored_text(&self) -> Result<&str, central_storage::StoreFailure> {
        self.text()
            .map_err(|_| fleet_runtime_ports::workspace_decode_error::corrupt(self.name))
    }

    pub fn stored_optional_text(&self) -> Result<Option<&str>, central_storage::StoreFailure> {
        self.nullable_text()
            .map_err(|_| fleet_runtime_ports::workspace_decode_error::corrupt(self.name))
    }

    pub(super) fn text(&self) -> Result<&str, AttemptCorruption> {
        self.require_text()
    }

    pub(super) fn nullable_text(&self) -> Result<Option<&str>, AttemptCorruption> {
        match self.raw {
            SqlValue::Null => Ok(None),
            _ => self.require_text().map(Some),
        }
    }
}

impl ObservedColumn<i64> {
    pub(super) fn integer(&self) -> Result<i64, AttemptCorruption> {
        self.require_integer()
    }
}

impl ObservedColumn<Option<i64>> {
    pub(super) fn integer(&self) -> Result<i64, AttemptCorruption> {
        self.require_integer()
    }
}

macro_rules! attempt_projection {
    ($projection:ident { $($field:ident : $column:ty),+ $(,)? }) => {
        pub(super) struct $projection<Storage: $crate::machine_event_engine::attempt_projection::ProjectionStorage = $crate::machine_event_engine::attempt_projection::ObservedStorage> {
            $(pub(super) $field: Storage::Column<$column>,)+
        }

        impl $projection {
            pub(super) fn read_named(
                outcome: &central_storage::ReadOutcome,
                row: &[central_storage::SqlValue],
            ) -> Result<Self, central_storage::StoreFailure> {
                Ok(Self {
                    $($field: $crate::machine_event_engine::attempt_projection::ObservedColumn::new(
                        stringify!($field),
                        fleet_runtime_ports::stored_values::column(outcome, row, stringify!($field))?.clone(),
                    ),)+
                })
            }
        }
    };
}

pub(super) use attempt_projection;
