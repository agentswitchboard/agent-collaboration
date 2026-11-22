//! One positional bind declaration feeds SQLx and the host statement.
use central_storage::{SafeInteger, SqlValue};
use std::marker::PhantomData;

pub trait AttemptBind {
    type SqlxValue;

    fn sqlx_value(&self) -> Self::SqlxValue;

    fn into_storage(self) -> SqlValue;
}

impl AttemptBind for String {
    type SqlxValue = String;

    fn sqlx_value(&self) -> String {
        self.clone()
    }

    fn into_storage(self) -> SqlValue {
        SqlValue::Text(self)
    }
}

impl AttemptBind for Option<String> {
    type SqlxValue = Self;

    fn sqlx_value(&self) -> Self {
        self.clone()
    }

    fn into_storage(self) -> SqlValue {
        self.map_or(SqlValue::Null, SqlValue::Text)
    }
}

impl AttemptBind for SafeInteger {
    type SqlxValue = i64;

    fn sqlx_value(&self) -> i64 {
        self.get()
    }

    fn into_storage(self) -> SqlValue {
        SqlValue::Integer(self)
    }
}

/// SQLx checks the slot against the normal column type. The host must bind
/// the actual cell verbatim, including a damaged cell's other storage class.
pub struct ObservedBind<Value> {
    raw: SqlValue,
    expected: PhantomData<Value>,
}

impl<Value> ObservedBind<Value> {
    pub fn new(raw: SqlValue) -> Self {
        Self {
            raw,
            expected: PhantomData,
        }
    }
}

impl<Value> AttemptBind for ObservedBind<Value> {
    type SqlxValue = Option<Value>;

    fn sqlx_value(&self) -> Option<Value> {
        None
    }

    fn into_storage(self) -> SqlValue {
        self.raw
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! checked_engine_statement {
    ($query:ident, [$($projection:path)?], $file:literal $(, $bind:ident = $value:expr)* $(,)?) => {{
        let ($($bind,)*) = ($($value,)*);
        #[cfg(test)]
        let _ = sqlx::$query!(
            $($projection,)?
            $file
            $(, $crate::owner_sql::AttemptBind::sqlx_value(&$bind))*
        );
        central_storage::Statement {
            sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)),
            args: vec![$($crate::owner_sql::AttemptBind::into_storage($bind)),*],
        }
    }};
}

pub(super) use crate::checked_engine_statement as attempt_statement;
