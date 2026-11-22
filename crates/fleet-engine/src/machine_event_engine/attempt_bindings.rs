//! One positional bind declaration feeds SQLx and the host statement.
use central_storage::{SafeInteger, SqlValue};
use std::marker::PhantomData;

pub(super) trait AttemptBind {
    #[cfg(test)]
    type SqlxValue;

    #[cfg(test)]
    fn sqlx_value(&self) -> Self::SqlxValue;

    fn into_storage(self) -> SqlValue;
}

impl AttemptBind for String {
    #[cfg(test)]
    type SqlxValue = String;

    #[cfg(test)]
    fn sqlx_value(&self) -> String {
        self.clone()
    }

    fn into_storage(self) -> SqlValue {
        SqlValue::Text(self)
    }
}

impl AttemptBind for Option<String> {
    #[cfg(test)]
    type SqlxValue = Self;

    #[cfg(test)]
    fn sqlx_value(&self) -> Self {
        self.clone()
    }

    fn into_storage(self) -> SqlValue {
        self.map_or(SqlValue::Null, SqlValue::Text)
    }
}

impl AttemptBind for SafeInteger {
    #[cfg(test)]
    type SqlxValue = i64;

    #[cfg(test)]
    fn sqlx_value(&self) -> i64 {
        self.get()
    }

    fn into_storage(self) -> SqlValue {
        SqlValue::Integer(self)
    }
}

/// SQLx checks the slot against the normal column type. The host must bind
/// the actual cell verbatim, including a damaged cell's other storage class.
pub(super) struct ObservedBind<Value> {
    raw: SqlValue,
    expected: PhantomData<Value>,
}

impl<Value> ObservedBind<Value> {
    pub(super) fn new(raw: SqlValue) -> Self {
        Self {
            raw,
            expected: PhantomData,
        }
    }
}

impl<Value> AttemptBind for ObservedBind<Value> {
    #[cfg(test)]
    type SqlxValue = Option<Value>;

    #[cfg(test)]
    fn sqlx_value(&self) -> Option<Value> {
        None
    }

    fn into_storage(self) -> SqlValue {
        self.raw
    }
}

macro_rules! attempt_statement {
    ($query:ident, [$($projection:path)?], $file:literal $(, $bind:ident = $value:expr)* $(,)?) => {{
        $(let $bind = $value;)*
        #[cfg(test)]
        let _ = sqlx::$query!(
            $($projection,)?
            $file
            $(, $crate::machine_event_engine::attempt_bindings::AttemptBind::sqlx_value(&$bind))*
        );
        central_storage::Statement {
            sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)),
            args: vec![$($crate::machine_event_engine::attempt_bindings::AttemptBind::into_storage($bind)),*],
        }
    }};
}

pub(super) use attempt_statement;
