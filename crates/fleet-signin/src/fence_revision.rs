use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A fence revision: a positive JavaScript-safe integer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct FenceRevision(i64);
#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
impl specta::Type for FenceRevision {
    fn definition(types: &mut specta::Types) -> specta::datatype::DataType {
        <specta_typescript::Number as specta::Type>::definition(types)
    }
}
impl TryFrom<i64> for FenceRevision {
    type Error = FenceRevisionError;
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if !(1..=9_007_199_254_740_991).contains(&value) {
            return Err(FenceRevisionError);
        }
        Ok(Self(value))
    }
}
impl From<FenceRevision> for i64 {
    fn from(value: FenceRevision) -> Self {
        value.0
    }
}
impl FenceRevision {
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}
impl JsonSchema for FenceRevision {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FenceRevision".into()
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type":"integer","minimum":1,"maximum":9_007_199_254_740_991_i64})
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("fence revision must be an integer in 1..=2^53-1")]
pub struct FenceRevisionError;
