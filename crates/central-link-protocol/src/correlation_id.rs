use serde::{Deserialize, Serialize};

/// Transport correlation has a separate type and lifetime from durable write idempotency.
#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(try_from = "String", into = "String")]
pub struct CorrelationId(String);

impl CorrelationId {
    /// The client supplies time from its injected clock and its one UUIDv7 context.
    pub fn mint(timestamp: uuid::Timestamp) -> Self {
        Self(uuid::Uuid::new_v7(timestamp).to_string())
    }
}

impl TryFrom<String> for CorrelationId {
    type Error = workspace_ids::IdValidationError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let id: workspace_ids::RequestId = value.parse()?;
        Ok(Self(id.into()))
    }
}
impl From<CorrelationId> for String {
    fn from(value: CorrelationId) -> Self {
        value.0
    }
}

impl std::str::FromStr for CorrelationId {
    type Err = workspace_ids::IdValidationError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value.to_owned())
    }
}
