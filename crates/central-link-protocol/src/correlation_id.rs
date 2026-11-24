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

/// A freshly allocated attempt ID that is consumed at pending registration.
pub struct FreshCorrelationId(CorrelationId);

impl FreshCorrelationId {
    /// Mint with fresh UUIDv7 entropy independent of reconstructing socket runtime state.
    pub fn mint_at(unix_millis: i64) -> Self {
        let millis = u64::try_from(unix_millis).unwrap_or_default();
        let nanos = u32::try_from(millis % 1_000).unwrap_or_default() * 1_000_000;
        let timestamp = uuid::Timestamp::from_unix(uuid::NoContext, millis / 1_000, nanos);
        Self(CorrelationId::mint(timestamp))
    }

    pub fn wire_id(&self) -> &CorrelationId {
        &self.0
    }

    pub(crate) fn into_wire_id(self) -> CorrelationId {
        self.0
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
