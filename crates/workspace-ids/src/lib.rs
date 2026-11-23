//! Workspace identifiers validate identity only; central owns minting and order.
use serde::{Deserialize, Serialize};
use uuid::{Uuid, Variant};

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{field} must be a canonical lowercase RFC UUIDv7")]
pub struct IdValidationError {
    pub field: &'static str,
}

// Ported from message-board::board_identity, without the system-clock mint API.
macro_rules! workspace_identifiers {
    ($($(#[$attribute:meta])* $name:ident $(=> $error:ident)?),+ $(,)?) => {$ (
        #[derive(
            Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
            schemars::JsonSchema,
        )]
        #[serde(try_from = "String", into = "String")]
        $(#[$attribute])*
        pub struct $name(String);

        workspace_identifiers!(@conversion $name $(=> $error)?);
        impl From<$name> for String { fn from(value: $name) -> Self { value.0 } }
        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }
        impl $name {
            #[must_use] pub fn as_str(&self) -> &str { &self.0 }
        }
    )+};
    (@conversion $name:ident) => {
        impl TryFrom<String> for $name {
            type Error = IdValidationError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                let invalid = || IdValidationError { field: stringify!($name) };
                let parsed = Uuid::parse_str(&value).map_err(|_| invalid())?;
                if parsed.get_version_num() != 7 || parsed.get_variant() != Variant::RFC4122
                    || parsed.hyphenated().to_string() != value {
                    return Err(invalid());
                }
                Ok(Self(value))
            }
        }
        impl std::str::FromStr for $name {
            type Err = IdValidationError;
            fn from_str(value: &str) -> Result<Self, Self::Err> { Self::try_from(value.to_owned()) }
        }
    };
    (@conversion $name:ident => $error:ident) => {
        impl TryFrom<String> for $name {
            type Error = $error;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                let parsed = Uuid::parse_str(&value).map_err(|_| $error::Malformed)?;
                if parsed.get_version_num() != 7 {
                    return Err($error::WrongVersion);
                }
                if parsed.get_variant() != Variant::RFC4122 {
                    return Err($error::WrongVariant);
                }
                if parsed.hyphenated().to_string() != value {
                    return Err($error::NonCanonical);
                }
                Ok(Self(value))
            }
        }
        impl std::str::FromStr for $name {
            type Err = $error;
            fn from_str(value: &str) -> Result<Self, Self::Err> { Self::try_from(value.to_owned()) }
        }
    };
}

workspace_identifiers!(
    WorkspaceId,
    RequestId,
    /// The Router install's durable identity, shared with central.
    #[schemars(extend("pattern" = "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"))]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    MachineId,
    /// One Router process run registered with central.
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    RouterRunId,
    /// One approval request raised by a Router.
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    ApprovalRequestId,
    /// The never-reused Central identity of one provider session incarnation.
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    CentralSessionId,
    /// One operation on a session, made by the caller before it sends, so a repeat can be recognised.
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    SessionOperationId => SessionOperationIdError,
    PersonId,
    AreaId,
    ProjectId,
    InitiativeId,
    ActorIdentityId,
    ProjectRoleId,
    DeliverableId,
    DeliverableRoleId,
    DependencyId,
    IssueId,
    ThreadId,
    ThreadMemberId,
    PostId,
    WorkLogEntryId,
    WorkLogTagId,
    BacklogItemId,
    ReferenceId,
    LinkId,
    ChangeId,
    ReadMarkId,
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    MessageId,
    NotificationWindowId,
    NotificationWindowItemId,
    WakeId,
    AutomationId
);

/// The session operation ID parser returns this when supplied text is not a canonical RFC UUIDv7.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionOperationIdError {
    /// The input does not parse as a UUID.
    #[error("SessionOperationId is not UUID text")]
    Malformed,
    /// The UUID's version is not 7.
    #[error("SessionOperationId UUID version must be 7")]
    WrongVersion,
    /// The UUID does not use the RFC 4122 variant.
    #[error("SessionOperationId UUID variant must be RFC 4122")]
    WrongVariant,
    /// The text differs from the UUID's lowercase hyphenated spelling.
    #[error("SessionOperationId must use lowercase hyphenated UUID text")]
    NonCanonical,
}

/// One agent conversation, as its provider (Claude Code, Codex) minted it:
/// stored as given, 1 to 4096 UTF-8 bytes without NUL.
#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct SessionId(String);
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("SessionId must contain 1 to 4096 UTF-8 bytes without NUL")]
pub struct SessionIdError;
impl TryFrom<String> for SessionId {
    type Error = SessionIdError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > 4096 || value.contains('\0') {
            return Err(SessionIdError);
        }
        Ok(Self(value))
    }
}
impl std::str::FromStr for SessionId {
    type Err = SessionIdError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value.to_owned())
    }
}
impl From<SessionId> for String {
    fn from(value: SessionId) -> Self {
        value.0
    }
}
impl std::fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}
impl SessionId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The one session address (owner, 2026-11-18): the Router install that holds
/// the session, by its machine id, and the provider's session id. Used on the
/// wire, at central and by agents. It has no endpoint: which harness holds a
/// session is the Router's own business.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct SessionRef {
    pub machine_id: MachineId,
    pub session_id: SessionId,
}

impl<'de> Deserialize<'de> for SessionRef {
    fn deserialize<Decoder: serde::Deserializer<'de>>(
        decoder: Decoder,
    ) -> Result<Self, Decoder::Error> {
        struct SessionAddressReader;

        impl<'de> serde::de::Visitor<'de> for SessionAddressReader {
            type Value = SessionRef;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a named session address")
            }

            fn visit_map<Map: serde::de::MapAccess<'de>>(
                self,
                fields: Map,
            ) -> Result<Self::Value, Map::Error> {
                #[derive(Deserialize)]
                #[serde(remote = "SessionRef", rename_all = "camelCase", deny_unknown_fields)]
                struct AddressFields {
                    machine_id: MachineId,
                    session_id: SessionId,
                }

                AddressFields::deserialize(serde::de::value::MapAccessDeserializer::new(fields))
            }
        }

        decoder.deserialize_map(SessionAddressReader)
    }
}

#[cfg(test)]
#[path = "identifier_test.rs"]
mod tests;
