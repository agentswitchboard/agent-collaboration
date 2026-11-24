//! Engine administration inputs and the historical retained receipt format.
use crate::AdminRefusal;
use fleet_engine::EngineFailureClass;
use serde::{Deserialize, Serialize};
use workspace_ids::{PersonId, RequestId};

/// A mutation of one retired attempt in the owning organization.
pub(crate) struct EngineAttemptCommand {
    pub organization_id: fleet_identity_protocol::OrganizationId,
    pub operation_id: RequestId,
    pub attempt_id: uuid::Uuid,
    pub person_id: PersonId,
    pub intent_digest: String,
}

/// The closed set of engine kinds an admin sees; test-support machines are hidden.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineKindName {
    WakeTimer,
    AutomationTimer,
    DirectMessage,
    SystemMessageExpiry,
    LegacyNoticeWindows,
    LegacyWorkspaceDrains,
    EngineSource,
}

/// One registered engine kind in EngineHealth; zero counts are listed too.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineKindHealth {
    pub kind: EngineKindName,
    pub failing: u32,
    pub retired: u32,
    pub oldest_first_failed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_failure: Option<EngineFailureClass>,
}

/// One retired failure record in EngineListRetired, ascending by attempt ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineRetiredItem {
    pub attempt_id: uuid::Uuid,
    pub kind: EngineKindName,
    pub subject_key: String,
    pub failure_class: EngineFailureClass,
    pub count: u32,
    pub retired_at: chrono::DateTime<chrono::Utc>,
    pub last_failure: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "result",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum EngineAdminResult {
    EngineHealth {
        kinds: Vec<EngineKindHealth>,
        alarm: Option<chrono::DateTime<chrono::Utc>>,
        alarm_missing: bool,
    },
    EngineRetired {
        items: Vec<EngineRetiredItem>,
        #[serde(deserialize_with = "Option::deserialize")]
        next: Option<uuid::Uuid>,
    },
    EngineRetried {},
    EngineDiscarded {},
    Refused {
        reason: AdminRefusal,
    },
}

#[cfg(test)]
#[path = "engine_admin_result_test.rs"]
mod tests;
