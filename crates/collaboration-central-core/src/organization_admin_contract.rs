//! Closed answers from a verified organization admin operation.
use crate::person_request::{AdmissionRefusal, MAXIMUM_RECORD_PAGE};
use fleet_contracts::WorkspaceView;
use fleet_engine::EngineFailureClass;
use fleet_signin::organization_change::OrganizationChangeRecord;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The closed set of engine kinds an admin sees; test-support machines are hidden.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineKindHealth {
    pub kind: EngineKindName,
    pub failing: u32,
    pub retired: u32,
    #[schemars(with = "Option<String>", extend("format" = "date-time"))]
    pub oldest_first_failed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_failure: Option<EngineFailureClass>,
}

/// One retired failure record in EngineListRetired, ascending by attempt ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineRetiredItem {
    #[schemars(with = "String", extend("format" = "uuid"))]
    pub attempt_id: uuid::Uuid,
    pub kind: EngineKindName,
    pub subject_key: String,
    pub failure_class: EngineFailureClass,
    pub count: u32,
    #[schemars(with = "String", extend("format" = "date-time"))]
    pub retired_at: chrono::DateTime<chrono::Utc>,
    pub last_failure: Option<String>,
}

/// Verification failures become a bare 401; answered operations have a body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrganizationAdminAnswer {
    Answered(OrganizationAdminResult),
    WorkspaceList(ListWorkspacesResult),
    MachineEnrollmentRevocation(MachineEnrollmentRevocationResult),
    Retained(RetainedOrganizationAdminAnswer),
    Unauthenticated,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", rename_all = "camelCase", deny_unknown_fields)]
pub enum ListWorkspacesResult {
    Workspaces {
        workspaces: WorkspacePageRows,
        #[serde(deserialize_with = "Option::deserialize")]
        next: Option<message_board::WorkspaceId>,
    },
    Refused {
        reason: AdminRefusal,
    },
}

/// The bounded workspace collection returned by one successful page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<WorkspaceView>", into = "Vec<WorkspaceView>")]
pub struct WorkspacePageRows(Vec<WorkspaceView>);

#[derive(Debug, thiserror::Error)]
#[error(
    "workspace page has {count} rows, exceeding the limit of {}",
    MAXIMUM_RECORD_PAGE
)]
pub struct WorkspacePageTooLarge {
    count: usize,
}

impl TryFrom<Vec<WorkspaceView>> for WorkspacePageRows {
    type Error = WorkspacePageTooLarge;

    fn try_from(workspaces: Vec<WorkspaceView>) -> Result<Self, Self::Error> {
        let count = workspaces.len();
        if count > usize::from(MAXIMUM_RECORD_PAGE) {
            return Err(WorkspacePageTooLarge { count });
        }
        Ok(Self(workspaces))
    }
}

impl From<WorkspacePageRows> for Vec<WorkspaceView> {
    fn from(workspaces: WorkspacePageRows) -> Self {
        workspaces.0
    }
}

impl JsonSchema for WorkspacePageRows {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "WorkspacePageRows".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let items = generator.subschema_for::<WorkspaceView>();
        schemars::json_schema!({"type":"array","items":items,"maxItems":MAXIMUM_RECORD_PAGE})
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", rename_all = "camelCase", deny_unknown_fields)]
pub enum MachineEnrollmentRevocationResult {
    Revoked {},
    NotFound {},
    AlreadyEnded {},
    Refused {
        reason: MachineEnrollmentRevocationRefusal,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum MachineEnrollmentRevocationRefusal {
    NotPermitted,
    FactorNotRecent,
}

/// Validated retained JSON travels to HTTP without another serialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedOrganizationAdminAnswer {
    text: String,
}
impl RetainedOrganizationAdminAnswer {
    pub(crate) fn validated(text: String) -> Self {
        Self { text }
    }
    pub(crate) fn into_text(self) -> String {
        self.text
    }
}

/// The closed body of a 200 on an admin route.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "result",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OrganizationAdminResult {
    Applied {
        workspace: WorkspaceView,
    },
    Found {
        workspace: Option<WorkspaceView>,
    },
    Records {
        records: Vec<OrganizationChangeRecord>,
    },
    EngineHealth {
        kinds: Vec<EngineKindHealth>,
        #[schemars(with = "Option<String>", extend("format" = "date-time"))]
        alarm: Option<chrono::DateTime<chrono::Utc>>,
        alarm_missing: bool,
    },
    EngineRetired {
        items: Vec<EngineRetiredItem>,
        #[schemars(with = "Option<String>", extend("format" = "uuid"))]
        next: Option<uuid::Uuid>,
    },
    EngineRetried {},
    EngineDiscarded {},
    Refused {
        reason: AdminRefusal,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AdminRefusal {
    NameInUse,
    NotPermitted,
    SecondFactorRequired,
    RefreshRequired,
    AuthorizationSyncRequired,
    Expired,
    RequestTime,
    RequestReused,
    /// The same operation ID arrived with a different request.
    Conflict,
    Rejected,
    /// No failure record has this attempt ID (or a concurrent retry took it).
    NotFound,
    /// The record is failing, not retired.
    NotRetired,
    /// A notice unit or engine_source record has no subject to finish.
    DiscardUnsupported,
    /// The subject moved; nothing changed.
    Stale,
    /// Discard on a retired subject whose row will not decode; nothing changed.
    SubjectUnreadable,
}

impl From<AdmissionRefusal> for AdminRefusal {
    fn from(refusal: AdmissionRefusal) -> Self {
        match refusal {
            AdmissionRefusal::NotPermitted => Self::NotPermitted,
            AdmissionRefusal::SecondFactorRequired => Self::SecondFactorRequired,
            AdmissionRefusal::RefreshRequired => Self::RefreshRequired,
            AdmissionRefusal::AuthorizationSyncRequired => Self::AuthorizationSyncRequired,
            AdmissionRefusal::Expired => Self::Expired,
            AdmissionRefusal::RequestTime => Self::RequestTime,
            AdmissionRefusal::RequestReused => Self::RequestReused,
        }
    }
}
