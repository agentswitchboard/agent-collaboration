//! Closed answers from a verified organization admin operation.
use crate::person_request::{AdmissionRefusal, MAXIMUM_RECORD_PAGE};
use fleet_contracts::{MachineEnrollmentRevocationResult, WorkspaceView};
use fleet_identity_protocol::UnixMillis;
use fleet_signin::organization_change::{
    ChangeRequestId, MachineAllowanceState, OrganizationChangeOrigin,
    OrganizationChangeRecord as TargetedOrganizationChangeRecord, StoredOrganizationChange,
};
use message_board::WorkspaceId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use workspace_ids::MachineId;

/// The organization change page projects both subject shapes under one kind tag.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum OrganizationChangeRecord {
    MachineAllowanceChanged {
        #[schemars(with = "String", extend("format" = "uuid"))]
        id: uuid::Uuid,
        request_id: ChangeRequestId,
        origin: OrganizationChangeOrigin,
        machine_id: MachineId,
        workspace_id: WorkspaceId,
        state: MachineAllowanceState,
        created_at_millis: UnixMillis,
    },
    #[serde(untagged)]
    TargetedChange(TargetedOrganizationChangeRecord),
}

impl TryFrom<StoredOrganizationChange> for OrganizationChangeRecord {
    type Error = central_storage::StoreFailure;

    fn try_from(record: StoredOrganizationChange) -> Result<Self, Self::Error> {
        Ok(match record {
            StoredOrganizationChange::MachineAllowanceChanged(change) => {
                Self::MachineAllowanceChanged {
                    id: change.id,
                    request_id: change.request_id,
                    origin: change.origin,
                    machine_id: change.machine_id,
                    workspace_id: String::from(change.workspace_id).try_into().map_err(|_| {
                        fleet_runtime_ports::workspace_decode_error::corrupt(
                            "signin_change_records.workspace_id",
                        )
                    })?,
                    state: change.state,
                    created_at_millis: change.created_at_millis,
                }
            }
            StoredOrganizationChange::TargetedChange(change) => Self::TargetedChange(change),
        })
    }
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
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ListWorkspacesResult {
    Workspaces {
        workspaces: WorkspacePageRows,
        #[serde(deserialize_with = "Option::deserialize")]
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = NullableWorkspacePageCursor))]
        next: NullableWorkspacePageCursor,
    },
    Refused {
        reason: AdminRefusal,
    },
}

type NullableWorkspacePageCursor = Option<message_board::WorkspaceId>;

#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
const _: fn(&ListWorkspacesResult) -> Option<&NullableWorkspacePageCursor> = |result| match result {
    ListWorkspacesResult::Workspaces { next, .. } => Some(next),
    ListWorkspacesResult::Refused { .. } => None,
};

/// The bounded workspace collection returned by one successful page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<WorkspaceView>", into = "Vec<WorkspaceView>")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
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
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
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
    Refused {
        reason: AdminRefusal,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
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
