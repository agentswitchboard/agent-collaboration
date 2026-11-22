//! Sign-in change records share the stamp/effect atomic batch.
use central_storage::{
    GuardedStatement, ReadOutcome, RowExpectation, SafeInteger, SqlValue, Statement, StoreFailure,
};
use fleet_identity_protocol::UnixMillis;
use fleet_runtime_ports::CentralStore;
use fleet_runtime_ports::{
    stored_values::{column, string},
    workspace_decode_error::corrupt as bad_value,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use workspace_ids::{MachineId, WorkspaceId};
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename = "TargetedOrganizationChangeKind", rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum OrganizationChangeKind {
    OrganizationProvisioned,
    StampRenewed,
    WorkspaceCreated,
    MachineEnrolled,
    MachineRevoked,
    MachineExpired,
}

impl OrganizationChangeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OrganizationProvisioned => "organizationProvisioned",
            Self::StampRenewed => "stampRenewed",
            Self::WorkspaceCreated => "workspaceCreated",
            Self::MachineEnrolled => "machineEnrolled",
            Self::MachineRevoked => "machineRevoked",
            Self::MachineExpired => "machineExpired",
        }
    }
    pub fn parse(value: &str) -> Result<Self, StoreFailure> {
        match value {
            "organizationProvisioned" => Ok(Self::OrganizationProvisioned),
            "stampRenewed" => Ok(Self::StampRenewed),
            "workspaceCreated" => Ok(Self::WorkspaceCreated),
            "machineEnrolled" => Ok(Self::MachineEnrolled),
            "machineRevoked" => Ok(Self::MachineRevoked),
            "machineExpired" => Ok(Self::MachineExpired),
            _ => Err(corrupt("kind", "unknown change kind")),
        }
    }
}

/// A sign-in-originated change carries no person or permission; none is invented.
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
pub enum OrganizationChangeOrigin {
    /// An empty struct variant so an unknown field is refused like any other.
    SignInProvisioning {},
    Automatic {},
    Person {
        person_id: String,
        permission_checked: String,
    },
}

/// The operation's own identity; a repeat of that operation finds this row.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ChangeRequestId(String);

impl TryFrom<String> for ChangeRequestId {
    type Error = StoreFailure;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > 128 || value.contains('\0') {
            return Err(corrupt(
                "request_id",
                "request identity is not bounded text",
            ));
        }
        Ok(Self(value))
    }
}
impl From<ChangeRequestId> for String {
    fn from(value: ChangeRequestId) -> Self {
        value.0
    }
}
impl JsonSchema for ChangeRequestId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ChangeRequestId".into()
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type":"string","minLength":1,"maxLength":128})
    }
}

impl ChangeRequestId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    rename = "TargetedOrganizationChangeRecord",
    rename_all = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct OrganizationChangeRecord {
    #[schemars(with = "String", extend("format" = "uuid"))]
    pub id: Uuid,
    pub request_id: ChangeRequestId,
    pub kind: OrganizationChangeKind,
    pub origin: OrganizationChangeOrigin,
    pub target_id: String,
    pub created_at_millis: UnixMillis,
}

pub const MACHINE_ALLOWANCE_CHANGE_KIND: &str = "machineAllowanceChanged";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum MachineAllowanceState {
    Active,
    Withdrawn,
}

impl MachineAllowanceState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Withdrawn => "withdrawn",
        }
    }

    fn from_stored(value: &str) -> Result<Self, StoreFailure> {
        match value {
            "active" => Ok(Self::Active),
            "withdrawn" => Ok(Self::Withdrawn),
            _ => Err(corrupt("allowance_state", "unknown allowance state")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MachineAllowanceChange {
    pub id: Uuid,
    pub request_id: ChangeRequestId,
    pub origin: OrganizationChangeOrigin,
    pub machine_id: MachineId,
    pub workspace_id: WorkspaceId,
    pub state: MachineAllowanceState,
    pub created_at_millis: UnixMillis,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoredOrganizationChange {
    TargetedChange(OrganizationChangeRecord),
    MachineAllowanceChanged(MachineAllowanceChange),
}

fn corrupt(field: &str, _message: impl std::fmt::Display) -> StoreFailure {
    bad_value(&format!("signin_change_records.{field}"))
}
fn text(value: impl ToString) -> SqlValue {
    SqlValue::Text(value.to_string())
}
fn integer(value: i64) -> Result<SqlValue, StoreFailure> {
    value
        .try_into()
        .map(SqlValue::Integer)
        .map_err(|_| bad_value("signin_change_records.created_at"))
}
fn private_id(value: &str) -> Result<Uuid, StoreFailure> {
    let id = Uuid::parse_str(value).map_err(|_| bad_value("signin_change_records.id"))?;
    if !fleet_runtime_ports::uuid_v7(id) || id.to_string() != value {
        return Err(bad_value("signin_change_records.id"));
    }
    Ok(id)
}
pub(crate) struct AuditInput<'record> {
    pub(crate) id: String,
    pub(crate) request: &'record str,
    pub(crate) kind: &'static str,
    pub(crate) origin: &'static str,
    pub(crate) person: Option<&'record str>,
    pub(crate) permission: Option<&'record str>,
    pub(crate) target: &'record str,
    pub(crate) timestamp: SafeInteger,
    pub(crate) allowance: Option<AuditAllowanceFields<'record>>,
}

#[derive(Clone, Copy)]
pub(crate) struct AuditAllowanceFields<'record> {
    pub(crate) workspace: &'record str,
    pub(crate) state: &'static str,
}

impl OrganizationChangeRecord {
    pub(crate) fn insert_values(&self) -> Result<AuditInput<'_>, StoreFailure> {
        let id = self.id.to_string();
        private_id(&id)?;
        let (origin, person, permission) = origin_values(&self.origin)?;
        if self.target_id.is_empty() {
            return Err(bad_value("signin_change_records.target_id"));
        }
        Ok(AuditInput {
            id,
            request: self.request_id.as_str(),
            kind: self.kind.as_str(),
            origin,
            person,
            permission,
            target: &self.target_id,
            timestamp: SafeInteger::new(self.created_at_millis.0)
                .map_err(|_| bad_value("signin_change_records.created_at"))?,
            allowance: None,
        })
    }
}

type OriginValues<'origin> = (&'static str, Option<&'origin str>, Option<&'origin str>);

fn origin_values(origin: &OrganizationChangeOrigin) -> Result<OriginValues<'_>, StoreFailure> {
    Ok(match origin {
        OrganizationChangeOrigin::SignInProvisioning {} => ("signInProvisioning", None, None),
        OrganizationChangeOrigin::Automatic {} => ("automatic", None, None),
        OrganizationChangeOrigin::Person {
            person_id,
            permission_checked,
        } => {
            if person_id.is_empty() || permission_checked.is_empty() {
                return Err(bad_value("signin_change_records.origin"));
            }
            (
                "person",
                Some(person_id.as_str()),
                Some(permission_checked.as_str()),
            )
        }
    })
}

pub fn insert_machine_allowance_change(
    record: &MachineAllowanceChange,
) -> Result<GuardedStatement, StoreFailure> {
    let id = record.id.to_string();
    private_id(&id)?;
    let (origin, person, permission) = origin_values(&record.origin)?;
    Ok(insert_audit(AuditInput {
        id,
        request: record.request_id.as_str(),
        kind: MACHINE_ALLOWANCE_CHANGE_KIND,
        origin,
        person,
        permission,
        target: record.machine_id.as_str(),
        timestamp: SafeInteger::new(record.created_at_millis.0)
            .map_err(|_| corrupt("created_at", "invalid millisecond"))?,
        allowance: Some(AuditAllowanceFields {
            workspace: record.workspace_id.as_str(),
            state: record.state.as_str(),
        }),
    }))
}

pub fn insert(record: &OrganizationChangeRecord) -> Result<GuardedStatement, StoreFailure> {
    Ok(insert_audit(record.insert_values()?))
}

fn insert_audit(audit: AuditInput<'_>) -> GuardedStatement {
    GuardedStatement {
        statement: Statement {
            sql: include_str!("queries/insert_change_record.sql"),
            args: vec![
                text(audit.id),
                text(audit.request),
                text(audit.kind),
                text(audit.origin),
                audit.person.map_or(SqlValue::Null, text),
                audit.permission.map_or(SqlValue::Null, text),
                text(audit.target),
                SqlValue::Integer(audit.timestamp),
                SqlValue::Integer(audit.timestamp),
                audit
                    .allowance
                    .map_or(SqlValue::Null, |fields| text(fields.workspace)),
                audit
                    .allowance
                    .map_or(SqlValue::Null, |fields| text(fields.state)),
            ],
        },
        expect: RowExpectation::Exactly(1),
    }
}
fn optional_text(
    rows: &ReadOutcome,
    row: &[SqlValue],
    name: &str,
) -> Result<Option<String>, StoreFailure> {
    match column(rows, row, name)? {
        SqlValue::Null => Ok(None),
        SqlValue::Text(value) => Ok(Some(value.clone())),
        _ => Err(bad_value(name)),
    }
}
struct StoredChangeRow {
    id: String,
    request_id: String,
    kind: String,
    origin: String,
    actor_person_id: Option<String>,
    permission_checked: Option<String>,
    target_id: String,
    created_at: i64,
    workspace_id: Option<String>,
    allowance_state: Option<String>,
}

fn decode(rows: &ReadOutcome, row: &[SqlValue]) -> Result<StoredOrganizationChange, StoreFailure> {
    let SqlValue::Integer(created_at) = column(rows, row, "created_at")? else {
        return Err(corrupt("created_at", "expected a stored millisecond"));
    };
    StoredChangeRow {
        id: string(rows, row, "id")?.to_owned(),
        request_id: string(rows, row, "request_id")?.to_owned(),
        kind: string(rows, row, "kind")?.to_owned(),
        origin: string(rows, row, "origin")?.to_owned(),
        actor_person_id: optional_text(rows, row, "actor_person_id")?,
        permission_checked: optional_text(rows, row, "permission_checked")?,
        target_id: string(rows, row, "target_id")?.to_owned(),
        created_at: created_at.get(),
        workspace_id: optional_text(rows, row, "workspace_id")?,
        allowance_state: optional_text(rows, row, "allowance_state")?,
    }
    .decode()
}

impl StoredChangeRow {
    fn decode(self) -> Result<StoredOrganizationChange, StoreFailure> {
        let id = private_id(&self.id)?;
        let request_id = self.request_id.try_into()?;
        let created_at_millis = self
            .created_at
            .try_into()
            .map_err(|_| corrupt("created_at", "invalid millisecond"))?;
        let origin = match (
            self.origin.as_str(),
            self.actor_person_id,
            self.permission_checked,
        ) {
            ("signInProvisioning", None, None) => OrganizationChangeOrigin::SignInProvisioning {},
            ("automatic", None, None) => OrganizationChangeOrigin::Automatic {},
            ("person", Some(person_id), Some(permission_checked))
                if !person_id.is_empty() && !permission_checked.is_empty() =>
            {
                OrganizationChangeOrigin::Person {
                    person_id,
                    permission_checked,
                }
            }
            _ => return Err(bad_value("signin_change_records.origin")),
        };
        let target_id = self.target_id;
        if target_id.is_empty() {
            return Err(bad_value("signin_change_records.target_id"));
        }
        if self.kind == MACHINE_ALLOWANCE_CHANGE_KIND {
            let machine_id = target_id
                .try_into()
                .map_err(|_| corrupt("target_id", "invalid machine id"))?;
            let workspace_id = self
                .workspace_id
                .ok_or_else(|| corrupt("workspace_id", "missing workspace id"))?
                .try_into()
                .map_err(|_| corrupt("workspace_id", "invalid workspace id"))?;
            let state = MachineAllowanceState::from_stored(
                self.allowance_state
                    .as_deref()
                    .ok_or_else(|| corrupt("allowance_state", "missing allowance state"))?,
            )?;
            return Ok(StoredOrganizationChange::MachineAllowanceChanged(
                MachineAllowanceChange {
                    id,
                    request_id,
                    origin,
                    machine_id,
                    workspace_id,
                    state,
                    created_at_millis,
                },
            ));
        }
        let kind = OrganizationChangeKind::parse(&self.kind)?;
        if self.workspace_id.is_some() {
            return Err(corrupt("workspace_id", "unexpected allowance workspace"));
        }
        if self.allowance_state.is_some() {
            return Err(corrupt("allowance_state", "unexpected allowance state"));
        }
        Ok(StoredOrganizationChange::TargetedChange(
            OrganizationChangeRecord {
                id,
                request_id,
                kind,
                origin,
                target_id,
                created_at_millis,
            },
        ))
    }
}
pub async fn read_page(
    host: &impl CentralStore,
    after: Option<Uuid>,
    limit: u16,
) -> Result<Vec<StoredOrganizationChange>, StoreFailure> {
    let cursor = after.map(|id| id.to_string()).unwrap_or_default();
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(
        StoredChangeRow,
        "src/queries/read_change_page.sql",
        &cursor,
        i64::from(limit)
    );
    let rows = host
        .read(Statement {
            sql: include_str!("queries/read_change_page.sql"),
            args: vec![text(cursor), integer(i64::from(limit))?],
        })
        .await?;
    rows.rows.iter().map(|row| decode(&rows, row)).collect()
}

/// Find the predecessor ended by this approval's activation, without paging
/// through unrelated audit history. The operation key already stores this link.
pub async fn read_machine_supersession(
    host: &impl CentralStore,
    originating_request: &str,
) -> Result<Option<OrganizationChangeRecord>, StoreFailure> {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(
        StoredChangeRow,
        "src/queries/read_machine_supersession.sql",
        originating_request,
    );
    let rows = host
        .read(Statement {
            sql: include_str!("queries/read_machine_supersession.sql"),
            args: vec![text(originating_request)],
        })
        .await?;
    match rows.rows.as_slice() {
        [] => Ok(None),
        [row] => match decode(&rows, row)? {
            StoredOrganizationChange::TargetedChange(record) => Ok(Some(record)),
            StoredOrganizationChange::MachineAllowanceChanged(_) => {
                Err(bad_value("signin_change_records.machine_supersession"))
            }
        },
        _ => Err(bad_value("signin_change_records.machine_supersession")),
    }
}

#[cfg(test)]
#[path = "organization_change_test.rs"]
mod tests;
