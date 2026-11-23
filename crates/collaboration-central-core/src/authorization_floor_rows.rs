//! Strict authorization-floor decoding and snapshot guards for modern central.
use crate::person_request::{
    FloorRevision, OrganizationFloor, OrganizationFloorState, PersonFloor, PersonFloorState,
};
use central_storage::{
    GuardedStatement, ReadOutcome, SafeInteger, SqlValue, Statement, StoreFailure,
};
use fleet_runtime_ports::{
    CentralStore,
    guarded_statements::{exact, text},
    stored_values::{column, string},
    workspace_decode_error::corrupt,
};

// SQLite describes bind arity. Count the actual CentralStore arguments in the
// witness, while keeping host-neutral SqlValue encoding on the runtime path.
macro_rules! checked_proof_statement {
    (read $row:ident, $file:literal, [$($argument:expr),* $(,)?]) => {{
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file_as!(
            $row, $file $(, checked_proof_statement!(@parameter $argument))*
        );
        Statement {
            sql: include_str!(concat!("../", $file)),
            args: vec![$($argument),*],
        }
    }};
    (write $file:literal, [$($argument:expr),* $(,)?]) => {{
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file!(
            $file $(, checked_proof_statement!(@parameter $argument))*
        );
        exact(include_str!(concat!("../", $file)), vec![$($argument),*])
    }};
    (@parameter $argument:expr) => {{
        let _ = stringify!($argument);
        0_i64
    }};
}
pub(super) use checked_proof_statement;

pub(crate) fn integer(value: i64, field: &str) -> Result<SqlValue, StoreFailure> {
    SafeInteger::new(value)
        .map(SqlValue::Integer)
        .map_err(|_| corrupt(field))
}

fn read_integer(rows: &ReadOutcome, row: &[SqlValue], field: &str) -> Result<i64, StoreFailure> {
    match column(rows, row, field)? {
        SqlValue::Integer(value) => Ok(value.get()),
        _ => Err(corrupt(field)),
    }
}

fn private_id(value: &str, field: &str) -> Result<(), StoreFailure> {
    let parsed = uuid::Uuid::parse_str(value).map_err(|_| corrupt(field))?;
    if !fleet_runtime_ports::uuid_v7(parsed) || parsed.to_string() != value {
        return Err(corrupt(field));
    }
    Ok(())
}

struct OrganizationFloorRow {
    id: String,
    minimum_revision: i64,
    state: String,
    permission_policy_digest: String,
    request_reject_through_ms: i64,
}

impl OrganizationFloorRow {
    fn decode(self, organization: &str) -> Result<OrganizationFloor, StoreFailure> {
        if self.id != organization {
            return Err(corrupt("signin_authorization_floors.id"));
        }
        if self.permission_policy_digest.is_empty() {
            return Err(corrupt(
                "signin_authorization_floors.permission_policy_digest",
            ));
        }
        if self.request_reject_through_ms < 0 {
            return Err(corrupt(
                "signin_authorization_floors.request_reject_through_ms",
            ));
        }
        Ok(OrganizationFloor {
            revision: FloorRevision::try_from(self.minimum_revision)?,
            state: OrganizationFloorState::parse(&self.state)?,
            policy_digest: self.permission_policy_digest,
            reject_through_ms: self.request_reject_through_ms,
        })
    }
}

struct PersonFloorRow {
    id: String,
    organization_id: String,
    person_id: String,
    minimum_revision: i64,
    state: String,
}

impl PersonFloorRow {
    fn decode(self, organization: &str, person: &str) -> Result<PersonFloor, StoreFailure> {
        private_id(&self.id, "signin_person_authorization_floors.id")?;
        if self.organization_id != organization || self.person_id != person {
            return Err(corrupt("signin_person_authorization_floors.person_id"));
        }
        Ok(PersonFloor {
            subject_id: self.id,
            revision: FloorRevision::try_from(self.minimum_revision)?,
            state: PersonFloorState::parse(&self.state)?,
        })
    }
}

pub(crate) fn decode_organization_floor(
    rows: &ReadOutcome,
    organization: &str,
) -> Result<Option<OrganizationFloor>, StoreFailure> {
    let row = match rows.rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        _ => return Err(corrupt("signin_authorization_floors.id")),
    };
    OrganizationFloorRow {
        id: string(rows, row, "id")?.to_owned(),
        minimum_revision: read_integer(rows, row, "minimum_revision")?,
        state: string(rows, row, "state")?.to_owned(),
        permission_policy_digest: string(rows, row, "permission_policy_digest")?.to_owned(),
        request_reject_through_ms: read_integer(rows, row, "request_reject_through_ms")?,
    }
    .decode(organization)
    .map(Some)
}

pub(crate) fn decode_person_floor(
    rows: &ReadOutcome,
    organization: &str,
    person: &str,
) -> Result<Option<PersonFloor>, StoreFailure> {
    let row = match rows.rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        _ => return Err(corrupt("signin_person_authorization_floors.person_id")),
    };
    PersonFloorRow {
        id: string(rows, row, "id")?.to_owned(),
        organization_id: string(rows, row, "organization_id")?.to_owned(),
        person_id: string(rows, row, "person_id")?.to_owned(),
        minimum_revision: read_integer(rows, row, "minimum_revision")?,
        state: string(rows, row, "state")?.to_owned(),
    }
    .decode(organization, person)
    .map(Some)
}

pub(crate) fn organization_floor_statement(organization: &str) -> Statement {
    checked_proof_statement!(
        read OrganizationFloorRow,
        "src/person_proof_queries/read_organization_floor.sql",
        [text(organization)]
    )
}

pub(crate) fn person_floor_statement(organization: &str, person: &str) -> Statement {
    checked_proof_statement!(
        read PersonFloorRow,
        "src/person_proof_queries/read_person_floor.sql",
        [text(organization), text(person)]
    )
}

pub(crate) async fn read_organization_floor(
    host: &impl CentralStore,
    organization: &str,
) -> Result<Option<OrganizationFloor>, StoreFailure> {
    let rows = host
        .read(organization_floor_statement(organization))
        .await?;
    decode_organization_floor(&rows, organization)
}

pub(crate) async fn read_person_floor(
    host: &impl CentralStore,
    organization: &str,
    person: &str,
) -> Result<Option<PersonFloor>, StoreFailure> {
    let rows = host
        .read(person_floor_statement(organization, person))
        .await?;
    decode_person_floor(&rows, organization, person)
}

pub(crate) fn organization_state(state: OrganizationFloorState) -> &'static str {
    match state {
        OrganizationFloorState::Active => "active",
        OrganizationFloorState::Stopped => "stopped",
        OrganizationFloorState::Deleted => "deleted",
    }
}

pub(crate) fn person_state(state: PersonFloorState) -> &'static str {
    match state {
        PersonFloorState::Active => "active",
        PersonFloorState::Inactive => "inactive",
    }
}

/// A zero-row guard means the read snapshot changed; the caller retries admission.
pub(crate) fn organization_guard(
    organization: &str,
    floor: &OrganizationFloor,
) -> Result<GuardedStatement, StoreFailure> {
    Ok(checked_proof_statement!(
        write "src/person_proof_queries/guard_organization_floor.sql",
        [
            text(organization),
            integer(floor.revision.get(), "minimum_revision")?,
            text(organization_state(floor.state)),
            text(&floor.policy_digest),
            integer(floor.reject_through_ms, "request_reject_through_ms")?,
        ]
    ))
}

pub(crate) fn person_guard(
    organization: &str,
    person: &str,
    floor: &PersonFloor,
) -> Result<GuardedStatement, StoreFailure> {
    Ok(checked_proof_statement!(
        write "src/person_proof_queries/guard_person_floor.sql",
        [
            text(&floor.subject_id),
            text(organization),
            text(person),
            integer(floor.revision.get(), "minimum_revision")?,
            text(person_state(floor.state)),
        ]
    ))
}
