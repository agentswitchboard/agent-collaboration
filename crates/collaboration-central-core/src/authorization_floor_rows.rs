//! Strict authorization-floor decoding and snapshot guards for modern central.
use crate::person_request::{
    FloorRevision, OrganizationFloor, OrganizationFloorState, PersonFloor, PersonFloorState,
};
use central_storage::{
    GuardedStatement, ReadOutcome, SafeInteger, SqlValue, Statement, StoreFailure,
};
use fleet_runtime_ports::CentralStore;
use fleet_runtime_ports::{
    stored_values::{column, string},
    workspace_decode_error::corrupt,
};

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

pub(crate) fn decode_organization_floor(
    rows: &ReadOutcome,
    organization: &str,
) -> Result<Option<OrganizationFloor>, StoreFailure> {
    if rows.rows.len() > 1 {
        return Err(corrupt("signin_authorization_floors.id"));
    }
    let Some(row) = rows.rows.first() else {
        return Ok(None);
    };
    if string(rows, row, "id")? != organization {
        return Err(corrupt("signin_authorization_floors.id"));
    }
    let policy_digest = string(rows, row, "permission_policy_digest")?.to_owned();
    if policy_digest.is_empty() {
        return Err(corrupt(
            "signin_authorization_floors.permission_policy_digest",
        ));
    }
    let reject_through_ms = read_integer(rows, row, "request_reject_through_ms")?;
    if reject_through_ms < 0 {
        return Err(corrupt(
            "signin_authorization_floors.request_reject_through_ms",
        ));
    }
    Ok(Some(OrganizationFloor {
        revision: FloorRevision::try_from(read_integer(rows, row, "minimum_revision")?)?,
        state: OrganizationFloorState::parse(string(rows, row, "state")?)?,
        policy_digest,
        reject_through_ms,
    }))
}

pub(crate) fn decode_person_floor(
    rows: &ReadOutcome,
    organization: &str,
    person: &str,
) -> Result<Option<PersonFloor>, StoreFailure> {
    if rows.rows.len() > 1 {
        return Err(corrupt("signin_person_authorization_floors.person_id"));
    }
    let Some(row) = rows.rows.first() else {
        return Ok(None);
    };
    let subject_id = string(rows, row, "id")?.to_owned();
    private_id(&subject_id, "signin_person_authorization_floors.id")?;
    if string(rows, row, "organization_id")? != organization
        || string(rows, row, "person_id")? != person
    {
        return Err(corrupt("signin_person_authorization_floors.person_id"));
    }
    Ok(Some(PersonFloor {
        subject_id,
        revision: FloorRevision::try_from(read_integer(rows, row, "minimum_revision")?)?,
        state: PersonFloorState::parse(string(rows, row, "state")?)?,
    }))
}

pub(crate) async fn read_organization_floor(
    host: &impl CentralStore,
    organization: &str,
) -> Result<Option<OrganizationFloor>, StoreFailure> {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file!(
        "src/person_proof_queries/read_organization_floor.sql",
        organization,
    );
    let rows = host
        .read(Statement {
            sql: include_str!("person_proof_queries/read_organization_floor.sql"),
            args: vec![SqlValue::Text(organization.to_owned())],
        })
        .await?;
    decode_organization_floor(&rows, organization)
}

pub(crate) async fn read_person_floor(
    host: &impl CentralStore,
    organization: &str,
    person: &str,
) -> Result<Option<PersonFloor>, StoreFailure> {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file!(
        "src/person_proof_queries/read_person_floor.sql",
        organization,
        person,
    );
    let rows = host
        .read(Statement {
            sql: include_str!("person_proof_queries/read_person_floor.sql"),
            args: vec![
                SqlValue::Text(organization.to_owned()),
                SqlValue::Text(person.to_owned()),
            ],
        })
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
    let state = organization_state(floor.state);
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file!(
        "src/person_proof_queries/guard_organization_floor.sql",
        organization,
        floor.revision.get(),
        state,
        &floor.policy_digest,
        floor.reject_through_ms,
    );
    Ok(fleet_runtime_ports::guarded_statements::exact(
        include_str!("person_proof_queries/guard_organization_floor.sql"),
        vec![
            SqlValue::Text(organization.to_owned()),
            integer(floor.revision.get(), "minimum_revision")?,
            SqlValue::Text(state.to_owned()),
            SqlValue::Text(floor.policy_digest.clone()),
            integer(floor.reject_through_ms, "request_reject_through_ms")?,
        ],
    ))
}
pub(crate) fn person_guard(
    organization: &str,
    person: &str,
    floor: &PersonFloor,
) -> Result<GuardedStatement, StoreFailure> {
    let state = person_state(floor.state);
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file!(
        "src/person_proof_queries/guard_person_floor.sql",
        &floor.subject_id,
        organization,
        person,
        floor.revision.get(),
        state,
    );
    Ok(fleet_runtime_ports::guarded_statements::exact(
        include_str!("person_proof_queries/guard_person_floor.sql"),
        vec![
            SqlValue::Text(floor.subject_id.clone()),
            SqlValue::Text(organization.to_owned()),
            SqlValue::Text(person.to_owned()),
            integer(floor.revision.get(), "minimum_revision")?,
            SqlValue::Text(state.to_owned()),
        ],
    ))
}
