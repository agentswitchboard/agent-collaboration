//! Static guarded statements for a pure decision, in its declared atomic order.
use crate::{
    enrollment_row_values as values,
    policy::{
        EnrollmentChange, EnrollmentFacts, IDLE_EXPIRY_SECONDS, MachineRow, MachineState,
        StoredEnrollment, StoredEnrollmentState,
    },
};
use central_storage::{GuardedStatement, SafeInteger, SqlValue, StoreFailure};
use fleet_contracts::EnrollmentRevocationReason;
use fleet_runtime_ports::{
    guarded_statements::{exact, text},
    workspace_decode_error::corrupt,
};
use serde::Serialize;
use std::collections::BTreeMap;

// SQLite reports parameter arity but no parameter types. Expand the native
// witness from the runtime bind list so the statement cannot drift in arity.
macro_rules! checked_statement {
    ($file:literal, [$($argument:expr),* $(,)?]) => {{
        let arguments = [$($argument),*];
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = checked_statement!(@check $file, [$($argument),*]);
        exact(include_str!(concat!("../", $file)), Vec::from(arguments))
    }};
    (@check $file:literal, [$($argument:expr),*]) => {{
        let _ = sqlx::query_file!($file $(, checked_statement!(@parameter $argument))*);
    }};
    (@parameter $argument:expr) => {{
        let _ = stringify!($argument);
        0_i64
    }};
}

pub fn statements(
    facts: &EnrollmentFacts,
    changes: &[EnrollmentChange],
    applying_ms: i64,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    let at = values::timestamp(applying_ms, "machine_login.applying_time")?;
    let mut machine = facts.machine.clone();
    let mut enrollments = BTreeMap::new();
    for enrollment in [&facts.enrollment, &facts.active, &facts.prior_approval]
        .into_iter()
        .flatten()
    {
        enrollments.insert(enrollment.id.as_str().to_owned(), enrollment.clone());
    }
    let mut statements = Vec::new();
    if !changes.is_empty()
        && let Some(before) = &machine
    {
        statements.push(checked_statement!(
            "src/queries/guard_observed_machine.sql",
            [
                text(before.machine_id.as_str()),
                generation(before.generation.get())?,
                text(machine_state(before.state)),
                text(before.label.as_str()),
                encode(
                    &before.allowed_workspaces,
                    "login_machine_allowances.workspace_id"
                )?
            ]
        ));
    }
    for change in changes {
        match change {
            EnrollmentChange::PutMachine(next) => {
                statements.push(put_machine(machine.as_ref(), next, &at)?);
                machine = Some(next.clone());
            }
            EnrollmentChange::InsertEnrollment(next) => {
                let owner = machine
                    .as_ref()
                    .ok_or_else(|| corrupt("login_machine_approvals.machine_id"))?;
                if owner.machine_id != next.machine_id || owner.generation != next.base_generation {
                    return Err(corrupt("login_machine_approvals.machine_id"));
                }
                statements.push(insert_enrollment(next, &at)?);
                enrollments.insert(next.id.as_str().to_owned(), next.clone());
            }
            EnrollmentChange::ReplaceEnrollment(next) => {
                let before = enrollments
                    .get(next.id.as_str())
                    .ok_or_else(|| corrupt("login_machine_approvals.approval_id"))?;
                if let Some(guard) = time_guard(before, next, applying_ms)? {
                    statements.push(guard);
                }
                statements.push(replace_enrollment(before, next, &at)?);
                enrollments.insert(next.id.as_str().to_owned(), next.clone());
            }
            EnrollmentChange::Record(record) => {
                if record.created_at_millis.0 != applying_ms {
                    return Err(corrupt("signin_change_records.created_at"));
                }
                statements.push(fleet_signin::organization_change::insert(record)?);
            }
            EnrollmentChange::RecordAllowance(record) => {
                if record.created_at_millis.0 != applying_ms {
                    return Err(corrupt("signin_change_records.created_at"));
                }
                statements.push(apply_allowance(record, &at)?);
                statements.push(
                    fleet_signin::organization_change::insert_machine_allowance_change(record)?,
                );
            }
        }
    }
    Ok(statements)
}

fn integer(value: i64, field: &str) -> Result<SqlValue, StoreFailure> {
    SafeInteger::new(value)
        .map(SqlValue::Integer)
        .map_err(|_| corrupt(field))
}
fn generation(value: u64) -> Result<SqlValue, StoreFailure> {
    integer(
        i64::try_from(value).map_err(|_| corrupt("login_machines.enrollment_generation"))?,
        "login_machines.enrollment_generation",
    )
}
fn encode(value: &impl Serialize, field: &str) -> Result<SqlValue, StoreFailure> {
    serde_json::to_string(value)
        .map(text)
        .map_err(|_| corrupt(field))
}
fn machine_state(state: MachineState) -> &'static str {
    match state {
        MachineState::Enrolled => "enrolled",
        MachineState::Revoked => "revoked",
    }
}
fn put_machine(
    before: Option<&MachineRow>,
    next: &MachineRow,
    at: &str,
) -> Result<GuardedStatement, StoreFailure> {
    match before {
        None => {
            if next.generation.get() != 0 || next.state != MachineState::Revoked {
                return Err(corrupt("login_machines.enrollment_generation"));
            }
            Ok(checked_statement!(
                "src/queries/insert_login_machine.sql",
                [
                    text(next.machine_id.as_str()),
                    text(next.label.as_str()),
                    text(machine_state(next.state)),
                    generation(next.generation.get())?,
                    text(at),
                    text(at),
                ]
            ))
        }
        Some(before) => {
            if before.machine_id != next.machine_id {
                return Err(corrupt("login_machines.machine_id"));
            }
            if next.generation != before.generation
                && next.generation != before.generation.next()?
            {
                return Err(corrupt("login_machines.enrollment_generation"));
            }
            Ok(checked_statement!(
                "src/queries/replace_observed_machine.sql",
                [
                    text(next.label.as_str()),
                    text(machine_state(next.state)),
                    generation(next.generation.get())?,
                    text(at),
                    text(before.machine_id.as_str()),
                    generation(before.generation.get())?,
                    text(machine_state(before.state)),
                    text(before.label.as_str()),
                ]
            ))
        }
    }
}

fn apply_allowance(
    record: &fleet_signin::organization_change::MachineAllowanceChange,
    at: &str,
) -> Result<GuardedStatement, StoreFailure> {
    use fleet_signin::organization_change::{MachineAllowanceState, OrganizationChangeOrigin};

    let origin = match &record.origin {
        OrganizationChangeOrigin::Person { .. } => "person",
        OrganizationChangeOrigin::Automatic {} => "automatic",
        OrganizationChangeOrigin::SignInProvisioning {} => {
            return Err(corrupt("login_machine_allowances.change_origin"));
        }
    };
    let machine = record.machine_id.as_str();
    let workspace = record.workspace_id.as_str();
    let request = record.request_id.as_str();
    Ok(match record.state {
        MachineAllowanceState::Active => checked_statement!(
            "src/queries/activate_machine_allowance.sql",
            [
                text(record.id),
                text(machine),
                text(workspace),
                text(request),
                text(origin),
                text(at)
            ]
        ),
        MachineAllowanceState::Withdrawn => checked_statement!(
            "src/queries/withdraw_machine_allowance.sql",
            [
                text(machine),
                text(workspace),
                text(request),
                text(origin),
                text(at)
            ]
        ),
    })
}

struct EncodedState {
    state: &'static str,
    reason: Option<&'static str>,
    client: Option<String>,
    last_grant: Option<String>,
    ended_at: Option<String>,
}
fn state(enrollment: &StoredEnrollment) -> Result<EncodedState, StoreFailure> {
    let ended_at = enrollment
        .ended_at_ms
        .map(|at| values::timestamp(at, "login_machine_approvals.ended_at"))
        .transpose()?;
    let (state, reason, client, last_grant) = match &enrollment.state {
        StoredEnrollmentState::Approved => ("approved", None, None, None),
        StoredEnrollmentState::Lapsed => ("ended", Some("lapsed"), None, None),
        StoredEnrollmentState::Enrolled {
            client_id,
            last_grant_at,
        } => (
            "enrolled",
            None,
            Some(client_id.as_str().to_owned()),
            Some(values::seconds_timestamp(
                *last_grant_at,
                "login_machine_approvals.last_grant_at",
            )?),
        ),
        StoredEnrollmentState::Revoked { client_id, reason } => (
            "ended",
            Some(match reason {
                EnrollmentRevocationReason::Owner {} => "owner",
                EnrollmentRevocationReason::Superseded {} => "superseded",
            }),
            Some(client_id.as_str().to_owned()),
            None,
        ),
        StoredEnrollmentState::Expired { client_id } => (
            "ended",
            Some("idle"),
            Some(client_id.as_str().to_owned()),
            None,
        ),
    };
    if (state == "ended") != ended_at.is_some() {
        return Err(corrupt("login_machine_approvals.ended_at"));
    }
    if let Some(client) = &client {
        values::private_id(client, "login_machine_approvals.machine_client_id")?;
    }
    Ok(EncodedState {
        state,
        reason,
        client,
        last_grant,
        ended_at,
    })
}
fn nullable(value: Option<impl ToString>) -> SqlValue {
    value.map_or(SqlValue::Null, text)
}

fn insert_enrollment(next: &StoredEnrollment, at: &str) -> Result<GuardedStatement, StoreFailure> {
    if next.state != StoredEnrollmentState::Approved {
        return Err(corrupt("login_machine_approvals.state"));
    }
    for (value, field) in [
        (next.id.as_str(), "login_machine_approvals.approval_id"),
        (
            next.request_id.as_str(),
            "login_machine_approvals.request_id",
        ),
        (
            next.approved_by.as_str(),
            "login_machine_approvals.approved_by_person_id",
        ),
    ] {
        values::private_id(value, field)?;
    }
    let encoded = state(next)?;
    Ok(checked_statement!(
        "src/queries/insert_approval_snapshot.sql",
        [
            text(next.id.as_str()),
            text(next.request_id.as_str()),
            text(next.machine_id.as_str()),
            text(next.label.as_str()),
            encode(
                &next.allowed_workspaces,
                "login_machine_approvals.allowed_workspaces",
            )?,
            text(encoded.state),
            nullable(encoded.reason),
            text(next.approved_by.as_str()),
            text("machine:enroll"),
            generation(next.base_generation.get())?,
            text(values::seconds_timestamp(
                next.code_expires_at,
                "login_machine_approvals.code_expires_at",
            )?),
            nullable(encoded.client),
            nullable(encoded.last_grant),
            nullable(encoded.ended_at),
            text(values::timestamp(
                next.approved_at_ms,
                "login_machine_approvals.created_at",
            )?),
            text(at),
        ]
    ))
}

fn replace_enrollment(
    before: &StoredEnrollment,
    next: &StoredEnrollment,
    at: &str,
) -> Result<GuardedStatement, StoreFailure> {
    if before.id != next.id
        || before.machine_id != next.machine_id
        || before.request_id != next.request_id
        || before.approved_by != next.approved_by
        || before.label != next.label
        || before.allowed_workspaces != next.allowed_workspaces
        || before.base_generation != next.base_generation
        || before.approved_at_ms != next.approved_at_ms
        || before.code_expires_at != next.code_expires_at
    {
        return Err(corrupt("login_machine_approvals.immutable_approval"));
    }
    let old = state(before)?;
    let new = state(next)?;
    let last_grant = match &before.state {
        StoredEnrollmentState::Enrolled { last_grant_at, .. } => Some(integer(
            last_grant_at.0,
            "login_machine_approvals.last_grant_at",
        )?),
        _ => None,
    };
    Ok(checked_statement!(
        "src/queries/replace_observed_approval.sql",
        [
            text(new.state),
            nullable(new.reason),
            nullable(new.client),
            nullable(new.last_grant),
            nullable(new.ended_at),
            text(at),
            text(before.id.as_str()),
            text(before.machine_id.as_str()),
            text(old.state),
            nullable(old.reason),
            nullable(old.client),
            last_grant.unwrap_or(SqlValue::Null),
            generation(before.base_generation.get())?,
            integer(
                before.code_expires_at.0,
                "login_machine_approvals.code_expires_at",
            )?,
            text(before.request_id.as_str()),
            text(before.label.as_str()),
            encode(
                &before.allowed_workspaces,
                "login_machine_approvals.allowed_workspaces",
            )?,
            text(before.approved_by.as_str()),
            integer(before.approved_at_ms, "login_machine_approvals.created_at")?,
        ]
    ))
}

fn time_guard(
    before: &StoredEnrollment,
    next: &StoredEnrollment,
    now_ms: i64,
) -> Result<Option<GuardedStatement>, StoreFailure> {
    let now = integer(now_ms, "machine_login.applying_time")?;
    let now_seconds = integer(now_ms.div_euclid(1000), "machine_login.applying_time")?;
    let idle = integer(IDLE_EXPIRY_SECONDS, "machine_login.idle_expiry")?;
    // These no-op guards use the same statement vocabulary as authorization floors.
    // A miss rolls back every earlier effect in this decision's batch.
    let guard = match (&before.state, &next.state) {
        (StoredEnrollmentState::Approved, StoredEnrollmentState::Enrolled { .. }) => {
            checked_statement!(
                "src/queries/guard_unexpired_approval.sql",
                [text(before.id.as_str()), now]
            )
        }
        (StoredEnrollmentState::Approved, StoredEnrollmentState::Lapsed) => checked_statement!(
            "src/queries/guard_lapsing_approval.sql",
            [text(before.id.as_str()), now]
        ),
        (StoredEnrollmentState::Enrolled { .. }, StoredEnrollmentState::Enrolled { .. }) => {
            checked_statement!(
                "src/queries/guard_refresh_under_idle_limit.sql",
                [text(before.id.as_str()), now_seconds, idle]
            )
        }
        (StoredEnrollmentState::Enrolled { .. }, StoredEnrollmentState::Expired { .. }) => {
            checked_statement!(
                "src/queries/guard_idle_expiry.sql",
                [text(before.id.as_str()), now_seconds, idle]
            )
        }
        (StoredEnrollmentState::Enrolled { .. }, StoredEnrollmentState::Revoked { .. }) => {
            return Ok(None);
        }
        _ => return Err(corrupt("login_machine_approvals.state")),
    };
    Ok(Some(guard))
}

#[cfg(test)]
#[path = "enrollment_writes_test.rs"]
mod tests;
