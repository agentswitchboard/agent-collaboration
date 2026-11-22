//! Read only the machine-login facts named by an organization-serialized query.
use crate::{
    enrollment_row_codec::{enrollment, machine, only_row},
    enrollment_row_values::{identifier, nonnegative_integer},
    policy::{EnrollmentFacts, EnrollmentQuery, StoredEnrollment, StoredEnrollmentState},
};
use central_storage::{Statement, StoreFailure};
use fleet_contracts::{EndedMachineClient, EnrollmentRevocationReason};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::CentralStore;
use fleet_runtime_ports::{
    guarded_statements::text, stored_values::string, workspace_decode_error::corrupt,
};
use fleet_signin::organization_change::{OrganizationChangeOrigin, read_machine_supersession};

macro_rules! checked_read {
    ($query:literal $(, $input:expr)* $(,)?) => {{
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file!($query $(, $input)*);
        Statement {
            sql: include_str!(concat!("../", $query)),
            args: vec![$(text($input)),*],
        }
    }};
}

pub async fn facts(
    host: &impl CentralStore,
    organization_id: OrganizationId,
    query: &EnrollmentQuery,
) -> Result<EnrollmentFacts, StoreFailure> {
    let prior_approval = if let Some(request) = &query.request {
        let rows = host
            .read(checked_read!(
                "src/queries/read_approval_by_request.sql",
                request.as_str()
            ))
            .await?;
        only_row(&rows, enrollment, "login_machine_approvals.request_id")?
    } else {
        None
    };
    if prior_approval.is_some() {
        return Ok(EnrollmentFacts {
            organization_id,
            organization_has_enrollment: true,
            prior_approval,
            enrollment: None,
            machine: None,
            active: None,
            superseded_client: None,
            missing_workspaces: vec![],
        });
    }
    let history = host
        .read(checked_read!("src/queries/read_approval_history.sql"))
        .await?;
    let organization_has_enrollment = only_row(
        &history,
        |rows, row| match nonnegative_integer(rows, row, "has_enrollment")?.get() {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(corrupt("login_machine_approvals.has_enrollment")),
        },
        "login_machine_approvals.has_enrollment",
    )?
    .ok_or_else(|| corrupt("login_machine_approvals.has_enrollment"))?;
    let enrollment = if let Some(id) = &query.enrollment {
        let rows = host
            .read(checked_read!(
                "src/queries/read_approval_by_id.sql",
                id.as_str()
            ))
            .await?;
        only_row(&rows, enrollment, "login_machine_approvals.approval_id")?
    } else {
        None
    };
    let machine = if let Some(machine_id) = &query.machine_id {
        let rows = host
            .read(checked_read!(
                "src/queries/read_machine_by_id.sql",
                machine_id.as_str()
            ))
            .await?;
        only_row(&rows, machine, "login_machines.machine_id")?
    } else if let Some(found) = &enrollment {
        let rows = host
            .read(checked_read!(
                "src/queries/read_machine_by_id.sql",
                found.machine_id.as_str()
            ))
            .await?;
        Some(
            only_row(&rows, machine, "login_machines.machine_id")?
                .ok_or_else(|| corrupt("login_machine_approvals.machine_id"))?,
        )
    } else {
        None
    };
    let active = if let Some(found) = &machine {
        let rows = host
            .read(checked_read!(
                "src/queries/read_active_approval.sql",
                found.machine_id.as_str()
            ))
            .await?;
        only_row(
            &rows,
            crate::enrollment_row_codec::enrollment,
            "login_machine_approvals.enrolled",
        )?
    } else {
        None
    };
    let superseded_client = match &enrollment {
        Some(current) if matches!(current.state, StoredEnrollmentState::Enrolled { .. }) => {
            superseded_client(host, current).await?
        }
        _ => None,
    };
    let mut missing_workspaces = Vec::new();
    for workspace in &query.workspaces {
        let rows = host
            .read(checked_read!(
                "src/queries/read_workspace_presence.sql",
                workspace.as_str()
            ))
            .await?;
        let found: Option<message_board::WorkspaceId> = only_row(
            &rows,
            |rows, row| {
                identifier(
                    string(rows, row, "workspace_id")?,
                    "directory_workspaces.workspace_id",
                )
            },
            "directory_workspaces.workspace_id",
        )?;
        if found.is_none() {
            missing_workspaces.push(workspace.clone());
        }
    }
    Ok(EnrollmentFacts {
        organization_id,
        organization_has_enrollment,
        prior_approval,
        enrollment,
        machine,
        active,
        superseded_client,
        missing_workspaces,
    })
}

async fn superseded_client(
    host: &impl CentralStore,
    current: &StoredEnrollment,
) -> Result<Option<EndedMachineClient>, StoreFailure> {
    let Some(record) = read_machine_supersession(host, current.request_id.as_str()).await? else {
        return Ok(None);
    };
    let rows = host
        .read(checked_read!(
            "src/queries/read_approval_by_id.sql",
            &record.target_id
        ))
        .await?;
    let previous = only_row(&rows, enrollment, "login_machine_approvals.superseded")?
        .ok_or_else(|| corrupt("login_machine_approvals.superseded"))?;
    let expected_origin = OrganizationChangeOrigin::Person {
        person_id: current.approved_by.as_str().to_owned(),
        permission_checked: "machine:enroll".to_owned(),
    };
    if previous.id == current.id
        || previous.machine_id != current.machine_id
        || previous.ended_at_ms != Some(record.created_at_millis.0)
        || record.origin != expected_origin
        || !matches!(
            previous.state,
            StoredEnrollmentState::Revoked {
                reason: EnrollmentRevocationReason::Superseded {},
                ..
            }
        )
    {
        return Err(corrupt("login_machine_approvals.superseded"));
    }
    previous
        .ended_client()
        .map(Some)
        .ok_or_else(|| corrupt("login_machine_approvals.superseded"))
}

#[cfg(test)]
#[path = "enrollment_reads_test.rs"]
mod tests;
