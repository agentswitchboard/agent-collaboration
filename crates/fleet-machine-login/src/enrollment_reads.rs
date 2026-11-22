//! Read only the machine-login facts named by an organization-serialized query.
#[cfg(all(test, not(target_arch = "wasm32")))]
use crate::enrollment_row_codec::{StoredApprovalRow, StoredMachineRow};
use crate::{
    enrollment_row_codec::{
        StoredAllowanceRow, StoredApprovalHistory, StoredAuthorityApprovalRow,
        StoredAuthorityMachineRow, StoredWorkspacePresence, enrollment, machine, only_row,
    },
    policy::{
        AllowanceInputs, AuthorityApproval, AuthorityMachine, EnrollmentAuthority, EnrollmentFacts,
        EnrollmentQuery, MachineRow, StoredAllowance, StoredAllowanceState, StoredEnrollment,
        StoredEnrollmentState,
    },
};
use central_storage::{Statement, StoreFailure};
use fleet_contracts::{EndedMachineClient, EnrollmentRevocationReason};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::{
    CentralStore, guarded_statements::text, workspace_decode_error::corrupt,
};
use fleet_signin::organization_change::{OrganizationChangeOrigin, read_machine_supersession};
use message_board::WorkspaceId;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, query_as::query_as, types::Type,
};
use std::collections::BTreeMap;
use workspace_ids::MachineId;

macro_rules! checked_read {
    ($row:ident, $query:literal $(, $input:expr)* $(,)?) => {{
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file_as!($row, $query $(, $input)*);
        Statement {
            sql: include_str!(concat!("../", $query)),
            args: vec![$(text($input)),*],
        }
    }};
}

macro_rules! single_projection {
    ($read:ident, $row:ident, $result:ident, $decoder:expr, $query:literal, $field:literal) => {
        async fn $read(
            host: &impl CentralStore,
            key: &str,
        ) -> Result<Option<$result>, StoreFailure> {
            let rows = host.read(checked_read!($row, $query, key)).await?;
            only_row(&rows, $decoder, $field)
        }
    };
}

// The two hosts lend different executors, but both routes use the same
// projection file, result type, column decoder and arity.
macro_rules! authority_projection {
    ($read:ident, $lent:ident, $row:ident, $result:ident, $query:literal, $field:literal) => {
        single_projection!(
            $read,
            $row,
            $result,
            |rows, row| $row::from_store(rows, row)?.decode(),
            $query,
            $field
        );

        async fn $lent<T: CentralTransaction>(
            tx: &mut T,
            key: &str,
        ) -> Result<Option<$result>, StoreFailure>
        where
            <T::Database as Database>::Arguments: IntoArguments<T::Database>,
            String: Type<T::Database>
                + for<'q> Encode<'q, T::Database>
                + for<'r> Decode<'r, T::Database>,
            Option<String>: Type<T::Database> + for<'r> Decode<'r, T::Database>,
            i64: Type<T::Database> + for<'r> Decode<'r, T::Database>,
            for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
        {
            let rows: Vec<$row> = query_as(include_str!(concat!("../", $query)))
                .bind(key.to_owned())
                .fetch_all(tx.executor())
                .await
                .map_err(lent_read_failure)?;
            match rows.len() {
                0 => Ok(None),
                1 => rows.into_iter().next().map($row::decode).transpose(),
                _ => Err(corrupt($field)),
            }
        }
    };
}

single_projection!(
    read_enrollment,
    StoredApprovalRow,
    StoredEnrollment,
    enrollment,
    "src/queries/read_approval_by_id.sql",
    "login_machine_approvals.approval_id"
);
single_projection!(
    read_machine,
    StoredMachineRow,
    MachineRow,
    machine,
    "src/queries/read_machine_by_id.sql",
    "login_machines.machine_id"
);
single_projection!(
    read_current_approval,
    StoredApprovalRow,
    StoredEnrollment,
    enrollment,
    "src/queries/read_active_approval.sql",
    "login_machine_approvals.current_enrollment"
);
authority_projection!(
    read_link_enrollment,
    read_lent_link_enrollment,
    StoredAuthorityApprovalRow,
    AuthorityApproval,
    "src/queries/read_link_approval.sql",
    "login_machine_approvals.approval_id"
);
authority_projection!(
    read_link_machine,
    read_lent_link_machine,
    StoredAuthorityMachineRow,
    AuthorityMachine,
    "src/queries/read_link_machine.sql",
    "login_machines.machine_id"
);
authority_projection!(
    read_current_link_approval,
    read_lent_current_link_approval,
    StoredAuthorityApprovalRow,
    AuthorityApproval,
    "src/queries/read_current_link_approval.sql",
    "login_machine_approvals.current_enrollment"
);

fn lent_read_failure(error: sqlx::Error) -> StoreFailure {
    match error {
        sqlx::Error::ColumnNotFound(column) | sqlx::Error::ColumnDecode { index: column, .. } => {
            corrupt(&column)
        }
        _ => StoreFailure::Unavailable,
    }
}

pub(crate) async fn read_authority(
    host: &impl CentralStore,
    machine_id: &MachineId,
    enrollment_id: &EnrollmentId,
) -> Result<EnrollmentAuthority, StoreFailure> {
    let candidate = read_link_enrollment(host, enrollment_id.as_str()).await?;
    let machine = read_link_machine(host, machine_id.as_str()).await?;
    let active = match &machine {
        None => None,
        Some(_) => read_current_link_approval(host, machine_id.as_str()).await?,
    };
    EnrollmentAuthority::from_projection(
        machine,
        candidate.filter(|row| row.machine_id == *machine_id),
        active,
    )
}

pub(crate) async fn read_lent_authority<T: CentralTransaction>(
    tx: &mut T,
    machine_id: &MachineId,
    enrollment_id: &EnrollmentId,
) -> Result<EnrollmentAuthority, StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    Option<String>: Type<T::Database> + for<'r> Decode<'r, T::Database>,
    i64: Type<T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    let candidate = read_lent_link_enrollment(tx, enrollment_id.as_str()).await?;
    let machine = read_lent_link_machine(tx, machine_id.as_str()).await?;
    let active = match &machine {
        None => None,
        Some(_) => read_lent_current_link_approval(tx, machine_id.as_str()).await?,
    };
    EnrollmentAuthority::from_projection(
        machine,
        candidate.filter(|row| row.machine_id == *machine_id),
        active,
    )
}

pub(crate) async fn facts(
    host: &impl CentralStore,
    organization_id: OrganizationId,
    query: &EnrollmentQuery,
) -> Result<EnrollmentFacts, StoreFailure> {
    if let Some(request) = &query.request {
        let rows = host
            .read(checked_read!(
                StoredApprovalRow,
                "src/queries/read_approval_by_request.sql",
                request.as_str()
            ))
            .await?;
        if let Some(prior_approval) =
            only_row(&rows, enrollment, "login_machine_approvals.request_id")?
        {
            let mut facts = EnrollmentFacts::checked(organization_id, None, None, None)?;
            facts.organization_has_enrollment = true;
            facts.prior_approval = Some(prior_approval);
            return Ok(facts);
        }
    }
    let history = host
        .read(checked_read!(
            StoredApprovalHistory,
            "src/queries/read_approval_history.sql"
        ))
        .await?;
    let organization_has_enrollment = only_row(
        &history,
        |rows, row| StoredApprovalHistory::from_store(rows, row)?.decode(),
        "login_machine_approvals.has_enrollment",
    )?
    .ok_or_else(|| corrupt("login_machine_approvals.has_enrollment"))?;

    let enrollment = match &query.enrollment {
        Some(id) => read_enrollment(host, id.as_str()).await?,
        None => None,
    };
    let machine = match (
        &query.machine_id,
        enrollment.as_ref().map(|row| &row.machine_id),
    ) {
        (Some(id), _) => read_machine(host, id.as_str()).await?,
        (None, Some(id)) => Some(
            read_machine(host, id.as_str())
                .await?
                .ok_or_else(|| corrupt("login_machine_approvals.machine_id"))?,
        ),
        (None, None) => None,
    };
    let active = match &machine {
        Some(machine) => read_current_approval(host, machine.machine_id.as_str()).await?,
        None => None,
    };
    let mut facts = EnrollmentFacts::checked(organization_id, machine, enrollment, active)?;
    let superseded_client = match &facts.enrollment {
        Some(current) if matches!(current.state, StoredEnrollmentState::Enrolled { .. }) => {
            superseded_client(host, current).await?
        }
        _ => None,
    };
    facts.organization_has_enrollment = organization_has_enrollment;
    facts.superseded_client = superseded_client;
    if let Some(machine_id) = &query.machine_id {
        load_approval_allowances(host, &mut facts, machine_id, &query.workspaces).await?;
    }
    Ok(facts)
}

async fn load_approval_allowances(
    host: &impl CentralStore,
    facts: &mut EnrollmentFacts,
    machine_id: &MachineId,
    workspaces: &[WorkspaceId],
) -> Result<(), StoreFailure> {
    let mut selected = BTreeMap::new();
    for workspace in workspaces {
        if selected.contains_key(workspace) || facts.missing_workspaces.contains(workspace) {
            continue;
        }
        let rows = host
            .read(checked_read!(
                StoredWorkspacePresence,
                "src/queries/read_workspace_presence.sql",
                workspace.as_str()
            ))
            .await?;
        if only_row(
            &rows,
            |rows, row| StoredWorkspacePresence::from_store(rows, row)?.decode(),
            "directory_workspaces.workspace_id",
        )?
        .is_none()
        {
            facts.missing_workspaces.push(workspace.clone());
        } else {
            let pair = match &facts.machine {
                Some(_) => read_allowance(host, machine_id, workspace).await?,
                None => None,
            };
            selected.insert(workspace.clone(), pair);
        }
    }
    facts.allowances = AllowanceInputs::Selected(selected);
    Ok(())
}

pub(crate) async fn facts_for_ending(
    host: &impl CentralStore,
    organization_id: OrganizationId,
    authority: EnrollmentAuthority,
) -> Result<EnrollmentFacts, StoreFailure> {
    let field = "login_machine_approvals.current_enrollment";
    let observed = authority
        .current()
        .ok_or_else(|| corrupt(field))?
        .enrollment();
    // Only an elapsed idle deadline needs the full snapshot and allowances.
    let machine = read_machine(host, observed.machine_id.as_str())
        .await?
        .ok_or_else(|| corrupt(field))?;
    let enrollment = read_enrollment(host, observed.id.as_str())
        .await?
        .ok_or_else(|| corrupt(field))?;
    if authority.machine() != Some(&AuthorityMachine::from(&machine))
        || *observed != AuthorityApproval::from(&enrollment)
    {
        return Err(corrupt(field));
    }
    let mut facts = EnrollmentFacts::checked(
        organization_id,
        Some(machine),
        Some(enrollment.clone()),
        Some(enrollment),
    )?;
    load_terminal_allowances(host, &mut facts).await?;
    Ok(facts)
}

pub(crate) async fn load_terminal_allowances(
    host: &impl CentralStore,
    facts: &mut EnrollmentFacts,
) -> Result<(), StoreFailure> {
    let current = facts
        .current
        .as_ref()
        .ok_or_else(|| corrupt("login_machine_approvals.current_enrollment"))?;
    let machine = &current.machine().machine_id;
    let mut pairs = BTreeMap::new();
    for pair in read_active_pairs(host, machine).await? {
        if pairs.insert(pair.workspace_id.clone(), pair).is_some() {
            return Err(corrupt("login_machine_allowances.workspace_id"));
        }
    }
    // The ending approval names the remaining relevant pairs. Point lookups
    // allow an already-withdrawn pair to hold without walking unrelated history.
    let mut checked = std::collections::BTreeSet::new();
    for workspace in current.enrollment().allowed_workspaces.as_slice() {
        if pairs.contains_key(workspace) || !checked.insert(workspace) {
            continue;
        }
        if let Some(pair) = read_allowance(host, machine, workspace).await? {
            if pair.state != StoredAllowanceState::Withdrawn {
                return Err(corrupt("login_machine_allowances.active_set"));
            }
            pairs.insert(workspace.clone(), pair);
        }
    }
    facts.allowances = AllowanceInputs::Ending(pairs);
    Ok(())
}

pub(crate) async fn read_allowance(
    host: &impl CentralStore,
    machine: &MachineId,
    workspace: &WorkspaceId,
) -> Result<Option<StoredAllowance>, StoreFailure> {
    let rows = host
        .read(checked_read!(
            StoredAllowanceRow,
            "src/queries/read_machine_allowance_pair.sql",
            machine.as_str(),
            workspace.as_str()
        ))
        .await?;
    only_row(
        &rows,
        |rows, row| StoredAllowanceRow::from_store(rows, row)?.decode(),
        "login_machine_allowances.workspace_id",
    )
}

pub(crate) async fn read_active_allowances(
    host: &impl CentralStore,
    machine: &MachineId,
) -> Result<Vec<WorkspaceId>, StoreFailure> {
    Ok(read_active_pairs(host, machine)
        .await?
        .into_iter()
        .map(|pair| pair.workspace_id)
        .collect())
}

async fn read_active_pairs(
    host: &impl CentralStore,
    machine: &MachineId,
) -> Result<Vec<StoredAllowance>, StoreFailure> {
    let rows = host
        .read(checked_read!(
            StoredAllowanceRow,
            "src/queries/read_machine_allowances.sql",
            machine.as_str()
        ))
        .await?;
    rows.rows
        .iter()
        .map(|row| {
            let pair = StoredAllowanceRow::from_store(&rows, row)?.decode()?;
            if pair.state != StoredAllowanceState::Active {
                return Err(corrupt("login_machine_allowances.active_set"));
            }
            Ok(pair)
        })
        .collect()
}
async fn superseded_client(
    host: &impl CentralStore,
    current: &StoredEnrollment,
) -> Result<Option<EndedMachineClient>, StoreFailure> {
    let Some(record) = read_machine_supersession(host, current.request_id.as_str()).await? else {
        return Ok(None);
    };
    let previous = read_enrollment(host, &record.target_id)
        .await?
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
