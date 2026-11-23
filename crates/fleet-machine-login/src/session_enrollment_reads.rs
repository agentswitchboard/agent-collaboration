//! Machine Login's enrollment projection for session custody.

use central_storage::StoreFailure;
use fleet_identity_protocol::EnrollmentId;
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, query_as::query_as, types::Type,
};
use workspace_ids::MachineId;

use crate::{
    enrollment_reads::lent_read_failure,
    enrollment_row_codec::{StoredAuthorityApprovalRow, StoredAuthorityMachineRow},
    policy::{EnrollmentAuthority, StoredEnrollmentState},
};

/// Facts about one enrollment within the organization transaction.
/// An access-token timeout has no effect on this projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionEnrollmentStatus {
    Current,
    Ended { at_millis: i64 },
    Unavailable,
}

/// The existing unique active-enrollment index makes it sufficient to validate
/// the one scoped approval against its machine and generation.
pub async fn read_session_enrollment<Transaction: CentralTransaction>(
    tx: &mut Transaction,
    machine_id: &MachineId,
    enrollment_id: &EnrollmentId,
) -> Result<SessionEnrollmentStatus, StoreFailure>
where
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    Option<String>: Type<Transaction::Database> + for<'r> Decode<'r, Transaction::Database>,
    i64: Type<Transaction::Database> + for<'r> Decode<'r, Transaction::Database>,
    Option<i64>: Type<Transaction::Database> + for<'r> Decode<'r, Transaction::Database>,
    for<'r> &'r str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    let enrollment_id = enrollment_id.as_str();
    let machine_id = machine_id.as_str();
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(
        StoredSessionEnrollmentRow,
        "src/queries/read_session_enrollment.sql",
        enrollment_id,
        machine_id
    );
    let stored = query_as::<_, StoredSessionEnrollmentRow>(include_str!(
        "queries/read_session_enrollment.sql"
    ))
    .bind(enrollment_id.to_owned())
    .bind(machine_id.to_owned())
    .fetch_optional(tx.executor())
    .await
    .map_err(lent_read_failure)?;
    stored.map_or(Ok(SessionEnrollmentStatus::Unavailable), |row| row.decode())
}

#[derive(sqlx::FromRow)]
struct StoredSessionEnrollmentRow {
    approval_id: Option<String>,
    machine_id: Option<String>,
    base_generation: i64,
    state: String,
    end_reason: Option<String>,
    machine_client_id: Option<String>,
    last_grant_at: Option<String>,
    ended_at: Option<String>,
    machine_state: Option<String>,
    enrollment_generation: Option<i64>,
}

impl StoredSessionEnrollmentRow {
    fn decode(self) -> Result<SessionEnrollmentStatus, StoreFailure> {
        let machine = StoredAuthorityMachineRow {
            machine_id: self.machine_id.clone(),
            state: self
                .machine_state
                .ok_or_else(|| corrupt("login_machines.state"))?,
            enrollment_generation: self
                .enrollment_generation
                .ok_or_else(|| corrupt("login_machines.enrollment_generation"))?,
        }
        .decode()?;
        let approval = StoredAuthorityApprovalRow {
            approval_id: self.approval_id,
            machine_id: self.machine_id,
            base_generation: self.base_generation,
            state: self.state,
            end_reason: self.end_reason,
            machine_client_id: self.machine_client_id,
            last_grant_at: self.last_grant_at,
            ended_at: self.ended_at,
        }
        .decode()?;
        match approval.state {
            StoredEnrollmentState::Enrolled { .. } => {
                EnrollmentAuthority::from_projection(Some(machine), None, Some(approval))?;
                Ok(SessionEnrollmentStatus::Current)
            }
            StoredEnrollmentState::Revoked { .. } | StoredEnrollmentState::Expired { .. } => {
                let at_millis = approval
                    .ended_at_ms
                    .ok_or_else(|| corrupt("login_machine_approvals.ended_at"))?;
                Ok(SessionEnrollmentStatus::Ended { at_millis })
            }
            StoredEnrollmentState::Approved | StoredEnrollmentState::Lapsed => {
                Ok(SessionEnrollmentStatus::Unavailable)
            }
        }
    }
}

/// Keep an inactive or absent allowance indistinguishable to the start caller.
pub async fn machine_can_start_in_workspace(
    host: &impl fleet_runtime_ports::CentralStore,
    machine: &MachineId,
    workspace: &workspace_ids::WorkspaceId,
) -> Result<bool, StoreFailure> {
    use central_storage::{SqlValue, Statement};
    let outcome = host
        .read(Statement {
            sql: include_str!("queries/read_session_workspace_allowance.sql"),
            args: vec![
                SqlValue::Text(machine.as_str().to_owned()),
                SqlValue::Text(workspace.as_str().to_owned()),
            ],
        })
        .await?;
    match outcome.rows.as_slice() {
        [] => Ok(false),
        [row] => match row.as_slice() {
            [SqlValue::Text(state)] => decode_start_allowance(Some(state)),
            _ => Err(corrupt("login_machine_allowances.state")),
        },
        _ => Err(corrupt("login_machine_allowances.state")),
    }
}

/// Recheck the allowance on the same transaction that writes the reservation.
pub async fn machine_can_start_in_workspace_in_transaction<Transaction: CentralTransaction>(
    tx: &mut Transaction,
    machine: &MachineId,
    workspace: &workspace_ids::WorkspaceId,
) -> Result<bool, StoreFailure>
where
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    for<'r> &'r str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    #[derive(sqlx::FromRow)]
    struct StoredStartAllowance {
        state: String,
    }

    let machine = machine.as_str();
    let workspace = workspace.as_str();
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(
        StoredStartAllowance,
        "src/queries/read_session_workspace_allowance.sql",
        machine,
        workspace
    );
    let stored = query_as::<_, StoredStartAllowance>(include_str!(
        "queries/read_session_workspace_allowance.sql"
    ))
    .bind(machine.to_owned())
    .bind(workspace.to_owned())
    .fetch_optional(tx.executor())
    .await
    .map_err(lent_read_failure)?;
    decode_start_allowance(stored.as_ref().map(|allowance| allowance.state.as_str()))
}

fn decode_start_allowance(state: Option<&str>) -> Result<bool, StoreFailure> {
    match state {
        Some("active") => Ok(true),
        Some("withdrawn") | None => Ok(false),
        Some(_) => Err(corrupt("login_machine_allowances.state")),
    }
}
