//! Establish the immutable project for a new reservation inside its write transaction.

use central_storage::StoreFailure;
use fleet_identity_protocol::{CreateSessionKind, CreateSessionRefusal, ProjectRef, StartProject};
use fleet_lent_transaction::CentralTransaction;
use fleet_machine_login::machine_can_start_in_workspace_in_transaction;
use fleet_runtime_ports::SessionProjectLookup;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use workspace_ids::MachineId;

pub(crate) async fn admit_session_project<Transaction: CentralTransaction>(
    transaction: &mut Transaction,
    machine: &MachineId,
    choice: &StartProject,
    kind: &CreateSessionKind,
    creator_project: Option<&ProjectRef>,
    lookup: SessionProjectLookup,
) -> Result<Result<ProjectRef, CreateSessionRefusal>, StoreFailure>
where
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    for<'r> &'r str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    let requested = match choice {
        StartProject::Named { project_ref } => project_ref,
        StartProject::SameAsParent {} => match (kind, creator_project) {
            (CreateSessionKind::Delegate { .. }, Some(project)) => project,
            (CreateSessionKind::Delegate { .. }, None) | (CreateSessionKind::Ordinary {}, _) => {
                return Ok(Err(CreateSessionRefusal::ProjectRequired {}));
            }
        },
    };
    if !machine_can_start_in_workspace_in_transaction(transaction, machine, &requested.workspace)
        .await?
    {
        return Ok(Err(CreateSessionRefusal::MachineNotAllowed {}));
    }
    if matches!(choice, StartProject::Named { .. }) {
        match lookup.consume_for(&requested.workspace, &requested.project)? {
            Some(true) => {}
            Some(false) => return Ok(Err(CreateSessionRefusal::ProjectNotInWorkspace {})),
            None => return Ok(Err(CreateSessionRefusal::MachineNotAllowed {})),
        }
    }
    if creator_project.is_some_and(|creator| creator.workspace != requested.workspace) {
        return Ok(Err(CreateSessionRefusal::HelperOutsideWorkspace {}));
    }
    Ok(Ok(requested.clone()))
}
