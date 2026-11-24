//! Opens the machine identity and enrollment store for the Router CLI.

use std::{ffi::OsStr, io, path::Path, sync::Arc};

use router_clock::{RuntimeClock, SystemRuntimeClock};
use switchboard_machine_enrollment::MachineId;
use switchboard_machine_enrollment_store::{
    MachineEnrollmentStore, read_machine_enrollment_status,
};

use crate::{
    enrollment_command::{
        EnrollArguments, EnrollBindings, EnrollCommandError, create_sign_in_client, run_enroll,
    },
    enrollment_status::{write_enrollment_status, write_router_link_status},
    host_command::{operator_client::OperatorClientError, read_message_system_status},
};

#[path = "enrollment_root.rs"]
mod enrollment_root;
use enrollment_root::ValidatedEnrollmentRoot;

pub(crate) async fn enroll_from_process(
    arguments: EnrollArguments,
    root: &Path,
    output: &mut (impl io::Write + Send),
) -> Result<(), EnrollCommandError> {
    let root = ValidatedEnrollmentRoot::prepare(root)?;
    let clock: Arc<dyn RuntimeClock + Send + Sync> = Arc::new(SystemRuntimeClock::new());
    let machine_id = root.load_machine_id(clock.now().unix_millis()).await?;
    let hostname = nix::unistd::gethostname().ok();
    let machine_label = enrollment_machine_label(hostname.as_deref(), &machine_id);
    let client = create_sign_in_client(&arguments.sign_in, Arc::clone(&clock))?;
    let store = MachineEnrollmentStore::open(
        &root.database_path()?,
        root.secret_root(),
        machine_id.clone(),
        clock,
    )
    .await
    .map_err(EnrollCommandError::StoreOpen)?;
    run_enroll(
        arguments.organization_id,
        EnrollBindings {
            machine_id: &machine_id,
            machine_label,
            store: &store,
        },
        &client,
        store.close(),
        output,
    )
    .await
}

pub(crate) async fn enrollment_status_from_process(
    root: &Path,
    output: &mut (impl io::Write + Send),
) -> Result<(), EnrollCommandError> {
    let stored_root = ValidatedEnrollmentRoot::from_process(root)?;
    let database = stored_root.database_path()?;
    match read_message_system_status(root).await {
        Ok(status) => write_router_link_status(output, status),
        Err(OperatorClientError::HostNotRunning) => writeln!(output, "Router not running"),
        Err(error) => return Err(EnrollCommandError::HostStatusRead(error)),
    }
    .map_err(|_| EnrollCommandError::Output)?;
    let status = read_machine_enrollment_status(&database)
        .await
        .map_err(EnrollCommandError::StatusRead)?;
    write_enrollment_status(output, &status).map_err(|_| EnrollCommandError::Output)
}

fn enrollment_machine_label<'label>(
    hostname: Option<&'label OsStr>,
    machine_id: &'label MachineId,
) -> &'label str {
    hostname
        .and_then(OsStr::to_str)
        .map(str::trim)
        .filter(|label| {
            !label.is_empty() && label.len() <= 256 && !label.chars().any(char::is_control)
        })
        .unwrap_or_else(|| machine_id.as_str())
}

#[cfg(test)]
#[path = "enrollment_process_test.rs"]
mod tests;
