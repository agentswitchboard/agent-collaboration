//! Opens the machine identity and enrollment store for the Router CLI.

use std::{
    ffi::OsStr,
    fs, io,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::Path,
    sync::Arc,
};

use router_clock::{RuntimeClock, SystemRuntimeClock};
use switchboard_machine_core_store::{MACHINE_LOCAL_DATABASE, load_or_create_machine_id};
use switchboard_machine_enrollment::MachineId;
use switchboard_machine_enrollment_store::{
    MachineEnrollmentStatus, MachineEnrollmentStore, read_machine_enrollment_status,
};

use crate::{
    enrollment_command::{
        EnrollArguments, EnrollBindings, EnrollCommandError, create_sign_in_client, run_enroll,
    },
    enrollment_status::write_enrollment_status,
};

pub(crate) async fn enroll_from_process(
    arguments: EnrollArguments,
    root: &Path,
    output: &mut (impl io::Write + Send),
) -> Result<(), EnrollCommandError> {
    if !validate_enrollment_root(root)? {
        match fs::DirBuilder::new().mode(0o700).create(root) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(EnrollCommandError::RootFilesystem(error)),
        }
    }
    let clock: Arc<dyn RuntimeClock + Send + Sync> = Arc::new(SystemRuntimeClock::new());
    let machine_id = load_or_create_machine_id(root, clock.now().unix_millis())
        .await
        .map_err(EnrollCommandError::MachineIdentity)?;
    let hostname = nix::unistd::gethostname().ok();
    let machine_label = enrollment_machine_label(hostname.as_deref(), &machine_id);
    let client = create_sign_in_client(&arguments.sign_in, Arc::clone(&clock))?;
    let store = MachineEnrollmentStore::open(
        &root.join(MACHINE_LOCAL_DATABASE),
        root.join("secrets"),
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
    validate_enrollment_root(root)?;
    let status = read_machine_enrollment_status(&root.join(MACHINE_LOCAL_DATABASE))
        .await
        .map_err(EnrollCommandError::StatusRead)?;
    let metadata = match status {
        MachineEnrollmentStatus::DatabaseAbsent | MachineEnrollmentStatus::MachineAbsent => {
            switchboard_machine_enrollment::EnrollmentMetadata::Empty
        }
        MachineEnrollmentStatus::Present(metadata) => metadata,
    };
    write_enrollment_status(output, &metadata).map_err(|_| EnrollCommandError::Output)
}

fn validate_enrollment_root(root: &Path) -> Result<bool, EnrollCommandError> {
    #[cfg(debug_assertions)]
    validate_debug_enrollment_root(root)?;
    if !root.is_absolute() {
        return Err(EnrollCommandError::InvalidRoot);
    }
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() && metadata.permissions().mode() & 0o077 == 0 => Ok(true),
        Ok(_) => Err(EnrollCommandError::InvalidRoot),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(EnrollCommandError::RootFilesystem(error)),
    }
}

#[cfg(debug_assertions)]
fn validate_debug_enrollment_root(root: &Path) -> Result<(), EnrollCommandError> {
    let user = nix::unistd::User::from_uid(nix::unistd::getuid())
        .map_err(|_| {
            EnrollCommandError::DebugRoot("Could not identify the account's home directory.")
        })?
        .ok_or(EnrollCommandError::DebugRoot(
            "Could not identify the account's home directory.",
        ))?;
    if let Some(home) = std::env::var_os("HOME").filter(|home| !home.is_empty()) {
        codex_native_integration::validate_debug_directory(
            root,
            &Path::new(&home).join(crate::DEFAULT_ROUTER_ROOT_DIR),
        )
        .map_err(EnrollCommandError::DebugRoot)?;
    }
    codex_native_integration::validate_debug_directory(
        root,
        &user.dir.join(crate::DEFAULT_ROUTER_ROOT_DIR),
    )
    .map_err(EnrollCommandError::DebugRoot)
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
