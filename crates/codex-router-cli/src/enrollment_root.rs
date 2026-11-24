//! Protects local enrollment database opens from accidental production aliases.

use std::{
    fs, io,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
};

use switchboard_machine_core_store::{MACHINE_LOCAL_DATABASE, load_or_create_machine_id};
use switchboard_machine_enrollment::MachineId;

use crate::enrollment_command::EnrollCommandError;

pub(super) struct ValidatedEnrollmentRoot<'root> {
    directory: &'root Path,
    #[cfg(debug_assertions)]
    protected_roots: Vec<PathBuf>,
}

impl<'root> ValidatedEnrollmentRoot<'root> {
    pub(super) fn from_process(directory: &'root Path) -> Result<Self, EnrollCommandError> {
        let root = Self {
            directory,
            #[cfg(debug_assertions)]
            protected_roots: protected_roots()?,
        };
        root.validate_directory()?;
        Ok(root)
    }

    pub(super) fn prepare(directory: &'root Path) -> Result<Self, EnrollCommandError> {
        let root = Self::from_process(directory)?;
        if !root.validate_directory()? {
            match fs::DirBuilder::new().mode(0o700).create(directory) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(EnrollCommandError::RootFilesystem(error)),
            }
            root.validate_directory()?;
        }
        Ok(root)
    }

    pub(super) async fn load_machine_id(
        &self,
        now_ms: i64,
    ) -> Result<MachineId, EnrollCommandError> {
        self.database_path()?;
        load_or_create_machine_id(self.directory, now_ms)
            .await
            .map_err(EnrollCommandError::MachineIdentity)
    }

    pub(super) fn database_path(&self) -> Result<PathBuf, EnrollCommandError> {
        self.validate_directory()?;
        let database = self.directory.join(MACHINE_LOCAL_DATABASE);
        // This guard catches accidental aliases in the account's private 0700
        // root. It cannot pin a pathname against replacement between this
        // check and SQLite opening it by an actor who can write in the root.
        for suffix in ["", "-wal", "-shm"] {
            let leaf = self
                .directory
                .join(format!("{MACHINE_LOCAL_DATABASE}{suffix}"));
            match fs::symlink_metadata(&leaf) {
                Ok(metadata) if metadata.is_file() => {
                    #[cfg(debug_assertions)]
                    self.check_protected_roots(&leaf)?;
                }
                Ok(_) => return Err(EnrollCommandError::InvalidDatabasePath),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(EnrollCommandError::RootFilesystem(error)),
            }
        }
        Ok(database)
    }

    pub(super) fn secret_root(&self) -> PathBuf {
        self.directory.join("secrets")
    }

    fn validate_directory(&self) -> Result<bool, EnrollCommandError> {
        #[cfg(debug_assertions)]
        self.check_protected_roots(self.directory)?;
        if !self.directory.is_absolute() {
            return Err(EnrollCommandError::InvalidRoot);
        }
        match fs::symlink_metadata(self.directory) {
            Ok(metadata) if metadata.is_dir() && metadata.permissions().mode() & 0o077 == 0 => {
                Ok(true)
            }
            Ok(_) => Err(EnrollCommandError::InvalidRoot),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(EnrollCommandError::RootFilesystem(error)),
        }
    }

    #[cfg(debug_assertions)]
    fn check_protected_roots(&self, candidate: &Path) -> Result<(), EnrollCommandError> {
        for protected in &self.protected_roots {
            codex_native_integration::validate_debug_directory(candidate, protected)
                .map_err(EnrollCommandError::DebugRoot)?;
        }
        Ok(())
    }
}

#[cfg(debug_assertions)]
fn protected_roots() -> Result<Vec<PathBuf>, EnrollCommandError> {
    const UNKNOWN_HOME: EnrollCommandError =
        EnrollCommandError::DebugRoot("Could not identify the account's home directory.");
    let user = nix::unistd::User::from_uid(nix::unistd::getuid())
        .map_err(|_| UNKNOWN_HOME)?
        .ok_or(UNKNOWN_HOME)?;
    let mut roots = vec![user.dir.join(crate::DEFAULT_ROUTER_ROOT_DIR)];
    if let Some(home) = std::env::var_os("HOME").filter(|home| !home.is_empty()) {
        roots.push(Path::new(&home).join(crate::DEFAULT_ROUTER_ROOT_DIR));
    }
    Ok(roots)
}
