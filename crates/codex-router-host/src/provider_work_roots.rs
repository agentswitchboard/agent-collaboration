//! Host discovery and installation of the work-roots hook for Claude.

use std::fs;
use std::io;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

/// The hook the Router installs for Claude sessions it starts in auto.
const WORK_ROOTS_HOOK_SOURCE: &str = include_str!("provider_work_roots/router-work-roots-hook.ts");
const WORK_ROOTS_HOOK_FILE: &str = "router-work-roots-hook.ts";

/// The Node a launch runs: the executable itself when it is `node`, or the
/// absolute Node a script's `#!` line names. `None` otherwise (a script run
/// through `env`, say), so Claude in auto is refused rather than unguarded.
pub(crate) fn launch_node(executable: &Path) -> Option<PathBuf> {
    let is_node = |path: &Path| path.file_name() == Some(std::ffi::OsStr::new("node"));
    if is_node(executable) {
        return Some(executable.to_path_buf());
    }
    let mut first_line = [0_u8; 256];
    let read = io::Read::read(&mut fs::File::open(executable).ok()?, &mut first_line).ok()?;
    let text = std::str::from_utf8(first_line.get(..read)?).ok()?;
    let interpreter = text
        .strip_prefix("#!")?
        .lines()
        .next()?
        .split_whitespace()
        .next()?;
    let interpreter = Path::new(interpreter);
    (interpreter.is_absolute() && is_node(interpreter)).then(|| interpreter.to_path_buf())
}

/// Writes the hook, owner-only, into `directory` and returns its path. The
/// file is replaced whole, so a session never reads a half-written hook.
pub(crate) fn install_work_roots_hook(directory: &Path) -> io::Result<PathBuf> {
    fs::create_dir_all(directory)?;
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    let hook = directory.join(WORK_ROOTS_HOOK_FILE);
    let staged = directory.join(format!(".{WORK_ROOTS_HOOK_FILE}.{}", uuid::Uuid::now_v7()));
    {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&staged)?;
        io::Write::write_all(&mut file, WORK_ROOTS_HOOK_SOURCE.as_bytes())?;
    }
    fs::rename(&staged, &hook)?;
    Ok(hook)
}

#[cfg(test)]
#[path = "provider_work_roots_test.rs"]
mod tests;
