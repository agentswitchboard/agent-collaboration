use std::{io::Write, path::Path};

use super::CentralCommandError;
#[cfg(not(target_os = "macos"))]
use central_person_link_client::FleetCredentialError;

#[cfg(target_os = "macos")]
use super::person_connection::build_person_client;

#[cfg(target_os = "macos")]
pub(super) async fn login(
    target: &Path,
    stdout: &mut (impl Write + Send),
) -> Result<(), CentralCommandError> {
    build_person_client(
        target,
        std::sync::Arc::new(router_clock::SystemRuntimeClock::new()),
    )
    .await?
    .login(|address| {
        writeln!(stdout, "Open this address in your browser to sign in:")?;
        writeln!(stdout, "{address}")?;
        stdout.flush()
    })
    .await?;
    writeln!(stdout, "Central login saved.")?;
    stdout.flush()?;
    Ok(())
}

#[cfg(target_os = "macos")]
pub(super) async fn run(
    target: &Path,
    interactive: bool,
    stdout: &mut (impl Write + Send),
) -> Result<(), CentralCommandError> {
    let mut client = build_person_client(
        target,
        std::sync::Arc::new(router_clock::SystemRuntimeClock::new()),
    )
    .await?;
    loop {
        for workspace in client.list_workspaces().await? {
            writeln!(stdout, "{}", workspace.name.as_str())?;
        }
        stdout.flush()?;
        if !interactive {
            return Ok(());
        }

        writeln!(stdout, "Press Return to refresh or Ctrl-D to exit.")?;
        stdout.flush()?;
        let bytes_read =
            tokio::task::spawn_blocking(|| std::io::stdin().read_line(&mut String::new()))
                .await
                .map_err(|_| CentralCommandError::LocalTask)??;
        if bytes_read == 0 {
            return Ok(());
        }
    }
}

#[cfg(not(target_os = "macos"))]
pub(super) async fn login(
    _target: &Path,
    _stdout: &mut (impl Write + Send),
) -> Result<(), CentralCommandError> {
    std::future::ready(Err(central_person_link_client::FleetLoginError::from(
        FleetCredentialError::UnsupportedPlatform,
    )
    .into()))
    .await
}

#[cfg(not(target_os = "macos"))]
pub(super) async fn run(
    _target: &Path,
    _interactive: bool,
    _stdout: &mut (impl Write + Send),
) -> Result<(), CentralCommandError> {
    std::future::ready(Err(central_person_link_client::PersonLinkError::from(
        FleetCredentialError::UnsupportedPlatform,
    )
    .into()))
    .await
}
