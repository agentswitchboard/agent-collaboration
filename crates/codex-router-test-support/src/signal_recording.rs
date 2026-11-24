//! Signal-recording child fixtures for process lifecycle proof.

use std::path::Path;

/// Signal behavior selected for a fixture child.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignalFixtureMode {
    /// Record one SIGTERM and exit successfully.
    ExitOnTerminate,
    /// Record SIGTERM and remain alive until externally killed.
    IgnoreTerminate,
    /// Record the first SIGTERM and exit on the second.
    ExitOnSecondTerminate,
}

/// Runs a child fixture until its selected signal outcome.
pub async fn run_signal_fixture(mode: SignalFixtureMode, event_file: &Path) -> std::io::Result<()> {
    run_signal_fixture_with_ready(mode, event_file, || Ok(())).await
}

/// Announces readiness once the SIGTERM handler is installed and its event log is ready.
pub async fn run_signal_fixture_with_ready(
    mode: SignalFixtureMode,
    event_file: &Path,
    announce_ready: impl FnOnce() -> std::io::Result<()>,
) -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    std::fs::write(event_file, "ready\n")?;
    announce_ready()?;
    let _signal = terminate.recv().await;
    append_event(event_file, "sigterm\n")?;
    match mode {
        SignalFixtureMode::ExitOnTerminate => Ok(()),
        SignalFixtureMode::IgnoreTerminate => {
            tokio::task::yield_now().await;
            std::future::pending::<std::io::Result<()>>().await
        }
        SignalFixtureMode::ExitOnSecondTerminate => {
            let _signal = terminate.recv().await;
            append_event(event_file, "sigterm\n")?;
            Ok(())
        }
    }
}

fn append_event(event_file: &Path, event: &str) -> std::io::Result<()> {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new().append(true).open(event_file)?;
    file.write_all(event.as_bytes())
}
