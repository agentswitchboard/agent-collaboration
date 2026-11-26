mod approvals;
mod arguments;
mod person_connection;
mod root_lock;
mod serve_configuration;
mod workspace_directory;

pub use approvals::ApprovalExit;
pub(crate) use arguments::CentralCommand;
use collaboration_central_tokio::TokioWorkspaceServer;
use std::{io::Write, sync::Arc};

#[derive(Debug, thiserror::Error)]
pub enum CentralCommandError {
    #[error("{0}")]
    Arguments(String),
    #[error("{0}")]
    InvalidInput(&'static str),
    #[error("central local operation failed")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Server(#[from] collaboration_central_tokio::TokioWorkspaceServerError),
    #[error(transparent)]
    ServeConfiguration(#[from] serve_configuration::ServeConfigurationError),
    #[error("central local task failed")]
    LocalTask,
    #[error(transparent)]
    FleetTarget(#[from] central_person_link_client::FleetTargetError),
    #[error(transparent)]
    PersonLink(#[from] central_person_link_client::PersonLinkError),
    #[error(transparent)]
    PersonLogin(#[from] central_person_link_client::FleetLoginError),
    #[error("central commands require async dispatch")]
    AsyncDispatchRequired,
    #[error("could not allocate a decide operation ID")]
    OperationId,
    #[error("could not render a Central approval reply")]
    ApprovalJson(#[from] serde_json::Error),
    #[error("{0}")]
    ApprovalOutcome(ApprovalExit),
}

pub(crate) async fn run_central_command(
    command: CentralCommand,
    interactive: bool,
    stdout: &mut (impl Write + Send),
    stderr: &mut impl Write,
) -> Result<(), CentralCommandError> {
    match command {
        CentralCommand::Help(help) => stdout.write_all(help.as_bytes())?,
        CentralCommand::Login(arguments) => {
            workspace_directory::login(&arguments.target, stdout).await?;
        }
        CentralCommand::Workspaces(arguments) => {
            workspace_directory::run(&arguments.target, interactive, stdout).await?;
        }
        CentralCommand::Approvals(command) => {
            let clock = Arc::new(router_clock::SystemRuntimeClock::new());
            let mut client =
                person_connection::build_person_client(command.target(), clock.clone()).await?;
            let status =
                approvals::run(command, &mut client, clock.as_ref(), stdout, stderr).await?;
            if status != ApprovalExit::Success {
                return Err(CentralCommandError::ApprovalOutcome(status));
            }
        }
        CentralCommand::Serve(arguments) => {
            let (config, _root_lock) = tokio::task::spawn_blocking(move || {
                let config = serve_configuration::ServeConfiguration::read(&arguments.config)?;
                let root_lock = root_lock::lock_root(&config.root)?;
                Ok::<_, CentralCommandError>((config, root_lock))
            })
            .await
            .map_err(|_| CentralCommandError::LocalTask)??;
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            let server = TokioWorkspaceServer::start(
                &config.root,
                config.listen,
                config.sign_in,
                Arc::new(router_clock::SystemRuntimeClock::new()),
                nix::unistd::geteuid().as_raw(),
            )
            .await?;
            let running = async {
                writeln!(
                    stdout,
                    "central listening: http://{} (private sign-in socket {})",
                    server.address(),
                    server.sign_in_socket().display(),
                )?;
                stdout.flush()?;
                tokio::select! {
                    result = tokio::signal::ctrl_c() => result?,
                    _ = terminate.recv() => {}
                }
                Ok::<_, CentralCommandError>(())
            }
            .await;
            let shutdown = server.shutdown().await;
            running?;
            shutdown?;
            writeln!(stdout, "central stopped")?;
        }
    }
    Ok(())
}
