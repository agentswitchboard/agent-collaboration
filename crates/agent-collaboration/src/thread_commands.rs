//! `agent-collaboration thread show`: the command-line door to the `thread_show` tool.
//!
//! It sends the same arguments over the collaboration API socket and prints the same typed
//! output, its text or (with `--json`) the whole output, exiting non-zero for every answer the
//! MCP door reports as a tool error.
use std::{ffi::OsString, io::Write, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use collaboration_client::{
    CollaborationClient,
    protocol::{
        link_tool_result::LinkToolOutput,
        workspace_tools::{SessionId, ThreadId, ThreadShowArguments, WorkspaceId},
    },
};

use crate::{
    current_session_identity::read_harness_session_identity,
    endpoint_commands::{report_failure, resolve_directory},
};

const REFUSED_EXIT: i32 = 3;

#[derive(Parser)]
#[command(
    name = "agent-collaboration thread",
    bin_name = "agent-collaboration thread",
    arg_required_else_help = true
)]
struct ThreadArguments {
    #[command(subcommand)]
    operation: ThreadCommand,
    #[arg(long, global = true)]
    json: bool,
    #[arg(long, global = true)]
    service_directory: Option<PathBuf>,
}

#[derive(Subcommand)]
enum ThreadCommand {
    /// Read one thread from Central.
    Show(ShowArguments),
}

#[derive(Args)]
struct ShowArguments {
    #[arg(long)]
    workspace_id: WorkspaceId,
    thread_id: ThreadId,
}

/// Whether the CLI arguments name a thread command this build carries: `thread show`, or the
/// group's own help (`thread`, `thread --help`), which lists `thread show`. Every other
/// `thread` subcommand stays retired.
pub fn is_thread_command(arguments: &[OsString]) -> bool {
    arguments.first().is_some_and(|group| group == "thread")
        && arguments
            .get(1)
            .is_none_or(|verb| verb == "show" || verb == "--help" || verb == "-h")
}

/// Runs `thread show` through the collaboration API socket and returns the exit code.
pub fn run_thread_command(arguments: Vec<OsString>) -> i32 {
    let args =
        match crate::automation_argument_feedback::parse_arguments::<ThreadArguments>(arguments) {
            Ok(args) => args,
            Err(exit) => return exit,
        };
    let ThreadCommand::Show(show) = args.operation;
    let caller = match read_harness_session_identity()
        .map_err(|error| error.to_string())
        .and_then(|identity| {
            identity
                .session_id
                .parse::<SessionId>()
                .map_err(|error| error.to_string())
        }) {
        Ok(caller) => caller,
        Err(message) => return report_failure("identityUnavailable", &message, 2, args.json),
    };
    let directory = match resolve_directory(args.service_directory) {
        Ok(directory) => directory,
        Err(message) => return report_failure("invalidUsage", &message, 2, args.json),
    };
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return report_failure("unavailable", "Client runtime unavailable", 3, args.json);
    };
    let input = ThreadShowArguments {
        caller_session_id: caller,
        workspace_id: show.workspace_id,
        thread_id: show.thread_id,
    };
    let answer = runtime.block_on(async {
        let client = CollaborationClient::connect(
            &directory,
            "agent-collaboration",
            env!("CARGO_PKG_VERSION"),
        )
        .await?;
        client.thread_show(input).await
    });
    match answer {
        Ok(output) => print_output(&output, args.json),
        Err(error) => report_failure("unavailable", &error.to_string(), 3, args.json),
    }
}

fn print_output(output: &LinkToolOutput, json: bool) -> i32 {
    let mut stdout = std::io::stdout().lock();
    let printed = if json {
        serde_json::to_writer(&mut stdout, output).is_ok() && writeln!(stdout).is_ok()
    } else {
        writeln!(stdout, "{}", output.text).is_ok()
    };
    if !printed {
        return 5;
    }
    if output.result.succeeded() {
        0
    } else {
        REFUSED_EXIT
    }
}
