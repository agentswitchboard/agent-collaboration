use std::{ffi::OsString, io::Write, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use collaboration_client::{
    CollaborationClient,
    protocol::message_tools::{
        MessageCallerRefusal, MessageSendArguments, MessageSendOutcome, MessageStatusArguments,
        MessageToolRefusal, MessageToolResult, MessageWriteStatus, SessionId, message_send_text,
        message_status_text,
    },
};

use crate::{
    current_session_identity::read_harness_session_identity, endpoint_commands::resolve_directory,
};

#[derive(Parser)]
#[command(name = "agent-collaboration message")]
struct MessageArguments {
    #[command(subcommand)]
    operation: MessageCommand,
    #[arg(long, global = true)]
    json: bool,
    #[arg(long, global = true)]
    service_directory: Option<PathBuf>,
}

#[derive(Subcommand)]
enum MessageCommand {
    Send(SendArguments),
    Status(StatusArguments),
}

#[derive(Args)]
struct SendArguments {
    #[arg(long)]
    to: String,
    #[arg(long)]
    body: String,
    #[arg(long)]
    request_id: Option<String>,
}

#[derive(Args)]
struct StatusArguments {
    #[arg(long)]
    request_id: String,
}

enum PreparedMessageCommand {
    Send(MessageSendArguments),
    Status(MessageStatusArguments),
}

impl MessageCommand {
    fn prepare(self) -> Result<PreparedMessageCommand, MessageToolRefusal> {
        let caller = || -> Result<SessionId, MessageToolRefusal> {
            read_harness_session_identity()
                .map_err(|error| match error {
                    crate::current_session_identity::CurrentSessionIdentityError::Missing => {
                        MessageToolRefusal::Caller(MessageCallerRefusal::CallerRequired)
                    }
                    crate::current_session_identity::CurrentSessionIdentityError::Ambiguous {
                        ..
                    }
                    | crate::current_session_identity::CurrentSessionIdentityError::NotUnicode {
                        ..
                    } => MessageToolRefusal::InvalidCaller,
                })?
                .session_id
                .parse()
                .map_err(|_| MessageToolRefusal::InvalidCaller)
        };
        match self {
            Self::Send(args) => Ok(PreparedMessageCommand::Send(MessageSendArguments {
                to: serde_json::from_str(&args.to)
                    .map_err(|_| MessageToolRefusal::InvalidAddress)?,
                body: args
                    .body
                    .try_into()
                    .map_err(|_| MessageToolRefusal::InvalidBody)?,
                request_id: args
                    .request_id
                    .as_deref()
                    .map(str::parse)
                    .transpose()
                    .map_err(|_| MessageToolRefusal::InvalidRequestId)?,
                caller_session_id: Some(caller()?),
            })),
            Self::Status(args) => Ok(PreparedMessageCommand::Status(MessageStatusArguments {
                request_id: args
                    .request_id
                    .parse()
                    .map_err(|_| MessageToolRefusal::InvalidRequestId)?,
                caller_session_id: Some(caller()?),
            })),
        }
    }
}

/// Whether the CLI arguments name one of the two active message commands.
pub fn is_message_command(arguments: &[OsString]) -> bool {
    arguments.first().is_some_and(|group| group == "message")
        && arguments
            .get(1)
            .is_some_and(|verb| verb == "send" || verb == "status")
}

/// Runs the send or status command through the collaboration API socket.
pub fn run_message_command(arguments: Vec<OsString>) -> i32 {
    let args =
        match crate::automation_argument_feedback::parse_arguments::<MessageArguments>(arguments) {
            Ok(args) => args,
            Err(exit) => return exit,
        };
    let command = match args.operation.prepare() {
        Ok(command) => command,
        Err(refusal) => return report_send(MessageToolResult::Refused(refusal), args.json, 2),
    };
    let Ok(directory) = resolve_directory(args.service_directory) else {
        return report_send(
            MessageToolResult::Refused(MessageToolRefusal::InvalidArguments),
            args.json,
            2,
        );
    };
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return report_transport(args.json);
    };
    runtime.block_on(async {
        let Ok(client) = CollaborationClient::connect(
            &directory,
            "agent-collaboration",
            env!("CARGO_PKG_VERSION"),
        )
        .await
        else {
            return report_transport(args.json);
        };
        match command {
            PreparedMessageCommand::Send(input) => match client.message_send(input).await {
                Ok(result) => report_send(result, args.json, 3),
                Err(_) => report_transport(args.json),
            },
            PreparedMessageCommand::Status(input) => match client.message_status(input).await {
                Ok(result) => report_status(result, args.json),
                Err(_) => report_transport(args.json),
            },
        }
    })
}

fn report_transport(machine_output: bool) -> i32 {
    report_send(
        MessageToolResult::Refused(MessageToolRefusal::TransportUnavailable),
        machine_output,
        3,
    )
}

fn report_send(value: MessageToolResult<MessageSendOutcome>, json: bool, failure_exit: i32) -> i32 {
    report(&value, json, failure_exit, || message_send_text(&value))
}

fn report_status(value: MessageToolResult<MessageWriteStatus>, json: bool) -> i32 {
    report(&value, json, 3, || message_status_text(&value))
}

fn report<Value: serde::Serialize>(
    value: &MessageToolResult<Value>,
    json: bool,
    failure_exit: i32,
    text: impl FnOnce() -> String,
) -> i32 {
    let mut stdout = std::io::stdout().lock();
    let printed = if json {
        serde_json::to_writer(&mut stdout, value).is_ok() && writeln!(stdout).is_ok()
    } else {
        writeln!(stdout, "{}", text()).is_ok()
    };
    if !printed {
        return 5;
    }
    match value {
        MessageToolResult::Returned(_) => 0,
        MessageToolResult::Refused(_) => failure_exit,
    }
}
