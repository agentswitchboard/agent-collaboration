use std::{io::Write, os::unix::ffi::OsStrExt, path::Path};

use central_person_link_client::{
    PersonLinkClient,
    approvals::{
        ApprovalDecideReply, ApprovalDecisionOutcome, ApprovalInvalidReason, ApprovalListReply,
        ApprovalReadReply, ApprovalSummaryEnding, ApprovalSummaryState, DecidedBy, InvalidAnswer,
        RequestId, RequestKind, RequestSummary, RequestView, SessionCallOperation,
    },
};
use router_clock::RuntimeClock;
use serde::Serialize;
use uuid::{NoContext, Timestamp, Uuid};

use super::{
    CentralCommandError,
    arguments::{ApprovalChoice, ApprovalCommand, ApprovalDecisionArguments},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApprovalExit {
    #[error("Central approval command completed")]
    Success,
    #[error("Central approval was already decided")]
    AlreadyDecided,
    #[error("Central approval ended before this answer")]
    TooLate,
    #[error("Central rejected the approval answer")]
    Invalid,
    #[error("Central approval is not visible")]
    NotVisible,
    #[error("Central approval operation conflicts with a saved operation")]
    OperationConflict,
    #[error("Central approval outcome is unknown")]
    Unknown,
}

impl ApprovalExit {
    pub const fn code(self) -> i32 {
        match self {
            Self::Success => 0,
            Self::AlreadyDecided => 3,
            Self::TooLate => 4,
            Self::Invalid => 5,
            Self::NotVisible => 6,
            Self::OperationConflict => 7,
            Self::Unknown => 8,
        }
    }
}

pub(super) async fn run(
    command: ApprovalCommand,
    client: &mut PersonLinkClient,
    clock: &dyn RuntimeClock,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> Result<ApprovalExit, CentralCommandError> {
    let status = match command {
        ApprovalCommand::List { output, filter } => {
            let reply = client.list_approvals(filter).await?;
            if output.json {
                write_json(stdout, &reply)?;
            } else {
                let ApprovalListReply::Page { items, .. } = reply;
                if items.is_empty() {
                    writeln!(stdout, "No approval requests.")?;
                }
                for item in &items {
                    write_summary(stdout, item)?;
                }
            }
            ApprovalExit::Success
        }
        ApprovalCommand::Read { output, request_id } => {
            let reply = client.read_approval(request_id).await?;
            if output.json {
                write_json(stdout, &reply)?;
            } else {
                match &reply {
                    ApprovalReadReply::Request { view } => write_request(stdout, view)?,
                    ApprovalReadReply::NotVisible {} => {
                        writeln!(stdout, "Request is not visible.")?
                    }
                }
            }
            ApprovalExit::Success
        }
        ApprovalCommand::Decide(arguments) => {
            decide(arguments, client, clock, stdout, stderr).await?
        }
    };
    stdout.flush()?;
    Ok(status)
}

async fn decide(
    arguments: ApprovalDecisionArguments,
    client: &mut PersonLinkClient,
    clock: &dyn RuntimeClock,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> Result<ApprovalExit, CentralCommandError> {
    let operation_id = match arguments.operation_id.clone() {
        Some(id) => id,
        None => mint_operation_id(clock).map_err(CentralCommandError::before_decide)?,
    };
    writeln!(stderr, "Operation ID {operation_id}")
        .and_then(|()| stderr.flush())
        .map_err(|error| CentralCommandError::before_decide(error.into()))?;

    let reply = client
        .decide_approval(
            arguments.request_id.clone(),
            operation_id,
            arguments.decision.answer(),
        )
        .await?;
    match reply {
        ApprovalDecisionOutcome::Reply(reply) => {
            if arguments.output.json {
                write_json(stdout, &reply)?;
            } else {
                writeln!(stdout, "{}", decision_text(&reply))?;
            }
            Ok(decision_status(&reply))
        }
        ApprovalDecisionOutcome::Unknown {
            operation_id,
            cause,
        } => {
            let retry_command = retry_command(&arguments, &operation_id);
            if arguments.output.json {
                write_json(
                    stdout,
                    &UnknownDecision::Unknown {
                        operation_id: &operation_id,
                        cause: &cause.to_string(),
                        retry_command: &retry_command,
                    },
                )?;
            } else {
                writeln!(
                    stdout,
                    "Outcome unknown for operation {operation_id}. Cause: {cause}."
                )?;
                writeln!(stdout, "Retry with the same operation ID and answer:")?;
                writeln!(stdout, "{retry_command}")?;
            }
            Ok(ApprovalExit::Unknown)
        }
    }
}

fn mint_operation_id(clock: &dyn RuntimeClock) -> Result<RequestId, CentralCommandError> {
    let milliseconds =
        u64::try_from(clock.now().unix_millis()).map_err(|_| CentralCommandError::OperationId)?;
    let nanos = u32::try_from(milliseconds % 1_000)
        .map_err(|_| CentralCommandError::OperationId)?
        * 1_000_000;
    Uuid::new_v7(Timestamp::from_unix(NoContext, milliseconds / 1_000, nanos))
        .to_string()
        .parse()
        .map_err(|_| CentralCommandError::OperationId)
}

#[derive(Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum UnknownDecision<'a> {
    Unknown {
        operation_id: &'a RequestId,
        cause: &'a str,
        retry_command: &'a str,
    },
}

fn retry_command(arguments: &ApprovalDecisionArguments, operation_id: &RequestId) -> String {
    let target = quote_target(&arguments.output.fleet.target);
    let choice = match &arguments.decision {
        ApprovalChoice::Option(option_id) => format!("--option {}", option_id.get()),
        ApprovalChoice::Allow => "--allow".to_owned(),
        ApprovalChoice::Decline => "--decline".to_owned(),
    };
    let json = if arguments.output.json { " --json" } else { "" };
    format!(
        "codex-router central approvals decide --target={target} {} {choice} --operation {operation_id}{json}",
        arguments.request_id
    )
}

fn quote_target(target: &Path) -> String {
    let name = target.as_os_str();
    if let Some(name) = name
        .to_str()
        .filter(|name| !name.chars().any(char::is_control))
    {
        return crate::token::quote_posix(name);
    }
    let escaped: String = name
        .as_bytes()
        .iter()
        .map(|byte| format!("\\{byte:03o}"))
        .collect();
    format!("$'{escaped}'")
}

fn decision_status(reply: &ApprovalDecideReply) -> ApprovalExit {
    match reply {
        ApprovalDecideReply::SavedDecision { .. } => ApprovalExit::Success,
        ApprovalDecideReply::AlreadyDecided { .. } => ApprovalExit::AlreadyDecided,
        ApprovalDecideReply::TooLate {} => ApprovalExit::TooLate,
        ApprovalDecideReply::Invalid { .. } => ApprovalExit::Invalid,
        ApprovalDecideReply::NotVisible {} => ApprovalExit::NotVisible,
        ApprovalDecideReply::OperationConflict {} => ApprovalExit::OperationConflict,
    }
}

fn decision_text(reply: &ApprovalDecideReply) -> String {
    match reply {
        ApprovalDecideReply::SavedDecision { .. } => "Decision saved.".to_owned(),
        ApprovalDecideReply::AlreadyDecided { by, at } => {
            let actor = match by {
                DecidedBy::Person { person_id } => format!("person {person_id}"),
                DecidedBy::Session { session } => format!("session {session}"),
            };
            format!("Already decided by {actor} at {}.", at.to_rfc3339())
        }
        ApprovalDecideReply::TooLate {} => "Too late. The request has ended.".to_owned(),
        ApprovalDecideReply::Invalid { reason } => {
            format!("Invalid answer. {}", invalid_reason(reason))
        }
        ApprovalDecideReply::NotVisible {} => "Request is not visible.".to_owned(),
        ApprovalDecideReply::OperationConflict {} => {
            "Operation ID is already bound to a different request or answer.".to_owned()
        }
    }
}

fn invalid_reason(reason: &ApprovalInvalidReason) -> String {
    match reason {
        ApprovalInvalidReason::InvalidAnswer {
            reason: InvalidAnswer::WrongKind {},
        } => "The answer does not match the request kind.".to_owned(),
        ApprovalInvalidReason::InvalidAnswer {
            reason: InvalidAnswer::UnofferedOption { option_id },
        } => format!("Option {} was not offered.", option_id.get()),
        ApprovalInvalidReason::InvalidAnswer { reason } => format!("{reason:?}"),
        ApprovalInvalidReason::InvalidApprovalEnding { reason } => reason.to_string(),
    }
}

fn write_json(stdout: &mut impl Write, reply: &impl Serialize) -> Result<(), CentralCommandError> {
    serde_json::to_writer(&mut *stdout, reply)?;
    writeln!(stdout)?;
    Ok(())
}

fn write_summary(stdout: &mut impl Write, summary: &RequestSummary) -> std::io::Result<()> {
    let kind = match summary.kind {
        RequestKind::ProviderApproval => "provider approval",
        RequestKind::ProviderQuestion => "provider question",
        RequestKind::UnprovenCall => "unproven call",
    };
    let state = match summary.state {
        ApprovalSummaryState::Waiting {} => "waiting",
        ApprovalSummaryState::Ended {
            ending: ApprovalSummaryEnding::Decided,
        } => "decided",
        ApprovalSummaryState::Ended {
            ending: ApprovalSummaryEnding::Cancelled,
        } => "cancelled",
        ApprovalSummaryState::Ended {
            ending: ApprovalSummaryEnding::TimedOut,
        } => "timed out",
    };
    writeln!(stdout, "{}  {kind}  {state}", summary.request_id)
}

fn write_request(stdout: &mut impl Write, view: &RequestView) -> std::io::Result<()> {
    write_summary(stdout, &RequestSummary::from(view))?;
    writeln!(stdout, "Deadline {}", view.window().deadline().to_rfc3339())?;
    if let Some(offer) = view.provider_approval_offer() {
        writeln!(stdout, "{}", offer.title.as_str().escape_debug())?;
        if let Some(description) = &offer.description {
            writeln!(stdout, "{}", description.as_str().escape_debug())?;
        }
        for option in offer.options.as_slice() {
            writeln!(
                stdout,
                "Option {}  {}",
                option.id.get(),
                option.label.as_str().escape_debug()
            )?;
        }
    }
    if let Some(offer) = view.provider_question_offer() {
        writeln!(stdout, "{}", offer.prompt.as_str().escape_debug())?;
        for field in offer.fields.as_slice() {
            writeln!(stdout, "Field {}", field.field_id().get())?;
        }
    }
    if let Some(offer) = view.unproven_call_offer() {
        match &offer.operation {
            SessionCallOperation::StartSession {
                agent_program,
                working_folder,
                mode,
                work_folders,
            } => {
                writeln!(
                    stdout,
                    "Start {agent_program:?} in {} with mode {}",
                    working_folder.as_str().escape_debug(),
                    mode.as_str().escape_debug()
                )?;
                for folder in work_folders.as_slice() {
                    writeln!(stdout, "Work folder {}", folder.as_str().escape_debug())?;
                }
            }
            SessionCallOperation::EndSessionForGood { session_id } => {
                writeln!(stdout, "End session {session_id} for good")?
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "approvals_test.rs"]
mod tests;
