use approval_request_protocol::{
    Answer, ApprovalListFilter, ApprovalRequestId, OfferedOptionId, SessionCallDecision,
};
use clap::{Args, Parser, Subcommand};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};
use workspace_ids::RequestId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CentralCommand {
    Serve(ServeArguments),
    Login(FleetTargetArguments),
    Workspaces(FleetTargetArguments),
    Approvals(ApprovalCommand),
    Help(String),
}
impl CentralCommand {
    pub fn parse(arguments: Vec<OsString>) -> Result<Self, super::CentralCommandError> {
        match CentralParser::try_parse_from(
            std::iter::once(OsString::from("central")).chain(arguments),
        ) {
            Ok(parser) => Ok(match parser.action {
                CentralAction::Serve(args) => Self::Serve(args),
                CentralAction::Login(args) => Self::Login(args),
                CentralAction::Workspaces(args) => Self::Workspaces(args),
                CentralAction::Approvals { action } => Self::Approvals(action.into_command()?),
            }),
            Err(error)
                if matches!(
                    error.kind(),
                    clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
                ) =>
            {
                Ok(Self::Help(error.to_string()))
            }
            Err(error) => Err(super::CentralCommandError::Arguments(error.to_string())),
        }
    }
}
#[derive(Parser)]
#[command(name = "central", about = "Serve Central or connect as a person")]
struct CentralParser {
    #[command(subcommand)]
    action: CentralAction,
}
#[derive(Subcommand)]
enum CentralAction {
    /// Launch the machine link listener and the private sign-in listener.
    Serve(ServeArguments),
    /// Sign in to Central as a person.
    Login(FleetTargetArguments),
    /// List the organization’s workspaces through Sign-in.
    Workspaces(FleetTargetArguments),
    /// List, read or answer approval requests as a signed-in person.
    Approvals {
        #[command(subcommand)]
        action: ApprovalAction,
    },
}
#[derive(Args, Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServeArguments {
    /// JSONC with listen, root, and signIn settings.
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Args, Clone, Debug, Eq, PartialEq)]
pub(crate) struct FleetTargetArguments {
    /// Existing JSON target used by the Node owner CLI's Fleet commands.
    #[arg(long, value_name = "FILE")]
    pub target: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ApprovalCommand {
    List {
        output: ApprovalOutput,
        filter: ApprovalListFilter,
    },
    Read {
        output: ApprovalOutput,
        request_id: ApprovalRequestId,
    },
    Decide(ApprovalDecisionArguments),
}

impl ApprovalCommand {
    pub(super) fn target(&self) -> &Path {
        let output = match self {
            Self::List { output, .. } | Self::Read { output, .. } => output,
            Self::Decide(arguments) => &arguments.output,
        };
        &output.fleet.target
    }
}

#[derive(Args, Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApprovalOutput {
    #[command(flatten)]
    pub fleet: FleetTargetArguments,
    /// Print one JSON result document.
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApprovalDecisionArguments {
    pub output: ApprovalOutput,
    pub request_id: ApprovalRequestId,
    pub operation_id: Option<RequestId>,
    pub decision: ApprovalChoice,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ApprovalChoice {
    Option(OfferedOptionId),
    Allow,
    Decline,
}

impl ApprovalChoice {
    pub(super) fn answer(&self) -> Answer {
        match self {
            Self::Option(option_id) => Answer::Approval {
                option_id: option_id.clone(),
            },
            Self::Allow => Answer::UnprovenCall {
                decision: SessionCallDecision::Allow,
            },
            Self::Decline => Answer::UnprovenCall {
                decision: SessionCallDecision::Decline,
            },
        }
    }
}

#[derive(Subcommand)]
enum ApprovalAction {
    /// List requests waiting for an answer.
    List {
        #[command(flatten)]
        output: ApprovalOutput,
        /// Include ended requests still visible in Central.
        #[arg(long)]
        all: bool,
    },
    /// Read the offer and status of one request.
    Read {
        #[command(flatten)]
        output: ApprovalOutput,
        request_id: ApprovalRequestId,
    },
    /// Answer a provider approval or an unproven call.
    Decide {
        #[command(flatten)]
        output: ApprovalOutput,
        request_id: ApprovalRequestId,
        #[command(flatten)]
        choice: ApprovalChoiceFlags,
        /// Reuse this operation ID to recover a decide whose outcome is unknown.
        #[arg(long = "operation", value_name = "OPERATION_ID")]
        operation_id: Option<RequestId>,
    },
}

impl ApprovalAction {
    fn into_command(self) -> Result<ApprovalCommand, super::CentralCommandError> {
        Ok(match self {
            Self::List { output, all } => ApprovalCommand::List {
                output,
                filter: if all {
                    ApprovalListFilter::All
                } else {
                    ApprovalListFilter::Waiting
                },
            },
            Self::Read { output, request_id } => ApprovalCommand::Read { output, request_id },
            Self::Decide {
                output,
                request_id,
                choice,
                operation_id,
            } => ApprovalCommand::Decide(ApprovalDecisionArguments {
                output,
                request_id,
                operation_id,
                decision: choice.into_choice()?,
            }),
        })
    }
}

#[derive(Args)]
#[group(required = true, multiple = false)]
struct ApprovalChoiceFlags {
    /// Select one of the provider approval's offered option IDs.
    #[arg(long, value_parser = parse_option, value_name = "OPTION_ID")]
    option: Option<OfferedOptionId>,
    /// Allow an unproven call.
    #[arg(long)]
    allow: bool,
    /// Decline an unproven call.
    #[arg(long)]
    decline: bool,
}

impl ApprovalChoiceFlags {
    fn into_choice(self) -> Result<ApprovalChoice, super::CentralCommandError> {
        match (self.option, self.allow, self.decline) {
            (Some(option), false, false) => Ok(ApprovalChoice::Option(option)),
            (None, true, false) => Ok(ApprovalChoice::Allow),
            (None, false, true) => Ok(ApprovalChoice::Decline),
            _ => Err(super::CentralCommandError::InvalidInput(
                "choose exactly one of --option, --allow or --decline",
            )),
        }
    }
}

fn parse_option(value: &str) -> Result<OfferedOptionId, String> {
    let ordinal: u8 = value
        .parse()
        .map_err(|_| "use a numeric approval option ID".to_owned())?;
    OfferedOptionId::new(ordinal).map_err(|error| error.to_string())
}

#[cfg(test)]
#[path = "arguments_test.rs"]
mod tests;
