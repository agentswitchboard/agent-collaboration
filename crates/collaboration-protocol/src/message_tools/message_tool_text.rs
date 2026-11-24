use super::{
    CentralMailRefusal, MessageCallerRefusal, MessageDeliveryObservation, MessageFrontRefusal,
    MessageSendOutcome, MessageStatus, MessageSubmitRefusal, MessageToolRefusal, MessageToolResult,
    MessageWriteRefusal, MessageWriteStatus, RefusalReason,
};
use workspace_model::central_mail::TimingRequirement;

pub const ADDRESS_FORMAT: &str = "Use machineId and sessionId.";
pub const MESSAGE_CLI_HELP: &str = "  message send --to PAIR_JSON --body TEXT [--request-id UUID] [--json]\n  message status --request-id UUID [--json]\n";
pub const MESSAGE_SEND_DESCRIPTION: &str = "Sends one direct message to an exact machineId and sessionId. Report your own harness session ID as callerSessionId. An outbox receipt confirms this Mac kept the write for a scheduled send.";
pub const MESSAGE_STATUS_DESCRIPTION: &str = "Checks your original write by requestId. Report your own harness session ID as callerSessionId. A write accepted by central also shows the current delivery observation.";

pub fn message_send_text(result: &MessageToolResult<MessageSendOutcome>) -> String {
    match result {
        MessageToolResult::Returned(MessageSendOutcome::InOutbox {
            request_id,
            flush_at,
        }) => {
            format!(
                "In outbox. Leaves at {}.\nRequest id {request_id}.",
                flush_at.to_rfc3339()
            )
        }
        MessageToolResult::Returned(MessageSendOutcome::Answered {
            request_id,
            message_id,
        }) => {
            format!("Accepted by central. Message id {message_id}.\nRequest id {request_id}.")
        }
        MessageToolResult::Returned(MessageSendOutcome::Uncertain { request_id }) => {
            format!("Write outcome uncertain. Check request id {request_id} before sending again.")
        }
        MessageToolResult::Refused(refusal) => refusal.to_string(),
    }
}

pub fn message_status_text(result: &MessageToolResult<MessageWriteStatus>) -> String {
    match result {
        MessageToolResult::Returned(status) => match status {
            MessageWriteStatus::InOutbox { flush_at } => {
                format!("In outbox. Leaves at {}.", flush_at.to_rfc3339())
            }
            MessageWriteStatus::InFlight => "Being sent to central. No answer yet.".into(),
            MessageWriteStatus::Answered {
                message_id,
                delivery,
            } => {
                format!(
                    "Accepted by central. Message id {message_id}.\n{}",
                    delivery_text(*delivery)
                )
            }
            MessageWriteStatus::Refused { reason } => {
                format!("Refused: {}.", write_refusal(reason))
            }
            MessageWriteStatus::NotSent => "Not sent: it did not leave this Mac within 15 minutes.".into(),
            MessageWriteStatus::NotConfirmed => "Not confirmed: it left this Mac but central did not confirm it within 15 minutes. It may have been delivered.".into(),
            MessageWriteStatus::NotFound => "Request not found.".into(),
            MessageWriteStatus::StatusUnavailable => "Status unavailable.".into(),
        },
        MessageToolResult::Refused(refusal) => refusal.to_string(),
    }
}

fn delivery_text(observation: MessageDeliveryObservation) -> &'static str {
    match observation {
        MessageDeliveryObservation::Unknown => "Delivery not known yet.",
        MessageDeliveryObservation::Checked(status) => match status {
            MessageStatus::Sent { .. } => {
                "Central has no delivery confirmation from the recipient's Mac."
            }
            MessageStatus::Delivered { .. } => "Delivered to the recipient's Mac.",
            MessageStatus::Seen { .. } => "Seen: the recipient session was told.",
            MessageStatus::Refused { .. } => "Refused by the recipient's Mac.",
            MessageStatus::NotFound {} => "Central has no record of this message.",
        },
    }
}

impl std::error::Error for MessageToolRefusal {}

impl std::fmt::Display for MessageToolRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidAddress => ADDRESS_FORMAT,
            Self::InvalidBody => "Provide a valid nonempty message body.",
            Self::InvalidRequestId => "Use a canonical UUIDv7 request id.",
            Self::InvalidCaller => "Provide your exact harness session id.",
            Self::InvalidArguments => "Use the arguments listed for this message tool.",
            Self::RequestIdUnavailable => "This Mac could not create a request id.",
            Self::UnexpectedReply => "The message system returned an unexpected write result.",
            Self::ReadAnswerLost => "The message system lost its read answer.",
            Self::TransportUnavailable => "The Router's message tool reply is unavailable.",
            Self::NoMessageLink => "This Router has no message link.",
            Self::Caller(reason) => match reason {
                MessageCallerRefusal::CallerRequired => "Report your current harness session id.",
                MessageCallerRefusal::UnknownSession => {
                    "No session on this Mac matches the reported id."
                }
                MessageCallerRefusal::AmbiguousSession => {
                    "Two sessions on this Mac match the reported id."
                }
                MessageCallerRefusal::CouldNotTell => {
                    "This Mac could not verify that the reported session exists."
                }
            },
            Self::Front(reason) => match reason {
                MessageFrontRefusal::AwaitingFirstRead => "Waiting for the first enrollment check.",
                MessageFrontRefusal::NotEnrolled => "This Mac is not enrolled.",
                MessageFrontRefusal::EnrollmentEnded => "This Mac's enrollment ended.",
                MessageFrontRefusal::MetadataUnreadable => {
                    "This Mac's enrollment status could not be read."
                }
                MessageFrontRefusal::Starting => "The message system is starting.",
                MessageFrontRefusal::Restarting => {
                    "The message system is restarting. Try again when it is ready."
                }
                MessageFrontRefusal::EnrollmentMismatch => {
                    "This Mac is enrolled in another organization or sign-in deployment."
                }
                MessageFrontRefusal::ShuttingDown => "The message system is shutting down.",
                MessageFrontRefusal::CouldNotStart => "The message system could not start.",
            },
            Self::Submission(reason) => match reason {
                MessageSubmitRefusal::CentralUnavailable => "Central is unavailable.",
                MessageSubmitRefusal::OutboxUnavailable => {
                    "This Mac could not admit the write to its outbox."
                }
                MessageSubmitRefusal::RequestIdConflict => {
                    "This request id conflicts with a stored write."
                }
                MessageSubmitRefusal::UnstampedMessageRequired => {
                    "Send the recipient and body. The Router sets the send time."
                }
                MessageSubmitRefusal::TooLargeToSend => {
                    "The message is too large for the link. Shorten the body."
                }
                MessageSubmitRefusal::InvalidSendTime => {
                    "This Mac could not stamp the message time."
                }
                MessageSubmitRefusal::UnmeasurableWrite => {
                    "The Router could not measure the encoded message."
                }
                MessageSubmitRefusal::InvalidRequest => {
                    "The Router could not form a valid link request."
                }
            },
            Self::Recorded(reason) => {
                return write!(formatter, "Refused: {}.", write_refusal(reason));
            }
        };
        formatter.write_str(message)
    }
}

fn write_refusal(reason: &MessageWriteRefusal) -> &'static str {
    match reason {
        MessageWriteRefusal::RequestIdConflict => "this request id conflicts with a stored write",
        MessageWriteRefusal::CentralMail(reason) => match reason {
            CentralMailRefusal::NotSession => "Central requires a session caller",
            CentralMailRefusal::TimeOutOfRange { .. } => "Central rejected the write's time range",
            CentralMailRefusal::TimingViolation { requirement, .. } => match requirement {
                TimingRequirement::FiveMinuteGrid => "Central requires a five-minute mark",
                TimingRequirement::WithinSevenDays => "Central requires a time within seven days",
                TimingRequirement::FutureTime => "Central requires a future time",
            },
            CentralMailRefusal::Workspace(reason) => workspace_refusal(reason),
            CentralMailRefusal::UnknownSession { .. } => {
                "Central could not route to that enrolled machine"
            }
            CentralMailRefusal::NoOccurrence => "Central found no occurrence for the schedule",
            CentralMailRefusal::InvalidInput { .. } => "Central rejected invalid mail input",
            CentralMailRefusal::WakeNotFound { .. } => "Central could not find the wake",
            CentralMailRefusal::WakeAlreadyFired { .. } => "the wake already fired",
            CentralMailRefusal::AutomationNotFound { .. } => {
                "Central could not find the automation"
            }
            CentralMailRefusal::AutomationAlreadyDone { .. } => {
                "the automation is already finished"
            }
        },
        MessageWriteRefusal::CentralWorkspace(reason) => workspace_refusal(reason),
    }
}

fn workspace_refusal(reason: &RefusalReason) -> &'static str {
    match reason {
        RefusalReason::DependencyNotFound { .. } => "Central could not find the dependency",
        RefusalReason::MachineRevoked => "Central did not admit this Mac's enrollment",
        RefusalReason::RequestIdReused => {
            "Central found this request id used by another caller or operation"
        }
        RefusalReason::WorkspaceNotAllowed => "Central did not allow the workspace",
        RefusalReason::WorkspaceNameInUse { .. } => "the workspace name is already in use",
        RefusalReason::NotFound { .. } => "Central could not find the item",
        RefusalReason::NotLead { .. } => "only the project's lead may grant roles",
        RefusalReason::LeadRequired { .. } => "the project must retain its lead",
        RefusalReason::NotCreator { .. } => "only the thread's creator may change it",
        RefusalReason::ThreadResolved { .. } => "the thread is resolved",
        RefusalReason::NotParticipant { .. } => "the caller is not a thread participant",
        RefusalReason::NotMember { .. } => "the caller is not a current thread member",
        RefusalReason::WouldCycle { .. } => "the dependency would create a cycle",
        RefusalReason::CrossProject { .. } => "the dependency crosses projects",
        RefusalReason::DuplicateDependency { .. } => "the dependency already exists",
        RefusalReason::EmptyDescription => "a backlog description is required",
        RefusalReason::BacklogItemClosed { .. } => "the backlog item is closed",
        RefusalReason::SourceKindNotAllowed { .. } => "Central did not allow the source kind",
        RefusalReason::ChildrenNotClosed { .. } => "the item still has open children",
        RefusalReason::ParentClosed { .. } => "the parent is closed",
        RefusalReason::UnknownActor { .. } => "Central does not know the actor",
        RefusalReason::UnheldAddressee => "the addressed role has no holder",
        RefusalReason::InvalidInput { .. } => "Central rejected invalid workspace input",
    }
}
