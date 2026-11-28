//! Projects outbox results and the linked delivery observation for the two message tools.
use central_link_protocol::{ReplyPayload, RequestScope, RequestStatusQuery};
use collaboration_protocol::message_tools::{
    AdmittedLocalSession, DirectMessageWrite, MachineId, MessageDeliveryObservation,
    MessageFrontRefusal, MessageId, MessageInboxAcknowledgment, MessageInboxPage, MessageInboxRead,
    MessageOperationFuture, MessageSendOutcome, MessageStatus, MessageSubmitRefusal,
    MessageToolRefusal, MessageUnitPort, MessageWriteRefusal, MessageWriteStatus, RequestId,
    Timestamp, inbox_read_token,
};
use switchboard_machine_core_store::{
    LocalOutboxRefusal, OutboxCancellation, OutboxCentralRefusal, OutboxRefusal,
};
use switchboard_message_custody::{
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload, SubmitRefusal},
    custody_task::CustodyTaskHandle,
    inbox_read::{InboxReadError, RecipientInboxRead},
};
use workspace_model::{
    Caller,
    central_mail::{CentralMailAnswer, CentralMailResult, MessageDelivery, SendMessageRequest},
};

use crate::{FrontRefusal, MessageFront};

impl MessageUnitPort for MessageFront {
    fn check_available(&self) -> Result<(), MessageToolRefusal> {
        self.outbox_and_inbox().map(drop)
    }

    fn read_inbox<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        read: MessageInboxRead,
    ) -> MessageOperationFuture<'call, MessageInboxPage> {
        Box::pin(async move {
            let handle = self.outbox_and_inbox()?;
            let acknowledgment = if let Some(ids) = read.acknowledge {
                let settled = handle
                    .record_inbox_read(caller.clone(), RecipientInboxRead::for_session(caller, ids))
                    .await
                    .map_err(|error| self.inbox_error(error))?;
                Some(MessageInboxAcknowledgment {
                    changed: settled.changed,
                    unchanged: settled.unchanged,
                })
            } else {
                None
            };
            let messages = handle
                .read_inbox(caller.clone(), usize::from(read.limit))
                .await
                .map_err(|error| self.inbox_error(error))?
                .into_messages();
            let read_token = inbox_read_token(&messages)?;
            Ok(MessageInboxPage {
                messages,
                read_token,
                acknowledgment,
            })
        })
    }

    fn submit<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        write: DirectMessageWrite,
    ) -> MessageOperationFuture<'call, MessageSendOutcome> {
        Box::pin(async move {
            let handle = self.outbox_and_inbox()?;
            let admission = handle.reserve().await.map_err(|_| self.stopped_tool())?;
            let result = admission
                .submit(
                    Caller::Session(caller.address().clone()),
                    SubmissionPayload::SendMessage {
                        request: write.request_id.clone(),
                        message: SendMessageRequest {
                            to: write.to,
                            body: write.body,
                            delivery: MessageDelivery::Auto,
                            reply_to: None,
                        },
                    },
                )
                .resolve()
                .await;
            match result {
                Submission::InOutbox(receipt) => Ok(MessageSendOutcome::InOutbox {
                    request_id: receipt.id,
                    flush_at: mark_time(receipt.flush_at.unix_millis())?,
                }),
                Submission::Answered(reply) => Ok(MessageSendOutcome::Answered {
                    request_id: write.request_id,
                    message_id: answered_message(reply)?,
                }),
                Submission::Uncertain { request, .. } => Ok(MessageSendOutcome::Uncertain {
                    request_id: request,
                }),
                Submission::Refused(reason) => {
                    if matches!(reason, SubmitRefusal::OutboxUnavailable) {
                        self.check_available()?;
                    }
                    Err(MessageToolRefusal::Submission(submit_refusal(reason)))
                }
                Submission::ReadAnswerLost => Err(MessageToolRefusal::ReadAnswerLost),
            }
        })
    }

    fn request_status<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        request_id: RequestId,
    ) -> MessageOperationFuture<'call, MessageWriteStatus> {
        Box::pin(async move {
            let handle = self.outbox_and_inbox()?;
            let admission = handle.reserve().await.map_err(|_| self.stopped_tool())?;
            let caller = Caller::Session(caller.address().clone());
            let status = admission
                .request_status(
                    caller.clone(),
                    RequestStatusQuery {
                        request_id,
                        scope: RequestScope::Organization,
                    },
                )
                .resolve()
                .await;
            match status {
                CustodyRequestStatus::InOutbox { flush_at } => Ok(MessageWriteStatus::InOutbox {
                    flush_at: mark_time(flush_at.unix_millis())?,
                }),
                CustodyRequestStatus::Sent => Ok(MessageWriteStatus::InFlight),
                CustodyRequestStatus::Answered(reply) => {
                    let message_id = answered_message(reply)?;
                    let delivery = match handle.message_status(caller, message_id.clone()).await {
                        Ok(status) => MessageDeliveryObservation::Checked(
                            status.unwrap_or(MessageStatus::NotFound {}),
                        ),
                        Err(_) => MessageDeliveryObservation::Unknown,
                    };
                    Ok(MessageWriteStatus::Answered {
                        message_id,
                        delivery,
                    })
                }
                CustodyRequestStatus::Refused(reason) => Ok(MessageWriteStatus::Refused {
                    reason: write_refusal(reason),
                }),
                CustodyRequestStatus::Ended {
                    reason: OutboxCancellation::NotSentWithin15Minutes,
                } => Ok(MessageWriteStatus::NotSent),
                CustodyRequestStatus::Ended {
                    reason: OutboxCancellation::NotConfirmedWithin15Minutes,
                } => Ok(MessageWriteStatus::NotConfirmed),
                CustodyRequestStatus::NotFound => Ok(MessageWriteStatus::NotFound),
                CustodyRequestStatus::StatusUnavailable => {
                    self.check_available()?;
                    Ok(MessageWriteStatus::StatusUnavailable)
                }
            }
        })
    }
}

impl MessageFront {
    fn inbox_error(&self, error: InboxReadError) -> MessageToolRefusal {
        match error {
            InboxReadError::Closed => self.stopped_tool(),
            InboxReadError::RecipientChanged | InboxReadError::MessageNotKept => {
                MessageToolRefusal::InboxAcknowledgmentNotKept
            }
            InboxReadError::ChangedSinceRead => MessageToolRefusal::InboxChanged,
            InboxReadError::Handover(_) | InboxReadError::Inbox(_) => {
                MessageToolRefusal::InboxUnavailable
            }
        }
    }

    /// Returns the Fleet identity from the same generation that owns the tool queue.
    pub fn available_machine(&self) -> Result<MachineId, MessageToolRefusal> {
        self.forwarding_machine()
            .map_err(|reason| MessageToolRefusal::Front(front_refusal(reason)))
    }

    fn outbox_and_inbox(&self) -> Result<CustodyTaskHandle, MessageToolRefusal> {
        self.custody()
            .map_err(|reason| MessageToolRefusal::Front(front_refusal(reason)))
    }

    fn stopped_tool(&self) -> MessageToolRefusal {
        self.outbox_and_inbox()
            .err()
            .unwrap_or(MessageToolRefusal::Front(MessageFrontRefusal::ShuttingDown))
    }
}

fn mark_time(millis: i64) -> Result<Timestamp, MessageToolRefusal> {
    Timestamp::from_timestamp_millis(millis).ok_or(MessageToolRefusal::UnexpectedReply)
}

fn answered_message(reply: ReplyPayload) -> Result<MessageId, MessageToolRefusal> {
    match reply {
        ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) => {
            Ok(message_id)
        }
        ReplyPayload::Mail(CentralMailResult::Refused(reason)) => Err(
            MessageToolRefusal::Recorded(MessageWriteRefusal::CentralMail(reason)),
        ),
        ReplyPayload::Workspace(_) | ReplyPayload::Mail(_) => {
            Err(MessageToolRefusal::UnexpectedReply)
        }
    }
}

pub(crate) fn front_refusal(reason: FrontRefusal) -> MessageFrontRefusal {
    match reason {
        FrontRefusal::CouldNotStart => MessageFrontRefusal::CouldNotStart,
        FrontRefusal::AwaitingFirstRead => MessageFrontRefusal::AwaitingFirstRead,
        FrontRefusal::NotEnrolled => MessageFrontRefusal::NotEnrolled,
        FrontRefusal::EnrollmentEnded => MessageFrontRefusal::EnrollmentEnded,
        FrontRefusal::MetadataUnreadable => MessageFrontRefusal::MetadataUnreadable,
        FrontRefusal::Starting => MessageFrontRefusal::Starting,
        FrontRefusal::Restarting => MessageFrontRefusal::Restarting,
        FrontRefusal::EnrollmentMismatch => MessageFrontRefusal::EnrollmentMismatch,
        FrontRefusal::ShuttingDown => MessageFrontRefusal::ShuttingDown,
    }
}

fn submit_refusal(reason: SubmitRefusal) -> MessageSubmitRefusal {
    match reason {
        SubmitRefusal::CentralUnavailable => MessageSubmitRefusal::CentralUnavailable,
        SubmitRefusal::OutboxUnavailable => MessageSubmitRefusal::OutboxUnavailable,
        SubmitRefusal::RequestIdConflict => MessageSubmitRefusal::RequestIdConflict,
        SubmitRefusal::UnstampedMessageRequired => MessageSubmitRefusal::UnstampedMessageRequired,
        SubmitRefusal::TooLargeToSend => MessageSubmitRefusal::TooLargeToSend,
        SubmitRefusal::InvalidSendTime => MessageSubmitRefusal::InvalidSendTime,
        SubmitRefusal::UnmeasurableWrite(_) => MessageSubmitRefusal::UnmeasurableWrite,
        SubmitRefusal::InvalidRequest(_) => MessageSubmitRefusal::InvalidRequest,
    }
}

fn write_refusal(reason: OutboxRefusal) -> MessageWriteRefusal {
    match reason {
        OutboxRefusal::Local(LocalOutboxRefusal::RequestIdConflict) => {
            MessageWriteRefusal::RequestIdConflict
        }
        OutboxRefusal::Central(OutboxCentralRefusal::Workspace(reason)) => {
            MessageWriteRefusal::CentralWorkspace(reason)
        }
        OutboxRefusal::Central(OutboxCentralRefusal::Mail(reason)) => {
            MessageWriteRefusal::CentralMail(reason)
        }
    }
}
