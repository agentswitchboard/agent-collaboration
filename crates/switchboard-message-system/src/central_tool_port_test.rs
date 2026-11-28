use central_link_protocol::{CorrelationId, ProtocolError, ReplyPayload, RequestScope};
use collaboration_protocol::central_tool_port::{
    CentralMailRequest, CentralSubmission, CentralToolPayload, ItemRef, RequestId,
    SendMessageRequest, WorkspaceOperation, WorkspaceReply, WorkspaceRequest, WorkspaceResult,
};
use collaboration_protocol::link_tool_result::{InvalidRequestReason, LinkRefusal};
use collaboration_protocol::message_tools::SessionRef;
use switchboard_message_custody::custody_submission::{
    Submission, SubmissionPayload, SubmitRefusal,
};
use workspace_model::central_mail::{CentralMailOperation, MessageDelivery};

use super::{central_submission, submission_payload};

const ID: &str = "019a0000-0000-7000-8000-000000000071";

fn request_id() -> RequestId {
    ID.parse().unwrap()
}

#[test]
fn custody_answers_cross_unchanged_and_each_refusal_keeps_its_own_code() {
    // Arrange
    let reply = ReplyPayload::Workspace(WorkspaceReply {
        result: WorkspaceResult::OutcomeUnknown,
        actors: vec![],
    });
    let invalid = |reason| Err(LinkRefusal::InvalidRequest { reason });
    let cases = [
        (
            Submission::Answered(reply.clone()),
            Ok(CentralSubmission::Answered(reply)),
        ),
        (
            Submission::Uncertain {
                request: request_id(),
                scope: RequestScope::Organization,
            },
            Ok(CentralSubmission::Uncertain {
                request_id: request_id(),
            }),
        ),
        (
            Submission::ReadAnswerLost,
            Ok(CentralSubmission::ReadAnswerLost),
        ),
        (
            Submission::Refused(SubmitRefusal::CentralUnavailable),
            Err(LinkRefusal::CentralUnavailable),
        ),
        (
            Submission::Refused(SubmitRefusal::OutboxUnavailable),
            Err(LinkRefusal::OutboxUnavailable),
        ),
        (
            Submission::Refused(SubmitRefusal::RequestIdConflict),
            Err(LinkRefusal::RequestIdConflict),
        ),
        (
            Submission::Refused(SubmitRefusal::UnstampedMessageRequired),
            invalid(InvalidRequestReason::UnstampedMessageRequired),
        ),
        (
            Submission::Refused(SubmitRefusal::TooLargeToSend),
            invalid(InvalidRequestReason::TooLargeToSend),
        ),
        (
            Submission::Refused(SubmitRefusal::InvalidSendTime),
            invalid(InvalidRequestReason::InvalidSendTime),
        ),
        (
            Submission::Refused(SubmitRefusal::InvalidRequest(ProtocolError::WrongDirection)),
            invalid(InvalidRequestReason::WrongDirection),
        ),
        (
            Submission::Refused(SubmitRefusal::InvalidRequest(
                ProtocolError::RequestIdPolicy { field: "requestId" },
            )),
            invalid(InvalidRequestReason::RequestIdPolicy {
                field: "requestId".into(),
            }),
        ),
        (
            Submission::Refused(SubmitRefusal::InvalidRequest(ProtocolError::EgressTooBig {
                bytes: 1,
            })),
            invalid(InvalidRequestReason::TooLargeToSend),
        ),
        (
            Submission::Refused(SubmitRefusal::InvalidRequest(
                ProtocolError::UnknownTypedCall {
                    correlation_id: ID.parse::<CorrelationId>().unwrap(),
                },
            )),
            invalid(InvalidRequestReason::UnknownTypedCall),
        ),
    ];

    for (submission, expected) in cases {
        let shown = format!("{submission:?}");

        // Act
        let actual = central_submission(submission);

        // Assert
        assert_eq!(actual, expected, "{shown}");
    }
}

#[test]
fn each_payload_reaches_custody_on_its_own_path_unchanged() {
    // Arrange
    let workspace = WorkspaceRequest {
        request_id: None,
        workspace_id: Some(ID.parse().unwrap()),
        operation: WorkspaceOperation::Show {
            item: ItemRef::Thread(ID.parse().unwrap()),
        },
    };
    let mail = CentralMailRequest {
        request_id: request_id(),
        operation: CentralMailOperation::CancelWake {
            wake_id: ID.parse().unwrap(),
        },
    };
    let message = SendMessageRequest {
        to: SessionRef {
            machine_id: ID.parse().unwrap(),
            session_id: "peer".parse().unwrap(),
        },
        body: "hello".to_owned().try_into().unwrap(),
        delivery: MessageDelivery::Auto,
        reply_to: None,
    };

    // Act and assert
    assert_eq!(
        submission_payload(CentralToolPayload::Workspace(workspace.clone())),
        SubmissionPayload::Workspace(workspace)
    );
    assert_eq!(
        submission_payload(CentralToolPayload::Mail(mail.clone())),
        SubmissionPayload::Mail(mail)
    );
    assert_eq!(
        submission_payload(CentralToolPayload::SendMessage {
            request: request_id(),
            message: message.clone(),
        }),
        SubmissionPayload::SendMessage {
            request: request_id(),
            message,
        }
    );
}
