use std::{
    collections::VecDeque,
    future::{Future, ready},
    pin::Pin,
    sync::{Arc, Mutex},
};

use collaboration_protocol::message_tools::{
    AdmittedLocalSession, DirectMessageWrite, MachineId, MessageCallerRefusal,
    MessageDeliveryObservation, MessageInboxAcknowledgment, MessageInboxRead,
    MessageOperationFuture, MessageSubmitRefusal, MessageUnitPort, Timestamp,
};
use collaboration_protocol::workspace_tools::{ThreadId, WorkspaceId, WorkspaceReply};
use collaboration_service::collaboration_application::message_operations::{
    MessageOperations, MessageSessionDirectory, MessageSessionPresence, V0MessageAdmission,
};
use serde_json::json;

use super::*;
use crate::api_test_harness::{ServedApi, api_config, test_identity};

struct FixtureDirectory;

impl MessageSessionDirectory for FixtureDirectory {
    fn lookup<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>> {
        Box::pin(ready(if session_id.as_str() == "sender" {
            MessageSessionPresence::KnownOnce
        } else {
            MessageSessionPresence::UnknownSession
        }))
    }
}

type FixtureWrite = (SessionRef, RequestId, SessionRef, PostText);

#[derive(Default)]
struct FixtureMessageUnit {
    last: Mutex<Option<FixtureWrite>>,
    thread_reads: Mutex<Vec<(SessionRef, WorkspaceId, ThreadId)>>,
    thread_answers: Mutex<VecDeque<Result<WorkspaceReply, MessageToolRefusal>>>,
}

impl MessageUnitPort for FixtureMessageUnit {
    fn show_thread<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        workspace_id: WorkspaceId,
        thread_id: ThreadId,
    ) -> MessageOperationFuture<'call, WorkspaceReply> {
        self.thread_reads
            .lock()
            .unwrap()
            .push((caller.address().clone(), workspace_id, thread_id));
        let reply = self.thread_answers.lock().unwrap().pop_front().unwrap();
        Box::pin(ready(reply))
    }

    fn read_inbox<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        read: MessageInboxRead,
    ) -> MessageOperationFuture<'call, MessageInboxPage> {
        assert_eq!(caller.address().session_id.as_str(), "sender");
        assert_eq!(read.limit, 20);
        Box::pin(ready(Ok(MessageInboxPage {
            messages: Vec::new(),
            read_token: "W10".to_owned(),
            acknowledgment: read.acknowledge.map(|ids| MessageInboxAcknowledgment {
                changed: 0,
                unchanged: ids.len(),
            }),
        })))
    }

    fn submit<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        write: DirectMessageWrite,
    ) -> MessageOperationFuture<'call, MessageSendOutcome> {
        *self.last.lock().unwrap() = Some((
            caller.address().clone(),
            write.request_id.clone(),
            write.to,
            write.body,
        ));
        Box::pin(ready(Ok(MessageSendOutcome::InOutbox {
            request_id: write.request_id,
            flush_at: "2026-11-23T02:00:00Z".parse().unwrap(),
        })))
    }

    fn request_status<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        request_id: RequestId,
    ) -> MessageOperationFuture<'call, MessageWriteStatus> {
        let matched = self
            .last
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|(owner, saved_id, _, _)| {
                owner == caller.address() && saved_id == &request_id
            });
        Box::pin(ready(Ok(if matched {
            MessageWriteStatus::Answered {
                delivery: MessageDeliveryObservation::Unknown,
                message_id: "019a0000-0000-7000-8000-000000000004".parse().unwrap(),
            }
        } else {
            MessageWriteStatus::NotFound
        })))
    }
}

#[tokio::test]
async fn thread_show_round_trips_central_answers_and_refuses_removed_inputs() {
    let directory = tempfile::tempdir().unwrap();
    let machine_id: MachineId = "019a0000-0000-7000-8000-000000000001".parse().unwrap();
    let workspace_id: WorkspaceId = "019a0000-0000-7000-8000-000000000011".parse().unwrap();
    let thread_id: ThreadId = "019a0000-0000-7000-8000-000000000012".parse().unwrap();
    let args =
        json!({"callerSessionId":"sender", "workspaceId":workspace_id, "threadId":thread_id});
    let found = json!({
        "result":{"kind":"done","answer":{"kind":"item","item":{"kind":"thread","item":{
            "thread":{"threadId":thread_id,"areaId":"019a0000-0000-7000-8000-000000000013",
                "title":"A stored topic","creator":"019a0000-0000-7000-8000-000000000014","state":"open"},
            "me":null
        }}}},
        "actors":[{"actorIdentityId":"019a0000-0000-7000-8000-000000000014",
            "identity":{"kind":"agentSession","session":{"machineId":machine_id,"sessionId":"sender"}}}]
    });
    let not_found = json!({"result":{"kind":"notFound","answer":{
        "workspaceId":workspace_id,"item":{"kind":"thread","id":thread_id}
    }},"actors":[]});
    let refused =
        json!({"result":{"kind":"refused","answer":{"kind":"workspaceNotAllowed"}},"actors":[]});
    let central_unavailable = json!({"result":{"kind":"centralUnavailable"},"actors":[]});
    let cases = [
        (found, "Thread found."),
        (
            not_found,
            "Thread not found. Check workspaceId and threadId before reading again.",
        ),
        (
            refused,
            "Refused by Central. workspace is not allowed. Check that this Mac is allowed to read the workspace.",
        ),
        (
            central_unavailable,
            "Central answered that it is unavailable. Reading again is safe.",
        ),
    ];
    let unit = Arc::new(FixtureMessageUnit {
        thread_answers: Mutex::new(
            cases
                .iter()
                .map(|(reply, _)| Ok(serde_json::from_value(reply.clone()).unwrap()))
                .chain([
                    Err(MessageToolRefusal::Submission(
                        MessageSubmitRefusal::CentralUnavailable,
                    )),
                    Err(MessageToolRefusal::ReadAnswerLost),
                ])
                .collect(),
        ),
        ..FixtureMessageUnit::default()
    });
    let messages = MessageOperations::new(
        Arc::new(V0MessageAdmission::new(
            machine_id.clone(),
            Arc::new(FixtureDirectory),
        )),
        unit.clone(),
        Arc::new(router_clock::ManualClock::new(
            "2026-11-23T02:00:00Z".parse().unwrap(),
        )),
    );
    let served = ServedApi::tcp(&api_config(
        test_identity().build().with_messages(messages),
        directory.path(),
    ))
    .await;

    for (reply, text) in cases {
        let result = served.call_tool("thread_show", args.clone()).await;
        assert_eq!(
            result.structured_content,
            Some(json!({
                "result":{"state":"answered","reply":reply},"text":text
            }))
        );
        assert_eq!(
            result
                .content
                .first()
                .and_then(ContentBlock::as_text)
                .unwrap()
                .text,
            text
        );
    }
    for (result_shape, text) in [
        (
            json!({"state":"refused","code":{"kind":"centralUnavailable"}}),
            "Central unavailable. Nothing was sent. Check the link or read again.",
        ),
        (
            json!({"state":"readAnswerLost"}),
            "Read failed because its answer was lost. Reading again is safe.",
        ),
    ] {
        let result = served.call_tool("thread_show", args.clone()).await;
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            result.structured_content,
            Some(json!({"result":result_shape,"text":text}))
        );
    }

    for (input, field, text) in [
        (
            "includeRedacted",
            "includeRedacted",
            "includeRedacted is no longer accepted. Redaction deletes the text, so there is nothing to include. Omit this field.",
        ),
        (
            "projectId",
            "projectQualifier",
            "The project qualifier (projectId or project) is no longer accepted. Identify the thread with workspaceId and threadId.",
        ),
        (
            "project",
            "projectQualifier",
            "The project qualifier (projectId or project) is no longer accepted. Identify the thread with workspaceId and threadId.",
        ),
        (
            "protocolVersion",
            "protocolVersion",
            "protocolVersion is no longer accepted. The link selects its version. Omit this field.",
        ),
    ] {
        let mut invalid = args.clone();
        invalid
            .as_object_mut()
            .unwrap()
            .insert(input.to_owned(), Value::Null);
        let result = served.call_tool("thread_show", invalid).await;
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            result.structured_content,
            Some(json!({
                "result":{"state":"refused","code":{"kind":"removedInput","field":field}},"text":text
            }))
        );
        assert_eq!(
            result
                .content
                .first()
                .and_then(ContentBlock::as_text)
                .unwrap()
                .text,
            text
        );
    }
    for input in ["callerSessionId", "workspaceId", "threadId"] {
        let mut missing = args.clone();
        missing.as_object_mut().unwrap().remove(input);
        let result = served.call_tool("thread_show", missing).await;
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            result
                .structured_content
                .unwrap()
                .pointer("/result/code/kind"),
            Some(&json!("invalidArguments"))
        );
    }
    let mut unknown = args;
    unknown
        .as_object_mut()
        .unwrap()
        .insert("callerSessionId".into(), json!("unknown"));
    let result = served.call_tool("thread_show", unknown).await;
    assert_eq!(
        result
            .structured_content
            .unwrap()
            .pointer("/result/code/reason/reason"),
        Some(&json!("unknownSession"))
    );

    let caller = SessionRef {
        machine_id,
        session_id: "sender".parse().unwrap(),
    };
    assert_eq!(
        *unit.thread_reads.lock().unwrap(),
        vec![(caller, workspace_id, thread_id); 6]
    );
    assert!(unit.thread_answers.lock().unwrap().is_empty());
    assert!(unit.last.lock().unwrap().is_none());
    served.stop().await;
}

#[tokio::test]
async fn message_routes_preserve_typed_results_on_loopback_and_unix() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let machine_id: MachineId = "019a0000-0000-7000-8000-000000000001".parse().unwrap();
    let mark = "2026-11-23T02:00:00Z";
    let unit = Arc::new(FixtureMessageUnit::default());
    let messages = MessageOperations::new(
        Arc::new(V0MessageAdmission::new(
            machine_id.clone(),
            Arc::new(FixtureDirectory),
        )),
        unit.clone(),
        Arc::new(router_clock::ManualClock::new(mark.parse().unwrap())),
    );
    let application = test_identity().build().with_messages(messages);
    let tcp = ServedApi::tcp(&api_config(application.clone(), directory.path())).await;
    let unix = ServedApi::unix(
        &api_config(application, directory.path()),
        &directory.path().join("c.sock"),
    );

    for served in [tcp, unix] {
        let invalid = served.call_tool("message_send", json!({
            "callerSessionId":"sender",
            "to":{"endpoint":{"serviceId":"legacy","endpointId":"legacy"},"sessionId":"recipient"},
            "body":"hello"
        })).await;
        let decoded = serde_json::from_value::<MessageToolResult<MessageSendOutcome>>(
            invalid.structured_content.unwrap(),
        )
        .unwrap();
        assert_eq!(
            decoded,
            MessageToolResult::Refused(MessageToolRefusal::InvalidAddress)
        );
        assert_eq!(
            invalid.content[0].as_text().unwrap().text,
            "Use machineId and sessionId."
        );

        let refused = served
            .call_tool(
                "message_status",
                json!({
                    "callerSessionId":"unknown",
                    "requestId":"019a0000-0000-7000-8000-000000000002"
                }),
            )
            .await;
        let decoded = serde_json::from_value::<MessageToolResult<MessageWriteStatus>>(
            refused.structured_content.unwrap(),
        )
        .unwrap();
        assert_eq!(
            decoded,
            MessageToolResult::Refused(MessageToolRefusal::Caller(
                MessageCallerRefusal::UnknownSession
            ))
        );

        let sent = served.call_tool("message_send", json!({
            "callerSessionId":"sender",
            "to":{"machineId":"019a0000-0000-7000-8000-000000000003","sessionId":"recipient"},
            "body":"hello"
        })).await;
        let decoded = serde_json::from_value::<MessageToolResult<MessageSendOutcome>>(
            sent.structured_content.unwrap(),
        )
        .unwrap();
        let MessageToolResult::Returned(MessageSendOutcome::InOutbox {
            request_id,
            flush_at,
        }) = decoded
        else {
            panic!("expected the observed outbox receipt, got {decoded:?}");
        };
        assert_eq!(flush_at, mark.parse::<Timestamp>().unwrap());
        assert_eq!(
            sent.content[0].as_text().unwrap().text,
            format!("In outbox. Leaves at 2026-11-23T02:00:00+00:00.\nRequest id {request_id}.")
        );
        {
            let writes = unit.last.lock().unwrap();
            let (caller, saved_id, target, body) = writes.as_ref().unwrap();
            assert_eq!(caller.machine_id, machine_id);
            assert_eq!(caller.session_id.as_str(), "sender");
            assert_eq!(target.session_id.as_str(), "recipient");
            assert_eq!(body.as_str(), "hello");
            assert_eq!(saved_id, &request_id);
        }
        let status = served
            .call_tool(
                "message_status",
                json!({
                    "callerSessionId":"sender", "requestId":request_id
                }),
            )
            .await;
        let decoded = serde_json::from_value::<MessageToolResult<MessageWriteStatus>>(
            status.structured_content.unwrap(),
        )
        .unwrap();
        assert_eq!(
            decoded,
            MessageToolResult::Returned(MessageWriteStatus::Answered {
                message_id: "019a0000-0000-7000-8000-000000000004".parse().unwrap(),
                delivery: MessageDeliveryObservation::Unknown,
            })
        );
        let page = served
            .call_tool("message_inbox", json!({"callerSessionId":"sender"}))
            .await;
        let decoded: MessageToolResult<MessageInboxPage> =
            serde_json::from_value(page.structured_content.unwrap()).unwrap();
        let MessageToolResult::Returned(page) = decoded else {
            panic!("expected the admitted inbox page, got {decoded:?}");
        };
        assert!(page.messages.is_empty());
        assert_eq!(page.acknowledgment, None);
        let acknowledged = served
            .call_tool(
                "message_inbox",
                json!({"callerSessionId":"sender", "acknowledge":page.read_token}),
            )
            .await;
        let decoded: MessageToolResult<MessageInboxPage> =
            serde_json::from_value(acknowledged.structured_content.unwrap()).unwrap();
        assert!(matches!(
            decoded,
            MessageToolResult::Returned(MessageInboxPage {
                acknowledgment: Some(MessageInboxAcknowledgment {
                    changed: 0,
                    unchanged: 0
                }),
                ..
            })
        ));
        let malformed = served
            .call_tool(
                "message_inbox",
                json!({"callerSessionId":"sender", "acknowledge":"!"}),
            )
            .await;
        assert_eq!(
            serde_json::from_value::<MessageToolResult<MessageInboxPage>>(
                malformed.structured_content.unwrap()
            )
            .unwrap(),
            MessageToolResult::Refused(MessageToolRefusal::InvalidInboxReadToken)
        );
        served.stop().await;
    }
}
