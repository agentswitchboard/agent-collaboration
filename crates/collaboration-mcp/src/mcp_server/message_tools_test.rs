use std::{
    future::{Future, ready},
    pin::Pin,
    sync::{Arc, Mutex},
};

use collaboration_protocol::message_tools::{
    AdmittedLocalSession, DirectMessageWrite, MachineId, MessageCallerRefusal,
    MessageDeliveryObservation, MessageInboxAcknowledgment, MessageInboxRead,
    MessageOperationFuture, MessageUnitPort, Timestamp,
};
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
}

impl MessageUnitPort for FixtureMessageUnit {
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
