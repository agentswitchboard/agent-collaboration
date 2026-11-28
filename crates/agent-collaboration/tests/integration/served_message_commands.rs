use std::{
    future::{Future, ready},
    os::unix::fs::PermissionsExt,
    path::Path,
    pin::Pin,
    sync::Arc,
};

use collaboration_client::protocol::message_tools::{
    AdmittedLocalSession, DirectMessageWrite, MessageCallerRefusal, MessageInboxAcknowledgment,
    MessageInboxPage, MessageInboxRead, MessageOperationFuture, MessageSendOutcome,
    MessageToolRefusal, MessageToolResult, MessageUnitPort, MessageWriteStatus, RequestId,
    SessionId,
};
use collaboration_mcp::test_support::ServedCollaborationApi;
use collaboration_service::{
    ApplicationFixture,
    collaboration_application::message_operations::{
        MessageOperations, MessageSessionDirectory, MessageSessionPresence, V0MessageAdmission,
    },
};

const REQUEST: &str = "019a0000-0000-7000-8000-000000000026";
const TARGET: &str =
    r#"{"machineId":"019a0000-0000-7000-8000-000000000027","sessionId":"other-mac"}"#;
const MARK: &str = "2026-11-23T02:00:00Z";

struct CliFixture;

impl MessageSessionDirectory for CliFixture {
    fn lookup<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>> {
        Box::pin(ready(if session_id.as_str() == "message-fixture-caller" {
            MessageSessionPresence::KnownOnce
        } else {
            MessageSessionPresence::UnknownSession
        }))
    }
}

impl MessageUnitPort for CliFixture {
    fn read_inbox<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        read: MessageInboxRead,
    ) -> MessageOperationFuture<'call, MessageInboxPage> {
        assert_eq!(
            caller.address().session_id.as_str(),
            "message-fixture-caller"
        );
        assert_eq!(read.limit, 5);
        Box::pin(ready(Ok(MessageInboxPage {
            messages: Vec::new(),
            read_token: "W10".to_owned(),
            acknowledgment: read.acknowledge.map(|ids| MessageInboxAcknowledgment {
                changed: 0,
                unchanged: ids.len(),
            }),
        })))
    }

    #[expect(
        clippy::unwrap_used,
        reason = "the static fixture mark must be a valid timestamp"
    )]
    fn submit<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        write: DirectMessageWrite,
    ) -> MessageOperationFuture<'call, MessageSendOutcome> {
        assert_eq!(
            caller.address().session_id.as_str(),
            "message-fixture-caller"
        );
        assert_eq!(
            caller.address().machine_id.as_str(),
            "019a0000-0000-7000-8000-000000000028"
        );
        assert_eq!(write.to.session_id.as_str(), "other-mac");
        assert_eq!(write.body.as_str(), "hello");
        Box::pin(ready(Ok(MessageSendOutcome::InOutbox {
            request_id: write.request_id,
            flush_at: MARK.parse().unwrap(),
        })))
    }

    fn request_status<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        request_id: RequestId,
    ) -> MessageOperationFuture<'call, MessageWriteStatus> {
        assert_eq!(
            caller.address().session_id.as_str(),
            "message-fixture-caller"
        );
        assert_eq!(request_id.as_str(), REQUEST);
        Box::pin(ready(Ok(MessageWriteStatus::NotConfirmed)))
    }
}

#[expect(clippy::unwrap_used, reason = "the CLI fixture must execute")]
async fn command(directory: &Path, caller: &str, arguments: &[&str]) -> std::process::Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-collaboration"))
        .args(arguments)
        .args(["--json", "--service-directory"])
        .arg(directory)
        .env("CLAUDE_CODE_SESSION_ID", caller)
        .env_remove("CODEX_THREAD_ID")
        .env_remove("CURSOR_CONVERSATION_ID")
        .output()
        .await
        .unwrap()
}

#[tokio::test]
async fn cli_send_and_status_use_the_harness_report_on_the_served_unix_socket() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let fixture = Arc::new(CliFixture);
    let ops = MessageOperations::new(
        Arc::new(V0MessageAdmission::new(
            "019a0000-0000-7000-8000-000000000028".parse().unwrap(),
            fixture.clone(),
        )),
        fixture,
        Arc::new(router_clock::ManualClock::new(MARK.parse().unwrap())),
    );
    let application = ApplicationFixture::new(
        "00000000-0000-4000-8000-000000000301",
        "00000000-0000-4000-8000-000000000302",
    )
    .unwrap()
    .build()
    .with_messages(ops);
    let api = ServedCollaborationApi::start(directory.path(), application).unwrap();

    let send = command(
        directory.path(),
        "message-fixture-caller",
        &[
            "message",
            "send",
            "--to",
            TARGET,
            "--body",
            "hello",
            "--request-id",
            REQUEST,
        ],
    )
    .await;
    assert!(send.status.success(), "{send:?}");
    let schemas = collaboration_client::protocol::protocol_type_schemas().unwrap();
    let send_record: serde_json::Value = serde_json::from_slice(&send.stdout).unwrap();
    assert!(jsonschema::is_valid(
        &schemas["MessageToolResult_for_MessageSendOutcome"],
        &send_record,
    ));
    let send =
        serde_json::from_slice::<MessageToolResult<MessageSendOutcome>>(&send.stdout).unwrap();
    assert_eq!(
        send,
        MessageToolResult::Returned(MessageSendOutcome::InOutbox {
            request_id: REQUEST.parse().unwrap(),
            flush_at: MARK.parse().unwrap(),
        })
    );

    let args = ["message", "status", "--request-id", REQUEST];
    let status = command(directory.path(), "message-fixture-caller", &args).await;
    assert!(status.status.success(), "{status:?}");
    let status =
        serde_json::from_slice::<MessageToolResult<MessageWriteStatus>>(&status.stdout).unwrap();
    assert_eq!(
        status,
        MessageToolResult::Returned(MessageWriteStatus::NotConfirmed)
    );

    let status = command(directory.path(), "unknown", &args).await;
    assert_eq!(status.status.code(), Some(3));
    let status =
        serde_json::from_slice::<MessageToolResult<MessageWriteStatus>>(&status.stdout).unwrap();
    assert_eq!(
        status,
        MessageToolResult::Refused(MessageToolRefusal::Caller(
            MessageCallerRefusal::UnknownSession
        ))
    );
    let inbox = command(
        directory.path(),
        "message-fixture-caller",
        &["message", "inbox", "--limit", "5"],
    )
    .await;
    assert!(inbox.status.success(), "{inbox:?}");
    let shown: serde_json::Value = serde_json::from_slice(&inbox.stdout).unwrap();
    assert!(jsonschema::is_valid(
        &schemas["MessageToolResult_for_MessageInboxPage"],
        &shown
    ));
    let page = serde_json::from_value::<MessageToolResult<MessageInboxPage>>(shown).unwrap();
    let MessageToolResult::Returned(page) = page else {
        panic!("expected an inbox page, got {page:?}");
    };
    assert_eq!(page.acknowledgment, None);
    let ack = command(
        directory.path(),
        "message-fixture-caller",
        &[
            "message",
            "inbox",
            "--limit",
            "5",
            "--acknowledge",
            &page.read_token,
        ],
    )
    .await;
    assert!(ack.status.success(), "{ack:?}");
    assert!(matches!(
        serde_json::from_slice::<MessageToolResult<MessageInboxPage>>(&ack.stdout).unwrap(),
        MessageToolResult::Returned(MessageInboxPage {
            acknowledgment: Some(MessageInboxAcknowledgment {
                changed: 0,
                unchanged: 0
            }),
            ..
        })
    ));
    api.stop().await.unwrap();
}
