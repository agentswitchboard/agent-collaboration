use std::{
    future::{Future, ready},
    os::unix::fs::PermissionsExt,
    path::Path,
    pin::Pin,
    sync::Arc,
};

use collaboration_client::protocol::central_tool_port::{
    CentralSubmission, CentralToolPayload, ItemRef, WorkspaceOperation, WorkspaceRequest,
};
use collaboration_client::protocol::link_tool_result::LinkToolOutput;
use collaboration_client::protocol::message_tools::{SessionId, SessionRef};
use collaboration_mcp::test_support::ServedCollaborationApi;
use collaboration_service::{
    ApplicationFixture, FakeCentralToolPort, ScriptedCentralOutcome,
    collaboration_application::{
        link_tool_operations::LinkToolOperations,
        message_operations::{MessageSessionDirectory, MessageSessionPresence, V0MessageAdmission},
    },
};
use serde_json::{Value, json};

const MACHINE: &str = "019a0000-0000-7000-8000-000000000081";
const WORKSPACE: &str = "019a0000-0000-7000-8000-000000000082";
const THREAD: &str = "019a0000-0000-7000-8000-000000000083";

struct OneKnownSession;

impl MessageSessionDirectory for OneKnownSession {
    fn lookup<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>> {
        Box::pin(ready(if session_id.as_str() == "thread-reader" {
            MessageSessionPresence::KnownOnce
        } else {
            MessageSessionPresence::UnknownSession
        }))
    }
}

#[expect(clippy::unwrap_used, reason = "the CLI fixture must execute")]
async fn thread_show(
    directory: &Path,
    caller: Option<&str>,
    arguments: &[&str],
) -> (Option<i32>, Vec<u8>) {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-collaboration"));
    command
        .args(["thread", "show"])
        .args(arguments)
        .arg("--service-directory")
        .arg(directory)
        .env_remove("CLAUDE_CODE_SESSION_ID")
        .env_remove("CODEX_THREAD_ID")
        .env_remove("CURSOR_CONVERSATION_ID");
    if let Some(caller) = caller {
        command.env("CLAUDE_CODE_SESSION_ID", caller);
    }
    let output = command.output().await.unwrap();
    (output.status.code(), output.stdout)
}

#[expect(
    clippy::unwrap_used,
    reason = "each sample is a literal of Central's schema"
)]
fn central_answer(result: Value) -> CentralSubmission {
    CentralSubmission::Answered(
        serde_json::from_value(json!({"kind":"workspace","reply":{"result":result,"actors":[]}}))
            .unwrap(),
    )
}

fn answered(result: Value) -> ScriptedCentralOutcome {
    Ok(central_answer(result))
}

#[tokio::test]
async fn cli_thread_show_prints_the_tool_s_output_and_exits_non_zero_unless_central_answered() {
    // Arrange
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let found = json!({"kind":"done","answer":{"kind":"item","item":{"kind":"thread","item":{
        "thread":{"threadId":THREAD,"areaId":"019a0000-0000-7000-8000-000000000084",
            "title":"A stored topic","creator":"019a0000-0000-7000-8000-000000000085","state":"open"},
        "me":null}}}});
    let not_member = json!({"kind":"refused","answer":{"kind":"notMember","threadId":THREAD}});
    let port = Arc::new(FakeCentralToolPort::answering([
        answered(found.clone()),
        answered(found),
        answered(not_member.clone()),
    ]));
    let link_tools = LinkToolOperations::new(
        Arc::new(V0MessageAdmission::new(
            MACHINE.parse().unwrap(),
            Arc::new(OneKnownSession),
        )),
        port.clone(),
    );
    let application = ApplicationFixture::new(
        "00000000-0000-4000-8000-000000000311",
        "00000000-0000-4000-8000-000000000312",
    )
    .unwrap()
    .build()
    .with_link_tools(link_tools);
    let api = ServedCollaborationApi::start(directory.path(), application).unwrap();
    let path = directory.path();
    let target = ["--workspace-id", WORKSPACE, THREAD];
    let json_target = ["--workspace-id", WORKSPACE, THREAD, "--json"];

    // Act
    let text = thread_show(path, Some("thread-reader"), &target).await;
    let done = thread_show(path, Some("thread-reader"), &json_target).await;
    let refused = thread_show(path, Some("thread-reader"), &json_target).await;
    let unknown = thread_show(path, Some("someone-else"), &json_target).await;
    let no_caller = thread_show(path, None, &json_target).await;
    let bad_id = thread_show(
        path,
        Some("thread-reader"),
        &["--workspace-id", "nope", THREAD],
    )
    .await;

    // Assert
    assert_eq!(text, (Some(0), b"Central answered.\n".to_vec()));
    assert_eq!(done.0, Some(0));
    let schemas = collaboration_client::protocol::protocol_type_schemas().unwrap();
    let done: Value = serde_json::from_slice(&done.1).unwrap();
    assert!(
        jsonschema::is_valid(&schemas["LinkToolOutput"], &done),
        "{done}"
    );
    assert_eq!(done["result"]["state"], "answered");
    assert_eq!(done["text"], "Central answered.");

    assert_eq!(refused.0, Some(3));
    let refused: LinkToolOutput = serde_json::from_slice(&refused.1).unwrap();
    assert_eq!(
        serde_json::to_value(&refused.result).unwrap(),
        json!({"state":"answered","reply":{"kind":"workspace","reply":{"result":not_member,"actors":[]}}})
    );
    assert!(
        refused.text.starts_with("Refused by Central: "),
        "{}",
        refused.text
    );

    assert_eq!(unknown.0, Some(3));
    let unknown: LinkToolOutput = serde_json::from_slice(&unknown.1).unwrap();
    assert_eq!(
        serde_json::to_value(&unknown.result).unwrap(),
        json!({"state":"refused","code":{"kind":"caller","reason":"unknownSession"}})
    );

    assert_eq!(no_caller.0, Some(2));
    let no_caller: Value = serde_json::from_slice(&no_caller.1).unwrap();
    assert_eq!(no_caller["error"]["kind"], "identityUnavailable");
    assert_eq!(bad_id.0, Some(2));

    let caller = SessionRef {
        machine_id: MACHINE.parse().unwrap(),
        session_id: "thread-reader".parse().unwrap(),
    };
    let show = CentralToolPayload::Workspace(WorkspaceRequest {
        request_id: None,
        workspace_id: Some(WORKSPACE.parse().unwrap()),
        operation: WorkspaceOperation::Show {
            item: ItemRef::Thread(THREAD.parse().unwrap()),
        },
    });
    assert_eq!(port.submitted(), vec![(caller, show); 3]);
    api.stop().await.unwrap();
}
