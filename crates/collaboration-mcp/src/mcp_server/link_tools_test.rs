use std::{
    future::{Future, ready},
    pin::Pin,
    sync::Arc,
};

use collaboration_protocol::central_tool_port::{
    CentralSubmission, CentralToolPayload, ItemRef, WorkspaceOperation, WorkspaceRequest,
};
use collaboration_protocol::link_tool_result::{LinkRefusal, MessageFrontRefusal};
use collaboration_protocol::message_tools::{SessionId, SessionRef};
use collaboration_service::FakeCentralToolPort;
use collaboration_service::collaboration_application::link_tool_operations::LinkToolOperations;
use collaboration_service::collaboration_application::message_operations::{
    MessageSessionDirectory, MessageSessionPresence, V0MessageAdmission,
};
use rmcp::model::ContentBlock;
use serde_json::{Value, json};

use crate::api_test_harness::{ServedApi, api_config, test_identity};

const MACHINE: &str = "019a0000-0000-7000-8000-000000000061";
const WORKSPACE: &str = "019a0000-0000-7000-8000-000000000062";
const THREAD: &str = "019a0000-0000-7000-8000-000000000063";
/// The text a shown thread reads as, one fact per line.
const SHOWN_THREAD: &str = "Thread: A stored topic\nState: open\nThread ID: 019a0000-0000-7000-8000-000000000063\nArea ID: 019a0000-0000-7000-8000-000000000064\nCreated by: 019a0000-0000-7000-8000-000000000065";

struct OneKnownSession;

impl MessageSessionDirectory for OneKnownSession {
    fn lookup<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>> {
        Box::pin(ready(if session_id.as_str() == "reader" {
            MessageSessionPresence::KnownOnce
        } else {
            MessageSessionPresence::UnknownSession
        }))
    }
}

fn reply(result: &Value) -> Value {
    json!({"kind":"workspace","reply":{"result":result,"actors":[]}})
}

fn central_answer(result: &Value) -> CentralSubmission {
    CentralSubmission::Answered(serde_json::from_value(reply(result)).unwrap())
}

fn answered(result: &Value) -> Result<CentralSubmission, LinkRefusal> {
    Ok(central_answer(result))
}

fn text_of(result: &rmcp::model::CallToolResult) -> String {
    result
        .content
        .first()
        .and_then(ContentBlock::as_text)
        .map(|text| text.text.clone())
        .unwrap_or_default()
}

#[tokio::test]
async fn thread_show_is_a_success_only_for_central_s_done_answer() {
    // Arrange
    let found = json!({"kind":"done","answer":{"kind":"item","item":{"kind":"thread","item":{
        "thread":{"threadId":THREAD,"areaId":"019a0000-0000-7000-8000-000000000064",
            "title":"A stored topic","creator":"019a0000-0000-7000-8000-000000000065","state":"open"},
        "me":null}}}});
    let not_found = json!({"kind":"notFound","answer":{"workspaceId":WORKSPACE,
        "item":{"kind":"thread","id":THREAD}}});
    let not_member = json!({"kind":"refused","answer":{"kind":"notMember","threadId":THREAD}});
    let unavailable = json!({"kind":"centralUnavailable"});
    let conflict = json!({"kind":"conflict"});
    let unknown = json!({"kind":"outcomeUnknown"});
    let failing = [
        (
            json!({"state":"answered","reply":reply(&not_found)}),
            answered(&not_found),
        ),
        (
            json!({"state":"answered","reply":reply(&not_member)}),
            answered(&not_member),
        ),
        (
            json!({"state":"answered","reply":reply(&unavailable)}),
            answered(&unavailable),
        ),
        (
            json!({"state":"answered","reply":reply(&conflict)}),
            answered(&conflict),
        ),
        (
            json!({"state":"answered","reply":reply(&unknown)}),
            answered(&unknown),
        ),
        (
            json!({"state":"readAnswerLost"}),
            Ok(CentralSubmission::ReadAnswerLost),
        ),
        (
            json!({"state":"refused","code":{"kind":"centralUnavailable"}}),
            Err(LinkRefusal::CentralUnavailable),
        ),
        (
            json!({"state":"refused","code":{"kind":"routerNotReady","state":"starting"}}),
            Err(LinkRefusal::RouterNotReady {
                state: MessageFrontRefusal::Starting,
            }),
        ),
    ];
    let port = Arc::new(FakeCentralToolPort::answering(
        std::iter::once(answered(&found)).chain(failing.iter().map(|(_, outcome)| outcome.clone())),
    ));
    let directory = tempfile::tempdir().unwrap();
    let link_tools = LinkToolOperations::new(
        Arc::new(V0MessageAdmission::new(
            MACHINE.parse().unwrap(),
            Arc::new(OneKnownSession),
        )),
        port.clone(),
    );
    let served = ServedApi::tcp(&api_config(
        test_identity().build().with_link_tools(link_tools),
        directory.path(),
    ))
    .await;
    let args = json!({"callerSessionId":"reader","workspaceId":WORKSPACE,"threadId":THREAD});

    // Act
    let done = served.call_tool("thread_show", args.clone()).await;
    let mut errors = Vec::new();
    for _ in &failing {
        errors.push(served.call_tool("thread_show", args.clone()).await);
    }

    // Assert
    assert_ne!(done.is_error, Some(true), "{done:?}");
    assert_eq!(
        done.structured_content,
        Some(json!({"result":{"state":"answered","reply":reply(&found)},"text":SHOWN_THREAD}))
    );
    assert_eq!(text_of(&done), SHOWN_THREAD);
    for ((expected, _), error) in failing.iter().zip(errors) {
        assert_eq!(error.is_error, Some(true), "{expected}");
        let structured = error.structured_content.clone().unwrap();
        assert_eq!(structured["result"], *expected);
        let text = structured["text"].as_str().unwrap();
        assert!(text.len() > "Central answered.".len(), "{expected}: {text}");
        assert_eq!(text_of(&error), text, "the content text is the typed text");
    }
    let caller = SessionRef {
        machine_id: MACHINE.parse().unwrap(),
        session_id: "reader".parse().unwrap(),
    };
    let show = CentralToolPayload::Workspace(WorkspaceRequest {
        request_id: None,
        workspace_id: Some(WORKSPACE.parse().unwrap()),
        operation: WorkspaceOperation::Show {
            item: ItemRef::Thread(THREAD.parse().unwrap()),
        },
    });
    assert_eq!(port.submitted(), vec![(caller, show); failing.len() + 1]);
    assert_eq!(port.unused_outcomes(), 0);
    served.stop().await;
}

#[tokio::test]
async fn thread_show_refuses_removed_invalid_and_unadmitted_input_before_central() {
    // Arrange
    let port = Arc::new(FakeCentralToolPort::default());
    let directory = tempfile::tempdir().unwrap();
    let link_tools = LinkToolOperations::new(
        Arc::new(V0MessageAdmission::new(
            MACHINE.parse().unwrap(),
            Arc::new(OneKnownSession),
        )),
        port.clone(),
    );
    let served = ServedApi::tcp(&api_config(
        test_identity().build().with_link_tools(link_tools),
        directory.path(),
    ))
    .await;
    let args = json!({"callerSessionId":"reader","workspaceId":WORKSPACE,"threadId":THREAD});
    let with = |name: &str, value: Value| {
        let mut input = args.clone();
        input
            .as_object_mut()
            .unwrap()
            .insert(name.to_owned(), value);
        input
    };
    let without = |name: &str| {
        let mut input = args.clone();
        input.as_object_mut().unwrap().remove(name);
        input
    };
    let cases = [
        (
            with("includeRedacted", json!(true)),
            json!({"kind":"removedInput","field":"includeRedacted"}),
        ),
        (
            with("protocolVersion", json!(1)),
            json!({"kind":"removedInput","field":"protocolVersion"}),
        ),
        (
            with("projectId", Value::Null),
            json!({"kind":"removedInput","field":"projectQualifier"}),
        ),
        (
            with("project", json!("router")),
            json!({"kind":"removedInput","field":"projectQualifier"}),
        ),
        (
            without("callerSessionId"),
            json!({"kind":"invalidArguments"}),
        ),
        (without("workspaceId"), json!({"kind":"invalidArguments"})),
        (
            with("threadId", json!("not-a-uuid")),
            json!({"kind":"invalidArguments"}),
        ),
        (
            with("callerSessionId", json!("someone-else")),
            json!({"kind":"caller","reason":"unknownSession"}),
        ),
    ];

    for (input, code) in cases {
        // Act
        let result = served.call_tool("thread_show", input).await;

        // Assert
        assert_eq!(result.is_error, Some(true), "{code}");
        let structured = result.structured_content.clone().unwrap();
        assert_eq!(structured["result"], json!({"state":"refused","code":code}));
        assert_eq!(text_of(&result), structured["text"].as_str().unwrap());
    }
    assert!(
        port.submitted().is_empty(),
        "no refused input reaches Central"
    );
    served.stop().await;
}
