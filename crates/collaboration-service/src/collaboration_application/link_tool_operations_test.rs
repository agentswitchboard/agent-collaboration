use std::{
    future::{Future, ready},
    pin::Pin,
    sync::Arc,
};

use collaboration_protocol::central_tool_port::{
    CentralSubmission, CentralToolPayload, ItemRef, ReplyPayload, WorkspaceOperation,
    WorkspaceReply, WorkspaceRequest,
};
use collaboration_protocol::link_tool_result::{
    LinkRefusal, LinkToolResult, MessageCallerRefusal, MessageFrontRefusal,
};
use collaboration_protocol::message_tools::{SessionId, SessionRef};
use collaboration_protocol::workspace_tools::{ThreadId, ThreadShowArguments, WorkspaceId};
use hegel::generators as gs;
use serde_json::json;

use super::LinkToolOperations;
use crate::FakeCentralToolPort;
use crate::collaboration_application::message_operations::{
    MessageSessionDirectory, MessageSessionPresence, V0MessageAdmission,
};

const MACHINE: &str = "019a0000-0000-7000-8000-000000000051";
const CALLER: &str = "link-family-caller";

struct OneKnownSession;

impl MessageSessionDirectory for OneKnownSession {
    fn lookup<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>> {
        Box::pin(ready(if session_id.as_str() == CALLER {
            MessageSessionPresence::KnownOnce
        } else {
            MessageSessionPresence::UnknownSession
        }))
    }
}

fn link_tools(port: Arc<FakeCentralToolPort>) -> LinkToolOperations {
    LinkToolOperations::new(
        Arc::new(V0MessageAdmission::new(
            MACHINE.parse().unwrap(),
            Arc::new(OneKnownSession),
        )),
        port,
    )
}

fn workspace_reply(result: serde_json::Value) -> ReplyPayload {
    ReplyPayload::Workspace(
        serde_json::from_value::<WorkspaceReply>(json!({"result":result,"actors":[]})).unwrap(),
    )
}

/// Each answer the port can give a read, and whether an agent should see it as a success.
fn port_answers(workspace: &WorkspaceId, thread: &ThreadId) -> Vec<(ScriptedAnswer, bool)> {
    let thread_view = json!({"kind":"done","answer":{"kind":"item","item":{"kind":"thread","item":{
        "thread":{"threadId":thread,"areaId":"019a0000-0000-7000-8000-000000000052",
            "title":"A stored topic","creator":"019a0000-0000-7000-8000-000000000053","state":"open"},
        "me":null}}}});
    let answered = |result| Ok(CentralSubmission::Answered(workspace_reply(result)));
    vec![
        (answered(thread_view), true),
        (
            answered(json!({"kind":"notFound","answer":{"workspaceId":workspace,
                "item":{"kind":"thread","id":thread}}})),
            false,
        ),
        (
            answered(json!({"kind":"refused","answer":{"kind":"notMember","threadId":thread}})),
            false,
        ),
        (
            answered(json!({"kind":"refused","answer":{"kind":"workspaceNotAllowed"}})),
            false,
        ),
        (answered(json!({"kind":"centralUnavailable"})), false),
        (answered(json!({"kind":"conflict"})), false),
        (answered(json!({"kind":"outcomeUnknown"})), false),
        (Ok(CentralSubmission::ReadAnswerLost), false),
        (Err(LinkRefusal::CentralUnavailable), false),
        (
            Err(LinkRefusal::RouterNotReady {
                state: MessageFrontRefusal::Restarting,
            }),
            false,
        ),
    ]
}

type ScriptedAnswer = Result<CentralSubmission, LinkRefusal>;

fn expected_result(answer: ScriptedAnswer) -> LinkToolResult {
    match answer {
        Ok(CentralSubmission::Answered(reply)) => LinkToolResult::Answered { reply },
        Ok(CentralSubmission::ReadAnswerLost) => LinkToolResult::ReadAnswerLost,
        Ok(CentralSubmission::InOutbox {
            request_id,
            flush_at,
        }) => LinkToolResult::InOutbox {
            request_id,
            flush_at,
        },
        Ok(CentralSubmission::Uncertain { request_id }) => LinkToolResult::Uncertain { request_id },
        Err(code) => LinkToolResult::Refused { code },
    }
}

#[hegel::test(test_cases = 100)]
fn thread_show_submits_one_show_of_the_generated_thread_and_returns_central_s_answer(
    case: hegel::TestCase,
) {
    // Arrange
    let workspace: WorkspaceId = format!(
        "019f0000-0000-7000-8000-0000{:08x}",
        case.draw(gs::integers::<u32>())
    )
    .parse()
    .unwrap();
    let thread: ThreadId = format!(
        "019f0000-0000-7000-8001-0000{:08x}",
        case.draw(gs::integers::<u32>())
    )
    .parse()
    .unwrap();
    let answers = port_answers(&workspace, &thread);
    let pick = case.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(answers.len() - 1),
    );
    let (answer, success) = answers.into_iter().nth(pick).unwrap();
    let port = Arc::new(FakeCentralToolPort::answering([answer.clone()]));
    let operations = link_tools(port.clone());
    let input = ThreadShowArguments {
        caller_session_id: CALLER.parse().unwrap(),
        workspace_id: workspace.clone(),
        thread_id: thread.clone(),
    };

    // Act
    let output = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(operations.show_thread(input));

    // Assert
    let caller = SessionRef {
        machine_id: MACHINE.parse().unwrap(),
        session_id: CALLER.parse().unwrap(),
    };
    assert_eq!(
        port.submitted(),
        vec![(
            caller,
            CentralToolPayload::Workspace(WorkspaceRequest {
                request_id: None,
                workspace_id: Some(workspace),
                operation: WorkspaceOperation::Show {
                    item: ItemRef::Thread(thread),
                },
            })
        )],
        "exactly one Show of the thread itself, with no write ID"
    );
    assert_eq!(output.result, expected_result(answer));
    assert_eq!(output.result.succeeded(), success, "{output:?}");
    if success {
        assert!(
            output
                .text
                .starts_with("Thread: A stored topic\nState: open\n"),
            "{output:?}"
        );
    } else {
        assert!(output.text.len() > "Central answered.".len(), "{output:?}");
    }
}

#[tokio::test]
async fn a_caller_this_mac_cannot_admit_reaches_no_port_and_an_unwired_router_sends_nothing() {
    // Arrange
    let port = Arc::new(FakeCentralToolPort::default());
    let input = |caller: &str| ThreadShowArguments {
        caller_session_id: caller.parse().unwrap(),
        workspace_id: "019a0000-0000-7000-8000-000000000054".parse().unwrap(),
        thread_id: "019a0000-0000-7000-8000-000000000055".parse().unwrap(),
    };

    // Act
    let unknown = link_tools(port.clone())
        .show_thread(input("someone-else"))
        .await;
    let unwired = LinkToolOperations::default()
        .show_thread(input(CALLER))
        .await;

    // Assert
    assert_eq!(
        unknown.result,
        LinkToolResult::Refused {
            code: LinkRefusal::Caller {
                reason: MessageCallerRefusal::UnknownSession
            }
        }
    );
    assert_eq!(
        unwired.result,
        LinkToolResult::Refused {
            code: LinkRefusal::CentralUnavailable
        }
    );
    assert!(port.submitted().is_empty());
}

/// Counts every caller lookup, so a test can show admission was never consulted.
#[derive(Default)]
struct CountingSessions(std::sync::atomic::AtomicUsize);

impl MessageSessionDirectory for CountingSessions {
    fn lookup<'lookup>(
        &'lookup self,
        _session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(ready(MessageSessionPresence::UnknownSession))
    }
}

#[tokio::test]
async fn a_router_that_is_not_ready_says_so_before_judging_the_caller() {
    // Arrange
    let not_ready = LinkRefusal::RouterNotReady {
        state: MessageFrontRefusal::NotEnrolled,
    };
    let port = Arc::new(FakeCentralToolPort::not_ready(not_ready.clone()));
    let sessions = Arc::new(CountingSessions::default());
    let operations = LinkToolOperations::new(
        Arc::new(V0MessageAdmission::new(
            MACHINE.parse().unwrap(),
            sessions.clone(),
        )),
        port.clone(),
    );

    // Act
    let output = operations
        .show_thread(ThreadShowArguments {
            caller_session_id: "someone-else".parse().unwrap(),
            workspace_id: "019a0000-0000-7000-8000-000000000054".parse().unwrap(),
            thread_id: "019a0000-0000-7000-8000-000000000055".parse().unwrap(),
        })
        .await;

    // Assert
    assert_eq!(output.result, LinkToolResult::Refused { code: not_ready });
    assert!(port.submitted().is_empty(), "nothing is submitted");
    assert_eq!(
        sessions.0.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "admission is not consulted"
    );
}
