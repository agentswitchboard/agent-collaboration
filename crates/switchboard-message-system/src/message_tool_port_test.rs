use collaboration_protocol::workspace_tools::{
    ThreadShowOutput, ThreadShowRefusal, ThreadShowResult,
};
use hegel::generators as gs;
use workspace_model::{
    ItemView, RefusalReason, Thread, ThreadState, WorkspaceAnswer, WorkspaceResult,
};

use super::*;

#[hegel::test(test_cases = 80)]
fn thread_show_keeps_generated_targets_and_classifies_each_read_answer(case: hegel::TestCase) {
    let workspace_suffix = case.draw(gs::integers::<u32>());
    let thread_suffix = case.draw(gs::integers::<u32>());
    let workspace: WorkspaceId = format!("019f0000-0000-7000-8000-0000{workspace_suffix:08x}")
        .parse()
        .unwrap();
    let thread: ThreadId = format!("019f0000-0000-7000-8001-0000{thread_suffix:08x}")
        .parse()
        .unwrap();

    let expected = WorkspaceRequest {
        request_id: None,
        workspace_id: Some(workspace.clone()),
        operation: WorkspaceOperation::Show {
            item: ItemRef::Thread(thread.clone()),
        },
    };
    let actual = thread_show_submission(workspace.clone(), thread.clone());
    assert!(
        matches!(actual, SubmissionPayload::Workspace(ref request) if request == &expected),
        "Show must submit its workspace and thread without a write id: {actual:?}"
    );

    let found = Thread {
        thread_id: thread.clone(),
        area_id: "019f0000-0000-7000-8002-000000000001".parse().unwrap(),
        title: format!("Thread {thread_suffix}").try_into().unwrap(),
        creator: "019f0000-0000-7000-8002-000000000002".parse().unwrap(),
        state: ThreadState::Open,
    };
    for result in [
        WorkspaceResult::Done(WorkspaceAnswer::Item {
            item: ItemView::Thread {
                thread: found,
                me: None,
            },
        }),
        WorkspaceResult::NotFound {
            workspace_id: workspace,
            item: ItemRef::Thread(thread),
        },
        WorkspaceResult::Refused(RefusalReason::WorkspaceNotAllowed),
        WorkspaceResult::CentralUnavailable,
    ] {
        let reply = WorkspaceReply {
            result,
            actors: vec![],
        };
        let shown = ThreadShowOutput::from(thread_show_answer(Submission::Answered(
            ReplyPayload::Workspace(reply.clone()),
        )));
        assert_eq!(
            shown.result,
            ThreadShowResult::Answered {
                reply: Box::new(reply)
            }
        );
    }
    assert_eq!(
        ThreadShowOutput::from(thread_show_answer(Submission::ReadAnswerLost)).result,
        ThreadShowResult::ReadAnswerLost
    );
    assert_eq!(
        ThreadShowOutput::from(thread_show_answer(Submission::Refused(
            SubmitRefusal::CentralUnavailable
        )))
        .result,
        ThreadShowResult::Refused {
            code: ThreadShowRefusal::CentralUnavailable
        }
    );
}
