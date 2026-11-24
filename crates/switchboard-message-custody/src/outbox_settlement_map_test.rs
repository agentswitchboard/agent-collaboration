use super::{central_status_reason, outcome_from_link_reply};
use central_link_protocol::{ReplyPayload, RequestRefusal};
use switchboard_machine_core_store::{
    InvalidOutboxOutcome, OutboxAccepted, OutboxCentralRefusal, OutboxQueuedWrite, OutboxRefusal,
    OutboxWriteOutcome,
};
use workspace_ids::{MessageId, PostId, SessionRef, ThreadId};
use workspace_model::{
    ActorView, ChangePosition, Identity, ItemRef, PostText, RefusalReason, Timestamp,
    WorkspaceAnswer, WorkspaceOperation, WorkspaceReply, WorkspaceRequest, WorkspaceResult,
    central_mail::{
        AutomationState, CentralMailAnswer, CentralMailRefusal, CentralMailResult, MessageDelivery,
        MessageStatus, NoticeCadence, SendMessageRequest,
    },
};

const WORKSPACE: &str = "019f0000-0000-7000-8000-000000000031";
const THREAD: &str = "019f0000-0000-7000-8000-000000000032";
const OTHER: &str = "019f0000-0000-7000-8000-000000000033";
const WRITE: &str = "019f0000-0000-7000-8000-000000000034";
const RETURNED: &str = "019f0000-0000-7000-8000-000000000035";

fn post(workspace_id: Option<&str>) -> OutboxQueuedWrite {
    OutboxQueuedWrite::Workspace(WorkspaceRequest {
        request_id: Some(WRITE.parse().unwrap()),
        workspace_id: workspace_id.map(|id| id.parse().unwrap()),
        operation: WorkspaceOperation::ThreadPost {
            thread_id: THREAD.parse().unwrap(),
            body: PostText::try_from("queued post".to_owned()).unwrap(),
            addressees: Vec::new(),
        },
    })
}

fn recipient() -> SessionRef {
    SessionRef {
        machine_id: WORKSPACE.parse().unwrap(),
        session_id: "recipient-session".parse().unwrap(),
    }
}

fn direct_write() -> OutboxQueuedWrite {
    OutboxQueuedWrite::SendMessage {
        request_id: WRITE.parse().unwrap(),
        message: SendMessageRequest {
            to: recipient(),
            body: PostText::try_from("direct message".to_owned()).unwrap(),
            delivery: MessageDelivery::Queue,
            reply_to: None,
        },
    }
}

fn workspace_reply(result: WorkspaceResult) -> ReplyPayload {
    ReplyPayload::Workspace(WorkspaceReply {
        result,
        actors: Vec::new(),
    })
}

fn expected_workspace_refusal(
    write: &OutboxQueuedWrite,
    reason: RefusalReason,
) -> OutboxWriteOutcome {
    OutboxWriteOutcome::from_refusal(
        write,
        OutboxRefusal::Central(OutboxCentralRefusal::Workspace(reason)),
    )
    .unwrap()
}

fn expected_mail_refusal(
    write: &OutboxQueuedWrite,
    reason: CentralMailRefusal,
) -> OutboxWriteOutcome {
    OutboxWriteOutcome::from_refusal(
        write,
        OutboxRefusal::Central(OutboxCentralRefusal::Mail(reason)),
    )
    .unwrap()
}

#[test]
fn a_created_post_retains_its_position_and_actors_and_only_settles_a_thread_post() {
    let write = post(Some(WORKSPACE));
    let post_id: PostId = RETURNED.parse().unwrap();
    let position = ChangePosition::try_from(37_u64).unwrap();
    let actors = vec![ActorView {
        actor_identity_id: THREAD.parse().unwrap(),
        identity: Identity::AgentSession {
            session: recipient(),
        },
    }];
    let reply = ReplyPayload::Workspace(WorkspaceReply {
        result: WorkspaceResult::Done(WorkspaceAnswer::Created {
            item: ItemRef::Post(post_id.clone()),
            position,
        }),
        actors: actors.clone(),
    });
    assert_eq!(
        outcome_from_link_reply(&write, reply.clone()),
        OutboxWriteOutcome::from_accepted(
            &write,
            OutboxAccepted::ThreadPost {
                post_id,
                position,
                actors
            }
        )
    );
    assert_eq!(
        outcome_from_link_reply(&direct_write(), reply),
        Err(InvalidOutboxOutcome)
    );
}

#[test]
fn a_created_non_post_or_other_workspace_success_does_not_settle() {
    let write = post(Some(WORKSPACE));
    for answer in [
        WorkspaceAnswer::Created {
            item: ItemRef::Thread(THREAD.parse().unwrap()),
            position: ChangePosition::try_from(37_u64).unwrap(),
        },
        WorkspaceAnswer::MessageStatus {
            status: MessageStatus::NotFound {},
        },
        WorkspaceAnswer::Unchanged {
            item: ItemRef::Thread(THREAD.parse().unwrap()),
        },
    ] {
        assert_eq!(
            outcome_from_link_reply(&write, workspace_reply(WorkspaceResult::Done(answer))),
            Err(InvalidOutboxOutcome)
        );
    }
}

#[test]
fn workspace_not_found_requires_the_exact_workspace_and_thread() {
    let write = post(Some(WORKSPACE));
    let thread = ItemRef::Thread(THREAD.parse().unwrap());
    let matching = workspace_reply(WorkspaceResult::NotFound {
        workspace_id: WORKSPACE.parse().unwrap(),
        item: thread.clone(),
    });
    assert_eq!(
        outcome_from_link_reply(&write, matching.clone()),
        OutboxWriteOutcome::from_workspace_not_found(
            &write,
            &write.request_scope(),
            thread.clone()
        )
    );
    assert_eq!(
        outcome_from_link_reply(&post(None), matching.clone()),
        Err(InvalidOutboxOutcome)
    );
    assert_eq!(
        outcome_from_link_reply(&direct_write(), matching),
        Err(InvalidOutboxOutcome)
    );
    for result in [
        WorkspaceResult::NotFound {
            workspace_id: OTHER.parse().unwrap(),
            item: thread,
        },
        WorkspaceResult::NotFound {
            workspace_id: WORKSPACE.parse().unwrap(),
            item: ItemRef::Thread(OTHER.parse().unwrap()),
        },
        WorkspaceResult::NotFound {
            workspace_id: WORKSPACE.parse().unwrap(),
            item: ItemRef::Post(THREAD.parse().unwrap()),
        },
    ] {
        assert_eq!(
            outcome_from_link_reply(&write, workspace_reply(result)),
            Err(InvalidOutboxOutcome)
        );
    }
}

#[test]
fn the_generic_not_found_refusal_uses_the_correlated_claims_scope() {
    let thread = ItemRef::Thread(THREAD.parse().unwrap());
    for write in [post(Some(WORKSPACE)), post(None)] {
        let reply = workspace_reply(WorkspaceResult::Refused(RefusalReason::NotFound {
            item: thread.clone(),
        }));
        let expected = OutboxWriteOutcome::from_workspace_not_found(
            &write,
            &write.request_scope(),
            thread.clone(),
        )
        .unwrap();
        assert_eq!(outcome_from_link_reply(&write, reply), Ok(expected));
    }
    let wrong_item = workspace_reply(WorkspaceResult::Refused(RefusalReason::NotFound {
        item: ItemRef::Thread(OTHER.parse().unwrap()),
    }));
    assert_eq!(
        outcome_from_link_reply(&post(Some(WORKSPACE)), wrong_item),
        Err(InvalidOutboxOutcome)
    );
}

#[test]
fn workspace_domain_refusals_retain_their_reason_and_validate_the_target() {
    let write = post(Some(WORKSPACE));
    let same = || THREAD.parse::<ThreadId>().unwrap();
    let different = || OTHER.parse::<ThreadId>().unwrap();
    for reason in [
        RefusalReason::RequestIdReused,
        RefusalReason::WorkspaceNotAllowed,
        RefusalReason::UnknownActor {
            actor_identity_id: OTHER.parse().unwrap(),
        },
        RefusalReason::UnheldAddressee,
        RefusalReason::InvalidInput {
            field: "body".to_owned(),
        },
        RefusalReason::ThreadResolved { thread_id: same() },
        RefusalReason::NotMember { thread_id: same() },
        RefusalReason::NotParticipant { thread_id: same() },
    ] {
        let result = workspace_reply(WorkspaceResult::Refused(reason.clone()));
        assert_eq!(
            outcome_from_link_reply(&write, result.clone()),
            Ok(expected_workspace_refusal(&write, reason))
        );
        assert_eq!(
            outcome_from_link_reply(&direct_write(), result),
            Err(InvalidOutboxOutcome)
        );
    }
    for reason in [
        RefusalReason::MachineRevoked,
        RefusalReason::ThreadResolved {
            thread_id: different(),
        },
        RefusalReason::NotMember {
            thread_id: different(),
        },
        RefusalReason::NotParticipant {
            thread_id: different(),
        },
    ] {
        assert_eq!(
            outcome_from_link_reply(&write, workspace_reply(WorkspaceResult::Refused(reason))),
            Err(InvalidOutboxOutcome)
        );
    }
}

#[test]
fn workspace_conflict_unknown_and_unavailable_leave_the_claim_recoverable() {
    let write = post(Some(WORKSPACE));
    for result in [
        WorkspaceResult::Conflict,
        WorkspaceResult::OutcomeUnknown,
        WorkspaceResult::CentralUnavailable,
    ] {
        assert_eq!(
            outcome_from_link_reply(&write, workspace_reply(result)),
            Err(InvalidOutboxOutcome)
        );
    }
}

#[test]
fn a_queued_mail_answer_retains_the_message_id_and_only_settles_a_direct_send() {
    let write = direct_write();
    let message_id: MessageId = RETURNED.parse().unwrap();
    let reply = ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued {
        message_id: message_id.clone(),
    }));
    let expected =
        OutboxWriteOutcome::from_accepted(&write, OutboxAccepted::SendMessage { message_id })
            .unwrap();
    assert_eq!(outcome_from_link_reply(&write, reply.clone()), Ok(expected));
    assert_eq!(
        outcome_from_link_reply(&post(Some(WORKSPACE)), reply),
        Err(InvalidOutboxOutcome)
    );
}

#[test]
fn another_mail_operations_success_does_not_settle_a_direct_send() {
    let write = direct_write();
    for answer in [
        CentralMailAnswer::NoticeCadenceSet {
            cadence: NoticeCadence::M15,
        },
        CentralMailAnswer::WakeSet {
            wake_id: RETURNED.parse().unwrap(),
            due_at: Timestamp::from_timestamp_millis(1_763_380_800_000).unwrap(),
        },
        CentralMailAnswer::WakeCancelled {
            wake_id: RETURNED.parse().unwrap(),
        },
        CentralMailAnswer::AutomationSet {
            automation_id: RETURNED.parse().unwrap(),
            next_due_at: None,
            state: AutomationState::Active,
        },
        CentralMailAnswer::AutomationDisabled {
            automation_id: RETURNED.parse().unwrap(),
        },
    ] {
        assert_eq!(
            outcome_from_link_reply(&write, ReplyPayload::Mail(CentralMailResult::Done(answer))),
            Err(InvalidOutboxOutcome)
        );
    }
}

#[test]
fn mail_permanent_refusals_validate_the_recipient_and_operation() {
    let write = direct_write();
    for reason in [
        CentralMailRefusal::UnknownSession {
            target: recipient(),
        },
        CentralMailRefusal::TimeOutOfRange {
            field: "anchorAt".to_owned(),
        },
        CentralMailRefusal::Workspace(RefusalReason::RequestIdReused),
        CentralMailRefusal::Workspace(RefusalReason::WorkspaceNotAllowed),
    ] {
        let reply = ReplyPayload::Mail(CentralMailResult::Refused(reason.clone()));
        assert_eq!(
            outcome_from_link_reply(&write, reply.clone()),
            Ok(expected_mail_refusal(&write, reason))
        );
        assert_eq!(
            outcome_from_link_reply(&post(Some(WORKSPACE)), reply),
            Err(InvalidOutboxOutcome)
        );
    }
    let mut wrong = recipient();
    wrong.session_id = "other-session".parse().unwrap();
    for reason in [
        CentralMailRefusal::UnknownSession { target: wrong },
        CentralMailRefusal::TimeOutOfRange {
            field: "sentAt".to_owned(),
        },
        CentralMailRefusal::Workspace(RefusalReason::MachineRevoked),
        CentralMailRefusal::WakeNotFound {
            wake_id: OTHER.parse().unwrap(),
        },
    ] {
        assert_eq!(
            outcome_from_link_reply(
                &write,
                ReplyPayload::Mail(CentralMailResult::Refused(reason))
            ),
            Err(InvalidOutboxOutcome)
        );
    }
}

#[test]
fn mail_conflict_unknown_and_unavailable_leave_the_claim_recoverable() {
    let write = direct_write();
    for result in [
        CentralMailResult::Conflict,
        CentralMailResult::OutcomeUnknown,
        CentralMailResult::CentralUnavailable,
    ] {
        assert_eq!(
            outcome_from_link_reply(&write, ReplyPayload::Mail(result)),
            Err(InvalidOutboxOutcome)
        );
    }
}

#[test]
fn request_status_retains_both_central_refusal_families() {
    let workspace = RefusalReason::WorkspaceNotAllowed;
    let mail = CentralMailRefusal::UnknownSession {
        target: recipient(),
    };
    assert_eq!(
        central_status_reason(RequestRefusal::Workspace(workspace.clone())),
        OutboxRefusal::Central(OutboxCentralRefusal::Workspace(workspace))
    );
    assert_eq!(
        central_status_reason(RequestRefusal::Mail(mail.clone())),
        OutboxRefusal::Central(OutboxCentralRefusal::Mail(mail))
    );
}
