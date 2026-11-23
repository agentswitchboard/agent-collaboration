#![expect(
    clippy::unwrap_used,
    reason = "The fixture fails directly if the setup or a peer response is unusable."
)]

use central_link_client::{
    ImmediateRequestCommand, LinkLaneEvent, RequestStatusCommand, ValidatedDeliveryOffer,
};
use central_link_protocol::{
    DeliveryMessage, ReplyPayload, RequestPayload, RequestScope, RequestStatusQuery,
};
use std::sync::Arc;
use switchboard_machine_core_store::{
    InboxStore, MachineLocalStore, OutboxCancellation, OutboxQueuedWrite, OutboxStorage,
    PendingWriteState,
};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
};
use workspace_ids::{MachineId, MessageId, SessionRef};
use workspace_model::{
    Caller, ListQuery, ResolvedSender, Timestamp, WorkspaceOperation,
    central_mail::{
        CentralMailAnswer, CentralMailResult, MessageBody, MessageOutcome, MessageRecord,
        MessageRefusal, MessageStatus,
    },
};

use super::{
    batch_replay::{
        KnownRecipient, NoFlushJitter, ScriptedBatchLane, ScriptedCommand, new_admission,
    },
    outbox_timing::{direct_message, manual_clock, open_outbox, thread_post},
};

async fn status_fixture() -> (tempfile::TempDir, CustodyLink, Arc<ScriptedBatchLane>) {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("machine_local.sqlite");
    let clock = manual_clock(1_763_380_780_000);
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let lane = Arc::new(ScriptedBatchLane::new(
        new_admission(machine.clone(), clock.clone()),
        clock.clone(),
        Default::default(),
    ));
    let outbox = OutboxWork::new(Box::new(open_outbox(&database).await), clock.clone());
    let inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(&database).await.unwrap()),
        machine,
        Arc::new(KnownRecipient),
        clock.clone(),
    );
    let custody = CustodyLink::new(lane.clone(), clock, Arc::new(NoFlushJitter), outbox, inbox);
    (root, custody, lane)
}

#[tokio::test]
async fn request_status_and_unsent_text_are_scoped_to_the_original_caller_and_store() {
    let (_root, mut custody, lane) = status_fixture().await;
    let item = thread_post(90);
    let OutboxQueuedWrite::Workspace(request) = item.payload else {
        panic!("fixture must be a thread post");
    };
    let workspace_id = request.workspace_id.clone().unwrap();
    assert!(matches!(
        custody
            .submit(item.caller.clone(), SubmissionPayload::Workspace(request))
            .await,
        Submission::InOutbox(_)
    ));

    let query = RequestStatusQuery {
        request_id: item.request,
        scope: RequestScope::Workspace(workspace_id),
    };
    assert!(matches!(
        custody
            .request_status(item.caller.clone(), query.clone())
            .await,
        CustodyRequestStatus::InOutbox { .. }
    ));
    assert!(
        lane.take_calls().is_empty(),
        "a valid local answer takes no link frame"
    );
    let other = Caller::Session(SessionRef {
        machine_id: "019f0000-0000-7000-8000-000000000011".parse().unwrap(),
        session_id: "different-session".parse().unwrap(),
    });
    assert!(
        matches!(custody.pending_writes(&item.caller).await.unwrap().as_slice(),
        [pending] if matches!(&pending.state, PendingWriteState::Unsent { body } if body.as_str() == "thread post"))
    );
    assert!(custody.pending_writes(&other).await.unwrap().is_empty());

    let wrong_scope = RequestStatusQuery {
        scope: RequestScope::Organization,
        ..query.clone()
    };
    assert_eq!(
        custody
            .request_status(item.caller.clone(), wrong_scope.clone())
            .await,
        CustodyRequestStatus::NotFound
    );
    assert_eq!(
        custody.request_status(other.clone(), query.clone()).await,
        CustodyRequestStatus::NotFound
    );
    let calls = lane.take_calls();
    assert!(matches!(calls.as_slice(), [
        ScriptedCommand::RequestStatus(RequestStatusCommand { caller: first, query: first_query, .. }),
        ScriptedCommand::RequestStatus(RequestStatusCommand { caller: second, query: second_query, .. }),
    ] if first == &item.caller && first_query == &wrong_scope && second == &other && second_query == &query));

    lane.admitted.send_replace(None);
    assert_eq!(
        custody.request_status(other, query).await,
        CustodyRequestStatus::StatusUnavailable
    );
}

#[tokio::test]
async fn message_status_is_one_immediate_sender_scoped_read() {
    let (_root, mut custody, lane) = status_fixture().await;
    let caller = thread_post(91).caller;
    let message_id: MessageId = "019f0000-0000-7000-8000-000000000099".parse().unwrap();

    assert_eq!(
        custody
            .message_status(caller.clone(), message_id.clone())
            .await,
        Ok(Some(MessageStatus::NotFound {}))
    );

    let calls = lane.take_calls();
    assert!(
        matches!(calls.as_slice(), [ScriptedCommand::ImmediateRequest(ImmediateRequestCommand { request, .. })]
        if request.caller() == &caller
        && matches!(request.payload(), RequestPayload::Workspace(payload)
            if payload.request_id.is_none() && payload.workspace_id.is_none()
            && matches!(&payload.operation, WorkspaceOperation::List { query: ListQuery::MessageStatus { message_id: sent }, .. } if sent == &message_id)))
    );
    assert!(custody.pending_writes(&caller).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_retained_central_notice_recovers_the_senders_original_acceptance() {
    let (root, mut custody, lane) = status_fixture().await;
    let item = direct_message(92);
    let created_at = 1_763_380_000_000;
    let mut store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    store.insert(&item, created_at).await.unwrap();
    let claim = store
        .claim_next_batch(item.flush_at_ms, 1, created_at + 1, created_at + 1)
        .await
        .unwrap();
    assert_eq!(claim.healthy.writes.len(), 1);
    let expired = store
        .return_unanswered(std::slice::from_ref(&item.request), created_at + 900_000)
        .await
        .unwrap();
    assert_eq!(expired.healthy.len(), 1);
    let query = RequestStatusQuery {
        request_id: item.request.clone(),
        scope: RequestScope::Organization,
    };
    assert_eq!(
        custody
            .request_status(item.caller.clone(), query.clone())
            .await,
        CustodyRequestStatus::Ended {
            reason: OutboxCancellation::NotConfirmedWithin15Minutes
        }
    );

    let Caller::Session(target) = item.caller.clone() else {
        panic!("the sender fixture is a session");
    };
    let message_id: MessageId = "019f0000-0000-7000-8000-0000000000a1".parse().unwrap();
    let notice_id: MessageId = "019f0000-0000-7000-8000-0000000000a2".parse().unwrap();
    let outcome = MessageOutcome::Refused {
        reason: MessageRefusal::UnknownSession,
    };
    let notice = MessageRecord::queued_central(
        notice_id.clone(),
        target,
        MessageBody::DeliveryOutcome {
            message_id: message_id.clone(),
            request_id: item.request.clone(),
            outcome,
        },
        Timestamp::from_timestamp_millis(created_at + 900_000).unwrap(),
    );
    let admitted = lane.admitted.borrow().clone().unwrap();
    lane.push_event(LinkLaneEvent::Deliver {
        connection: admitted.connection,
        offer: ValidatedDeliveryOffer::new(Box::new(DeliveryMessage::System(notice)), None)
            .unwrap(),
        sender: ResolvedSender::Central {},
    });
    custody.observe_next_event().await.unwrap();

    assert_eq!(
        custody.request_status(item.caller.clone(), query).await,
        CustodyRequestStatus::Answered(ReplyPayload::Mail(CentralMailResult::Done(
            CentralMailAnswer::Queued {
                message_id: message_id.clone()
            }
        )))
    );
    assert!(
        matches!(custody.sender_outcome(&item.caller, &item.request).await.unwrap(),
        Some(retained) if retained.inbox_message_id == notice_id && retained.message_id == message_id && retained.outcome == outcome)
    );
    assert!(lane.take_calls().is_empty());
}
