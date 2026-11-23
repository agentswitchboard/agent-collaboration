use central_link_protocol::{ReplyPayload, RequestPayload, RequestScope, RequestStatusQuery};
use router_clock::ClockInstant;
use std::time::Duration;
use switchboard_machine_core_store::{
    InboxStore, MachineLocalStore, OutboxCentralRefusal, OutboxQueuedWrite, OutboxRefusal,
};
use switchboard_message_custody::{
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
};
use workspace_model::central_mail::{CentralMailOperation, CentralMailRefusal, CentralMailResult};

use super::{
    batch_limits::limits_fixture,
    outbox_timing::direct_message,
    worker_orphans::{flush_event, start_worker},
};

#[tokio::test]
async fn local_and_remote_messages_share_the_minute_path_and_central_can_refuse_local() {
    let (root, mut custody, lane, clock) = limits_fixture().await;
    let local = direct_message(1);
    let remote = direct_message(2);
    let OutboxQueuedWrite::SendMessage {
        message: local_message,
        ..
    } = local.payload
    else {
        panic!("local direct fixture required");
    };
    let mut remote_message = local_message.clone();
    remote_message.to.machine_id = "019f0000-0000-7000-8000-000000000012".parse().unwrap();
    lane.refuse_messages_to(local_message.to.machine_id.clone());

    for (request, message) in [
        (local.request.clone(), local_message.clone()),
        (remote.request.clone(), remote_message.clone()),
    ] {
        let submission = custody
            .submit(
                local.caller.clone(),
                SubmissionPayload::SendMessage {
                    request: request.clone(),
                    message,
                },
            )
            .await;
        assert!(
            matches!(submission, Submission::InOutbox(receipt) if receipt.id == request
            && receipt.flush_at.unix_millis() == local.flush_at_ms)
        );
    }
    assert!(
        custody
            .waiting_for_session(&local_message.to)
            .await
            .unwrap()
            .is_empty(),
        "a same-Mac message reached the session inbox without Central's offer"
    );
    assert!(lane.batch_commands().is_empty());

    let (calls, stop, mut events, running) = start_worker(custody);
    let wake = ClockInstant::ZERO.after(Duration::from_secs(20));
    clock.not_due_check_now(wake).await;
    assert!(
        lane.batch_commands().is_empty(),
        "no wire batch before the minute mark"
    );
    clock.advance(Duration::from_secs(20)).unwrap();
    let report = flush_event(&mut events, "the same-Mac and remote message outcomes").await;
    assert!(report.issues.is_empty());
    assert_eq!(report.answered.len(), 2);

    let commands = lane.batch_commands();
    let [batch] = commands.as_slice() else {
        panic!("both writes must use one batch");
    };
    let members = batch.requests.requests();
    let expected = [
        (local.request.clone(), &local_message),
        (remote.request, &remote_message),
    ];
    assert_eq!(members.len(), expected.len());
    for (member, (expected_id, source)) in members.iter().zip(expected) {
        assert_eq!(member.caller(), &local.caller);
        assert!(
            matches!(member.payload(), RequestPayload::Mail(payload) if payload.request_id == expected_id
            && matches!(&payload.operation, CentralMailOperation::SendMessage { to, body, delivery, reply_to, sent_at }
                if to == &source.to && body == &source.body && delivery == &source.delivery && reply_to == &source.reply_to
                && sent_at.timestamp_millis() == local.flush_at_ms - 20_000))
        );
    }

    let refusal = CentralMailRefusal::UnknownSession {
        target: local_message.to.clone(),
    };
    assert!(
        report
            .answered
            .iter()
            .any(|answer| answer.request == local.request
                && answer.reply == ReplyPayload::Mail(CentralMailResult::Refused(refusal.clone())))
    );
    let query = RequestStatusQuery {
        request_id: local.request,
        scope: RequestScope::Organization,
    };
    assert_eq!(
        calls.request_status(local.caller, query).await,
        CustodyRequestStatus::Refused(OutboxRefusal::Central(OutboxCentralRefusal::Mail(refusal)))
    );
    assert!(
        calls
            .waiting_for_session(local_message.to.clone())
            .await
            .unwrap()
            .is_empty(),
        "a Central refusal cannot create a same-Mac inbox row"
    );
    let mut inbox = InboxStore::new(
        MachineLocalStore::open(&root.path().join("machine_local.sqlite"))
            .await
            .unwrap(),
    );
    assert!(inbox.pending_receipts().await.unwrap().healthy.is_empty());
    stop.send_replace(true);
    running.await.unwrap();
}
