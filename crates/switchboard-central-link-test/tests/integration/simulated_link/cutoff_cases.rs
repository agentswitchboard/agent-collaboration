use central_link_protocol::ReplyPayload;
use central_storage::SqlValue;
use switchboard_machine_core_store::{
    MacOutbox, NewOutboxItem, OutboxAccepted, OutboxCancellation, OutboxInsert, OutboxQueuedWrite,
    OutgoingOutcome,
};

use super::{
    CustodyRequestStatus, CustodyTaskEvent, DataFrame, Duration, FlushJitter, FrameKind, LinkFrame,
    MachineLocalStore, NetworkCustody, OutboxReceipt, PostText, RuntimeClock, SendMessageRequest,
    next_mark_after, queued_message, request_id, run_link_case, shared_time_at, wake_for_mark,
};
use crate::simulated_link::packet_flight::decode_segment;

#[test]
fn held_second_reply_crosses_cutoff_without_changing_how_either_answer_is_read() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let anchor = case.link.admission.clock_anchor;
        let now = case.link.clock.monotonic_now();
        let mark = next_mark_after(shared_time_at(anchor, now));
        let wake = wake_for_mark(anchor, mark, case.jitter.jitter_millis(mark));
        let cutoff = wake + Duration::from_millis(1);
        let cutoff_ms = shared_time_at(anchor, cutoff).unix_millis();
        // Seed already-aged writes through the normal outbox store so both
        // expire 1ms after this mark's first send opportunity.
        let created_at_ms = cutoff_ms - 15 * 60 * 1_000;
        let caller = case.caller();
        let database = case
            .link
            .central
            .root_path()
            .join("simulated-custody.sqlite");
        let mut reader = MachineLocalStore::open(&database).await.unwrap();
        let mut outgoing = MacOutbox::new(&mut reader);
        let requests = [request_id(1), request_id(2)];
        for (index, request) in requests.iter().enumerate() {
            let write = NewOutboxItem {
                request: request.clone(),
                caller: caller.clone(),
                payload: OutboxQueuedWrite::SendMessage {
                    request_id: request.clone(),
                    message: SendMessageRequest {
                        to: case.session.clone(),
                        body: PostText::try_from(format!("held cutoff batch member {index}"))
                            .unwrap(),
                        delivery: super::MessageDelivery::Auto,
                        reply_to: None,
                    },
                },
                flush_at_ms: mark.unix_millis(),
            };
            assert_eq!(
                outgoing.insert(&write, created_at_ms).await.unwrap(),
                OutboxInsert::Stored
            );
            assert_eq!(
                case.status(request.clone()).await,
                CustodyRequestStatus::InOutbox { flush_at: mark }
            );
        }

        let processed = case.link.read_probe.observe_next_read_turn();
        let after = case.link.network.hold_after(FrameKind::WriteAnswer, 1);
        case.first_flush(&OutboxReceipt {
            id: requests.first().unwrap().clone(),
            flush_at: mark,
        })
        .await;
        let batch = case.link.network.seen(after, FrameKind::Batch).await;
        let LinkFrame::Data(DataFrame::Batch { requests: members }) = &batch.frame else {
            panic!("expected one two-member write batch");
        };
        let [first_member, second_member] = members.requests() else {
            panic!("both writes must leave in the same frame");
        };
        assert!(
            members
                .requests()
                .iter()
                .map(|member| member.payload().write_request_id().unwrap())
                .eq(requests.iter())
        );
        assert_eq!(batch.sent_at, wake);

        let first = case.link.network.seen(after, FrameKind::WriteAnswer).await;
        let second = case
            .link
            .network
            .seen_nth(after, FrameKind::WriteAnswer, 1)
            .await;
        assert_eq!(first.connection_port, batch.connection_port);
        assert_eq!(second.connection_port, batch.connection_port);
        assert!(
            matches!(&first.frame, LinkFrame::Data(DataFrame::Reply { correlation_id, .. })
            if correlation_id == first_member.correlation_id())
        );
        assert!(
            matches!(&second.frame, LinkFrame::Data(DataFrame::Reply { correlation_id, .. })
            if correlation_id == second_member.correlation_id())
        );
        // A full reply was read and the real driver re-entered its socket read
        // loop. B is still withheld, so A was processed at the frozen C-1ms.
        assert_eq!(
            decode_segment(&processed.await.unwrap(), false),
            std::slice::from_ref(&first.frame)
        );
        assert_eq!(
            case.link.clock.monotonic_now() + Duration::from_millis(1),
            cutoff
        );
        for request in &requests {
            assert_eq!(
                case.status(request.clone()).await,
                CustodyRequestStatus::Sent
            );
        }
        assert!(
            case.events.try_recv().is_err(),
            "no ending or exposed answer before the shared cutoff"
        );
        assert!(outgoing.outgoing_for(&caller, 10).await.unwrap().is_empty());

        case.advance_to(cutoff);
        let ended = case.next_write_report().await;
        assert_eq!(case.link.clock.monotonic_now(), cutoff);
        assert!(ended.issues.is_empty() && ended.answered.is_empty());
        assert_eq!(ended.ended.len(), requests.len());
        for request in &requests {
            assert_eq!(
                ended
                    .ended
                    .iter()
                    .filter(|event| event.caller == caller
                        && &event.request_id == request
                        && event.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
                    .count(),
                1,
                "each held member must end as not confirmed exactly once at its cutoff"
            );
            assert_eq!(
                case.status(request.clone()).await,
                CustodyRequestStatus::Ended {
                    reason: OutboxCancellation::NotConfirmedWithin15Minutes,
                }
            );
        }
        assert!(outgoing.outgoing_for(&caller, 10).await.unwrap().is_empty());

        let late = cutoff + Duration::from_millis(1);
        case.advance_to(late);
        let processed = case.link.read_probe.observe_next_read_turn();
        case.link.network.release_and_wait().await;
        let late_bytes = processed.await.unwrap();
        assert_eq!(
            decode_segment(&late_bytes, false).first(),
            Some(&second.frame)
        );
        assert_eq!(case.link.clock.monotonic_now(), late);
        for (request, wire) in requests.iter().zip([&first, &second]) {
            let LinkFrame::Data(DataFrame::Reply { result, .. }) = &wire.frame else {
                panic!("expected Central's canonical write reply");
            };
            let checked: &ReplyPayload = result;
            loop {
                let status = case.status(request.clone()).await;
                if let CustodyRequestStatus::Answered(answer) = status {
                    assert_eq!(&answer, checked);
                    break;
                }
                assert_eq!(
                    status,
                    CustodyRequestStatus::Ended {
                        reason: OutboxCancellation::NotConfirmedWithin15Minutes,
                    }
                );
                case.link.network.step().await;
            }
            let message = queued_message(wire);
            let history = outgoing.outgoing_for(&caller, 10).await.unwrap();
            let matching = history
                .iter()
                .filter(|row| &row.request_id == request)
                .collect::<Vec<_>>();
            let [row] = matching.as_slice() else {
                panic!(
                    "the outgoing list must show exactly one row for {request} after status learns its answer"
                );
            };
            assert_eq!(
                row.outcome,
                OutgoingOutcome::Answered(OutboxAccepted::SendMessage {
                    message_id: message.clone()
                })
            );
            assert_eq!(
                row.answered_at_ms, None,
                "a late outcome has no answer time"
            );
        }
        assert_eq!(outgoing.outgoing_for(&caller, 10).await.unwrap().len(), 2);

        case.advance_to_next_mark().await;
        case.wait_until_next_mark_is_armed().await;
        // A request and answer cross the same socket after this send turn.
        let _ = case.central_status(&queued_message(&first)).await;
        while let Ok(event) = case.events.try_recv() {
            match event {
                CustodyTaskEvent::Flush(report) => {
                    assert!(report.ended.is_empty() && report.issues.is_empty())
                }
                CustodyTaskEvent::Receipt(_) => {}
                other => panic!("unexpected custody event after the cutoff: {other:?}"),
            }
        }
        assert_eq!(
            case.link
                .network
                .frames()
                .iter()
                .filter(|frame| frame.kind == FrameKind::Batch)
                .count(),
            1,
            "neither ended write can leave again at a later mark"
        );
        for (request, wire) in requests.iter().zip([&first, &second]) {
            assert_eq!(
                case.direct_message_rows(request).await,
                [vec![SqlValue::Text(queued_message(wire).to_string())]],
                "each request has one durable effect at Central"
            );
        }
        case.close().await;
    });
}
