use super::{
    DataFrame, FrameKind, LinkFrame, NetworkCustody, ReceiptKind, ReceiptResult, run_link_case,
};
use router_clock::{ClockInstant, RuntimeClock};
use runtime_primitives::Clock;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use switchboard_machine_core_store::{
    DirectHandoff, DirectHandoffChange, HandoffCommit, InboxStore, NoticeAttemptId,
    SeenReceiptState,
};
use switchboard_message_custody::flush_marks::next_mark_after;
use workspace_ids::SessionRef;
use workspace_model::{Timestamp, central_mail::MessageStatus};

use crate::managed_prompt_fixture::{ManagedPromptPort, NoticeResultScript};

#[test]
fn a_kept_message_yields_one_managed_notice_and_an_owned_seen_status_at_central() {
    run_link_case(|link| async move {
        let (offers, mut written) = tokio::sync::mpsc::unbounded_channel();
        let takes = Arc::new(AtomicUsize::new(0));
        let enabled = Arc::new(AtomicBool::new(false));
        let port = Arc::new(ManagedPromptPort {
            clock: link.clock.clone(),
            enabled: enabled.clone(),
            calls: takes.clone(),
            offers,
            result: NoticeResultScript::ReturnWriterTake,
        });
        let (mut case, ()) = NetworkCustody::setup_with_custody(
            link,
            |_| (),
            |custody| custody.with_notice_port(port),
        )
        .await;
        let sent = case.enqueue(1).await;
        case.first_flush(&sent).await;
        let message = case.finish_write(&sent).await;
        case.wait_for_kept(&message).await;
        case.confirm_receipt(&message).await;
        enabled.store(true, Ordering::SeqCst);

        let before_receipt = case.link.network.hold_next(FrameKind::ReceiptResult);
        let winner = advance_local_mark(&case).await;
        let notice = written.recv().await.unwrap();
        assert_eq!(notice.target(), &case.session);
        assert_eq!(
            notice.text(),
            "You have 1 new message. Read them with message_inbox."
        );
        let taken_at = case.link.clock.utc_now();
        assert_eq!(case.link.clock.monotonic_now(), winner);
        wait_for_seen(&mut case.inbox, taken_at, SeenReceiptState::Owed).await;

        // Await the first current or later admitted receipt opportunity.
        let matching = |frame: &super::InFlightFrame| {
            matches!(&frame.frame, LinkFrame::Data(DataFrame::Receipts { receipts, .. })
                if receipts.receipts().iter().any(|receipt| receipt.message_id == message
                    && receipt.kind == (ReceiptKind::Seen { completed_at: taken_at })))
        };
        let frame = tokio::select! {
            biased;
            frame = case.link.network.seen_matching(before_receipt, matching, 0) => frame,
            () = case.custody_clock.wait_until_pending(case.wake_at_next_mark()) => {
                case.advance_to_next_mark().await;
                case.link.network.seen_matching(before_receipt, matching, 0).await
            }
        };
        assert_eq!(frame.kind, FrameKind::Receipt);
        let LinkFrame::Data(DataFrame::Receipts {
            receipts,
            correlation_id,
        }) = frame.frame
        else {
            panic!("matched Seen receipt frame");
        };
        let results = vec![ReceiptResult::Recorded; receipts.receipts().len()];
        case.link
            .network
            .seen_matching(
                before_receipt,
                |reply| {
                    matches!(&reply.frame, LinkFrame::Data(DataFrame::ReceiptResults {
                correlation_id: actual_correlation, results: actual_results,
            }) if actual_correlation == &correlation_id && actual_results == &results)
                },
                0,
            )
            .await;
        case.link.network.release_and_wait().await;
        wait_for_seen(&mut case.inbox, taken_at, SeenReceiptState::Recorded).await;
        assert_eq!(
            case.central_status(&message).await,
            MessageStatus::Seen {
                completed_at: taken_at
            }
        );

        advance_local_mark(&case).await;
        case.custody_clock
            .wait_until_pending(
                case.link
                    .clock
                    .wall_deadline(next_mark_after(case.link.clock.now())),
            )
            .await;
        assert_eq!(takes.load(Ordering::SeqCst), 1);
        case.close().await;
    });
}

#[test]
fn recovered_writing_is_suppressed_and_only_an_admitted_read_owes_seen() {
    use super::MACHINE_LINK_FIXTURE_MACHINE;
    use switchboard_machine_core_store::{InboxDecision, InboxOfferExpiry, MachineLocalStore};
    use switchboard_message_custody::inbox_read::{
        AdmittedLocalSession, InboxReadError, RecipientInboxRead,
    };
    use workspace_model::{
        PostText,
        central_mail::{MessageBody, MessageDelivery, MessageRecord},
    };

    run_link_case(|link| async move {
        let (offers, _written) = tokio::sync::mpsc::unbounded_channel();
        let takes = Arc::new(AtomicUsize::new(0));
        let port = Arc::new(ManagedPromptPort {
            clock: link.clock.clone(),
            enabled: Arc::new(AtomicBool::new(true)),
            calls: takes.clone(),
            offers,
            result: NoticeResultScript::ReturnWriterTake,
        });
        let path = link.central.root_path().join("simulated-custody.sqlite");
        let recipient = SessionRef {
            machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
            session_id: "simulated-custody-session".parse().unwrap(),
        };
        let arrival = link.clock.utc_now();
        let message = MessageRecord::queued_central(
            "019f0000-0000-7000-8000-000000000088".parse().unwrap(),
            recipient.clone(),
            MessageBody::Direct {
                body: PostText::try_from("retained local fixture".to_owned()).unwrap(),
                delivery: MessageDelivery::Auto,
                reply_to: None,
            },
            arrival,
        );
        let mut before_restart = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
        before_restart
            .record_delivery(
                &message,
                InboxDecision::Accepted,
                arrival.timestamp_millis(),
                InboxOfferExpiry::DirectWithExpiry(
                    Timestamp::from_timestamp_millis(arrival.timestamp_millis() + 900_000).unwrap(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            before_restart
                .compare_and_set_direct_handoffs(
                    &recipient,
                    &[DirectHandoffChange {
                        message_id: message.message_id.clone(),
                        expected: DirectHandoff::Waiting,
                        next: DirectHandoff::Writing {
                            attempt: NoticeAttemptId::mint(arrival)
                        },
                    }],
                    arrival
                )
                .await
                .unwrap(),
            HandoffCommit::Committed
        );
        drop(before_restart);
        let (mut case, ()) = NetworkCustody::setup_with_custody(
            link,
            |_| (),
            |custody| custody.with_notice_port(port),
        )
        .await;
        let wake = case
            .link
            .clock
            .wall_deadline(next_mark_after(case.link.clock.now()));
        case.custody_clock.wait_until_pending(wake).await;
        let stored = case.inbox.scan_direct_handoffs().await.unwrap();
        assert_eq!(stored.healthy[0].handoff, DirectHandoff::NotKnown);
        advance_local_mark(&case).await;
        case.custody_clock
            .wait_until_pending(
                case.link
                    .clock
                    .wall_deadline(next_mark_after(case.link.clock.now())),
            )
            .await;
        assert_eq!(takes.load(Ordering::SeqCst), 0);

        let admitted = AdmittedLocalSession::admitted_by_local_session_directory(recipient.clone());
        let mut other = recipient;
        other.session_id = "another-managed-session".parse().unwrap();
        let another = AdmittedLocalSession::admitted_by_local_session_directory(other);
        assert!(
            case.calls
                .read_inbox(another.clone(), 20)
                .await
                .unwrap()
                .messages()
                .is_empty()
        );
        let read = case.calls.read_inbox(admitted.clone(), 20).await.unwrap();
        assert_eq!(read.messages(), std::slice::from_ref(&message));
        assert!(matches!(
            case.calls
                .record_inbox_read(another.clone(), read.into_acknowledgment())
                .await,
            Err(InboxReadError::RecipientChanged)
        ));
        let copied = RecipientInboxRead::for_session(&another, vec![message.message_id.clone()]);
        assert!(matches!(
            case.calls.record_inbox_read(another, copied).await,
            Err(InboxReadError::MessageNotKept)
        ));
        let mixed = RecipientInboxRead::for_session(
            &admitted,
            vec![
                message.message_id.clone(),
                "019f0000-0000-7000-8000-000000000089".parse().unwrap(),
            ],
        );
        assert!(matches!(
            case.calls.record_inbox_read(admitted.clone(), mixed).await,
            Err(InboxReadError::MessageNotKept)
        ));
        assert_eq!(
            case.inbox.scan_direct_handoffs().await.unwrap().healthy[0].handoff,
            DirectHandoff::NotKnown
        );
        let taken_at = case.link.clock.utc_now();
        let receipt = RecipientInboxRead::for_session(&admitted, vec![message.message_id.clone()]);
        let recorded = case
            .calls
            .record_inbox_read(admitted.clone(), receipt)
            .await
            .unwrap();
        assert_eq!((recorded.changed, recorded.unchanged), (1, 0));
        assert_eq!(
            case.inbox.scan_direct_handoffs().await.unwrap().healthy[0].handoff,
            DirectHandoff::Seen {
                completed_at: taken_at,
                receipt: SeenReceiptState::Owed
            }
        );
        let repeated = RecipientInboxRead::for_session(&admitted, vec![message.message_id.clone()]);
        let recorded = case
            .calls
            .record_inbox_read(admitted.clone(), repeated)
            .await
            .unwrap();
        assert_eq!((recorded.changed, recorded.unchanged), (0, 1));
        assert_eq!(
            case.calls
                .read_inbox(admitted, 20)
                .await
                .unwrap()
                .messages(),
            [message]
        );
        case.close().await;
    });
}

async fn advance_local_mark(case: &NetworkCustody) -> ClockInstant {
    let mark = next_mark_after(case.link.clock.now());
    let wake = case.link.clock.wall_deadline(mark);
    case.custody_clock.wait_until_pending(wake).await;
    case.advance_to(wake);
    wake
}

async fn wait_for_seen(inbox: &mut InboxStore, completed_at: Timestamp, receipt: SeenReceiptState) {
    let expected = DirectHandoff::Seen {
        completed_at,
        receipt,
    };
    loop {
        let rows = inbox.scan_direct_handoffs().await.unwrap();
        assert!(rows.corrupt_rows.is_empty());
        let [row] = rows.healthy.as_slice() else {
            panic!("one kept message")
        };
        if row.handoff == expected {
            return;
        }
        if let DirectHandoff::Seen {
            completed_at: observed,
            receipt: observed_receipt,
        } = row.handoff
        {
            assert_eq!(
                observed, completed_at,
                "Seen must carry the actual writer take"
            );
            assert_eq!(
                observed_receipt,
                SeenReceiptState::Owed,
                "only an owed receipt may still be awaiting confirmation"
            );
        }
    }
}
