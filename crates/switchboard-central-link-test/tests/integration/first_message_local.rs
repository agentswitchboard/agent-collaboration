//! Two message systems and native Tokio Central share one manual clock.
//! The tests enter at MessageFront with fixture-built AdmittedLocalSession values, below
//! MessageOperations, V0MessageAdmission, and MCP/CLI. Each Mac's enrollment is written
//! directly into its store with a fixture bearer. The recipient directory and managed
//! notice port are fixtures. Sign-in, Cloudflare, and the ACP Claude transport are outside
//! this journey. PR 411's Host tests exercise the ACP step separately.

#![expect(
    clippy::panic,
    reason = "The integration fixture reports the exact unexpected wire or tool outcome."
)]

use std::{
    future::Future,
    os::unix::fs::DirBuilderExt as _,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use central_link_client::WelcomeClockAnchor;
use central_link_protocol::{ReceiptResult, ReplyPayload};
use collaboration_central_tokio::test_support::{MachineLinkFixture, start_machine_link_fixture};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use runtime_primitives::{Clock, SyncTime};
use switchboard_central_link::{
    DialAddress,
    link_driver::{DriverSettings, LinkDriver},
};
use switchboard_machine_core_store::{
    DirectHandoff, DirectHandoffRow, InboxStore, MACHINE_LOCAL_DATABASE, MachineLocalStore,
    load_or_create_machine_id,
};
use switchboard_message_custody::{
    custody_task::CustodyTaskEvent,
    flush_clock::{FlushJitter, MachineFlushJitter, wake_for_mark},
    flush_marks::{earliest_open_mark, next_mark_after},
    handover_notice::{ClaimedNotice, ManagedClaudeNoticePort},
    receipt_delivery::ReceiptFrameOutcome,
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use switchboard_message_system::{
    CloseReport, MessageFront, MessageRoots, MessageSystem, MessageSystemEvents,
    MessageSystemStatus,
    test_support::{
        AdmittedLocalSession, DirectMessageWrite, LinkFixture, MessageInboxAcknowledgment,
        MessageInboxPage, MessageInboxRead, MessageSendOutcome, MessageToolResult, MessageUnitPort,
        MessageWriteStatus, message_inbox_text, message_send_text, message_status_text,
        start_with_link_fixture,
    },
};
use tokio::sync::{mpsc, watch};
use workspace_ids::{MessageId, RequestId, SessionId, SessionRef};
use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult};

use super::{
    custody_journey::next_receipt,
    managed_prompt_fixture::{ManagedPromptPort, NoticeResultScript},
    message_unit_fixture::{
        EPOCH, FixtureDiscovery, FixtureRefresh, KnownRecipient, RecordCustody, enroll,
        grant_at_central, open_store,
    },
};

const SENDER_REQUEST: &str = "019f0000-0000-7000-8000-00000000cd13";
const MESSAGE_TEXT: &str = "First direct message between the two local Macs";
const NOTICE_TEXT: &str = "You have 1 new message. Read them with message_inbox.";
const CENTRAL_AWAITING_KEEP: &str =
    "Central has no delivery confirmation from the recipient's Mac.";
const DELIVERED_WORDS: &str = "Delivered to the recipient's Mac.";
const SEEN_WORDS: &str = "Seen: the recipient session was told.";

pub(super) struct EnrolledFixtureMac {
    pub(super) root: PathBuf,
    pub(super) address: SessionRef,
    refresh: FixtureRefresh,
}

pub(super) struct RunningFixtureMac {
    pub(super) system: MessageSystem,
    pub(super) front: MessageFront,
    events: mpsc::UnboundedReceiver<CustodyTaskEvent>,
}

impl EnrolledFixtureMac {
    pub(super) async fn enroll(
        central: &MachineLinkFixture,
        name: &str,
        approval_request: RequestId,
    ) -> Self {
        let root = central.root_path().join(name);
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let clock = central.clock.clone();
        let machine_id = load_or_create_machine_id(&root, clock.utc_now().timestamp_millis())
            .await
            .unwrap();
        let (enrollment_id, bearer) =
            grant_at_central(central, &machine_id, approval_request).await;
        let store = open_store(&root, machine_id.clone(), clock).await;
        enroll(
            &store,
            machine_id.clone(),
            central.organization_id.as_str().parse().unwrap(),
            enrollment_id,
        )
        .await;
        store.request_close();
        assert!(store.join().await.unwrap().outcome.is_ok());
        let address = SessionRef {
            machine_id,
            session_id: name.parse().unwrap(),
        };
        Self {
            root,
            address,
            refresh: FixtureRefresh {
                enrollment_id,
                bearer,
            },
        }
    }

    pub(super) async fn start(
        &self,
        central: &MachineLinkFixture,
        recipients: Arc<dyn RecipientSessionDirectory>,
        notice: Option<Arc<ManagedPromptPort>>,
    ) -> RunningFixtureMac {
        let machine = self.address.machine_id.clone();
        let clock = central.clock.clone();
        let store = open_store(&self.root, machine.clone(), clock.clone()).await;
        let (events, observed) = mpsc::unbounded_channel();
        let events = MessageSystemEvents::new(Arc::new(RecordCustody(events)));
        let events = match notice {
            Some(port) => events
                .with_managed_notice(move |_| port.clone() as Arc<dyn ManagedClaudeNoticePort>),
            None => events,
        };
        let address = central
            .url(central.organization_id.as_str())
            .replacen("http://", "ws://", 1);
        let (discovery, _discoveries) = mpsc::unbounded_channel();
        let system = start_with_link_fixture(
            MessageRoots::new(self.root.clone(), self.root.join("secrets")),
            machine.clone(),
            store,
            clock,
            recipients,
            events,
            LinkFixture {
                discovery: FixtureDiscovery(discovery),
                refresh: self.refresh.clone(),
                run_driver: move |mut driver: LinkDriver<_, _>| {
                    let address = address.clone();
                    async move {
                        let loopback_settings = {
                            let published = driver
                                .settings
                                .wait_for(|found| found.address.is_ok())
                                .await
                                .unwrap();
                            DriverSettings {
                                binding: published.binding.clone(),
                                address: DialAddress::test_loopback(&address),
                            }
                        };
                        let (_settings, updates) = watch::channel(loopback_settings);
                        driver.settings = updates;
                        driver.run().await;
                    }
                },
            },
        );
        let mut status = system.status();
        drop(
            status
                .wait_for(|found| *found == MessageSystemStatus::Connected)
                .await
                .unwrap(),
        );
        let front = system.front();
        assert_eq!(front.available_machine(), Ok(machine));
        RunningFixtureMac {
            system,
            front,
            events: observed,
        }
    }
}

struct HeldRecipientLookup {
    session_id: SessionId,
    may_keep: watch::Receiver<bool>,
}

impl RecipientSessionDirectory for HeldRecipientLookup {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            if session_id != &self.session_id {
                return RecipientSessionAnswer::UnknownSession;
            }
            if self
                .may_keep
                .clone()
                .wait_for(|allowed| *allowed)
                .await
                .is_ok()
            {
                RecipientSessionAnswer::KnownOnce
            } else {
                RecipientSessionAnswer::CouldNotTell
            }
        })
    }
}

#[tokio::test]
async fn first_message_local_two_macs_reports_one_notice_and_a_seen_receipt() {
    first_message_local(FirstMessageCase::NoticeThenRead).await;
}

#[tokio::test]
async fn first_message_local_two_macs_restart_does_not_repeat_an_unconfirmed_notice() {
    first_message_local(FirstMessageCase::RestartDuringNotice).await;
}

#[tokio::test]
async fn first_message_local_two_macs_can_read_and_ack_after_the_delivery_window() {
    first_message_local(FirstMessageCase::ReadAfterDeliveryWindow).await;
}

#[tokio::test]
async fn first_message_local_two_macs_acknowledging_while_waiting_records_seen_without_a_notice() {
    first_message_local(FirstMessageCase::ReadBeforeNotice).await;
}

#[derive(Clone, Copy)]
enum FirstMessageCase {
    NoticeThenRead,
    RestartDuringNotice,
    ReadAfterDeliveryWindow,
    ReadBeforeNotice,
}

async fn first_message_local(journey: FirstMessageCase) {
    let clock = Arc::new(ManualClock::new(EPOCH.parse().unwrap()));
    let central = start_machine_link_fixture(clock.clone()).await.unwrap();
    let sender_mac = EnrolledFixtureMac::enroll(
        &central,
        "sender-mac",
        "019f0000-0000-7000-8000-00000000cd11".parse().unwrap(),
    )
    .await;
    let recipient_mac = EnrolledFixtureMac::enroll(
        &central,
        "managed-claude",
        "019f0000-0000-7000-8000-00000000cd12".parse().unwrap(),
    )
    .await;
    assert_ne!(
        sender_mac.address.machine_id,
        recipient_mac.address.machine_id
    );
    let sender =
        AdmittedLocalSession::admitted_by_local_session_directory(sender_mac.address.clone());
    let recipient =
        AdmittedLocalSession::admitted_by_local_session_directory(recipient_mac.address.clone());
    let (may_keep, allowed) = watch::channel(false);
    let directory = Arc::new(HeldRecipientLookup {
        session_id: recipient.address().session_id.clone(),
        may_keep: allowed,
    });
    let (offers, mut written) = mpsc::unbounded_channel();
    let enabled = Arc::new(AtomicBool::new(false));
    let takes = Arc::new(AtomicUsize::new(0));
    let notice_result = match journey {
        FirstMessageCase::RestartDuringNotice => NoticeResultScript::HoldAfterWriterTake,
        _ => NoticeResultScript::ReturnWriterTake,
    };
    let port = Arc::new(ManagedPromptPort {
        clock: clock.clone(),
        enabled: enabled.clone(),
        calls: takes.clone(),
        offers,
        result: notice_result,
    });
    let mut mac_a = sender_mac
        .start(
            &central,
            Arc::new(KnownRecipient(sender.address().session_id.clone())),
            None,
        )
        .await;
    let mut mac_b = recipient_mac
        .start(&central, directory.clone(), Some(port.clone()))
        .await;
    let observer = MachineLocalStore::open(&recipient_mac.root.join(MACHINE_LOCAL_DATABASE))
        .await
        .unwrap();
    let mut inbox = InboxStore::new(observer);
    let anchor = WelcomeClockAnchor {
        central_time: clock.utc_now(),
        observed_at: clock.monotonic_now(),
    };
    let request_id: RequestId = SENDER_REQUEST.parse().unwrap();

    let submitted = mac_a
        .front
        .submit(
            &sender,
            DirectMessageWrite {
                request_id: request_id.clone(),
                to: recipient.address().clone(),
                body: MESSAGE_TEXT.to_owned().try_into().unwrap(),
            },
        )
        .await
        .unwrap();
    let MessageSendOutcome::InOutbox { flush_at, .. } = &submitted else {
        panic!("send was not admitted to the outbox: {submitted:?}");
    };
    let send_mark = SyncTime::from_unix_millis(flush_at.timestamp_millis());
    assert_eq!(
        message_send_text(&MessageToolResult::Returned(submitted)),
        format!("In outbox. Leaves at 2030-03-17T17:47:00+00:00.\nRequest id {request_id}.")
    );
    let in_outbox = mac_a
        .front
        .request_status(&sender, request_id.clone())
        .await
        .unwrap();
    assert_eq!(
        message_status_text(&MessageToolResult::Returned(in_outbox)),
        "In outbox. Leaves at 2030-03-17T17:47:00+00:00."
    );

    let send_wake = wake_for_mark(
        anchor,
        send_mark,
        MachineFlushJitter::new(&sender_mac.address.machine_id).jitter_millis(send_mark),
    );
    clock.not_due_check_now(send_wake).await;
    move_clock(&clock, send_wake);
    let CustodyTaskEvent::Flush(flush) = mac_a.events.recv().await.unwrap() else {
        panic!("expected A to flush the outbox at its mark");
    };
    assert!(flush.issues.is_empty(), "{:?}", flush.issues);
    let [answer] = flush.answered.as_slice() else {
        panic!("expected one checked Central answer");
    };
    assert_eq!(answer.request, request_id);
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
        &answer.reply
    else {
        panic!("Central did not accept the exact write: {:?}", answer.reply);
    };
    assert_sender_words(
        &mac_a.front,
        &sender,
        &request_id,
        message_id,
        CENTRAL_AWAITING_KEEP,
    )
    .await;

    may_keep.send_replace(true);
    observe_handoff(&mut inbox, message_id, |state| {
        state == DirectHandoff::Waiting
    })
    .await;
    run_receipt_turn(&clock, anchor, &recipient_mac, send_mark, &mut mac_b.events).await;
    assert_sender_words(
        &mac_a.front,
        &sender,
        &request_id,
        message_id,
        DELIVERED_WORDS,
    )
    .await;

    if matches!(journey, FirstMessageCase::ReadBeforeNotice) {
        let completed_at = clock.utc_now();
        read_and_acknowledge(
            &mac_b.front,
            &recipient,
            message_id,
            MessageInboxAcknowledgment {
                changed: 1,
                unchanged: 0,
            },
        )
        .await;
        assert!(matches!(
            handoff_row(&mut inbox, message_id).await.unwrap().handoff,
            DirectHandoff::Seen { completed_at: at, .. } if at == completed_at
        ));
        enabled.store(true, Ordering::SeqCst);
        let notice_mark = assert_quiet_notice_mark(&clock, &mut written).await;
        assert_eq!(takes.load(Ordering::SeqCst), 0);
        run_receipt_turn(
            &clock,
            anchor,
            &recipient_mac,
            notice_mark,
            &mut mac_b.events,
        )
        .await;
        assert_sender_words(&mac_a.front, &sender, &request_id, message_id, SEEN_WORDS).await;
        assert_eq!(mac_a.system.close().await, CloseReport::Closed);
        assert_eq!(mac_b.system.close().await, CloseReport::Closed);
        central.close().await.unwrap();
        return;
    }

    enabled.store(true, Ordering::SeqCst);
    let notice_mark = next_mark_after(clock.now());
    move_clock(&clock, clock.wall_deadline(notice_mark));
    let notice = written.recv().await.unwrap();
    assert_eq!(notice.target(), recipient.address());
    assert_eq!(notice.count(), 1);
    assert_eq!(notice.text(), NOTICE_TEXT);

    let expected_acknowledgment = match notice_result {
        NoticeResultScript::ReturnWriterTake => {
            observe_handoff(&mut inbox, message_id, |state| {
                matches!(state, DirectHandoff::Seen { .. })
            })
            .await;
            run_receipt_turn(
                &clock,
                anchor,
                &recipient_mac,
                notice_mark,
                &mut mac_b.events,
            )
            .await;
            assert_sender_words(&mac_a.front, &sender, &request_id, message_id, SEEN_WORDS).await;
            MessageInboxAcknowledgment {
                changed: 0,
                unchanged: 1,
            }
        }
        NoticeResultScript::HoldAfterWriterTake => {
            assert!(matches!(
                handoff_row(&mut inbox, message_id).await.unwrap().handoff,
                DirectHandoff::Writing { .. }
            ));
            read_and_acknowledge(
                &mac_b.front,
                &recipient,
                message_id,
                MessageInboxAcknowledgment {
                    changed: 0,
                    unchanged: 1,
                },
            )
            .await;
            assert!(matches!(
                handoff_row(&mut inbox, message_id).await.unwrap().handoff,
                DirectHandoff::Writing { .. }
            ));
            assert_eq!(mac_b.system.close().await, CloseReport::Closed);
            mac_b = recipient_mac.start(&central, directory, Some(port)).await;
            observe_handoff(&mut inbox, message_id, |state| {
                !matches!(state, DirectHandoff::Writing { .. })
            })
            .await;
            assert_eq!(
                handoff_row(&mut inbox, message_id).await.unwrap().handoff,
                DirectHandoff::NotKnown
            );
            assert_quiet_notice_mark(&clock, &mut written).await;
            assert_eq!(
                takes.load(Ordering::SeqCst),
                1,
                "recovered Writing must suppress a second writer take"
            );
            assert!(written.try_recv().is_err());
            assert_eq!(
                handoff_row(&mut inbox, message_id).await.unwrap().handoff,
                DirectHandoff::NotKnown
            );
            assert_sender_words(
                &mac_a.front,
                &sender,
                &request_id,
                message_id,
                DELIVERED_WORDS,
            )
            .await;
            MessageInboxAcknowledgment {
                changed: 1,
                unchanged: 0,
            }
        }
    };

    if matches!(journey, FirstMessageCase::ReadAfterDeliveryWindow) {
        let expiry = handoff_row(&mut inbox, message_id)
            .await
            .unwrap()
            .expires_at;
        clock
            .advance_to(SyncTime::from_unix_millis(expiry.timestamp_millis() + 1))
            .unwrap();
        assert!(clock.utc_now() > expiry);
    }
    read_and_acknowledge(
        &mac_b.front,
        &recipient,
        message_id,
        expected_acknowledgment,
    )
    .await;
    if matches!(notice_result, NoticeResultScript::HoldAfterWriterTake) {
        observe_handoff(&mut inbox, message_id, |state| {
            matches!(state, DirectHandoff::Seen { .. })
        })
        .await;
        run_receipt_turn(
            &clock,
            anchor,
            &recipient_mac,
            earliest_open_mark(clock.now(), None),
            &mut mac_b.events,
        )
        .await;
        assert_sender_words(&mac_a.front, &sender, &request_id, message_id, SEEN_WORDS).await;
    }
    assert_eq!(takes.load(Ordering::SeqCst), 1);
    assert!(written.try_recv().is_err());

    assert_eq!(mac_a.system.close().await, CloseReport::Closed);
    assert_eq!(mac_b.system.close().await, CloseReport::Closed);
    central.close().await.unwrap();
}

async fn read_and_acknowledge(
    front: &MessageFront,
    recipient: &AdmittedLocalSession,
    message_id: &MessageId,
    expected_acknowledgment: MessageInboxAcknowledgment,
) {
    let shown = front
        .read_inbox(
            recipient,
            MessageInboxRead::from_arguments(None, None).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(shown.acknowledgment, None);
    assert_inbox_words(&shown, message_id, None);
    let acknowledged = front
        .read_inbox(
            recipient,
            MessageInboxRead::from_arguments(None, Some(&shown.read_token)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(acknowledged.acknowledgment, Some(expected_acknowledgment));
    assert_eq!(acknowledged.messages, shown.messages);
    assert_inbox_words(&acknowledged, message_id, Some(expected_acknowledgment));
}

async fn assert_sender_words(
    front: &MessageFront,
    sender: &AdmittedLocalSession,
    request: &RequestId,
    message: &MessageId,
    delivery_words: &str,
) {
    let status = front.request_status(sender, request.clone()).await.unwrap();
    assert!(
        matches!(&status, MessageWriteStatus::Answered { message_id, .. } if message_id == message)
    );
    assert_eq!(
        message_status_text(&MessageToolResult::Returned(status)),
        format!("Accepted by central. Message id {message}.\n{delivery_words}")
    );
}

fn assert_inbox_words(
    page: &MessageInboxPage,
    message: &MessageId,
    acknowledgment: Option<MessageInboxAcknowledgment>,
) {
    let [shown] = page.messages.as_slice() else {
        panic!("expected one message in B's inbox: {:?}", page.messages);
    };
    assert_eq!(&shown.message_id, message);
    let prefix = acknowledgment.map_or_else(String::new, |ack| {
        format!(
            "Acknowledged. {} changed, {} unchanged.\n",
            ack.changed, ack.unchanged
        )
    });
    assert_eq!(
        message_inbox_text(&MessageToolResult::Returned(page.clone())),
        format!(
            "{prefix}{} · {message}\n{MESSAGE_TEXT}\nRead token {}.",
            shown.created_at.to_rfc3339(),
            page.read_token
        )
    );
}

async fn handoff_row(inbox: &mut InboxStore, message: &MessageId) -> Option<DirectHandoffRow> {
    let scan = inbox.scan_direct_handoffs().await.unwrap();
    assert!(scan.corrupt_rows.is_empty());
    scan.healthy
        .into_iter()
        .find(|row| &row.message_id == message)
}

async fn observe_handoff(
    inbox: &mut InboxStore,
    message: &MessageId,
    wanted: impl Fn(DirectHandoff) -> bool,
) {
    // The durable row is read through an independent SQLite connection. Its commit watch
    // belongs to that connection, so each asynchronous read checks the shared database instead.
    loop {
        if handoff_row(inbox, message)
            .await
            .is_some_and(|row| wanted(row.handoff))
        {
            return;
        }
    }
}

fn move_clock(clock: &ManualClock, deadline: ClockInstant) {
    clock
        .advance(deadline.saturating_duration_since(clock.monotonic_now()))
        .unwrap();
}

async fn assert_quiet_notice_mark(
    clock: &ManualClock,
    written: &mut mpsc::UnboundedReceiver<ClaimedNotice>,
) -> SyncTime {
    let mark = next_mark_after(clock.now());
    move_clock(clock, clock.wall_deadline(mark));
    let following_wake = clock.wall_deadline(next_mark_after(mark));
    tokio::select! {
        biased;
        notice = written.recv() => panic!("a settled notice reached the writer: {}", notice.unwrap().text()),
        () = clock.not_due_check_now(following_wake) => {}
    }
    mark
}

async fn run_receipt_turn(
    clock: &ManualClock,
    anchor: WelcomeClockAnchor,
    mac: &EnrolledFixtureMac,
    mark: SyncTime,
    events: &mut mpsc::UnboundedReceiver<CustodyTaskEvent>,
) {
    let jitter = MachineFlushJitter::new(&mac.address.machine_id);
    move_clock(
        clock,
        wake_for_mark(anchor, mark, jitter.jitter_millis(mark)),
    );
    let following_mark = next_mark_after(mark);
    let following_wake =
        wake_for_mark(anchor, following_mark, jitter.jitter_millis(following_mark));
    // Keep or Seen may land on either side of B's receipt wake. Wait for the current
    // answer or for B to arm the following mark before moving past the open window.
    let outcome = tokio::select! {
        biased;
        outcome = next_receipt(events) => outcome,
        () = clock.not_due_check_now(following_wake) => {
            move_clock(clock, following_wake);
            next_receipt(events).await
        }
    };
    assert_eq!(
        outcome,
        ReceiptFrameOutcome::Confirmed {
            results: vec![ReceiptResult::Recorded]
        }
    );
}
