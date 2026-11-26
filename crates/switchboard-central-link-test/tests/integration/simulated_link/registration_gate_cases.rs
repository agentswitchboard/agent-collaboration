use central_link_client::ReadyLinkLane;
use central_link_protocol::{
    CentralCall, DataFrame, FrameDirection, LinkFrame, Receipt, ReceiptKind, decode_text,
};
use collaboration_central_conformance::{
    machine_link_boundary::{
        assert_empty_welcome, offered_message, queue_direct_mail, register_mail_session,
    },
    signin_fixtures::machine_token,
};
use collaboration_central_tokio::test_support::MachineLinkFixture;
use router_clock::{ClockInstant, RuntimeClock};
use std::{collections::BTreeSet, time::Duration};
use switchboard_central_link::link_policy::LinkStatus;
use switchboard_machine_core_store::{
    DirectHandoff, DirectHandoffChange, HandoffCommit, InboxDecision, InboxRefusalReason,
    MachineLocalOutbox, MachineLocalStore, NewOutboxItem, OutboxQueuedWrite, OutboxStorage,
    SeenReceiptState,
};
use switchboard_message_custody::{
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload, SubmitRefusal},
    flush_clock::{FlushJitter, wake_for_mark},
    flush_marks::next_mark_after,
    receipt_delivery::ReceiptFrameOutcome,
};
use tungstenite::Message;
use workspace_ids::{MessageId, SessionRef};
use workspace_model::central_mail::{MessageDelivery, SendMessageRequest};
use workspace_model::{Caller, PostText, WorkspaceOperation, WorkspaceRequest};

use super::super::{
    MACHINE_LINK_FIXTURE_MACHINE, SimulatedLink,
    central_receive::{CentralReceiveTrace, ReceivedFrame},
    packet_flight::{FrameKind, PacketFlight, decode_segment},
    run_link_case_with_trace,
};
use super::{NetworkCustody, cadence_request, request_id};

fn application_frame(entry: &ReceivedFrame) -> Option<LinkFrame> {
    match &entry.message {
        Message::Text(text) => Some(decode_text(text, FrameDirection::ToCentral).unwrap()),
        Message::Binary(_) | Message::Frame(_) => panic!("unexpected carrier data: {entry:?}"),
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) => None,
    }
}

pub(super) async fn seed_backlog(central: &MachineLinkFixture, count: u32) -> Vec<MessageId> {
    let mut source = central.connect().await.unwrap();
    assert_empty_welcome(&mut source).await.unwrap();
    let known = SessionRef {
        machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
        session_id: "simulated-custody-session".parse().unwrap(),
    };
    let unknown = SessionRef {
        machine_id: known.machine_id.clone(),
        session_id: "unknown-on-this-mac".parse().unwrap(),
    };
    register_mail_session(&mut source, &known, 0x800)
        .await
        .unwrap();
    register_mail_session(&mut source, &unknown, 0x801)
        .await
        .unwrap();
    let mut ids = Vec::new();
    for index in 0..count {
        let to = if index == 1 { &unknown } else { &known };
        let queued = queue_direct_mail(
            &mut source,
            &known,
            to,
            0x810 + index,
            central.clock.utc_now(),
        )
        .await
        .unwrap();
        assert_eq!(offered_message(&mut source).await.unwrap(), queued);
        ids.push(queued);
    }
    ids
}

#[derive(Clone, Copy)]
enum ReleaseMark {
    StillOpen,
    Closed,
}

async fn joined_hold(first: SimulatedLink, trace: CentralReceiveTrace, mark: ReleaseMark) {
    let central = first.central.clone();
    let clock = first.clock.clone();
    let network = first.network.clone();
    first.close().await;
    let ids = seed_backlog(&central, 3).await;
    let [kept_id, refused_id, seen_id] = ids.as_slice() else {
        panic!("three offers must be seeded");
    };
    let registration_start = network.hold_next(FrameKind::RegisterReply);
    let now_seconds = clock.utc_now().timestamp();
    let token = machine_token(
        central.organization_id.as_str(),
        MACHINE_LINK_FIXTURE_MACHINE,
        central.enrollment_id.as_str(),
        now_seconds,
        now_seconds + 42,
    )
    .unwrap();
    let link = SimulatedLink::start_enrolled(
        clock.clone(),
        network.clone(),
        central,
        MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
        token,
        None,
    )
    .await;
    let ack = network
        .seen(registration_start, FrameKind::RegisterReply)
        .await;
    let port = ack.connection_port;
    let (mut case, ()) = NetworkCustody::start_custody(link, |_| (), |custody| custody).await;

    case.wait_for_kept(kept_id).await;
    case.wait_for_kept(seen_id).await;
    assert!(
        matches!(case.inbox.receipt_for(refused_id).await.unwrap().healthy,
        Some(receipt) if receipt.decision == InboxDecision::Refused { reason: InboxRefusalReason::UnknownSession })
    );
    assert_eq!(
        case.inbox
            .compare_and_set_direct_handoffs(
                &case.session,
                &[DirectHandoffChange {
                    message_id: seen_id.clone(),
                    expected: DirectHandoff::Waiting,
                    next: DirectHandoff::Seen {
                        completed_at: clock.utc_now(),
                        receipt: SeenReceiptState::Owed
                    },
                }],
                clock.utc_now()
            )
            .await
            .unwrap(),
        HandoffCommit::Committed
    );
    assert!(case.inbox.has_seen_receipt_owed().await.unwrap());

    // Keep only the complete acknowledgement above the ordered transport. Offers
    // have crossed already and heartbeat and renewal replies can still arrive.
    let hold = case.link.read_probe.discard_next_read();
    network.release();
    let ack_bytes = hold.await.unwrap();
    assert_eq!(decode_segment(&ack_bytes, false), [ack.frame]);
    assert_eq!(*case.link.status.borrow(), LinkStatus::Registering);
    assert!(case.link.lane.watch_registered().borrow().is_none());

    let direct = case.enqueue(31).await;
    let post = request_id(32);
    assert!(
        matches!(case.calls.submit(case.caller(), SubmissionPayload::Workspace(WorkspaceRequest {
        request_id: Some(post.clone()),
        workspace_id: Some("019f0000-0000-7000-8000-000000000098".parse().unwrap()),
        operation: WorkspaceOperation::ThreadPost {
            thread_id: "019f0000-0000-7000-8000-000000000099".parse().unwrap(),
            body: PostText::try_from("thread post during registration".to_owned()).unwrap(),
            addressees: Vec::new(),
        },
    })).await, Submission::InOutbox(receipt) if receipt.flush_at == direct.flush_at)
    );
    assert_eq!(
        case.status(direct.id.clone()).await,
        CustodyRequestStatus::InOutbox {
            flush_at: direct.flush_at
        }
    );
    let missing_status = request_id(33);
    assert_eq!(
        case.status(missing_status.clone()).await,
        CustodyRequestStatus::StatusUnavailable
    );
    assert!(matches!(
        case.calls.submit(case.caller(), cadence_request(34)).await,
        Submission::Refused(SubmitRefusal::CentralUnavailable)
    ));

    let heartbeat = ClockInstant::ZERO.after(Duration::from_secs(20));
    let renewal = ClockInstant::ZERO.after(Duration::from_secs(21));
    case.link.driver_clock.wait_until_pending(heartbeat).await;
    let pong_read = case.link.read_probe.observe_next_read_turn();
    case.advance_to(heartbeat);
    trace
        .wait_for(|frames| {
            frames
                .iter()
                .filter(|frame| frame.port == port)
                .any(|frame| application_frame(frame) == Some(LinkFrame::Ping))
        })
        .await;
    assert_eq!(
        decode_segment(&pong_read.await.unwrap(), false),
        [LinkFrame::Pong]
    );
    let renewal_read = case.link.read_probe.observe_next_read_turn();
    case.advance_to(renewal);
    trace
        .wait_for(|frames| {
            frames
                .iter()
                .filter(|frame| frame.port == port)
                .any(|frame| {
                    matches!(
                        application_frame(frame),
                        Some(LinkFrame::Data(DataFrame::RenewMachineToken { .. }))
                    )
                })
        })
        .await;
    assert!(matches!(
        decode_segment(&renewal_read.await.unwrap(), false).as_slice(),
        [LinkFrame::Data(DataFrame::MachineTokenRenewal { .. })]
    ));

    let wake = wake_for_mark(
        case.link.admission.clock_anchor,
        direct.flush_at,
        case.jitter.jitter_millis(direct.flush_at),
    );
    let in_mark = wake.max(renewal);
    case.advance_to(in_mark);
    assert_eq!(
        case.status(direct.id.clone()).await,
        CustodyRequestStatus::InOutbox {
            flush_at: direct.flush_at
        }
    );
    assert!(case.link.lane.watch_registered().borrow().is_none());
    let held_frames: Vec<_> = trace
        .snapshot()
        .iter()
        .filter(|entry| entry.port == port)
        .filter_map(application_frame)
        .collect();
    assert!(
        matches!(held_frames.as_slice(), [
        LinkFrame::Data(DataFrame::MacCapabilities(_)),
        LinkFrame::Data(DataFrame::CallCentral { request, .. }),
        LinkFrame::Ping,
        LinkFrame::Data(DataFrame::RenewMachineToken { .. }),
    ] if matches!(request.as_ref(), CentralCall::RegisterRun(_))),
        "{held_frames:?}"
    );

    let expected_send = match mark {
        ReleaseMark::StillOpen => in_mark,
        ReleaseMark::Closed => {
            case.advance_to(ClockInstant::ZERO.after(Duration::from_secs(24)));
            case.wake_at_next_mark()
        }
    };
    let hold_boundary = trace.snapshot().len();
    case.link.read_probe.replay_read(ack_bytes);
    let registered = case
        .link
        .lane
        .watch_registered()
        .wait_for(Option::is_some)
        .await
        .unwrap()
        .clone()
        .unwrap();
    assert_eq!(registered.admitted(), &case.link.admission);
    if matches!(mark, ReleaseMark::Closed) {
        case.custody_clock.wait_until_pending(expected_send).await;
        assert_eq!(
            case.status(direct.id.clone()).await,
            CustodyRequestStatus::InOutbox {
                flush_at: direct.flush_at
            }
        );
        assert!(
            trace
                .snapshot()
                .iter()
                .skip(hold_boundary)
                .filter(|entry| entry.port == port)
                .filter_map(application_frame)
                .all(|frame| !matches!(
                    frame,
                    LinkFrame::Data(DataFrame::Batch { .. } | DataFrame::Receipts { .. })
                ))
        );
        case.advance_to(expected_send);
    }
    case.next_write_report().await;
    if matches!(mark, ReleaseMark::StillOpen) {
        let following = next_mark_after(direct.flush_at);
        let following_wake = wake_for_mark(
            case.link.admission.clock_anchor,
            following,
            case.jitter.jitter_millis(following),
        );
        tokio::select! {
            biased;
            () = trace.wait_for(|frames| frames.iter().skip(hold_boundary).filter(|entry| entry.port == port)
                .filter_map(application_frame).any(|frame| matches!(frame, LinkFrame::Data(DataFrame::Receipts { .. })))) => {}
            () = case.custody_clock.wait_until_pending(following_wake) => {
                panic!("the held registration consumed the original receipt mark");
            }
        }
    }
    assert!(matches!(
        case.next_receipt_outcome().await,
        ReceiptFrameOutcome::Confirmed { .. }
    ));
    assert!(!case.inbox.has_seen_receipt_owed().await.unwrap());
    assert_eq!(
        case.status(missing_status.clone()).await,
        CustodyRequestStatus::NotFound
    );

    let after = trace.snapshot();
    let post_registration: Vec<_> = after
        .iter()
        .skip(hold_boundary)
        .filter(|entry| entry.port == port)
        .filter_map(|entry| application_frame(entry).map(|frame| (entry.at, frame)))
        .collect();
    let batches: Vec<_> = post_registration
        .iter()
        .filter_map(|(at, frame)| match frame {
            LinkFrame::Data(DataFrame::Batch { requests }) => Some((*at, requests)),
            _ => None,
        })
        .collect();
    assert!(
        matches!(batches.as_slice(), [(at, batch)] if *at == expected_send
        && batch.requests().iter().filter_map(|request| request.payload().write_request_id()).collect::<Vec<_>>() == [&direct.id, &post])
    );
    let receipts: Vec<_> = post_registration
        .iter()
        .filter_map(|(at, frame)| match frame {
            LinkFrame::Data(DataFrame::Receipts { receipts, .. }) => Some((*at, receipts)),
            _ => None,
        })
        .collect();
    let [(at, receipts)] = receipts.as_slice() else {
        panic!("one receipt frame is owed within the open mark: {receipts:?}")
    };
    assert_eq!(*at, expected_send);
    assert_eq!(receipts.receipts().len(), 4);
    for (id, predicate) in [
        (
            kept_id,
            (|receipt: &ReceiptKind| matches!(receipt, ReceiptKind::Delivered { .. }))
                as fn(&ReceiptKind) -> bool,
        ),
        (refused_id, |receipt| {
            matches!(receipt, ReceiptKind::Refused { .. })
        }),
        (seen_id, |receipt| {
            matches!(receipt, ReceiptKind::Seen { .. })
        }),
    ] {
        assert_eq!(
            receipts
                .receipts()
                .iter()
                .filter(|Receipt { message_id, kind }| message_id == id && predicate(kind))
                .count(),
            1
        );
    }
    assert!(
        post_registration
            .iter()
            .all(|(_, frame)| !matches!(frame, LinkFrame::Data(DataFrame::Request { .. })))
    );
    assert_eq!(post_registration.iter().filter(|(_, frame)| matches!(frame, LinkFrame::Data(DataFrame::RequestStatus { query, .. }) if query.request_id == missing_status)).count(), 1);
    case.close().await;
}

#[test]
fn native_central_receives_only_housekeeping_until_registration_opens_the_same_mark() {
    let trace = CentralReceiveTrace::default();
    run_link_case_with_trace(PacketFlight::new(), Some(trace.clone()), move |link| {
        joined_hold(link, trace, ReleaseMark::StillOpen)
    });
}

#[test]
fn registration_after_a_missed_mark_sends_once_in_the_next_window() {
    let trace = CentralReceiveTrace::default();
    run_link_case_with_trace(PacketFlight::new(), Some(trace.clone()), move |link| {
        joined_hold(link, trace, ReleaseMark::Closed)
    });
}

async fn orphan_claims_wait_for_registration(link: SimulatedLink, trace: CentralReceiveTrace) {
    let clock = link.clock.clone();
    let network = link.network.clone();
    let ack = network.seen(0, FrameKind::RegisterReply).await;
    let port = ack.connection_port;
    let database = link.central.root_path().join("simulated-custody.sqlite");
    let session = SessionRef {
        machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
        session_id: "simulated-custody-session".parse().unwrap(),
    };
    let expired = request_id(45);
    let live = request_id(46);
    let now = clock.utc_now().timestamp_millis();
    let mut outbox = MachineLocalOutbox::new(MachineLocalStore::open(&database).await.unwrap());
    for (request, created) in [(&expired, now - 890_000), (&live, now - 10_000)] {
        let mark = now + 20_000;
        outbox
            .insert(
                &NewOutboxItem {
                    caller: Caller::Session(session.clone()),
                    payload: OutboxQueuedWrite::SendMessage {
                        request_id: request.clone(),
                        message: SendMessageRequest {
                            to: session.clone(),
                            body: PostText::try_from("persisted claim".to_owned()).unwrap(),
                            delivery: MessageDelivery::Auto,
                            reply_to: None,
                        },
                    },
                    request: request.clone(),
                    flush_at_ms: mark,
                },
                created,
            )
            .await
            .unwrap();
        let claimed = outbox
            .claim_next_batch(mark, 1, created, created, &BTreeSet::new())
            .await
            .unwrap();
        assert!(matches!(claimed.healthy.writes.as_slice(), [claim] if &claim.request == request));
    }
    let (mut case, ()) = NetworkCustody::start_custody(link, |_| (), |custody| custody).await;
    let cutoff = ClockInstant::ZERO.after(Duration::from_secs(10));
    let live_cutoff = ClockInstant::ZERO.after(Duration::from_secs(890));
    case.custody_clock.wait_until_pending(cutoff).await;
    case.advance_to(cutoff);
    let report = case.next_write_report().await;
    assert!(matches!(report.ended.as_slice(), [ending] if ending.request_id == expired));
    case.custody_clock.wait_until_pending(live_cutoff).await;
    assert!(case.link.lane.watch_registered().borrow().is_none());
    assert!(
        trace
            .snapshot()
            .iter()
            .filter(|entry| entry.port == port)
            .filter_map(application_frame)
            .all(|frame| !matches!(frame, LinkFrame::Data(DataFrame::RequestStatus { .. })))
    );

    network.release();
    trace.wait_for(|frames| frames.iter().filter(|entry| entry.port == port).any(|entry| {
        entry.at == cutoff && matches!(application_frame(entry),
            Some(LinkFrame::Data(DataFrame::RequestStatus { query, .. })) if query.request_id == live)
    })).await;
    assert_eq!(clock.monotonic_now(), cutoff);
    assert_eq!(
        case.link
            .lane
            .watch_registered()
            .borrow()
            .as_ref()
            .unwrap()
            .admitted(),
        &case.link.admission
    );
    case.close().await;
}

#[test]
fn registration_wakes_the_live_orphan_probe_on_the_native_connection_without_a_clock_tick() {
    let trace = CentralReceiveTrace::default();
    let network = PacketFlight::new();
    network.hold_next(FrameKind::RegisterReply);
    run_link_case_with_trace(network, Some(trace.clone()), move |link| {
        orphan_claims_wait_for_registration(link, trace)
    });
}
