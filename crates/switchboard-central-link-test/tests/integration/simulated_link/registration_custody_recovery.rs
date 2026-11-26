use std::time::Duration;

use central_link_client::ReadyLinkLane;
use central_link_protocol::{
    CentralCall, CentralCallReply, DataFrame, FrameDirection, LinkCallRefusal, LinkFrame,
    ReceiptResult, TypedCallResult, approval_rpc::RegisterRunReply, encode_text,
};
use router_clock::ClockInstant;
use switchboard_central_link::link_policy::{LinkStatus, RegistrationFailure};
use switchboard_message_custody::custody_status::CustodyRequestStatus;
use tungstenite::protocol::frame::{
    Frame,
    coding::{Data, OpCode},
};
use workspace_ids::RouterRunId;

use super::super::{
    MACHINE_LINK_FIXTURE_MACHINE, SimulatedLink,
    central_receive::SocketFrameTrace,
    packet_flight::{FrameKind, InFlightFrame, PacketFlight, decode_segment},
    run_link_case_with_trace,
};
use super::{
    NetworkCustody,
    registration_gate_cases::{application_frame, seed_backlog},
    request_id,
};

async fn hold_acknowledgement(link: &SimulatedLink, ack: &InFlightFrame) -> Vec<u8> {
    let hold = link.read_probe.discard_next_read();
    link.network.release();
    let bytes = hold.await.unwrap();
    assert_eq!(
        decode_segment(&bytes, false),
        std::slice::from_ref(&ack.frame)
    );
    bytes
}

fn refused_registration(ack: &InFlightFrame) -> Vec<u8> {
    let LinkFrame::Data(DataFrame::CentralCallReply { correlation_id, .. }) = &ack.frame else {
        panic!("expected the connection's actual registration acknowledgement");
    };
    let wire = encode_text(
        &LinkFrame::Data(DataFrame::CentralCallReply {
            correlation_id: correlation_id.clone(),
            result: Box::new(TypedCallResult::Refused {
                reason: LinkCallRefusal::NotPermitted,
            }),
        }),
        FrameDirection::ToRouter,
    )
    .unwrap();
    let mut bytes = Vec::new();
    Frame::message(wire, OpCode::Data(Data::Text), true)
        .format(&mut bytes)
        .unwrap();
    bytes
}

fn registration_run(frame: &InFlightFrame) -> &RouterRunId {
    let LinkFrame::Data(DataFrame::CallCentral { request, .. }) = &frame.frame else {
        panic!("expected the registration request");
    };
    let CentralCall::RegisterRun(registration) = request.as_ref() else {
        panic!("expected the RegisterRun command");
    };
    &registration.body.router_run
}

#[derive(Clone, Copy)]
enum RegistrationEnding {
    Refused,
    TimedOut,
    Replaced,
}

fn elapsed(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

async fn custody_survives_registration_ending(
    first: SimulatedLink,
    ending: RegistrationEnding,
    trace: SocketFrameTrace,
) {
    let central = first.central.clone();
    let clock = first.clock.clone();
    let network = first.network.clone();
    first.close().await;
    let messages = seed_backlog(&central, 1).await;
    let [incoming] = messages.as_slice() else {
        panic!("expected one queued offer")
    };
    let prior_run = matches!(ending, RegistrationEnding::Replaced)
        .then(|| "019f0000-0000-7000-8000-00000000ce84".parse().unwrap());
    let token = central.token.clone();
    let reply_index = usize::from(prior_run.is_some());
    let held_after = network.hold_after(FrameKind::RegisterReply, reply_index);
    let link = SimulatedLink::start_enrolled(
        clock.clone(),
        network.clone(),
        central,
        MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
        token,
        prior_run,
    )
    .await;
    let first_reply = network
        .seen_nth(held_after, FrameKind::RegisterReply, reply_index)
        .await;
    let first_request = network
        .seen_nth(held_after, FrameKind::RegisterRun, reply_index)
        .await;
    let first_attempt = link.admission.connection.clone();
    let (mut case, ()) = NetworkCustody::start_custody(link, |_| (), |custody| custody).await;
    case.wait_for_kept(incoming).await;
    let outgoing = case.enqueue(41).await;
    assert!(case.link.lane.watch_registered().borrow().is_none());
    let held_bytes = hold_acknowledgement(&case.link, &first_reply).await;
    match ending {
        RegistrationEnding::Refused => {
            // The native server acknowledged the real request. Substitute only
            // the matching, retryable refusal on the driver's input boundary.
            case.link
                .read_probe
                .replay_read(refused_registration(&first_reply));
            case.link
                .status
                .wait_for(|status| {
                    *status
                        == LinkStatus::RegistrationFault {
                            reason: LinkCallRefusal::NotPermitted,
                        }
                })
                .await
                .unwrap();
            for (seconds, pong_index) in [(20, 0), (40, 1)] {
                case.advance_to(elapsed(seconds));
                network
                    .seen_nth(held_after, FrameKind::Pong, pong_index)
                    .await;
            }
            assert_eq!(
                case.link.lane.watch_admitted().borrow().as_ref(),
                Some(&case.link.admission)
            );
        }
        RegistrationEnding::TimedOut => {
            case.advance_to(elapsed(20));
            network.seen(held_after, FrameKind::Pong).await;
            case.advance_to(elapsed(30));
            case.link
                .status
                .wait_for(|status| {
                    *status
                        == LinkStatus::RegistrationFailed {
                            reason: RegistrationFailure::TimedOut,
                        }
                })
                .await
                .unwrap();
            assert_eq!(
                case.link.lane.watch_admitted().borrow().as_ref(),
                Some(&case.link.admission)
            );
        }
        RegistrationEnding::Replaced => {
            assert!(
                matches!(&first_reply.frame, LinkFrame::Data(DataFrame::CentralCallReply { result, .. })
                if matches!(result.as_ref(), TypedCallResult::Answered { reply: CentralCallReply::RegisterRun(reply) }
                    if reply.body == RegisterRunReply::RunNotRegistered {}))
            );
            case.link.read_probe.replay_read(held_bytes);
            case.link.break_socket.closed().await;
            assert!(case.link.lane.watch_admitted().borrow().is_none());
        }
    }

    case.wait_for_kept(incoming).await;
    assert!(
        case.inbox
            .pending_receipts()
            .await
            .unwrap()
            .healthy
            .iter()
            .any(|receipt| receipt.message_id == *incoming)
    );
    assert_eq!(
        case.status(outgoing.id.clone()).await,
        CustodyRequestStatus::InOutbox {
            flush_at: outgoing.flush_at
        }
    );
    assert!(
        trace
            .snapshot()
            .iter()
            .filter_map(application_frame)
            .all(|frame| !matches!(
                frame,
                LinkFrame::Data(DataFrame::Batch { .. } | DataFrame::Receipts { .. })
            ))
    );
    match ending {
        RegistrationEnding::Refused => case.advance_to(elapsed(60)),
        RegistrationEnding::TimedOut => case.advance_to(elapsed(31)),
        RegistrationEnding::Replaced => case.link.reconnect().await,
    }
    let success = network
        .seen_nth(held_after, FrameKind::RegisterReply, reply_index + 1)
        .await;
    let registered = case
        .link
        .lane
        .watch_registered()
        .wait_for(Option::is_some)
        .await
        .unwrap()
        .clone()
        .unwrap();
    assert_eq!(registered.router_run(), registration_run(&first_request));
    assert_eq!(
        &registered.admitted().connection,
        &case.link.admission.connection
    );
    match ending {
        RegistrationEnding::Refused | RegistrationEnding::TimedOut => {
            assert_eq!(registered.admitted().connection, first_attempt);
            assert_eq!(first_reply.connection_port, success.connection_port);
        }
        RegistrationEnding::Replaced => {
            assert_ne!(registered.admitted().connection, first_attempt);
            assert_ne!(first_reply.connection_port, success.connection_port);
            let offer_ports: Vec<_> = network.frames().iter().filter_map(|frame| {
                matches!(&frame.frame, LinkFrame::Data(DataFrame::Deliver { message, .. }) if message.message_id() == incoming)
                    .then_some(frame.connection_port)
            }).collect();
            assert_eq!(
                offer_ports,
                [first_reply.connection_port, success.connection_port]
            );
        }
    }
    case.wait_for_kept(incoming).await;
    let send_at = case.wake_at_next_mark();
    case.advance_to_next_mark().await;
    case.finish_write(&outgoing).await;
    case.wait_for_confirmation(&[ReceiptResult::Recorded]).await;
    assert_eq!(
        case.status(request_id(42)).await,
        CustodyRequestStatus::NotFound
    );
    let received = trace.snapshot();
    let batches: Vec<_> = received
        .iter()
        .filter_map(|entry| {
            let Some(LinkFrame::Data(DataFrame::Batch { requests })) = application_frame(entry)
            else {
                return None;
            };
            let count = requests
                .requests()
                .iter()
                .filter(|request| request.payload().write_request_id() == Some(&outgoing.id))
                .count();
            (count > 0).then_some((entry.at, entry.port, count))
        })
        .collect();
    let receipts: Vec<_> = received
        .iter()
        .filter_map(|entry| {
            let Some(LinkFrame::Data(DataFrame::Receipts { receipts, .. })) =
                application_frame(entry)
            else {
                return None;
            };
            let count = receipts
                .receipts()
                .iter()
                .filter(|receipt| receipt.message_id == *incoming)
                .count();
            (count > 0).then_some((entry.at, entry.port, count))
        })
        .collect();
    assert_eq!(batches, [(send_at, success.connection_port, 1)]);
    assert_eq!(receipts, [(send_at, success.connection_port, 1)]);
    case.close().await;
}

fn observe_ending(ending: RegistrationEnding) {
    let trace = SocketFrameTrace::default();
    run_link_case_with_trace(PacketFlight::new(), Some(trace.clone()), move |link| {
        custody_survives_registration_ending(link, ending, trace)
    });
}

#[test]
fn a_framed_refusal_keeps_inbox_and_outbox_until_registration_on_the_same_socket() {
    observe_ending(RegistrationEnding::Refused);
}

#[test]
fn a_registration_timeout_keeps_inbox_and_outbox_until_retry_on_the_same_socket() {
    observe_ending(RegistrationEnding::TimedOut);
}

#[test]
fn run_not_registered_reconnects_and_keeps_the_reoffered_record_once() {
    observe_ending(RegistrationEnding::Replaced);
}
