use std::{sync::Arc, time::Duration};

use central_link_client::{
    AdmittedConnection, ClientAction, ConnectionAttempt, CorrelatedRequestReply, LinkClient,
    LinkLaneCommand, LinkLaneDisposition, LinkLaneEvent, LinkLaneReply, ReadyLinkLane,
    WriteBatchCommand, WriteBatchDisposition, WriteBatchMemberDisposition,
};
use central_link_protocol::{
    CorrelationId, DataFrame, FrameDirection, LinkFrame, LinkRequest, LinkRequestBatch,
    MachineLinkCloseCode, Receipt, ReceiptBatch, ReceiptKind, ReceiptResult, ReplyPayload,
    RequestScope, RequestStatusAnswer, RequestStatusQuery, decode_text, encode_text,
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use router_clock::ManualClock;
use switchboard_central_link::{link_lane::LinkLaneHandle, link_policy::LinkStatus};
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

use super::{DELIVER_FRAME, Fixture, machine, text, tick, welcome};

fn correlation(number: u8) -> CorrelationId {
    format!("01890f2e-7b4c-7cc0-98c4-{number:012x}")
        .parse()
        .unwrap()
}

#[expect(
    clippy::panic,
    reason = "The fixture must fail if its hand-built frame is not a request."
)]
fn mail_write(number: u8) -> LinkRequest {
    let wire = format!(
        r#"{{"kind":"request","request":{{"correlationId":"{}","caller":{{"kind":"session","value":{{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"}}}},"payload":{{"kind":"mail","request":{{"requestId":"01890f2e-7b4c-7cc0-98c4-0000000000{number:02x}","operation":{{"kind":"setNoticeCadence","cadence":"m25"}}}}}}}}}}"#,
        String::from(correlation(number))
    );
    let LinkFrame::Data(DataFrame::Request { request }) =
        decode_text(&wire, FrameDirection::ToCentral).unwrap()
    else {
        panic!("fixture expected a write request");
    };
    *request
}

#[expect(
    clippy::panic,
    reason = "The fixture must fail if it cannot mint a connection attempt."
)]
fn unadmitted_attempt() -> ConnectionAttempt {
    let clock = Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap()));
    let Some(ClientAction::Connect { attempt }) = LinkClient::new(machine(), clock).poll_time()
    else {
        panic!("fixture expected an attempt");
    };
    attempt
}

async fn admit(
    fixture: &mut Fixture,
    socket: &mut WebSocketStream<TcpStream>,
    lane: &LinkLaneHandle,
) -> AdmittedConnection {
    welcome(socket).await;
    fixture
        .status
        .wait_for(|status| matches!(status, LinkStatus::Welcomed { .. }))
        .await
        .unwrap();
    let admitted = lane.watch_admitted().borrow().clone();
    assert!(
        admitted.is_some(),
        "a valid Welcome must publish admitted readiness without a registered run"
    );
    assert!(lane.watch_registered().borrow().is_none());
    let admitted = admitted.unwrap();
    assert_eq!(
        lane.next_event().await,
        Some(LinkLaneEvent::ConnectionAdmitted(admitted.clone()))
    );
    admitted
}

async fn send_answer(socket: &mut WebSocketStream<TcpStream>, frame: DataFrame) {
    let wire = encode_text(&LinkFrame::Data(frame), FrameDirection::ToRouter).unwrap();
    socket.send(Message::Text(wire.into())).await.unwrap();
}

async fn delivery_barrier(
    fixture: &Fixture,
    socket: &mut WebSocketStream<TcpStream>,
    admitted: &AdmittedConnection,
) {
    socket
        .send(Message::Text(DELIVER_FRAME.into()))
        .await
        .unwrap();
    let event = fixture.inbox_events.recv().await;
    assert!(
        matches!(event, Some(LinkLaneEvent::Deliver { connection, time_left: Some(time_left), .. }) if connection == admitted.connection && u32::from(time_left) == 1)
    );
}

#[tokio::test]
async fn welcome_publishes_only_admitted_readiness_with_the_elapsed_anchor() {
    let mut fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    let command = LinkLaneCommand::ImmediateRequest {
        connection: unadmitted_attempt(),
        request: Box::new(mail_write(1)),
    };
    assert!(lane.watch_admitted().borrow().is_none());
    assert_eq!(
        lane.submit_command(command).now_or_never(),
        Some(LinkLaneDisposition::NotReached),
        "pre-admission command must be known not reached immediately"
    );
    assert!(socket.next().now_or_never().is_none());
    fixture.clock.advance(Duration::from_secs(3)).unwrap();
    fixture
        .clock
        .set_utc("2070-01-01T00:00:00Z".parse().unwrap());
    let admitted = admit(&mut fixture, &mut socket, &lane).await;
    assert_eq!(
        admitted.clock_anchor.central_time.to_rfc3339(),
        "2030-01-01T00:00:00+00:00"
    );
    assert_eq!(admitted.clock_anchor.observed_at, tick(3));
    delivery_barrier(&fixture, &mut socket, &admitted).await;
    socket.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        lane.next_event().await,
        Some(LinkLaneEvent::ConnectionNotAdmitted {
            connection: admitted.connection
        })
    );
    assert!(lane.watch_admitted().borrow().is_none());
    fixture.stop().await;
}

#[tokio::test]
async fn partial_batch_keeps_answered_members_and_never_reuses_a_prior_connection() {
    let mut fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    let admitted = admit(&mut fixture, &mut socket, &lane).await;
    let first = mail_write(1);
    let second = mail_write(2);
    let first_answer = ReplyPayload::central_unavailable(first.payload());
    let conflicting =
        ReplyPayload::authority_refused(first.payload(), MachineLinkCloseCode::Inactive);
    let requests = LinkRequestBatch::new(vec![first, second]).unwrap();
    let command = WriteBatchCommand {
        connection: admitted.connection.clone(),
        requests: requests.clone(),
    };
    let mut result = lane.submit_write_batch(command.clone());
    let outbound = tokio::select! { frame = text(&mut socket) => frame, disposition = &mut result => panic!("batch ended before its wire frame: {disposition:?}") };
    assert_eq!(
        decode_text(&outbound, FrameDirection::ToCentral).unwrap(),
        LinkFrame::Data(DataFrame::Batch { requests })
    );
    send_answer(
        &mut socket,
        DataFrame::Reply {
            correlation_id: correlation(1),
            result: Box::new(first_answer.clone()),
        },
    )
    .await;
    for bad_id in [correlation(9), correlation(1)] {
        send_answer(
            &mut socket,
            DataFrame::Reply {
                correlation_id: bad_id,
                result: Box::new(conflicting.clone()),
            },
        )
        .await;
    }
    delivery_barrier(&fixture, &mut socket, &admitted).await;
    assert!(
        result.as_mut().now_or_never().is_none(),
        "unknown and repeated replies settle no remaining member"
    );

    socket.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        result.await,
        WriteBatchDisposition::MemberResults {
            members: [
                WriteBatchMemberDisposition::Answered {
                    reply: CorrelatedRequestReply {
                        correlation_id: correlation(1),
                        result: Box::new(first_answer)
                    }
                },
                WriteBatchMemberDisposition::AnswerLost {
                    correlation_id: correlation(2)
                }
            ]
            .into(),
        }
    );
    assert_eq!(
        lane.next_event().await,
        Some(LinkLaneEvent::ConnectionNotAdmitted {
            connection: admitted.connection.clone()
        })
    );
    fixture.clock.not_due_check_now(tick(1)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let mut replacement = fixture.accept().await;
    let reopened = admit(&mut fixture, &mut replacement, &lane).await;
    assert_ne!(reopened.connection, admitted.connection);
    assert_eq!(
        lane.submit_write_batch(command).now_or_never(),
        Some(WriteBatchDisposition::NotReached),
        "a prior socket's command is never sent on the replacement"
    );
    fixture.stop().await;
}

#[tokio::test]
async fn receipts_and_status_settle_only_on_the_expected_correlation_and_length() {
    let mut fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    let admitted = admit(&mut fixture, &mut socket, &lane).await;
    let receipt = Receipt {
        message_id: "01890f2e-7b4c-7cc0-98c4-000000000009".parse().unwrap(),
        kind: ReceiptKind::Seen {
            completed_at: "2030-01-01T00:00:00Z".parse().unwrap(),
        },
    };
    let receipts = ReceiptBatch::new(vec![receipt]).unwrap();
    let mut receipt_result = lane.submit_command(LinkLaneCommand::SubmitReceipts {
        connection: admitted.connection.clone(),
        correlation_id: correlation(3),
        receipts: receipts.clone(),
    });
    let outbound = tokio::select! { frame = text(&mut socket) => frame, disposition = &mut receipt_result => panic!("receipts ended before its wire frame: {disposition:?}") };
    assert_eq!(
        decode_text(&outbound, FrameDirection::ToCentral).unwrap(),
        LinkFrame::Data(DataFrame::Receipts {
            correlation_id: correlation(3),
            receipts
        })
    );
    for (correlation_id, results) in [
        (correlation(3), vec![]),
        (correlation(9), vec![ReceiptResult::Recorded]),
    ] {
        send_answer(
            &mut socket,
            DataFrame::ReceiptResults {
                correlation_id,
                results,
            },
        )
        .await;
    }
    delivery_barrier(&fixture, &mut socket, &admitted).await;
    assert!(
        receipt_result.as_mut().now_or_never().is_none(),
        "bad length or correlation must never settle receipts"
    );
    send_answer(
        &mut socket,
        DataFrame::ReceiptResults {
            correlation_id: correlation(3),
            results: vec![ReceiptResult::Recorded],
        },
    )
    .await;
    assert_eq!(
        receipt_result.await,
        LinkLaneDisposition::Answered {
            reply: LinkLaneReply::ReceiptResults {
                correlation_id: correlation(3),
                results: vec![ReceiptResult::Recorded]
            }
        }
    );

    let request = mail_write(4);
    let caller = request.caller().clone();
    let query = RequestStatusQuery {
        request_id: request.payload().write_request_id().unwrap().clone(),
        scope: RequestScope::Organization,
    };
    let mut status = lane.submit_command(LinkLaneCommand::RequestStatus {
        connection: admitted.connection.clone(),
        correlation_id: correlation(4),
        caller: caller.clone(),
        query: query.clone(),
    });
    let outbound = tokio::select! { frame = text(&mut socket) => frame, disposition = &mut status => panic!("status ended before its wire frame: {disposition:?}") };
    assert_eq!(
        decode_text(&outbound, FrameDirection::ToCentral).unwrap(),
        LinkFrame::Data(DataFrame::RequestStatus {
            correlation_id: correlation(4),
            caller,
            query
        })
    );
    send_answer(
        &mut socket,
        DataFrame::RequestStatusReply {
            correlation_id: correlation(3),
            result: Box::new(RequestStatusAnswer::NotFound),
        },
    )
    .await;
    delivery_barrier(&fixture, &mut socket, &admitted).await;
    assert!(
        status.as_mut().now_or_never().is_none(),
        "late correlation must never settle request status"
    );
    send_answer(
        &mut socket,
        DataFrame::RequestStatusReply {
            correlation_id: correlation(4),
            result: Box::new(RequestStatusAnswer::NotFound),
        },
    )
    .await;
    assert_eq!(
        status.await,
        LinkLaneDisposition::Answered {
            reply: LinkLaneReply::RequestStatus {
                correlation_id: correlation(4),
                result: Box::new(RequestStatusAnswer::NotFound)
            }
        }
    );
    fixture.stop().await;
}

#[tokio::test]
async fn immediate_request_answers_on_admission_and_reports_loss_after_write() {
    let mut fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    let admitted = admit(&mut fixture, &mut socket, &lane).await;
    for request_number in [5, 6] {
        let request = Box::new(mail_write(request_number));
        let mut reply = lane.submit_command(LinkLaneCommand::ImmediateRequest {
            connection: admitted.connection.clone(),
            request: request.clone(),
        });
        let outbound = tokio::select! { frame = text(&mut socket) => frame, disposition = &mut reply => panic!("immediate request ended before its wire frame: {disposition:?}") };
        assert_eq!(
            decode_text(&outbound, FrameDirection::ToCentral).unwrap(),
            LinkFrame::Data(DataFrame::Request {
                request: request.clone()
            })
        );
        if request_number == 5 {
            let result = Box::new(ReplyPayload::central_unavailable(request.payload()));
            send_answer(
                &mut socket,
                DataFrame::Reply {
                    correlation_id: correlation(5),
                    result: result.clone(),
                },
            )
            .await;
            assert_eq!(
                reply.await,
                LinkLaneDisposition::Answered {
                    reply: LinkLaneReply::ImmediateRequest {
                        reply: CorrelatedRequestReply {
                            correlation_id: correlation(5),
                            result
                        }
                    }
                }
            );
        } else {
            socket.send(Message::Close(None)).await.unwrap();
            assert_eq!(
                reply.await,
                LinkLaneDisposition::AnswerLost,
                "a written single-frame command has an unknown result after close"
            );
        }
    }
    fixture.stop().await;
}
