use std::{sync::Arc, time::Duration};

use central_link_client::{
    AdmittedConnection, CheckedBatchResults, ClientAction, ConnectionAttempt,
    CorrelatedReceiptResults, CorrelatedRequestReply, CorrelatedStatusReply,
    ImmediateRequestCommand, LinkClient, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane,
    RequestStatusCommand, SubmitReceiptsCommand, UntouchedCommand, WriteBatchCommand,
    WriteBatchDisposition, WriteBatchMemberDisposition,
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
        matches!(event, Some(LinkLaneEvent::Deliver { connection, offer, .. }) if connection == admitted.connection && offer.time_left().map(u32::from) == Some(1))
    );
}

#[tokio::test]
async fn welcome_publishes_only_admitted_readiness_with_the_elapsed_anchor() {
    let mut fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    let command = ImmediateRequestCommand {
        connection: unadmitted_attempt(),
        send_before: tick(50),
        request: Box::new(mail_write(1)),
    };
    assert!(lane.watch_admitted().borrow().is_none());
    assert_eq!(
        lane.submit_immediate_request(command).now_or_never(),
        Some(LinkLaneDisposition::NotReached {
            cause: UntouchedCommand::ConnectionUnavailable
        }),
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
        send_before: tick(50),
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
        WriteBatchDisposition::MemberResults(
            CheckedBatchResults::new(
                &command.requests,
                [
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
            )
            .unwrap()
        )
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
        Some(WriteBatchDisposition::NotReached {
            cause: UntouchedCommand::ConnectionUnavailable
        }),
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
    let mut receipt_result = lane.submit_receipts(SubmitReceiptsCommand {
        connection: admitted.connection.clone(),
        send_before: tick(50),
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
            reply: CorrelatedReceiptResults {
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
    let mut status = lane.request_status(RequestStatusCommand {
        connection: admitted.connection.clone(),
        send_before: tick(50),
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
            reply: CorrelatedStatusReply {
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
        let mut reply = lane.submit_immediate_request(ImmediateRequestCommand {
            connection: admitted.connection.clone(),
            send_before: tick(50),
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
                    reply: CorrelatedRequestReply {
                        correlation_id: correlation(5),
                        result
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

fn status_command(admitted: &AdmittedConnection, number: u8) -> RequestStatusCommand {
    let request = mail_write(number);
    RequestStatusCommand {
        connection: admitted.connection.clone(),
        send_before: tick(60),
        correlation_id: correlation(number),
        caller: request.caller().clone(),
        query: RequestStatusQuery {
            request_id: request.payload().write_request_id().unwrap().clone(),
            scope: RequestScope::Organization,
        },
    }
}

#[tokio::test]
async fn full_delivery_queue_backpressures_reads_without_losing_stamps_or_reconnecting() {
    let mut fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    let admitted = admit(&mut fixture, &mut socket, &lane).await;
    let ids = (1..=68)
        .map(|index| format!("01890f2e-7b4c-7cc0-98c4-{index:012x}"))
        .collect::<Vec<_>>();
    for id in &ids {
        let wire = DELIVER_FRAME.replace("01890f2e-7b4c-7cc0-98c4-000000000009", id);
        let _ = socket.send(Message::Text(wire.into())).await;
    }
    let paused = tokio::select! {
        () = fixture.clock.not_due_check_now(tick(60)) => true,
        _ = fixture.status.wait_for(|status| !matches!(status, LinkStatus::Welcomed { .. })) => false,
    };
    assert!(
        paused,
        "a full queue pauses socket reads without retiring the admitted connection"
    );
    fixture.clock.advance(Duration::from_secs(2)).unwrap();

    let mut status = lane.request_status(status_command(&admitted, 70));
    let wire = tokio::select! { wire = text(&mut socket) => wire, _ = &mut status => panic!("control write must proceed while offer reads are paused") };
    assert!(matches!(
        decode_text(&wire, FrameDirection::ToCentral),
        Ok(LinkFrame::Data(DataFrame::RequestStatus { .. }))
    ));
    send_answer(
        &mut socket,
        DataFrame::RequestStatusReply {
            correlation_id: correlation(70),
            result: Box::new(RequestStatusAnswer::NotFound),
        },
    )
    .await;

    for id in ids {
        let event = lane.next_event().await;
        assert!(
            matches!(event, Some(LinkLaneEvent::Deliver { connection, offer, .. })
            if connection == admitted.connection && offer.message().message_id().as_str() == id
            && offer.time_left().map(u32::from) == Some(1)),
            "each offer keeps its socket, order and original time-left"
        );
    }
    assert!(matches!(status.await, LinkLaneDisposition::Answered { .. }));
    assert_eq!(lane.watch_admitted().borrow().as_ref(), Some(&admitted));
    assert!(fixture.listener.accept().now_or_never().is_none());

    socket.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        lane.next_event().await,
        Some(LinkLaneEvent::ConnectionNotAdmitted {
            connection: admitted.connection.clone()
        })
    );
    fixture.clock.not_due_check_now(tick(3)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let mut replacement = fixture.accept().await;
    let replacement = admit(&mut fixture, &mut replacement, &lane).await;
    assert_ne!(replacement.connection, admitted.connection);
    fixture.stop().await;
}

#[tokio::test]
async fn queued_batch_expires_untouched_and_immediate_request_reports_busy() {
    let mut fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    let admitted = admit(&mut fixture, &mut socket, &lane).await;
    let mut status = lane.request_status(status_command(&admitted, 12));
    let _ = tokio::select! { wire = text(&mut socket) => wire, _ = &mut status => panic!("fixture expected status on the wire") };
    let mut batch = lane.submit_write_batch(WriteBatchCommand {
        connection: admitted.connection.clone(),
        send_before: tick(3),
        requests: LinkRequestBatch::new(vec![mail_write(13)]).unwrap(),
    });
    assert!(batch.as_mut().now_or_never().is_none());
    assert_eq!(
        lane.submit_immediate_request(ImmediateRequestCommand {
            connection: admitted.connection,
            send_before: tick(8),
            request: Box::new(mail_write(14))
        })
        .now_or_never(),
        Some(LinkLaneDisposition::NotReached {
            cause: UntouchedCommand::WriterBusy
        }),
        "immediate work must never wait behind the status answer timer",
    );

    fixture.clock.advance(Duration::from_secs(3)).unwrap();
    send_answer(
        &mut socket,
        DataFrame::RequestStatusReply {
            correlation_id: correlation(12),
            result: Box::new(RequestStatusAnswer::NotFound),
        },
    )
    .await;
    assert!(matches!(status.await, LinkLaneDisposition::Answered { .. }));
    let expired =
        tokio::select! { outcome = &mut batch => Some(outcome), _ = socket.next() => None };
    assert_eq!(
        expired,
        Some(WriteBatchDisposition::NotReached {
            cause: UntouchedCommand::SendWindowElapsed
        }),
        "queued batch must not leave at its exclusive boundary"
    );
    fixture.stop().await;
}

fn cadence_answer(cadence: workspace_model::central_mail::NoticeCadence) -> ReplyPayload {
    use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult};
    ReplyPayload::Mail(CentralMailResult::Done(
        CentralMailAnswer::NoticeCadenceSet { cadence },
    ))
}

#[tokio::test]
async fn batch_checks_same_family_success_and_preserves_distinguishable_answers_in_order() {
    use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult, NoticeCadence};

    let mut fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    let admitted = admit(&mut fixture, &mut socket, &lane).await;
    let first = mail_write(17);
    let wire = encode_text(
        &LinkFrame::Data(DataFrame::Request {
            request: Box::new(mail_write(18)),
        }),
        FrameDirection::ToCentral,
    )
    .unwrap();
    let LinkFrame::Data(DataFrame::Request { request: second }) =
        decode_text(&wire.replace("m25", "m55"), FrameDirection::ToCentral).unwrap()
    else {
        panic!("expected test request");
    };
    let requests = LinkRequestBatch::new(vec![first, *second]).unwrap();
    let mut batch = lane.submit_write_batch(WriteBatchCommand {
        connection: admitted.connection.clone(),
        send_before: tick(10),
        requests: requests.clone(),
    });
    let _ = tokio::select! { wire = text(&mut socket) => wire, _ = &mut batch => panic!("fixture expected batch on the wire") };

    let wrong_kind = ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::WakeSet {
        wake_id: "01890f2e-7b4c-7cc0-98c4-000000000099".parse().unwrap(),
        due_at: "2030-01-01T00:05:00Z".parse().unwrap(),
    }));
    for answer in [wrong_kind, cadence_answer(NoticeCadence::M55)] {
        send_answer(
            &mut socket,
            DataFrame::Reply {
                correlation_id: correlation(17),
                result: Box::new(answer),
            },
        )
        .await;
    }
    send_answer(
        &mut socket,
        DataFrame::Reply {
            correlation_id: correlation(18),
            result: Box::new(cadence_answer(NoticeCadence::M55)),
        },
    )
    .await;
    delivery_barrier(&fixture, &mut socket, &admitted).await;
    assert!(
        batch.as_mut().now_or_never().is_none(),
        "a same-family wrong success or a wrong echoed cadence settles nothing"
    );

    send_answer(
        &mut socket,
        DataFrame::Reply {
            correlation_id: correlation(17),
            result: Box::new(cadence_answer(NoticeCadence::M25)),
        },
    )
    .await;
    let members = [(17, NoticeCadence::M25), (18, NoticeCadence::M55)].map(|(number, cadence)| {
        WriteBatchMemberDisposition::Answered {
            reply: CorrelatedRequestReply {
                correlation_id: correlation(number),
                result: Box::new(cadence_answer(cadence)),
            },
        }
    });
    assert_eq!(
        batch.await,
        WriteBatchDisposition::MemberResults(CheckedBatchResults::new(&requests, members).unwrap())
    );
    fixture.stop().await;
}

#[expect(
    clippy::panic,
    reason = "The fixture must fail if its encoded direct mail frame does not decode."
)]
fn direct_request(number: u8, bytes: usize) -> LinkRequest {
    let body = "a".repeat(bytes);
    let wire = format!(
        r#"{{"kind":"request","request":{{"correlationId":"{}","caller":{{"kind":"session","value":{{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"}}}},"payload":{{"kind":"mail","request":{{"requestId":"01890f2e-7b4c-7cc0-98c4-0000000000{number:02x}","operation":{{"kind":"sendMessage","to":{{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"}},"body":"{body}","delivery":"steer","replyTo":null,"sentAt":"2030-01-01T00:00:00Z"}}}}}}}}}}"#,
        String::from(correlation(number))
    );
    let LinkFrame::Data(DataFrame::Request { request }) =
        decode_text(&wire, FrameDirection::ToCentral).unwrap()
    else {
        panic!("expected a direct request");
    };
    *request
}

fn sized_batch(target: usize) -> LinkRequestBatch {
    use central_link_client::{WRITE_BATCH_ENVELOPE_BYTES, encoded_request_size};
    const LARGE_MEMBER: usize = 60 * 1024;
    let mut remaining = target - WRITE_BATCH_ENVELOPE_BYTES;
    let mut requests = Vec::new();
    for number in 20..84 {
        let overhead = encoded_request_size(&direct_request(number, 1)).unwrap() - 1
            + usize::from(!requests.is_empty());
        let content = (remaining - overhead).min(LARGE_MEMBER);
        requests.push(direct_request(number, content));
        remaining -= overhead + content;
        if remaining == 0 {
            break;
        }
    }
    assert_eq!(remaining, 0);
    LinkRequestBatch::new(requests).unwrap()
}

#[tokio::test]
async fn exact_byte_budget_is_accepted_and_oversize_batch_is_untouched_on_the_same_socket() {
    use central_link_client::LINK_LANE_FRAME_BYTES;

    let mut fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    let admitted = admit(&mut fixture, &mut socket, &lane).await;
    let exact = sized_batch(LINK_LANE_FRAME_BYTES);
    let mut too_large = lane.submit_write_batch(WriteBatchCommand {
        connection: admitted.connection.clone(),
        send_before: tick(50),
        requests: sized_batch(LINK_LANE_FRAME_BYTES + 1),
    });
    assert_eq!(
        too_large.as_mut().now_or_never(),
        Some(WriteBatchDisposition::NotReached {
            cause: UntouchedCommand::FrameTooLarge
        }),
        "a valid over-budget batch is declined before enqueue",
    );
    assert!(socket.next().now_or_never().is_none());

    let mut batch = lane.submit_write_batch(WriteBatchCommand {
        connection: admitted.connection.clone(),
        send_before: tick(50),
        requests: exact.clone(),
    });
    let wire = tokio::select! { wire = text(&mut socket) => wire, _ = &mut batch => panic!("exact budget must fit") };
    assert_eq!(wire.len(), LINK_LANE_FRAME_BYTES);
    let expected = exact
        .requests()
        .iter()
        .map(|request| WriteBatchMemberDisposition::Answered {
            reply: CorrelatedRequestReply {
                correlation_id: request.correlation_id().clone(),
                result: Box::new(ReplyPayload::central_unavailable(request.payload())),
            },
        })
        .collect::<Vec<_>>();
    for member in &expected {
        if let WriteBatchMemberDisposition::Answered { reply } = member {
            send_answer(
                &mut socket,
                DataFrame::Reply {
                    correlation_id: reply.correlation_id.clone(),
                    result: reply.result.clone(),
                },
            )
            .await;
        }
    }
    assert_eq!(
        batch.await,
        WriteBatchDisposition::MemberResults(CheckedBatchResults::new(&exact, expected).unwrap())
    );

    let mut small = lane.request_status(status_command(&admitted, 2));
    let _ = tokio::select! { wire = text(&mut socket) => wire, _ = &mut small => panic!("a following small command must reach the same socket") };
    send_answer(
        &mut socket,
        DataFrame::RequestStatusReply {
            correlation_id: correlation(2),
            result: Box::new(RequestStatusAnswer::NotFound),
        },
    )
    .await;
    assert!(matches!(small.await, LinkLaneDisposition::Answered { .. }));
    assert_eq!(lane.watch_admitted().borrow().as_ref(), Some(&admitted));
    fixture.stop().await;
}
