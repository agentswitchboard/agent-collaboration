use std::time::Duration;

use central_link_client::ReadyLinkLane;
use central_link_protocol::{
    CentralCall, CentralCallReply, CorrelationId, DataFrame, FrameDirection, LinkCallRefusal,
    LinkFrame, MacWorkCall, MacWorkRpc, MachineLinkCloseCode, TokenRenewalResult, TypedCallResult,
    approval_rpc::RegisterRunReply,
    decode_text, encode_text,
    session_lifecycle_rpc::{CreateFleetSessionRpc, EndFleetSessionRpc},
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use router_clock::RuntimeClock;
use serde_json::json;
use switchboard_central_link::central_call_lane::{
    AcceptedCentralCall, CallAdmissionRefusal, CallEnding, CallSettlement, CentralCallSubmission,
};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::CloseFrame},
};

use super::{
    Fixture, registration_request, reply_registration, text, tick, welcome_before_registration,
};

const ORGANIZATION: &str = "019f0000-0000-7000-8000-000000000001";
const OTHER: &str = "019f0000-0000-7000-8000-000000000002";

fn end_call() -> MacWorkCall {
    EndFleetSessionRpc::work_request(
        serde_json::from_value(json!(ORGANIZATION)).unwrap(),
        serde_json::from_value(json!({
            "target": {"kind": "centralId", "sessionId": OTHER}, "cause": {"kind": "givenUp"}
        }))
        .unwrap(),
    )
}

fn end_reply(target: &str, result: &str) -> CentralCallReply {
    serde_json::from_value(json!({
        "call":"endFleetSession", "envelope":{"target":target,"body":{"result":result}}
    }))
    .unwrap()
}

#[expect(
    clippy::panic,
    reason = "A scripted accepted-call case must have obtained a delivery receipt."
)]
fn accepted(submission: CentralCallSubmission) -> AcceptedCentralCall {
    match submission {
        CentralCallSubmission::Accepted(receipt) => receipt,
        CentralCallSubmission::RefusedByLink(reason) => panic!("refused submission: {reason:?}"),
        CentralCallSubmission::NotReached(reason) => panic!("unwritten submission: {reason:?}"),
    }
}

#[expect(
    clippy::panic,
    reason = "A missing on-wire work call must fail the scripted peer."
)]
async fn work_frame(socket: &mut WebSocketStream<TcpStream>) -> CorrelationId {
    let LinkFrame::Data(DataFrame::CallCentral {
        correlation_id,
        request,
    }) = decode_text(&text(socket).await, FrameDirection::ToCentral).unwrap()
    else {
        panic!("expected a Central work call")
    };
    assert!(matches!(*request, CentralCall::EndFleetSession(_)));
    correlation_id
}

async fn sent_reply(
    socket: &mut WebSocketStream<TcpStream>,
    correlation_id: CorrelationId,
    reply: TypedCallResult<CentralCallReply>,
) {
    let frame = LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result: Box::new(reply),
    });
    let wire = encode_text(&frame, FrameDirection::ToRouter).unwrap();
    socket.send(Message::Text(wire.into())).await.unwrap();
}

async fn observed_reads(socket: &mut WebSocketStream<TcpStream>) {
    // The driver's read loop processes an entire earlier text frame before
    // the WebSocket layer can read this Ping and flush the matching Pong.
    socket
        .send(Message::Ping(b"read-barrier".as_slice().into()))
        .await
        .unwrap();
    assert!(
        matches!(socket.next().await, Some(Ok(Message::Pong(bytes))) if bytes.as_ref() == b"read-barrier")
    );
}

#[tokio::test]
async fn work_waits_for_registration_and_three_kinds_of_wrong_reply_leave_it_pending() {
    let _diagnostics = super::credential_diagnostics::capture_trace();
    let fixture = Fixture::new().await;
    let lane = &fixture.inbox_events.lane;
    let work = lane.central_calls();
    let mut socket = fixture.accept().await;
    welcome_before_registration(&mut socket).await;
    let (registration_id, target, _) = registration_request(&mut socket).await;
    assert!(matches!(
        work.submit(end_call(), tick(10)),
        CentralCallSubmission::RefusedByLink(CallAdmissionRefusal::Unavailable)
    ));
    assert!(socket.next().now_or_never().is_none());
    reply_registration(
        &mut socket,
        registration_id.clone(),
        target.clone(),
        RegisterRunReply::Registered {},
    )
    .await;
    lane.watch_registered()
        .wait_for(Option::is_some)
        .await
        .unwrap();

    let receipt = accepted(work.submit(end_call(), tick(10)));
    let result = receipt.resolve();
    tokio::pin!(result);
    let correlation = work_frame(&mut socket).await;
    assert_ne!(registration_id, correlation);

    reply_registration(
        &mut socket,
        correlation.clone(),
        target,
        RegisterRunReply::Registered {},
    )
    .await;
    observed_reads(&mut socket).await;
    assert_eq!(super::credential_diagnostics::work_wrong_kinds(), 1);
    assert_eq!(super::credential_diagnostics::registration_mismatches(), 0);
    assert!(result.as_mut().now_or_never().is_none());

    let wrong_kind = serde_json::from_value(json!({
        "call":"createFleetSession","envelope":{"target":ORGANIZATION,"body":{"result":"superseded"}}
    })).unwrap();
    for reply in [
        wrong_kind,
        end_reply(OTHER, "noSuchStart"),
        end_reply(ORGANIZATION, "notYoursToEnd"),
    ] {
        sent_reply(
            &mut socket,
            correlation.clone(),
            TypedCallResult::Answered { reply },
        )
        .await;
        observed_reads(&mut socket).await;
        assert!(result.as_mut().now_or_never().is_none());
    }
    assert_eq!(super::credential_diagnostics::work_invalid_replies(), 1);
    sent_reply(
        &mut socket,
        registration_id,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::WrongTarget,
        },
    )
    .await;
    observed_reads(&mut socket).await;
    assert!(result.as_mut().now_or_never().is_none());

    sent_reply(
        &mut socket,
        OTHER.parse().unwrap(),
        TypedCallResult::Answered {
            reply: end_reply(ORGANIZATION, "noSuchStart"),
        },
    )
    .await;
    observed_reads(&mut socket).await;
    assert!(result.as_mut().now_or_never().is_none());

    sent_reply(
        &mut socket,
        correlation,
        TypedCallResult::Answered {
            reply: end_reply(ORGANIZATION, "noSuchStart"),
        },
    )
    .await;
    let CallSettlement::Answered(reply) = result.await else {
        panic!("fitting reply did not answer")
    };
    assert!(EndFleetSessionRpc::take_work_reply(*reply).is_some());
    fixture.stop().await;
}

#[tokio::test]
async fn written_calls_end_at_equality_and_do_not_replay_after_reconnect() {
    let _diagnostics = super::credential_diagnostics::capture_trace();
    let fixture = Fixture::new().await;
    let lane = &fixture.inbox_events.lane;
    let work = lane.central_calls();
    let mut socket = fixture.accept().await;
    super::welcome(&mut socket).await;
    let mut registered = lane.watch_registered();
    registered.wait_for(Option::is_some).await.unwrap();
    let before = accepted(work.submit(end_call(), tick(2)));
    let old = work_frame(&mut socket).await;
    sent_reply(
        &mut socket,
        old,
        TypedCallResult::Answered {
            reply: end_reply(ORGANIZATION, "notYoursToEnd"),
        },
    )
    .await;
    observed_reads(&mut socket).await;
    assert_eq!(super::credential_diagnostics::work_invalid_replies(), 1);
    fixture.clock.advance(Duration::from_secs(2)).unwrap();
    assert_eq!(fixture.clock.monotonic_now(), tick(2));
    assert!(matches!(
        before.resolve().await,
        CallSettlement::AnswerLost(CallEnding::Deadline)
    ));

    let ended = accepted(work.submit(end_call(), tick(10)));
    work_frame(&mut socket).await;
    socket.send(Message::Close(None)).await.unwrap();
    assert!(matches!(
        ended.resolve().await,
        CallSettlement::AnswerLost(CallEnding::CarrierClosed(None))
    ));
    registered.wait_for(Option::is_none).await.unwrap();
    let ready = fixture.clock.monotonic_now().after(Duration::from_secs(1));
    fixture.clock.not_due_check_now(ready).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let mut second = fixture.accept().await;
    super::welcome(&mut second).await;
    registered.wait_for(Option::is_some).await.unwrap();
    observed_reads(&mut second).await;
    assert!(second.next().now_or_never().is_none());
    fixture.stop().await;
}

#[tokio::test]
async fn an_invalid_create_with_a_secret_leaves_the_call_waiting_and_logs_no_secret() {
    use super::credential_diagnostics::{REPLY_SECRET_MARKER, capture_trace, work_invalid_replies};

    let diagnostics = capture_trace();
    let fixture = Fixture::new().await;
    let lane = &fixture.inbox_events.lane;
    let work = lane.central_calls();
    let mut socket = fixture.accept().await;
    super::welcome(&mut socket).await;
    lane.watch_registered()
        .wait_for(Option::is_some)
        .await
        .unwrap();
    let create = || {
        CreateFleetSessionRpc::work_request(
        serde_json::from_value(json!(ORGANIZATION)).unwrap(),
        serde_json::from_value(json!({
            "operationId": OTHER, "attempt": 1, "agentProgram":"codex", "sessionKind":{"kind":"ordinary"},
            "creator":{"kind":"session","sessionId":OTHER,"presented":{"kind":"nothing"}},
        })).unwrap(),
    )
    };
    let answered = accepted(work.submit(create(), tick(1)));
    let expiring = accepted(work.submit(create(), tick(2)));
    let answered_result = answered.resolve();
    let expiring_result = expiring.resolve();
    tokio::pin!(answered_result, expiring_result);
    let mut numbers = Vec::new();
    for _ in 0..2 {
        let LinkFrame::Data(DataFrame::CallCentral {
            correlation_id,
            request,
        }) = decode_text(&text(&mut socket).await, FrameDirection::ToCentral).unwrap()
        else {
            panic!("expected a Create call");
        };
        assert!(matches!(*request, CentralCall::CreateFleetSession(_)));
        sent_reply(&mut socket, correlation_id.clone(), TypedCallResult::Answered { reply: serde_json::from_value(json!({
            "call":"createFleetSession", "envelope":{"target":ORGANIZATION,"body":{
                "result":"reserved","sessionId":OTHER,"secret":REPLY_SECRET_MARKER,"reservedUntil":"2030-01-01T00:00:20.000Z"
            }}
        })).unwrap() }).await;
        numbers.push(correlation_id);
    }
    observed_reads(&mut socket).await;
    assert_eq!(work_invalid_replies(), 2);
    diagnostics.assert_no_session_secret_in_driver_diagnostics();
    assert!(answered_result.as_mut().now_or_never().is_none());
    assert!(expiring_result.as_mut().now_or_never().is_none());
    let [answered_id, _] = numbers.as_slice() else {
        panic!("expected two distinct work replies")
    };
    sent_reply(&mut socket, answered_id.clone(), TypedCallResult::Answered { reply: serde_json::from_value(json!({
        "call":"createFleetSession", "envelope":{"target":ORGANIZATION,"body":{"result":"noSecret"}}
    })).unwrap() }).await;
    assert!(matches!(answered_result.await, CallSettlement::Answered(_)));
    fixture.clock.advance(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        expiring_result.await,
        CallSettlement::AnswerLost(CallEnding::Deadline)
    ));
    fixture.stop().await;
}

#[tokio::test]
async fn operator_no_secret_and_operation_id_new_ended_replies_settle_nothing() {
    let _diagnostics = super::credential_diagnostics::capture_trace();
    let fixture = Fixture::new().await;
    let lane = &fixture.inbox_events.lane;
    let work = lane.central_calls();
    let mut socket = fixture.accept().await;
    super::welcome(&mut socket).await;
    lane.watch_registered()
        .wait_for(Option::is_some)
        .await
        .unwrap();

    let operator_create = || {
        CreateFleetSessionRpc::work_request(
            serde_json::from_value(json!(ORGANIZATION)).unwrap(),
            serde_json::from_value(json!({
                "operationId":OTHER,"attempt":1,"agentProgram":"codex",
                "sessionKind":{"kind":"ordinary"},"creator":{"kind":"routerOperator"},
            }))
            .unwrap(),
        )
    };
    let end_by_operation = || {
        EndFleetSessionRpc::work_request(
            serde_json::from_value(json!(ORGANIZATION)).unwrap(),
            serde_json::from_value(json!({
                "target":{"kind":"operationId","operationId":OTHER,
                    "contentsDigest":"SZwBM9eK4U9QL0d9-wgv_SVQcRUyZXc3HzhJojc6a7c"},
                "cause":{"kind":"givenUp"},
            }))
            .unwrap(),
        )
    };
    let settlement =
        json!({"state":"ended","cause":{"kind":"givenUp"},"time":"2030-01-01T00:00:00.000Z"});
    let cases: [(fn() -> MacWorkCall, _, _, _); 2] = [
        (
            operator_create,
            "createFleetSession",
            json!({"result":"noSecret"}),
            json!({"result":"superseded"}),
        ),
        (
            end_by_operation,
            "endFleetSession",
            json!({"result":"settled","settlement":settlement}),
            json!({"result":"alreadySettled","settlement":settlement}),
        ),
    ];
    let mut original_deadlines = Vec::new();
    let mut invalid_count = 0;
    for (request, marker, invalid, fitting) in cases {
        for later_positive in [true, false] {
            let receipt = accepted(work.submit(request(), tick(2)));
            let result = receipt.resolve();
            let mut result = Box::pin(result);
            let LinkFrame::Data(DataFrame::CallCentral {
                correlation_id,
                request,
            }) = decode_text(&text(&mut socket).await, FrameDirection::ToCentral).unwrap()
            else {
                panic!("there must be exactly one work call for this receipt");
            };
            assert_eq!(serde_json::to_value(request).unwrap()["call"], marker);
            let reply = |body| TypedCallResult::Answered {
                reply: serde_json::from_value(json!({
                    "call":marker,"envelope":{"target":ORGANIZATION,"body":body}
                }))
                .unwrap(),
            };
            sent_reply(&mut socket, correlation_id.clone(), reply(invalid.clone())).await;
            observed_reads(&mut socket).await;
            invalid_count += 1;
            assert_eq!(
                super::credential_diagnostics::work_invalid_replies(),
                invalid_count
            );
            assert!(result.as_mut().now_or_never().is_none());
            if later_positive {
                // End's historical settlement is valid even though the same new settlement is invalid.
                sent_reply(&mut socket, correlation_id, reply(fitting.clone())).await;
                assert!(matches!(result.await, CallSettlement::Answered(_)));
            } else {
                original_deadlines.push(result);
            }
        }
    }
    observed_reads(&mut socket).await;
    assert!(socket.next().now_or_never().is_none());
    fixture.clock.advance(Duration::from_secs(2)).unwrap();
    for call in original_deadlines {
        assert!(matches!(
            call.await,
            CallSettlement::AnswerLost(CallEnding::Deadline)
        ));
    }
    fixture.stop().await;
}

#[tokio::test]
async fn closing_the_socket_preserves_an_already_selected_central_refusal() {
    let fixture = Fixture::new().await;
    let lane = &fixture.inbox_events.lane;
    let work = lane.central_calls();
    let mut socket = fixture.accept().await;
    super::welcome(&mut socket).await;
    let mut registered = lane.watch_registered();
    registered.wait_for(Option::is_some).await.unwrap();
    let first = accepted(work.submit(end_call(), tick(10)));
    let correlation = work_frame(&mut socket).await;
    sent_reply(
        &mut socket,
        correlation,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::WrongTarget,
        },
    )
    .await;
    observed_reads(&mut socket).await;
    let second = accepted(work.submit(end_call(), tick(10)));
    work_frame(&mut socket).await;
    socket
        .send(Message::Close(Some(CloseFrame {
            code: MachineLinkCloseCode::Revoked.code().into(),
            reason: "".into(),
        })))
        .await
        .unwrap();
    registered.wait_for(Option::is_none).await.unwrap();
    assert!(matches!(
        first.resolve().await,
        CallSettlement::RefusedByCentral(LinkCallRefusal::WrongTarget)
    ));
    assert!(matches!(
        second.resolve().await,
        CallSettlement::AnswerLost(CallEnding::EnrollmentEnded(
            central_link_client::EnrollmentEnd::Revoked
        ))
    ));
    assert!(matches!(
        work.submit(end_call(), tick(10)),
        CentralCallSubmission::RefusedByLink(CallAdmissionRefusal::EnrollmentEnded)
    ));
    fixture.stop().await;
}

#[tokio::test]
async fn due_renewal_precedes_queued_work_and_extends_the_same_socket() {
    let fixture = Fixture::new().await;
    let lane = &fixture.inbox_events.lane;
    let work = lane.central_calls();
    let mut socket = fixture.accept().await;
    super::welcome(&mut socket).await;
    lane.watch_registered()
        .wait_for(Option::is_some)
        .await
        .unwrap();

    fixture.clock.advance(Duration::from_secs(60)).unwrap();
    let offered_with_renewal = accepted(work.submit(end_call(), tick(75)));
    assert_eq!(text(&mut socket).await, "ping");
    socket.send(Message::Text("pong".into())).await.unwrap();
    let LinkFrame::Data(DataFrame::RenewMachineToken { correlation_id, .. }) =
        decode_text(&text(&mut socket).await, FrameDirection::ToCentral).unwrap()
    else {
        panic!("the due renewal must be written before the offered work call");
    };
    let renewed = LinkFrame::Data(DataFrame::MachineTokenRenewal {
        correlation_id,
        result: TokenRenewalResult::Renewed {
            central_time: "2030-01-01T00:01:00Z".parse().unwrap(),
            effective_expires_at: "2030-01-01T00:06:00Z".parse().unwrap(),
        },
    });
    let wire = encode_text(&renewed, FrameDirection::ToRouter).unwrap();
    socket.send(Message::Text(wire.into())).await.unwrap();
    let work_correlation = work_frame(&mut socket).await;
    sent_reply(
        &mut socket,
        work_correlation,
        TypedCallResult::Answered {
            reply: end_reply(ORGANIZATION, "noSuchStart"),
        },
    )
    .await;
    assert!(matches!(
        offered_with_renewal.resolve().await,
        CallSettlement::Answered(_)
    ));

    for _ in 0..3 {
        fixture.clock.advance(Duration::from_secs(20)).unwrap();
        assert_eq!(text(&mut socket).await, "ping");
        socket.send(Message::Text("pong".into())).await.unwrap();
        observed_reads(&mut socket).await;
    }
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let after_old_expiry = accepted(work.submit(end_call(), tick(130)));
    let work_correlation = work_frame(&mut socket).await;
    sent_reply(
        &mut socket,
        work_correlation,
        TypedCallResult::Answered {
            reply: end_reply(ORGANIZATION, "noSuchStart"),
        },
    )
    .await;
    assert!(matches!(
        after_old_expiry.resolve().await,
        CallSettlement::Answered(_)
    ));
    fixture.stop().await;
}
