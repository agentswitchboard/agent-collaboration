#![expect(
    clippy::panic,
    reason = "Unexpected socket or lane outcomes fail the test."
)]

use std::time::Duration;

use central_link_client::{
    AdmittedConnection, CorrelatedRequestReply, ImmediateRequestCommand, LinkLaneDisposition,
    LinkLaneEvent, ReadyLinkLane,
};
use central_link_protocol::{
    DataFrame, FrameDirection, HandledMacCalls, LinkCallRefusal, LinkFrame, MacCapabilities,
    ProtocolError, ReplyPayload, TypedCallResult, decode_text,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use switchboard_central_link::link_lane::LinkLaneHandle;
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

use super::{
    Fixture, acknowledge_registration,
    admitted_commands::{mail_write, send_answer},
    machine, send_welcome, text, tick, welcome,
};

const KNOWN_CALL: &str = "01890f2e-7b4c-7cc0-98c4-0000000000a1";
const UNKNOWN_CALL: &str = "01890f2e-7b4c-7cc0-98c4-0000000000a2";

fn known_call() -> Value {
    json!({
        "kind": "callMac", "correlationId": KNOWN_CALL,
        "request": {"call": "approvalNotice", "envelope": {
            "target": {"organizationId": KNOWN_CALL, "machineId": machine(),
                       "enrollmentId": KNOWN_CALL, "sessionId": "01890f2e-7b4c-7cc0-98c4-0000000000a3"},
            "body": {"requestId": KNOWN_CALL}
        }}
    })
}

fn unknown_call() -> Value {
    json!({
        "kind": "callMac", "correlationId": UNKNOWN_CALL,
        "request": {"call": "fixtureFutureCall", "envelope": {
            "target": machine(), "body": {"sequence": 7}
        }}
    })
}

async fn admitted(lane: &LinkLaneHandle) -> AdmittedConnection {
    let Some(LinkLaneEvent::ConnectionAdmitted(admission)) = lane.next_event().await else {
        panic!("Welcome must admit the lane");
    };
    admission
}

async fn ordinary_exchange(
    lane: &LinkLaneHandle,
    socket: &mut WebSocketStream<TcpStream>,
    admission: &AdmittedConnection,
    expect_advertisement: bool,
) {
    let request = mail_write(21);
    let correlation_id = request.correlation_id().clone();
    let answer: ReplyPayload = serde_json::from_value(json!({
        "kind": "mail", "reply": {"kind": "done", "value": {
            "kind": "noticeCadenceSet", "cadence": "m25"
        }}
    }))
    .unwrap();
    let expected_request = LinkFrame::Data(DataFrame::Request {
        request: Box::new(request.clone()),
    });
    let mut outcome = lane.submit_immediate_request(ImmediateRequestCommand {
        connection: admission.connection.clone(),
        send_before: tick(50),
        request: Box::new(request),
    });
    if expect_advertisement {
        let first = tokio::select! {
            frame = text(socket) => frame,
            disposition = &mut outcome => panic!("request ended before the first wire frame: {disposition:?}"),
        };
        assert_eq!(
            decode_text(&first, FrameDirection::ToCentral).unwrap(),
            LinkFrame::Data(DataFrame::MacCapabilities(MacCapabilities {
                handled_calls: HandledMacCalls::default(),
            })),
            "the first outbound data frame after Welcome must advertise an empty call set"
        );
        acknowledge_registration(socket).await;
    }
    let next = tokio::select! {
        frame = text(socket) => frame,
        disposition = &mut outcome => panic!("ordinary request ended before its wire frame: {disposition:?}"),
    };
    assert_eq!(
        decode_text(&next, FrameDirection::ToCentral).unwrap(),
        expected_request,
        "the ordinary request must follow, without another advertisement"
    );
    send_answer(
        socket,
        DataFrame::Reply {
            correlation_id: correlation_id.clone(),
            result: Box::new(answer.clone()),
        },
    )
    .await;
    let received = tokio::select! {
        disposition = &mut outcome => disposition,
        frame = socket.next() => panic!("unexpected extra outbound frame during ordinary exchange: {frame:?}"),
    };
    assert!(
        matches!(received, LinkLaneDisposition::Answered { reply, .. }
            if reply == CorrelatedRequestReply { correlation_id, result: Box::new(answer) }),
        "the ordinary request must settle from its reply on this socket"
    );
}

#[tokio::test]
async fn empty_capabilities_are_first_and_once_on_each_socket() {
    let fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    send_welcome(&mut socket).await;
    let first = admitted(&lane).await;
    ordinary_exchange(&lane, &mut socket, &first, true).await;
    socket.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        lane.next_event().await,
        Some(LinkLaneEvent::ConnectionNotAdmitted {
            connection: first.connection.clone(),
        })
    );
    fixture.clock.not_due_check_now(tick(1)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();

    let mut replacement = fixture.accept().await;
    send_welcome(&mut replacement).await;
    let second = admitted(&lane).await;
    assert_ne!(first.connection, second.connection);
    ordinary_exchange(&lane, &mut replacement, &second, true).await;
    fixture.stop().await;
}

async fn expect_refusal(
    socket: &mut WebSocketStream<TcpStream>,
    correlation_id: &str,
    allow_advertisement: bool,
) {
    let mut frame = socket.next().await;
    if allow_advertisement
        && matches!(&frame, Some(Ok(Message::Text(text)))
            if matches!(decode_text(text, FrameDirection::ToCentral),
                Ok(LinkFrame::Data(DataFrame::MacCapabilities(_)))))
    {
        acknowledge_registration(socket).await;
        frame = socket.next().await;
    }
    let Some(Ok(Message::Text(frame))) = frame else {
        panic!("expected typed refusal for {correlation_id} on the original socket, got {frame:?}");
    };
    assert_eq!(
        decode_text(&frame, FrameDirection::ToCentral).unwrap(),
        LinkFrame::Data(DataFrame::MacCallReply {
            correlation_id: correlation_id.parse().unwrap(),
            result: Box::new(TypedCallResult::Refused {
                reason: LinkCallRefusal::UnknownCall,
            }),
        })
    );
}

#[tokio::test]
async fn both_unhandled_calls_are_refused_and_the_same_socket_carries_an_ordinary_reply() {
    let fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    send_welcome(&mut socket).await;
    let admission = admitted(&lane).await;

    let known = known_call().to_string();
    assert!(matches!(
        decode_text(&known, FrameDirection::ToRouter).unwrap(),
        LinkFrame::Data(DataFrame::CallMac { .. })
    ));
    socket.send(Message::Text(known.into())).await.unwrap();
    expect_refusal(&mut socket, KNOWN_CALL, true).await;

    let unknown = unknown_call().to_string();
    assert!(matches!(
        decode_text(&unknown, FrameDirection::ToRouter),
        Err(ProtocolError::UnknownTypedCall { correlation_id }) if correlation_id == UNKNOWN_CALL.parse().unwrap()
    ));
    socket.send(Message::Text(unknown.into())).await.unwrap();
    expect_refusal(&mut socket, UNKNOWN_CALL, false).await;

    ordinary_exchange(&lane, &mut socket, &admission, false).await;
    assert_eq!(lane.watch_admitted().borrow().as_ref(), Some(&admission));
    fixture.stop().await;
}

#[tokio::test]
async fn either_call_before_welcome_remains_a_phase_fault() {
    for call in [known_call(), unknown_call()] {
        let fixture = Fixture::new().await;
        let mut socket = fixture.accept().await;
        socket
            .send(Message::Text(call.to_string().into()))
            .await
            .unwrap();
        assert!(matches!(
            socket.next().await,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None
        ));
        assert!(
            fixture
                .inbox_events
                .lane
                .watch_admitted()
                .borrow()
                .is_none()
        );
        fixture.stop().await;
    }
}

#[tokio::test]
async fn malformed_or_wrong_direction_calls_still_close_an_admitted_socket() {
    let mut extra_field = unknown_call();
    extra_field["request"]["envelope"]["extra"] = json!(true);
    let mut bad_correlation = unknown_call();
    bad_correlation["correlationId"] = json!("invalid");
    let mut wrong_direction = unknown_call();
    wrong_direction["kind"] = json!("callCentral");
    for call in [extra_field, bad_correlation, wrong_direction] {
        let fixture = Fixture::new().await;
        let lane = fixture.inbox_events.lane.clone();
        let mut socket = fixture.accept().await;
        welcome(&mut socket).await;
        let admission = admitted(&lane).await;
        socket
            .send(Message::Text(call.to_string().into()))
            .await
            .unwrap();
        assert!(matches!(
            socket.next().await,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None
        ));
        assert_eq!(
            lane.next_event().await,
            Some(LinkLaneEvent::ConnectionNotAdmitted {
                connection: admission.connection
            })
        );
        fixture.stop().await;
    }
}
