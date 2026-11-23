use central_link_client::{EnrollmentEnd, LinkLaneEvent};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, MachineLinkCloseCode, TokenRenewalRefusal,
    TokenRenewalResult, decode_text, encode_text,
};
use futures_util::SinkExt;
use switchboard_central_link::link_policy::{LeaseVersion, LinkStatus};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::CloseFrame},
};

use super::{DELIVER_FRAME, Fixture, NEXT_REFRESHED, text, tick, welcome};
use std::time::Duration;

async fn send_renewal_result(
    socket: &mut WebSocketStream<TcpStream>,
    correlation_id: central_link_protocol::CorrelationId,
    result: TokenRenewalResult,
) {
    let wire = encode_text(
        &LinkFrame::Data(DataFrame::MachineTokenRenewal {
            correlation_id,
            result,
        }),
        FrameDirection::ToRouter,
    )
    .unwrap();
    socket.send(Message::Text(wire.into())).await.unwrap();
}

#[expect(
    clippy::panic,
    reason = "A missing typed renewal fails the fixture at the wire assertion."
)]
async fn refuse_renewal_with_reason(
    fixture: &mut Fixture,
    reason: TokenRenewalRefusal,
) -> WebSocketStream<TcpStream> {
    let mut socket = fixture.accept().await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    welcome(&mut socket).await;
    fixture
        .status
        .wait_for(|state| matches!(state, LinkStatus::Welcomed { .. }))
        .await
        .unwrap();
    fixture.clock.not_due_check_now(tick(20)).await;
    fixture.clock.advance(Duration::from_secs(90)).unwrap();
    assert_eq!(text(&mut socket).await, "ping");
    socket.send(Message::Text("pong".into())).await.unwrap();
    assert_eq!(fixture.acquired.recv().await, Some(Some(LeaseVersion(1))));
    let renewal = text(&mut socket).await;
    let LinkFrame::Data(DataFrame::RenewMachineToken { correlation_id, .. }) =
        decode_text(&renewal, FrameDirection::ToCentral).unwrap()
    else {
        panic!("expected a renewal on the existing connection");
    };
    send_renewal_result(
        &mut socket,
        correlation_id,
        TokenRenewalResult::Refused { reason },
    )
    .await;
    socket
}

async fn refuse_renewal(fixture: &mut Fixture) -> WebSocketStream<TcpStream> {
    refuse_renewal_with_reason(fixture, TokenRenewalRefusal::ConnectionEnrollmentEnded).await
}

#[tokio::test]
async fn presented_token_expired_refreshes_and_renews_on_the_same_link() {
    use futures_util::FutureExt;

    let mut fixture = Fixture::new().await;
    let mut socket =
        refuse_renewal_with_reason(&mut fixture, TokenRenewalRefusal::PresentedTokenExpired).await;
    assert!(receive_after_refusal(&mut fixture, &mut socket).await);
    assert_eq!(
        fixture.acquired.recv().now_or_never().flatten(),
        Some(Some(LeaseVersion(2))),
        "expired candidate must request a strictly fresher bearer on the original attempt"
    );
    let retry = text(&mut socket).await;
    let LinkFrame::Data(DataFrame::RenewMachineToken {
        correlation_id,
        token,
    }) = decode_text(&retry, FrameDirection::ToCentral).unwrap()
    else {
        panic!("expected another renewal on the original socket");
    };
    assert!(token.expose() == NEXT_REFRESHED);
    send_renewal_result(
        &mut socket,
        correlation_id,
        TokenRenewalResult::Renewed {
            central_time: "2030-01-01T00:01:30Z".parse().unwrap(),
            effective_expires_at: "2030-01-01T00:05:30Z".parse().unwrap(),
        },
    )
    .await;
    assert!(receive_after_refusal(&mut fixture, &mut socket).await);
    fixture.clock.advance(Duration::from_secs(31)).unwrap();
    assert_eq!(
        text(&mut socket).await,
        "ping",
        "only accepting the fresh bearer may carry the socket beyond its original deadline"
    );
    assert!(fixture.ended.try_recv().is_err());
    fixture.stop().await;
}

async fn receive_after_refusal(
    fixture: &mut Fixture,
    socket: &mut WebSocketStream<TcpStream>,
) -> bool {
    // TCP orders this Deliver after the refusal. Observing it proves the refusal was consumed.
    let _ = socket.send(Message::Text(DELIVER_FRAME.into())).await;
    tokio::select! {
        event = fixture.inbox_events.recv() => matches!(event, Some(LinkLaneEvent::Deliver { .. })),
        _ = fixture.status.wait_for(|status| !matches!(status, LinkStatus::Welcomed { .. })) => false,
    }
}

async fn send_final_close(socket: &mut WebSocketStream<TcpStream>, code: MachineLinkCloseCode) {
    socket
        .send(Message::Close(Some(CloseFrame {
            code: code.code().into(),
            reason: "".into(),
        })))
        .await
        .unwrap();
}

#[tokio::test]
async fn renewal_refusal_alone_preserves_enrollment_until_the_confirmed_deadline() {
    let mut fixture = Fixture::new().await;
    let mut socket = refuse_renewal(&mut fixture).await;
    assert!(
        receive_after_refusal(&mut fixture, &mut socket).await,
        "refusal alone must leave the socket welcomed"
    );
    assert!(fixture.ended.try_recv().is_err());
    fixture.clock.advance(Duration::from_secs(29)).unwrap();
    assert_eq!(
        text(&mut socket).await,
        "ping",
        "the last confirmed deadline still applies"
    );
    assert!(matches!(
        *fixture.status.borrow(),
        LinkStatus::Welcomed { .. }
    ));
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    fixture
        .status
        .wait_for(|status| *status == LinkStatus::Connecting)
        .await
        .unwrap();
    assert!(
        fixture.ended.try_recv().is_err(),
        "elapsed link authority cannot end the enrollment"
    );
    fixture.stop().await;
}

#[tokio::test]
async fn renewal_refusal_then_typed_close_retires_once_with_the_close_cause() {
    for (code, cause) in [
        (MachineLinkCloseCode::Revoked, EnrollmentEnd::Revoked),
        (MachineLinkCloseCode::Inactive, EnrollmentEnd::Inactive),
    ] {
        let mut fixture = Fixture::new().await;
        let mut socket = refuse_renewal(&mut fixture).await;
        // Wait until the refusal was read before sending the Close in a separate read.
        let still_open = receive_after_refusal(&mut fixture, &mut socket).await;
        if still_open {
            send_final_close(&mut socket, code).await;
        }
        assert_eq!(
            fixture.ended.recv().await,
            Some((1, cause)),
            "only the typed Close may retire this binding"
        );
        assert_eq!(
            *fixture.status.borrow(),
            LinkStatus::EnrollmentEnded { cause }
        );
        fixture.shutdown.send_replace(true);
        fixture.task.await.unwrap();
        assert_eq!(fixture.ended.recv().await, None, "the binding retires once");
    }
}

#[tokio::test]
async fn renewal_refusal_then_bare_drop_redials_before_a_final_close_can_retire() {
    let mut fixture = Fixture::new().await;
    let mut socket = refuse_renewal(&mut fixture).await;
    let was_welcomed = receive_after_refusal(&mut fixture, &mut socket).await;
    drop(socket);
    if was_welcomed {
        fixture
            .status
            .wait_for(|status| *status == LinkStatus::Connecting)
            .await
            .unwrap();
    }
    assert!(
        was_welcomed && fixture.ended.try_recv().is_err(),
        "a refused renewal and bare drop must leave the binding eligible to redial"
    );
    fixture.clock.not_due_check_now(tick(91)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let mut redial = fixture.accept().await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    send_final_close(&mut redial, MachineLinkCloseCode::Inactive).await;
    assert_eq!(
        fixture.ended.recv().await,
        Some((1, EnrollmentEnd::Inactive))
    );
    assert!(fixture.ended.try_recv().is_err());
    fixture.stop().await;
}
