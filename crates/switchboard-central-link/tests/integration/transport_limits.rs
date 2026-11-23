use central_link_client::LinkLaneEvent;
use futures_util::SinkExt;
use switchboard_central_link::link_policy::LinkStatus;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::frame::{
            Frame,
            coding::{Data, OpCode},
        },
    },
};

use super::{DELIVER_FRAME, Fixture, welcome};

const EXPECTED_MESSAGE_LIMIT: usize = 1024 * 1024;

async fn send_padded_delivery(
    socket: &mut WebSocketStream<TcpStream>,
    size: usize,
    fragmented: bool,
) {
    let mut wire = DELIVER_FRAME.as_bytes().to_vec();
    wire.resize(size, b' ');
    if fragmented {
        let tail = wire.split_off(size / 2);
        socket
            .send(Message::Frame(Frame::message(
                wire,
                OpCode::Data(Data::Text),
                false,
            )))
            .await
            .unwrap();
        let _ = socket
            .send(Message::Frame(Frame::message(
                tail,
                OpCode::Data(Data::Continue),
                true,
            )))
            .await;
    } else {
        let _ = socket
            .send(Message::Text(String::from_utf8(wire).unwrap().into()))
            .await;
    }
}

async fn assert_message_limit(fragmented: bool) {
    let mut fixture = Fixture::new().await;
    let mut socket = fixture.accept().await;
    welcome(&mut socket).await;
    fixture
        .status
        .wait_for(|status| matches!(status, LinkStatus::Welcomed { .. }))
        .await
        .unwrap();
    send_padded_delivery(&mut socket, EXPECTED_MESSAGE_LIMIT, fragmented).await;
    tokio::select! {
        event = fixture.inbox_events.recv() => assert!(matches!(event, Some(LinkLaneEvent::Deliver { .. }))),
        exit = fixture.status.wait_for(|status| *status == LinkStatus::Connecting) =>
            assert!(exit.is_err(), "one MiB must be accepted"),
    }
    send_padded_delivery(&mut socket, EXPECTED_MESSAGE_LIMIT + 1, fragmented).await;
    tokio::select! {
        event = fixture.inbox_events.recv() => assert!(event.is_none(), "one byte over one MiB must never reach the inbox"),
        exit = fixture.status.wait_for(|status| *status == LinkStatus::Connecting) => assert!(exit.is_ok()),
    }
    assert_eq!(*fixture.status.borrow(), LinkStatus::Connecting);
    fixture.stop().await;
}

#[tokio::test]
async fn single_frame_has_a_one_mib_inbound_limit() {
    assert_message_limit(false).await;
}

#[tokio::test]
async fn fragmented_message_has_a_one_mib_inbound_limit() {
    assert_message_limit(true).await;
}
