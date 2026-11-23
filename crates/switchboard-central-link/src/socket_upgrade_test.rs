use futures_util::SinkExt;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error, Message, protocol::Role},
};

use super::{MAX_OUTBOUND_TEXT_BYTES, socket_config};

#[tokio::test]
async fn masked_write_buffer_accepts_its_exact_limit_and_refuses_one_more_byte() {
    for difference in [0, 1] {
        let (stream, _blocked_peer) = tokio::io::duplex(1);
        let mut socket =
            WebSocketStream::from_raw_socket(stream, Role::Client, Some(socket_config())).await;
        let result = socket.start_send_unpin(Message::Text(
            "a".repeat(MAX_OUTBOUND_TEXT_BYTES + difference).into(),
        ));
        if difference == 0 {
            assert!(
                result.is_ok(),
                "the write buffer holds exactly one capped masked frame"
            );
        } else {
            assert!(
                matches!(result, Err(Error::WriteBufferFull(_))),
                "a masked frame one byte over the buffer cap must be refused before buffering"
            );
        }
    }
}

#[cfg(feature = "test-support")]
#[expect(
    clippy::unwrap_used,
    reason = "The transport fixture must fail on an unexpected setup error."
)]
#[tokio::test]
async fn oversized_bearer_fails_before_the_http_upgrade() {
    use central_link_protocol::MachineTokenText;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{DialAddress, DialFailure, MAX_CREDENTIAL_BYTES};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dial =
        DialAddress::test_loopback(&format!("ws://{}/fixture", listener.local_addr().unwrap()))
            .unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut first_byte = [0_u8];
        stream.read_exact(&mut first_byte).await.unwrap();
        stream
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });
    let token = MachineTokenText::try_from("a".repeat(MAX_CREDENTIAL_BYTES + 1)).unwrap();
    let outcome = dial.dial(&token).await;
    peer.abort();
    let _ = peer.await;
    assert!(
        matches!(outcome, Err(DialFailure::Unauthorized)),
        "an oversized bearer must fail locally before opening the HTTP upgrade"
    );
}
