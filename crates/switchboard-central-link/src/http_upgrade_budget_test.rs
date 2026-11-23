use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
};

use central_link_protocol::MachineLinkCloseCode;
use futures_util::StreamExt;
use http_body_util::Empty;
use hyper::{Request, body::Bytes};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::tungstenite::{Message, handshake::derive_accept_key};

use super::super::{DialFailure, Socket, upgrade_over_stream};

const BYTE_BUDGET: usize = 64 * 1024;
const READ_BUDGET: usize = 512;
const TEST_SOCKET_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

struct ObservedStream {
    stream: TcpStream,
    chunk_limit: usize,
    successful_reads: Arc<Mutex<Vec<usize>>>,
}

impl AsyncRead for ObservedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let size = buffer.remaining().min(self.chunk_limit);
        let mut chunk = ReadBuf::new(buffer.initialize_unfilled_to(size));
        ready!(Pin::new(&mut self.stream).poll_read(context, &mut chunk))?;
        let filled = chunk.filled().len();
        buffer.advance(filled);
        if filled != 0 {
            self.successful_reads
                .lock()
                .map_err(|_| io::Error::other("read observation lock poisoned"))?
                .push(filled);
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ObservedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

fn padded_head(prefix: &str, total_bytes: usize) -> Vec<u8> {
    let mut head = format!("{prefix}X-Padding: ").into_bytes();
    head.resize(total_bytes - 4, b'a');
    head.extend_from_slice(b"\r\n\r\n");
    head
}

fn switching_head(total_bytes: usize) -> Vec<u8> {
    switching_head_with_version(total_bytes, "HTTP/1.1")
}

fn switching_head_with_version(total_bytes: usize, version: &str) -> Vec<u8> {
    let accept_key = derive_accept_key(TEST_SOCKET_KEY.as_bytes());
    padded_head(
        &format!(
            "{version} 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept_key}\r\n"
        ),
        total_bytes,
    )
}

#[tokio::test]
async fn switching_protocols_requires_http_11_with_otherwise_identical_headers() {
    let (http_11, _) = dial_raw_response(switching_head(200), 4096).await;
    assert!(http_11.is_ok(), "a valid HTTP/1.1 101 must upgrade");

    let http_10_head = switching_head_with_version(200, "HTTP/1.0");
    let (http_10, _) = dial_raw_response(http_10_head, 4096).await;
    assert!(
        matches!(http_10, Err(DialFailure::Transport)),
        "an otherwise valid HTTP/1.0 101 must fail as a transport fault"
    );
}

fn inactive_close() -> [u8; 4] {
    let [high, low] = MachineLinkCloseCode::Inactive.code().to_be_bytes();
    [0x88, 2, high, low]
}

async fn serve_raw_response(listener: TcpListener, response: Vec<u8>) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut request_line = String::new();
    loop {
        request_line.clear();
        if reader.read_line(&mut request_line).await.unwrap() == 0 || request_line == "\r\n" {
            break;
        }
    }
    let _ = stream.write_all(&response).await;
    let _ = stream.shutdown().await;
}

async fn dial_raw_response(
    response: Vec<u8>,
    chunk_limit: usize,
) -> (Result<Socket, DialFailure>, Arc<Mutex<Vec<usize>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let successful_reads = Arc::new(Mutex::new(Vec::new()));
    let stream = ObservedStream {
        stream: client,
        chunk_limit,
        successful_reads: successful_reads.clone(),
    };
    let request = Request::get("/test/link")
        .header("Host", "127.0.0.1")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Key", TEST_SOCKET_KEY)
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let ((), upgraded) = tokio::join!(
        serve_raw_response(listener, response),
        upgrade_over_stream(stream, request, TEST_SOCKET_KEY)
    );
    (upgraded, successful_reads)
}

#[tokio::test]
async fn upgrade_byte_budget_accepts_a_head_just_under_and_rejects_one_byte_over() {
    let (below, _) = dial_raw_response(switching_head(BYTE_BUDGET - 1), 4095).await;
    assert!(
        below.is_ok(),
        "a valid 101 just under the byte limit must upgrade"
    );
    let (above, _) = dial_raw_response(switching_head(BYTE_BUDGET + 1), 4095).await;
    assert!(
        matches!(above, Err(DialFailure::Transport)),
        "a complete 101 one byte above the limit must fail"
    );
}

#[tokio::test]
async fn upgrade_read_budget_accepts_512_completed_reads_and_rejects_a_513th() {
    let (at_limit, reads) = dial_raw_response(switching_head(READ_BUDGET), 1).await;
    assert!(at_limit.is_ok());
    assert_eq!(reads.lock().unwrap().len(), READ_BUDGET);
    let (above, reads) = dial_raw_response(switching_head(READ_BUDGET + 1), 1).await;
    assert!(
        matches!(above, Err(DialFailure::Transport)),
        "a 513th small read must not complete the upgrade"
    );
    assert_eq!(reads.lock().unwrap().len(), READ_BUDGET);
}

#[tokio::test]
async fn informational_heads_share_one_upgrade_byte_budget() {
    let hints = padded_head("HTTP/1.1 103 Early Hints\r\n", BYTE_BUDGET / 3);
    let accepted = [hints.clone(), hints.clone(), switching_head(200)].concat();
    assert!(dial_raw_response(accepted, 4095).await.0.is_ok());
    let rejected = [hints.clone(), hints.clone(), hints, switching_head(200)].concat();
    assert!(
        matches!(
            dial_raw_response(rejected, 4095).await.0,
            Err(DialFailure::Transport)
        ),
        "informational heads and the final head spend the same byte budget"
    );
}

#[tokio::test]
async fn typed_close_in_the_same_read_as_the_101_survives_the_handoff() {
    let wire = [switching_head(200), inactive_close().to_vec()].concat();
    let size = wire.len();
    let (upgraded, reads) = dial_raw_response(wire, 4096).await;
    assert_eq!(
        *reads.lock().unwrap(),
        [size],
        "the fixture must deliver the HTTP head and Close in the same read"
    );
    let mut socket = upgraded.unwrap();
    assert!(
        matches!(socket.next().await, Some(Ok(Message::Close(Some(frame)))) if u16::from(frame.code) == MachineLinkCloseCode::Inactive.code()),
        "the first frame must be the buffered typed Close"
    );
}

#[tokio::test]
async fn websocket_reads_can_continue_after_consuming_the_entire_upgrade_read_budget() {
    let wire = [switching_head(READ_BUDGET), inactive_close().to_vec()].concat();
    let (upgraded, reads) = dial_raw_response(wire, 1).await;
    let mut socket = upgraded.unwrap();
    assert_eq!(reads.lock().unwrap().len(), READ_BUDGET);
    assert!(
        matches!(socket.next().await, Some(Ok(Message::Close(Some(frame)))) if u16::from(frame.code) == MachineLinkCloseCode::Inactive.code()),
        "the upgrade read limit must stop applying at the WebSocket handoff"
    );
    assert_eq!(
        reads.lock().unwrap().len(),
        READ_BUDGET + inactive_close().len()
    );
}
