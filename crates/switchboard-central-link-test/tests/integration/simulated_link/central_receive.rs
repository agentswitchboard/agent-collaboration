//! Observe the bytes Central reads, with a WebSocket decoder per accepted socket.

use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    pin::Pin,
    task::{Context, Poll},
};

use router_clock::{ClockInstant, RuntimeClock};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::watch,
};
use tungstenite::{Message, WebSocket, protocol::Role};

#[derive(Clone, Debug)]
pub(super) struct ReceivedFrame {
    pub(super) port: u16,
    pub(super) at: ClockInstant,
    pub(super) message: Message,
}

#[derive(Clone, Default)]
pub(super) struct CentralReceiveTrace(watch::Sender<Vec<ReceivedFrame>>);

impl CentralReceiveTrace {
    pub(super) fn snapshot(&self) -> Vec<ReceivedFrame> {
        self.0.borrow().clone()
    }

    pub(super) async fn wait_for(&self, matches: impl Fn(&[ReceivedFrame]) -> bool) {
        self.0
            .subscribe()
            .wait_for(|frames| matches(frames))
            .await
            .unwrap();
    }
}

struct ReceiveBuffer(VecDeque<u8>);

impl Read for ReceiveBuffer {
    fn read(&mut self, dest: &mut [u8]) -> io::Result<usize> {
        if self.0.is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.0.read(dest)
    }
}

impl Write for ReceiveBuffer {
    fn write(&mut self, source: &[u8]) -> io::Result<usize> {
        // A control response from the observer never goes onto the carrier.
        Ok(source.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) struct ObservedCentralStream<Stream> {
    stream: Stream,
    observer: Option<SocketObserver>,
}

struct SocketObserver {
    trace: CentralReceiveTrace,
    port: u16,
    clock: std::sync::Arc<router_clock::ManualClock>,
    upgrade_prefix: Option<Vec<u8>>,
    socket: WebSocket<ReceiveBuffer>,
}

impl<Stream> ObservedCentralStream<Stream> {
    pub(super) fn new(
        stream: Stream,
        trace: Option<CentralReceiveTrace>,
        port: u16,
        clock: std::sync::Arc<router_clock::ManualClock>,
    ) -> Self {
        Self {
            stream,
            observer: trace.map(|trace| SocketObserver {
                trace,
                port,
                clock,
                upgrade_prefix: Some(Vec::new()),
                socket: WebSocket::from_raw_socket(
                    ReceiveBuffer(VecDeque::new()),
                    Role::Server,
                    None,
                ),
            }),
        }
    }
}

impl SocketObserver {
    #[expect(
        clippy::panic,
        reason = "A malformed observer stream invalidates the transcript."
    )]
    fn observe(&mut self, bytes: &[u8]) {
        let payload = if let Some(prefix) = &mut self.upgrade_prefix {
            prefix.extend_from_slice(bytes);
            let Some(end) = prefix.windows(4).position(|window| window == b"\r\n\r\n") else {
                return;
            };
            let rest = prefix.split_off(end + 4);
            self.upgrade_prefix = None;
            rest
        } else {
            bytes.to_vec()
        };
        self.socket.get_mut().0.extend(payload);
        loop {
            match self.socket.read() {
                Ok(message) => self.trace.0.send_modify(|frames| {
                    frames.push(ReceivedFrame {
                        port: self.port,
                        at: self.clock.monotonic_now(),
                        message,
                    })
                }),
                Err(tungstenite::Error::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                    break;
                }
                Err(error) => panic!("Central receive transcript is incomplete: {error}"),
            }
        }
    }
}

impl<Stream: AsyncRead + Unpin> AsyncRead for ObservedCentralStream<Stream> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(())))
            && let Some(observer) = &mut self.observer
        {
            observer.observe(buf.filled().get(before..).unwrap());
        }
        result
    }
}

impl<Stream: AsyncWrite + Unpin> AsyncWrite for ObservedCentralStream<Stream> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
