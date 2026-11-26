//! Reassemble WebSocket frames independently at the simulated socket boundaries.

use std::{
    collections::{BTreeSet, VecDeque},
    io::{self, Cursor, Read, Write},
    pin::Pin,
    task::{Context, Poll},
};

use router_clock::{ClockInstant, RuntimeClock};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::watch,
};
use tungstenite::{
    Message, WebSocket,
    protocol::{
        Role,
        frame::{FrameHeader, coding::OpCode},
    },
};

#[derive(Clone, Debug)]
pub(crate) struct ObservedSocketFrame {
    pub(crate) port: u16,
    pub(super) at: ClockInstant,
    pub(crate) message: Message,
}

#[derive(Clone, Default)]
pub(crate) struct SocketFrameTrace(watch::Sender<SocketFrames>);

#[derive(Default)]
struct SocketFrames {
    frames: Vec<ObservedSocketFrame>,
    incomplete: BTreeSet<u16>,
}

impl SocketFrameTrace {
    pub(crate) fn snapshot(&self) -> Vec<ObservedSocketFrame> {
        self.0.borrow().frames.clone()
    }

    pub(super) fn is_at_message_boundary(&self, port: u16) -> bool {
        !self.0.borrow().incomplete.contains(&port)
    }

    pub(crate) async fn wait_for(&self, matches: impl Fn(&[ObservedSocketFrame]) -> bool) {
        self.0
            .subscribe()
            .wait_for(|observed| matches(&observed.frames))
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

pub(crate) struct SocketObserver {
    trace: SocketFrameTrace,
    port: u16,
    clock: std::sync::Arc<router_clock::ManualClock>,
    upgrade_prefix: Option<Vec<u8>>,
    frame_bytes: Vec<u8>,
    fragmented: bool,
    socket: WebSocket<ReceiveBuffer>,
}

impl<Stream> ObservedCentralStream<Stream> {
    pub(super) fn new(
        stream: Stream,
        trace: Option<SocketFrameTrace>,
        port: u16,
        clock: std::sync::Arc<router_clock::ManualClock>,
    ) -> Self {
        Self {
            stream,
            observer: trace.map(|trace| SocketObserver::new(trace, port, clock)),
        }
    }
}

impl SocketObserver {
    pub(super) fn new(
        trace: SocketFrameTrace,
        port: u16,
        clock: std::sync::Arc<router_clock::ManualClock>,
    ) -> Self {
        Self::with_role(trace, port, clock, Role::Server)
    }

    pub(crate) fn with_role(
        trace: SocketFrameTrace,
        port: u16,
        clock: std::sync::Arc<router_clock::ManualClock>,
        role: Role,
    ) -> Self {
        Self {
            trace,
            port,
            clock,
            upgrade_prefix: Some(Vec::new()),
            frame_bytes: Vec::new(),
            fragmented: false,
            socket: WebSocket::from_raw_socket(ReceiveBuffer(VecDeque::new()), role, None),
        }
    }

    pub(crate) fn observe(&mut self, bytes: &[u8]) {
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
        self.frame_bytes.extend(payload);
        let mut messages = Vec::new();
        loop {
            let mut cursor = Cursor::new(self.frame_bytes.as_slice());
            let Some((header, payload_len)) = FrameHeader::parse(&mut cursor).unwrap() else {
                break;
            };
            let frame_len = usize::try_from(cursor.position() + payload_len).unwrap();
            if self.frame_bytes.len() < frame_len {
                break;
            }
            if matches!(header.opcode, OpCode::Data(_)) {
                self.fragmented = !header.is_final;
            }
            self.socket
                .get_mut()
                .0
                .extend(self.frame_bytes.drain(..frame_len));
            self.decode_messages(&mut messages);
        }
        self.trace.0.send_modify(|observed| {
            if self.fragmented || !self.frame_bytes.is_empty() {
                observed.incomplete.insert(self.port);
            } else {
                observed.incomplete.remove(&self.port);
            }
            observed
                .frames
                .extend(messages.into_iter().map(|message| ObservedSocketFrame {
                    port: self.port,
                    at: self.clock.monotonic_now(),
                    message,
                }));
        });
    }

    #[expect(
        clippy::panic,
        reason = "A malformed observer stream invalidates the transcript."
    )]
    fn decode_messages(&mut self, messages: &mut Vec<Message>) {
        loop {
            match self.socket.read() {
                Ok(message) => messages.push(message),
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
