//! Holds already-received Central bytes just below the driver's WebSocket reader.

use std::{
    collections::VecDeque,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use central_link_protocol::{CentralCall, DataFrame, FrameDirection, LinkFrame, decode_text};
use router_clock::ManualClock;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tungstenite::{Message, protocol::Role};

use super::simulated_link::central_receive::{
    ObservedSocketFrame, SocketFrameTrace, SocketObserver,
};

#[derive(Clone, Copy, Default)]
enum ReaderMode {
    #[default]
    Passing,
    Holding,
    Cut,
}

#[derive(Default)]
struct ReaderControl {
    mode: ReaderMode,
    hold_registration: bool,
    waker: Option<Waker>,
}

#[derive(Clone, Default)]
pub(super) struct NativeWorkCarrier {
    control: Arc<Mutex<ReaderControl>>,
    pub(super) to_central: SocketFrameTrace,
    pub(super) to_driver: SocketFrameTrace,
}

impl NativeWorkCarrier {
    pub(super) fn hold_first_registration(&self) {
        self.control.lock().unwrap().hold_registration = true;
    }

    pub(super) fn hold(&self) {
        self.set_mode(ReaderMode::Holding);
    }

    pub(super) fn release(&self) {
        self.set_mode(ReaderMode::Passing);
    }

    pub(super) fn cut(&self) {
        self.set_mode(ReaderMode::Cut);
    }

    fn set_mode(&self, mode: ReaderMode) {
        let waker = {
            let mut state = self.control.lock().unwrap();
            state.mode = mode;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(super) async fn connect(
        &self,
        address: SocketAddr,
        clock: Arc<ManualClock>,
    ) -> io::Result<NativeWorkSocket> {
        let stream = TcpStream::connect(address).await?;
        let port = stream.local_addr()?.port();
        Ok(NativeWorkSocket {
            stream,
            carrier: self.clone(),
            incoming: SocketObserver::with_role(
                self.to_driver.clone(),
                port,
                clock.clone(),
                Role::Client,
            ),
            outgoing: SocketObserver::with_role(self.to_central.clone(), port, clock, Role::Server),
            held_bytes: VecDeque::new(),
        })
    }
}

pub(super) fn application(
    frame: &ObservedSocketFrame,
    direction: FrameDirection,
) -> Option<LinkFrame> {
    let Message::Text(wire) = &frame.message else {
        return None;
    };
    decode_text(wire, direction).ok()
}

pub(super) struct NativeWorkSocket {
    stream: TcpStream,
    carrier: NativeWorkCarrier,
    incoming: SocketObserver,
    outgoing: SocketObserver,
    held_bytes: VecDeque<u8>,
}

impl AsyncRead for NativeWorkSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mode = {
            let mut control = self.carrier.control.lock().unwrap();
            control.waker = Some(context.waker().clone());
            control.mode
        };
        match mode {
            ReaderMode::Passing => {
                if !self.held_bytes.is_empty() {
                    let bytes = self.held_bytes.make_contiguous();
                    let count = out.remaining().min(bytes.len());
                    out.put_slice(bytes.get(..count).unwrap());
                    self.held_bytes.drain(..count);
                    return Poll::Ready(Ok(()));
                }
                let before = out.filled().len();
                let result = Pin::new(&mut self.stream).poll_read(context, out);
                if matches!(result, Poll::Ready(Ok(()))) {
                    self.incoming.observe(out.filled().get(before..).unwrap());
                }
                result
            }
            ReaderMode::Holding => {
                let mut bytes = [0; 4096];
                let mut received = ReadBuf::new(&mut bytes);
                match Pin::new(&mut self.stream).poll_read(context, &mut received) {
                    Poll::Ready(Ok(())) if !received.filled().is_empty() => {
                        self.held_bytes.extend(received.filled());
                        self.incoming.observe(received.filled());
                        context.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(Ok(())) | Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                }
            }
            ReaderMode::Cut => Poll::Ready(Ok(())),
        }
    }
}

impl AsyncWrite for NativeWorkSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.stream).poll_write(context, bytes);
        if let Poll::Ready(Ok(written)) = result {
            self.outgoing.observe(bytes.get(..written).unwrap());
            let mut control = self.carrier.control.lock().unwrap();
            if control.hold_registration && self.carrier.to_central.snapshot().iter().any(|frame| {
                matches!(application(frame, FrameDirection::ToCentral), Some(LinkFrame::Data(DataFrame::CallCentral { request, .. })) if matches!(*request, CentralCall::RegisterRun(_)))
            }) {
                control.hold_registration = false;
                control.mode = ReaderMode::Holding;
            }
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}
