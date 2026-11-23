//! Tracks accepted TCP bytes beneath TLS and the WebSocket write buffer.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
};

use hyper::{
    Uri,
    rt::{Read, ReadBufCursor, Write},
};
use hyper_util::{
    client::legacy::connect::{Connected, Connection, HttpConnector},
    rt::TokioIo,
};
use router_clock::{ClockInstant, RuntimeClock};
use tokio::net::TcpStream;
use tower_service::Service;

pub(super) struct WireWriteFence {
    clock: Arc<dyn RuntimeClock>,
    state: Mutex<WireWriteState>,
}

#[derive(Clone, Copy)]
enum WireWriteState {
    Idle,
    AwaitingFirstByte { before: ClockInstant },
    Started,
    Discard { crossed: bool },
}

impl WireWriteFence {
    pub(super) fn new(clock: Arc<dyn RuntimeClock>) -> Self {
        Self {
            clock,
            state: Mutex::new(WireWriteState::Idle),
        }
    }

    pub(super) fn watch_first_byte(&self, before: ClockInstant) -> WireWriteGuard<'_> {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner) =
            WireWriteState::AwaitingFirstByte { before };
        WireWriteGuard { fence: self }
    }

    pub(super) fn must_discard(&self) -> bool {
        matches!(
            *self.state.lock().unwrap_or_else(PoisonError::into_inner),
            WireWriteState::Discard { .. }
        )
    }

    pub(super) fn crossed(&self) -> bool {
        matches!(
            *self.state.lock().unwrap_or_else(PoisonError::into_inner),
            WireWriteState::Started | WireWriteState::Discard { crossed: true }
        )
    }

    fn poll_write(
        &self,
        write: impl FnOnce() -> Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match *state {
            WireWriteState::AwaitingFirstByte { before }
                if self.clock.monotonic_now() >= before =>
            {
                *state = WireWriteState::Discard { crossed: false };
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "first write fence elapsed",
                )));
            }
            WireWriteState::Discard { .. } => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "discarded link write",
                )));
            }
            WireWriteState::Idle
            | WireWriteState::AwaitingFirstByte { .. }
            | WireWriteState::Started => {}
        }
        let result = write();
        if matches!(*state, WireWriteState::AwaitingFirstByte { .. })
            && matches!(&result, Poll::Ready(Ok(bytes)) if *bytes != 0)
        {
            *state = WireWriteState::Started;
        }
        result
    }
}

pub(super) struct WireWriteGuard<'fence> {
    fence: &'fence WireWriteFence,
}

impl WireWriteGuard<'_> {
    pub(super) fn crossed(&self) -> bool {
        self.fence.crossed()
    }

    // Called only after an exact pre-buffer rejection or a completed flush.
    pub(super) fn complete(self) {
        *self
            .fence
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = WireWriteState::Idle;
    }
}

impl Drop for WireWriteGuard<'_> {
    fn drop(&mut self) {
        let mut state = self
            .fence
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match *state {
            WireWriteState::Idle | WireWriteState::Discard { .. } => {}
            WireWriteState::AwaitingFirstByte { .. } => {
                *state = WireWriteState::Discard { crossed: false }
            }
            WireWriteState::Started => *state = WireWriteState::Discard { crossed: true },
        }
    }
}

pub(super) struct FencedConnector {
    connector: HttpConnector,
    fence: Arc<WireWriteFence>,
}

impl FencedConnector {
    pub(super) fn new(fence: Arc<WireWriteFence>) -> Self {
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        Self { connector, fence }
    }
}

impl Service<Uri> for FencedConnector {
    type Response = FencedTransport<TokioIo<TcpStream>>;
    type Error = <HttpConnector as Service<Uri>>::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connector.poll_ready(context)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let connect = self.connector.call(uri);
        let fence = self.fence.clone();
        Box::pin(async move { connect.await.map(|inner| FencedTransport { inner, fence }) })
    }
}

pub(super) struct FencedTransport<Stream> {
    pub(super) inner: Stream,
    pub(super) fence: Arc<WireWriteFence>,
}

impl<Stream: Connection> Connection for FencedTransport<Stream> {
    fn connected(&self) -> Connected {
        self.inner.connected()
    }
}

impl<Stream: Read + Unpin> Read for FencedTransport<Stream> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl<Stream: Write + Unpin> Write for FencedTransport<Stream> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let transport = self.get_mut();
        transport
            .fence
            .poll_write(|| Pin::new(&mut transport.inner).poll_write(context, bytes))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let transport = self.get_mut();
        transport
            .fence
            .poll_write(|| Pin::new(&mut transport.inner).poll_write_vectored(context, buffers))
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}
