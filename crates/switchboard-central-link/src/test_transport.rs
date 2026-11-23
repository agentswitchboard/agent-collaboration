//! Injects test sockets below the real HTTP, TLS and first-byte fence.

use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

use hyper::{
    Uri,
    rt::{Read, ReadBufCursor, Write},
};
use hyper_util::{
    client::legacy::connect::{Connected, Connection},
    rt::TokioIo,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tower_service::Service;

use crate::{lease_source::LeaseSource, link_driver::LinkDriver};

impl<Binding, Source> LinkDriver<Binding, Source>
where
    Binding: Clone + Eq + Send + Sync,
    Source: LeaseSource<Binding>,
{
    /// Run the driver with sockets supplied by the test's network.
    pub async fn run_with_test_transport<Connect, Connecting, Stream>(self, connect: Connect)
    where
        Connect: Fn() -> Connecting + Sync,
        Connecting: Future<Output = io::Result<Stream>> + Send + 'static,
        Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.run_with_connector(|| TestConnector(&connect)).await;
    }
}

struct TestConnector<Connect>(Connect);

impl<Connect, Connecting, Stream> Service<Uri> for TestConnector<Connect>
where
    Connect: Fn() -> Connecting,
    Connecting: Future<Output = io::Result<Stream>> + Send + 'static,
    Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Response = TestStream<Stream>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _address: Uri) -> Self::Future {
        let connecting = (self.0)();
        Box::pin(async {
            connecting
                .await
                .map(|stream| TestStream(TokioIo::new(stream)))
        })
    }
}

struct TestStream<Stream>(TokioIo<Stream>);

impl<Stream> Connection for TestStream<Stream> {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl<Stream: AsyncRead + Unpin> Read for TestStream<Stream> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(context, buffer)
    }
}

impl<Stream: AsyncWrite + Unpin> Write for TestStream<Stream> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, bytes)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(context)
    }
}
