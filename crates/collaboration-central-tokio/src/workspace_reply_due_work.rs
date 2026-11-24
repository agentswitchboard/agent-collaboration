use axum::{
    body::{Body, Bytes},
    extract::{ConnectInfo, Request, State, connect_info::Connected},
    middleware::Next,
    response::Response,
    serve::{IncomingStream, Listener},
};
use fleet_runtime::{
    DeferredDueTask, DueBatchYield, ReplyDueWork, ReplyDueWorkOwner, collect_reply_due_work,
};
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{UnixListener, UnixStream, unix::SocketAddr},
};

use crate::{TokioWorkspaceHost, workspace_background_tasks::WorkspaceBackgroundTasks};

impl ReplyDueWorkOwner for WorkspaceBackgroundTasks {
    fn run_after_reply(&self, task: DeferredDueTask) {
        self.schedule(task);
    }
}

pub(super) fn yield_tokio_due_batches() -> DueBatchYield {
    Box::pin(async {
        tokio::task::yield_now().await;
        Ok(())
    })
}

pub(super) async fn handoff_signin_reply(
    State(host): State<Arc<TokioWorkspaceHost>>,
    ConnectInfo(writer): ConnectInfo<SignInReplyWriter>,
    request: Request,
    next: Next,
) -> Response {
    let (reply, work) = collect_reply_due_work(next.run(request)).await;
    if work.is_empty() {
        return reply;
    }
    let (parts, body) = reply.into_parts();
    Response::from_parts(
        parts,
        Body::new(SignInReplyBody {
            body,
            writer,
            pending: Some((host.background_tasks.clone(), work)),
        }),
    )
}

type ReplyAfterFlush = (WorkspaceBackgroundTasks, ReplyDueWork);

#[derive(Clone, Default)]
pub(super) struct SignInReplyWriter(Arc<Mutex<Vec<ReplyAfterFlush>>>);

impl Connected<IncomingStream<'_, ReplyAwareSignInListener>> for SignInReplyWriter {
    fn connect_info(stream: IncomingStream<'_, ReplyAwareSignInListener>) -> Self {
        stream.io().writer.clone()
    }
}

impl SignInReplyWriter {
    fn final_frame_produced(&self, pending: ReplyAfterFlush) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(pending);
    }

    fn flush_settled(&self, accepted: bool) {
        let pending = std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner));
        if accepted {
            for (scheduler, work) in pending {
                work.handoff(Arc::new(scheduler));
            }
        }
    }
}

pub(super) struct ReplyAwareSignInListener(pub(super) UnixListener);

impl Listener for ReplyAwareSignInListener {
    type Io = SignInReplySocket<UnixStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (socket, addr) = Listener::accept(&mut self.0).await;
        (SignInReplySocket::new(socket), addr)
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

pub(super) struct SignInReplySocket<Socket> {
    io: Socket,
    writer: SignInReplyWriter,
}

impl<Socket> SignInReplySocket<Socket> {
    fn new(io: Socket) -> Self {
        Self {
            io,
            writer: SignInReplyWriter::default(),
        }
    }
}

impl<Socket: AsyncRead + Unpin> AsyncRead for SignInReplySocket<Socket> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(context, buffer)
    }
}

impl<Socket: AsyncWrite + Unpin> AsyncWrite for SignInReplySocket<Socket> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(context, bytes)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write_vectored(context, bytes)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let flushed = Pin::new(&mut this.io).poll_flush(context);
        // Hyper flushes this writer only once its buffered frames have been
        // written. A pending write or failed connection earns no handoff.
        if let Poll::Ready(result) = &flushed {
            this.writer.flush_settled(result.is_ok());
        }
        flushed
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(context)
    }
}

struct SignInReplyBody {
    body: Body,
    writer: SignInReplyWriter,
    pending: Option<ReplyAfterFlush>,
}

impl http_body::Body for SignInReplyBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = Pin::new(&mut this.body).poll_frame(context);
        if (matches!(frame, Poll::Ready(None))
            || matches!(frame, Poll::Ready(Some(Ok(_)))) && this.body.is_end_stream())
            && let Some(pending) = this.pending.take()
        {
            this.writer.final_frame_produced(pending);
        }
        frame
    }
}

#[cfg(test)]
#[path = "workspace_reply_due_work_test.rs"]
mod tests;
