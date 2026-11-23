use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use tokio::{io::AsyncWrite, sync::oneshot};

#[derive(Clone)]
pub(super) struct WriterGate(Arc<Mutex<Option<BlockedWrite>>>);

struct BlockedWrite {
    observed: Option<oneshot::Sender<Vec<u8>>>,
    waker: Option<Waker>,
}

impl WriterGate {
    pub(super) fn new() -> Self {
        Self(Arc::default())
    }

    pub(super) fn hold(&self) -> oneshot::Receiver<Vec<u8>> {
        let (observed, waiting) = oneshot::channel();
        let previous = self.0.lock().unwrap().replace(BlockedWrite {
            observed: Some(observed),
            waker: None,
        });
        assert!(previous.is_none());
        waiting
    }

    pub(super) fn release(&self) {
        if let Some(waker) = self.0.lock().unwrap().take().unwrap().waker {
            waker.wake();
        }
    }
}

pub(super) struct HeldWriter<Writer> {
    pub(super) raw: Writer,
    pub(super) gate: WriterGate,
}

impl<Writer: AsyncWrite + Unpin> AsyncWrite for HeldWriter<Writer> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        {
            let mut gate = self.gate.0.lock().unwrap();
            if let Some(ready) = gate.as_mut() {
                ready.waker = Some(context.waker().clone());
                if let Some(observed) = ready.observed.take() {
                    let _ = observed.send(bytes.to_vec());
                }
                return Poll::Pending;
            }
        }
        Pin::new(&mut self.raw).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.raw).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.raw).poll_shutdown(context)
    }
}
