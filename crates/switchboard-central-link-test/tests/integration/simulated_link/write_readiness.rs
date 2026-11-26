use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker, ready},
};

use tokio::{io::AsyncWrite, sync::oneshot};

use super::{central_receive::SocketObserver, registration_preface::RegistrationPreface};

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
    pub(super) registration_preface: Option<RegistrationPreface>,
    pub(super) observer: Option<SocketObserver>,
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
        let Self {
            raw,
            registration_preface,
            observer,
            ..
        } = self.as_mut().get_mut();
        if let Some(preface) = registration_preface
            && ready!(preface.poll_before_write(Pin::new(&mut *raw), context, bytes))?
        {
            *registration_preface = None;
        }
        let result = Pin::new(raw).poll_write(context, bytes);
        if let Poll::Ready(Ok(written)) = result
            && let Some(observer) = observer
        {
            observer.observe(bytes.get(..written).unwrap());
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.raw).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.raw).poll_shutdown(context)
    }
}
