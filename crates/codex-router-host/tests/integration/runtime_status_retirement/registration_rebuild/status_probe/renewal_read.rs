use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::oneshot,
};
use tokio_tungstenite::tungstenite::{
    Error,
    protocol::frame::{
        Frame,
        coding::{Data, OpCode},
    },
};

#[derive(Clone, Default)]
pub(super) struct RenewalReadCheckpoint(Arc<Mutex<Option<PendingRenewalRead>>>);

struct PendingRenewalRead {
    remaining: std::vec::IntoIter<u8>,
    resumed: oneshot::Sender<()>,
}

impl RenewalReadCheckpoint {
    pub(super) fn after_text(&self, text: &str) -> Result<oneshot::Receiver<()>, Error> {
        // The scripted peer is the WebSocket server, so it sends an unmasked frame.
        let mut frame_bytes = Vec::new();
        Frame::message(text.to_owned(), OpCode::Data(Data::Text), true).format(&mut frame_bytes)?;
        let (resumed, observed) = oneshot::channel();
        let mut pending = self
            .0
            .lock()
            .map_err(|_| io::Error::other("renewal checkpoint lock poisoned"))?;
        assert!(
            pending.is_none(),
            "only one renewal checkpoint can be armed"
        );
        *pending = Some(PendingRenewalRead {
            remaining: frame_bytes.into_iter(),
            resumed,
        });
        Ok(observed)
    }

    pub(super) fn wrap<Reader>(&self, reader: Reader) -> RenewalReader<Reader> {
        RenewalReader {
            reader,
            checkpoint: self.clone(),
        }
    }
}

pub(super) struct RenewalReader<Reader> {
    reader: Reader,
    checkpoint: RenewalReadCheckpoint,
}

impl<Reader: AsyncRead + Unpin> AsyncRead for RenewalReader<Reader> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        {
            let mut pending = self.checkpoint.0.lock().expect("renewal checkpoint lock");
            if pending
                .as_ref()
                .is_some_and(|read| read.remaining.as_slice().is_empty())
                && let Some(read) = pending.take()
            {
                let _ = read.resumed.send(());
            }
        }

        let before = buffer.filled().len();
        let result = Pin::new(&mut self.reader).poll_read(context, buffer);
        if matches!(result, Poll::Ready(Ok(())))
            && let Some(read) = self
                .checkpoint
                .0
                .lock()
                .expect("renewal checkpoint lock")
                .as_mut()
        {
            let chunk = buffer.filled().get(before..).expect("new read bytes");
            assert!(
                read.remaining
                    .by_ref()
                    .take(chunk.len())
                    .eq(chunk.iter().copied()),
                "the next carrier bytes must match the scripted renewal frame"
            );
        }
        result
    }
}

impl<Reader> Drop for RenewalReader<Reader> {
    fn drop(&mut self) {
        // If the driver closes before resuming, the waiting fixture sees a dropped checkpoint.
        self.checkpoint
            .0
            .lock()
            .expect("renewal checkpoint lock")
            .take();
    }
}
