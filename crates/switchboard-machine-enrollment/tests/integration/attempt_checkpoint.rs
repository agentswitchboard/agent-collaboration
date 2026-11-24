//! A driven poll and settled HTTP requests are independent of the clock's timers.
use std::{
    future::{Future, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use switchboard_machine_enrollment::LoopbackRequestEvent;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

#[derive(Default)]
pub struct RequestTrace {
    dispatched: AtomicUsize,
    settled: AtomicUsize,
}

impl RequestTrace {
    pub fn record(&self, event: LoopbackRequestEvent) {
        let counter = match event {
            LoopbackRequestEvent::Dispatched => &self.dispatched,
            LoopbackRequestEvent::Settled => &self.settled,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, finished: bool) -> AttemptProgress {
        AttemptProgress {
            dispatched: self.dispatched.load(Ordering::Relaxed),
            settled: self.settled.load(Ordering::Relaxed),
            finished,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttemptProgress {
    pub dispatched: usize,
    settled: usize,
    pub finished: bool,
}

pub struct AttemptCheckpoints {
    drives: mpsc::UnboundedSender<oneshot::Sender<AttemptProgress>>,
    progress: watch::Receiver<AttemptProgress>,
}

impl AttemptCheckpoints {
    pub fn spawn<Output: Send + 'static>(
        attempt: impl Future<Output = Output> + Send + 'static,
        trace: Arc<RequestTrace>,
    ) -> (Self, JoinHandle<Output>) {
        let (drives, mut drive_requests) =
            mpsc::unbounded_channel::<oneshot::Sender<AttemptProgress>>();
        let (updates, progress) = watch::channel(AttemptProgress::default());
        let task = tokio::spawn(async move {
            let mut attempt = std::pin::pin!(attempt);
            poll_fn(|context| {
                let independent_drive = drive_requests.poll_recv(context);
                let output = attempt.as_mut().poll(context);
                // Observe the result after the attempt has made its next decision.
                let current = trace.snapshot(output.is_ready());
                updates.send_if_modified(|previous| {
                    if *previous == current {
                        return false;
                    }
                    *previous = current;
                    true
                });
                if let Poll::Ready(Some(reply)) = independent_drive {
                    let _ = reply.send(current);
                }
                output
            })
            .await
        });
        (Self { drives, progress }, task)
    }

    pub async fn drive(&self) -> AttemptProgress {
        let (reply, driven) = oneshot::channel();
        if self.drives.send(reply).is_ok()
            && let Ok(progress) = driven.await
        {
            return progress;
        }
        *self.progress.borrow()
    }

    pub async fn after_response(&mut self, ordinal: usize) -> AttemptProgress {
        self.progress
            .wait_for(|progress| progress.finished || progress.settled >= ordinal)
            .await
            .map(|progress| *progress)
            .expect("attempt reports the response decision")
    }

    pub async fn finished(&mut self) {
        let _ = self.progress.wait_for(|progress| progress.finished).await;
    }
}
