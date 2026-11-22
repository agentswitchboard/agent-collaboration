//! Outbound calls and their answer waiters belong to one admitted socket.
use central_link_protocol::{
    CorrelationId, DataFrame, LinkFrame, MacCall, MacCallReply, TypedCallCorrelations,
    TypedCallResult,
};
use fleet_rpc::CallFailure;
use router_clock::{ClockInstant, RuntimeClock};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

type MacAnswer = TypedCallResult<MacCallReply>;
type CallOutcome = Result<MacAnswer, CallFailure>;

struct ConnectionCalls {
    pending: Mutex<TypedCallCorrelations<ReplyWaiter>>,
    ended: CancellationToken,
}

impl ConnectionCalls {
    fn pending(&self) -> MutexGuard<'_, TypedCallCorrelations<ReplyWaiter>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn end(&self) {
        // Serialize detachment against answers still arriving on the old socket.
        let mut pending = self.pending();
        self.ended.cancel();
        drop(pending.drain_pending());
    }
}

struct ReplyWaiter {
    deadline: ClockInstant,
    answer: oneshot::Sender<CallOutcome>,
}

pub(crate) struct QueuedMacCall {
    correlation: CorrelationId,
    request: Box<MacCall>,
    waiter: ReplyWaiter,
}

impl QueuedMacCall {
    pub(crate) fn deadline(&self) -> ClockInstant {
        self.waiter.deadline
    }

    pub(crate) fn refuse_before_dispatch(self) {
        let _ = self.waiter.answer.send(Err(CallFailure::NeverReached));
    }
}

pub(crate) struct MacCallSender {
    queue: mpsc::Sender<QueuedMacCall>,
    calls: Arc<ConnectionCalls>,
}

pub(crate) struct MacCallReceiver {
    queue: mpsc::Receiver<QueuedMacCall>,
    calls: Arc<ConnectionCalls>,
}

pub(crate) fn channel(ended: CancellationToken) -> (MacCallSender, MacCallReceiver) {
    const MAX_QUEUED_CALLS: usize = 64;
    let (sender, receiver) = mpsc::channel(MAX_QUEUED_CALLS);
    let calls = Arc::new(ConnectionCalls {
        pending: Mutex::default(),
        ended,
    });
    (
        MacCallSender {
            queue: sender,
            calls: calls.clone(),
        },
        MacCallReceiver {
            queue: receiver,
            calls,
        },
    )
}

impl MacCallSender {
    pub(crate) fn end(&self) {
        self.calls.end();
    }

    pub(crate) fn enqueue(
        &self,
        correlation: CorrelationId,
        request: MacCall,
        deadline: ClockInstant,
    ) -> Result<PendingMacCall, CallFailure> {
        let (answer, receiver) = oneshot::channel();
        self.queue
            .try_send(QueuedMacCall {
                correlation: correlation.clone(),
                request: Box::new(request),
                waiter: ReplyWaiter { deadline, answer },
            })
            .map_err(|_| CallFailure::NeverReached)?;
        Ok(PendingMacCall {
            correlation,
            calls: self.calls.clone(),
            answer: receiver,
            deadline,
        })
    }
}

impl MacCallReceiver {
    pub(crate) async fn recv(&mut self) -> Option<QueuedMacCall> {
        self.queue.recv().await
    }

    pub(crate) fn begin_write(
        &self,
        queued: QueuedMacCall,
        clock: &dyn RuntimeClock,
    ) -> Option<LinkFrame> {
        let mut pending = self.calls.pending();
        if self.calls.ended.is_cancelled()
            || queued.waiter.answer.is_closed()
            || clock.monotonic_now() >= queued.deadline()
        {
            return None;
        }
        pending
            .register(queued.correlation.clone(), queued.waiter)
            .ok()?;
        Some(LinkFrame::Data(DataFrame::CallMac {
            correlation_id: queued.correlation,
            request: queued.request,
        }))
    }

    pub(crate) fn answer(
        &self,
        correlation: &CorrelationId,
        result: MacAnswer,
        clock: &dyn RuntimeClock,
    ) {
        let mut pending = self.calls.pending();
        if let Some(waiter) = pending.retire(correlation)
            && clock.monotonic_now() < waiter.deadline
        {
            let _ = waiter.answer.send(Ok(result));
        }
    }
}

impl Drop for MacCallReceiver {
    fn drop(&mut self) {
        self.calls.end();
    }
}

pub(crate) struct PendingMacCall {
    correlation: CorrelationId,
    calls: Arc<ConnectionCalls>,
    answer: oneshot::Receiver<CallOutcome>,
    deadline: ClockInstant,
}

impl PendingMacCall {
    pub(crate) async fn wait(mut self, clock: &dyn RuntimeClock) -> Result<MacAnswer, CallFailure> {
        tokio::select! {
            biased;
            answer = &mut self.answer => answer.map_err(|_| CallFailure::AnswerLost)?,
            () = self.calls.ended.cancelled() => Err(CallFailure::AnswerLost),
            () = tokio::task::unconstrained(clock.sleep_until(self.deadline)) => Err(CallFailure::AnswerLost),
        }
    }
}

impl Drop for PendingMacCall {
    fn drop(&mut self) {
        // Close before taking the lock so a queued call cannot register after cancellation.
        self.answer.close();
        self.calls.pending().retire(&self.correlation);
    }
}
