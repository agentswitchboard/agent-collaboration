//! Outbound calls and their answer waiters belong to one admitted socket.
use central_link_protocol::{
    CorrelationId, DataFrame, LinkFrame, MAX_PENDING_TYPED_CALLS, MacCall, MacCallReply,
    MacLinkRpc, TypedCallCorrelations, TypedCallResult,
};
use fleet_rpc::{CallFailure, Envelope};
use router_clock::{ClockInstant, RuntimeClock};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

type MacAnswer = TypedCallResult<MacCallReply>;
type CallOutcome = Result<MacAnswer, CallFailure>;

struct ConnectionCalls {
    pending: Mutex<TypedCallCorrelations<ReplyWaiter>>,
    ended: CancellationToken,
    capacity: Arc<Semaphore>,
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
    waiter: ReplyWaiter,
    request: Box<MacCall>,
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
    let (sender, receiver) = mpsc::channel(MAX_PENDING_TYPED_CALLS);
    let calls = Arc::new(ConnectionCalls {
        pending: Mutex::default(),
        ended,
        capacity: Arc::new(Semaphore::new(MAX_PENDING_TYPED_CALLS)),
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

    pub(crate) fn enqueue<R: MacLinkRpc>(
        &self,
        correlation: CorrelationId,
        deadline: ClockInstant,
        request: Envelope<R::Target, R::Request>,
    ) -> Result<PendingMacCall, CallFailure> {
        // The same budget covers calls waiting to get on the wire and awaiting a reply.
        let slot = self
            .calls
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| CallFailure::NeverReached)?;
        let (answer, receiver) = oneshot::channel();
        self.queue
            .try_send(QueuedMacCall {
                correlation: correlation.clone(),
                waiter: ReplyWaiter { deadline, answer },
                request: box_registered_request(request, R::request),
            })
            .map_err(|_| CallFailure::NeverReached)?;
        Ok(PendingMacCall {
            correlation,
            calls: self.calls.clone(),
            answer: receiver,
            deadline,
            _slot: slot,
        })
    }
}

fn box_registered_request<Body, Wire>(body: Body, encode: impl FnOnce(Body) -> Wire) -> Box<Wire> {
    Box::new(encode(body))
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
        if let Err(waiter) = pending.register(queued.correlation.clone(), queued.waiter) {
            let _ = waiter.answer.send(Err(CallFailure::NeverReached));
            return None;
        }
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
    _slot: OwnedSemaphorePermit,
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
