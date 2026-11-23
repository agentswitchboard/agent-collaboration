//! The admitted socket owns outbound calls, its run binding and the reply correlation table.
use central_link_protocol::{
    CorrelationId, DataFrame, DuplicateMacCapabilities, FreshCorrelationId, LinkFrame,
    MAX_PENDING_TYPED_CALLS, MacCall, MacCallAddress, MacCallKind, MacCallReply, MacCapabilities,
    SocketMacCallCapabilities, TypedCallCorrelations, TypedCallResult,
};
use collaboration_central_core::ValidatedMachineConnection;
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_rpc::CallFailure;
use router_clock::{ClockInstant, RuntimeClock};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use workspace_ids::{MachineId, RouterRunId};

type MacAnswer = TypedCallResult<MacCallReply>;

#[derive(Default)]
struct ConnectionCallState {
    pending: TypedCallCorrelations<ReplyWaiter>,
    bound_run: Option<RouterRunId>,
    capabilities: SocketMacCallCapabilities,
}

struct ConnectionCalls {
    state: Mutex<ConnectionCallState>,
    ended: CancellationToken,
    capacity: Arc<Semaphore>,
}

impl ConnectionCalls {
    fn state(&self) -> MutexGuard<'_, ConnectionCallState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn end(&self) {
        // Serialize detachment against replies and write starts on the old socket.
        let mut state = self.state();
        self.ended.cancel();
        drop(state.pending.drain_pending());
    }
}

#[derive(Clone, Copy, Default)]
enum WritePhase {
    #[default]
    Queued,
    Started,
    Prevented,
}

#[derive(Default)]
struct AttemptWrite {
    phase: Mutex<WritePhase>,
    prevented: CancellationToken,
}

impl AttemptWrite {
    fn start(&self, clock: &dyn RuntimeClock, deadline: ClockInstant) -> bool {
        let mut phase = self.phase.lock().unwrap_or_else(PoisonError::into_inner);
        if !matches!(*phase, WritePhase::Queued) || clock.monotonic_now() >= deadline {
            return false;
        }
        *phase = WritePhase::Started;
        true
    }

    fn prevent_or_classify(&self) -> CallFailure {
        let mut phase = self.phase.lock().unwrap_or_else(PoisonError::into_inner);
        match *phase {
            WritePhase::Started => CallFailure::AnswerLost,
            WritePhase::Queued => {
                *phase = WritePhase::Prevented;
                drop(phase);
                self.prevented.cancel();
                CallFailure::NeverReached
            }
            WritePhase::Prevented => CallFailure::NeverReached,
        }
    }
}

struct ReplyWaiter {
    address: MacCallAddress,
    deadline: ClockInstant,
    answer: oneshot::Sender<MacAnswer>,
}

pub(crate) struct QueuedMacCall {
    correlation: FreshCorrelationId,
    waiter: ReplyWaiter,
    request: Box<MacCall>,
    write: Arc<AttemptWrite>,
}

impl QueuedMacCall {
    pub(crate) fn deadline(&self) -> ClockInstant {
        self.waiter.deadline
    }

    pub(crate) fn prewrite_cancellation(&self) -> CancellationToken {
        self.write.prevented.clone()
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn is_approval_ending(&self) -> bool {
        matches!(&*self.request, MacCall::DeliverEnding(_))
    }

    pub(crate) fn approval_notice(
        &self,
    ) -> Option<&central_link_protocol::approval_rpc::ApprovalNoticeEnvelope> {
        if let MacCall::ApprovalNotice(notice) = &*self.request {
            Some(notice)
        } else {
            None
        }
    }

    pub(crate) fn frame(&self) -> LinkFrame {
        LinkFrame::Data(DataFrame::CallMac {
            correlation_id: self.correlation.wire_id().clone(),
            request: self.request.clone(),
        })
    }
}

#[derive(Clone)]
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
        state: Mutex::default(),
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
    pub(crate) fn same_socket(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.calls, &other.calls)
    }

    pub(crate) fn end(&self) {
        self.calls.end();
    }

    pub(crate) fn permits(
        &self,
        address: &MacCallAddress,
        organization: &OrganizationId,
        machine: &MachineId,
        enrollment: &EnrollmentId,
    ) -> bool {
        !self.calls.ended.is_cancelled()
            && address.permits(
                organization,
                machine,
                enrollment,
                self.calls.state().bound_run.as_ref(),
            )
    }

    pub(crate) fn enqueue(
        &self,
        correlation: FreshCorrelationId,
        deadline: ClockInstant,
        request: MacCall,
        address: MacCallAddress,
    ) -> Result<PendingMacCall, CallFailure> {
        self.calls.state().capabilities.admit_outbound(&request)?;
        // The same budget covers calls waiting to get on the wire and awaiting a reply.
        let slot = self
            .calls
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| CallFailure::NeverReached)?;
        let (answer, receiver) = oneshot::channel();
        let write = Arc::new(AttemptWrite::default());
        let wire_id = correlation.wire_id().clone();
        self.queue
            .try_send(QueuedMacCall {
                correlation,
                waiter: ReplyWaiter {
                    address,
                    deadline,
                    answer,
                },
                request: Box::new(request),
                write: write.clone(),
            })
            .map_err(|_| CallFailure::NeverReached)?;
        Ok(PendingMacCall {
            correlation: wire_id,
            calls: self.calls.clone(),
            answer: receiver,
            deadline,
            write,
            _slot: slot,
        })
    }
}

impl MacCallReceiver {
    pub(crate) async fn recv(&mut self) -> Option<QueuedMacCall> {
        self.queue.recv().await
    }

    pub(crate) fn registered_run(&self) -> Option<RouterRunId> {
        self.calls.state().bound_run.clone()
    }

    pub(crate) fn announce_capabilities(
        &self,
        announcement: MacCapabilities,
    ) -> Result<Option<RouterRunId>, DuplicateMacCapabilities> {
        let mut state = self.calls.state();
        state.capabilities.announce(announcement)?;
        Ok(state
            .bound_run
            .as_ref()
            .filter(|_| {
                !self.calls.ended.is_cancelled()
                    && state.capabilities.handles(MacCallKind::DeliverEnding)
            })
            .cloned())
    }

    pub(crate) fn should_recover_after_registration(&self) -> bool {
        let state = self.calls.state();
        state.bound_run.is_none() && state.capabilities.handles(MacCallKind::DeliverEnding)
    }

    /// Bind a successful registration on this physical socket.
    pub(crate) fn bind_run(&self, run: RouterRunId) {
        let mut state = self.calls.state();
        if !self.calls.ended.is_cancelled() && state.bound_run.is_none() {
            state.bound_run = Some(run);
        }
    }

    /// Keep detachment and binding changes out of the first WebSocket write poll.
    pub(crate) fn start_write<WritePoll>(
        &self,
        queued: QueuedMacCall,
        clock: &dyn RuntimeClock,
        connection: &ValidatedMachineConnection,
        poll_write: impl FnOnce() -> WritePoll,
    ) -> Option<WritePoll> {
        let mut state = self.calls.state();
        if self.calls.ended.is_cancelled()
            || !queued.waiter.address.permits(
                connection.organization_id(),
                connection.machine_id(),
                connection.enrollment_id(),
                state.bound_run.as_ref(),
            )
        {
            return None;
        }
        let deadline = queued.deadline();
        let correlation = queued.correlation.wire_id().clone();
        state
            .pending
            .register(queued.correlation, queued.waiter)
            .ok()?;
        if !queued.write.start(clock, deadline) {
            state.pending.retire(&correlation);
            return None;
        }
        Some(poll_write())
    }

    pub(crate) fn answer(
        &self,
        correlation: &CorrelationId,
        result: MacAnswer,
        clock: &dyn RuntimeClock,
    ) {
        let mut state = self.calls.state();
        if let Some(waiter) = state.pending.take_if(correlation, |waiter| {
            clock.monotonic_now() < waiter.deadline && waiter.address.matches_reply(&result)
        }) {
            let _ = waiter.answer.send(result);
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
    answer: oneshot::Receiver<MacAnswer>,
    deadline: ClockInstant,
    write: Arc<AttemptWrite>,
    _slot: OwnedSemaphorePermit,
}

impl PendingMacCall {
    pub(crate) async fn wait(mut self, clock: &dyn RuntimeClock) -> Result<MacAnswer, CallFailure> {
        let answer = tokio::select! {
            biased;
            answer = &mut self.answer => answer.ok(),
            () = self.calls.ended.cancelled() => None,
            () = tokio::task::unconstrained(clock.sleep_until(self.deadline)) => None,
        };
        answer.ok_or_else(|| self.write.prevent_or_classify())
    }
}

impl Drop for PendingMacCall {
    fn drop(&mut self) {
        self.write.prevent_or_classify();
        self.answer.close();
        self.calls.state().pending.retire(&self.correlation);
    }
}
