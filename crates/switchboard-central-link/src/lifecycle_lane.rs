//! The bounded caller handoff for one link driver lifetime.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use central_link_client::{
    AdmittedConnection, AdmittedLifecycleLink, ConnectionAttempt, LifecycleCallError,
    LifecycleCallFuture, LifecycleCallNotSent, LifecycleLinkReadiness, SessionLifecycleScope,
};
use central_link_protocol::CentralCall;
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CheckedCreateReply, CreateFleetSession,
    CreateReplyContext, EndFleetSession, EndReplyContext, EndSessionOutcome,
};
use fleet_rpc::Envelope;
use router_clock::ClockInstant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use workspace_ids::SessionRef;

use crate::lifecycle_reply::{CheckedLifecycleReply, LifecycleReplyCheck, LifecycleResult};

const LIFECYCLE_CALL_CAPACITY: usize = 8;

#[derive(Clone)]
pub(super) struct LifecycleCallHandle {
    commands: mpsc::Sender<LifecycleRequest>,
    admission: LifecycleAdmissionGate,
    slots: Arc<Semaphore>,
}

pub(super) struct LifecycleQueue {
    pub(super) commands: mpsc::Receiver<LifecycleRequest>,
    pub(super) admission: LifecycleAdmissionGate,
}

pub(super) fn lifecycle_channel() -> (LifecycleCallHandle, LifecycleQueue) {
    let (send, commands) = mpsc::channel(LIFECYCLE_CALL_CAPACITY);
    let admission = LifecycleAdmissionGate::default();
    (
        LifecycleCallHandle {
            commands: send,
            admission: admission.clone(),
            slots: Arc::new(Semaphore::new(LIFECYCLE_CALL_CAPACITY)),
        },
        LifecycleQueue {
            commands,
            admission,
        },
    )
}

#[derive(Default)]
enum LifecycleAdmission {
    #[default]
    Closed,
    Admitted {
        scope: SessionLifecycleScope,
        admitted: AdmittedConnection,
    },
    Ended(SessionLifecycleScope),
}

#[derive(Clone, Default)]
pub(super) struct LifecycleAdmissionGate(Arc<Mutex<LifecycleAdmission>>);

impl LifecycleAdmissionGate {
    fn lock(&self) -> MutexGuard<'_, LifecycleAdmission> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn admit(&self, scope: &SessionLifecycleScope, admitted: &AdmittedConnection) {
        *self.lock() = LifecycleAdmission::Admitted {
            scope: scope.clone(),
            admitted: admitted.clone(),
        };
    }

    pub(super) fn withdraw(&self) {
        let mut admission = self.lock();
        if matches!(*admission, LifecycleAdmission::Admitted { .. }) {
            *admission = LifecycleAdmission::Closed;
        }
    }

    pub(super) fn end(&self, scope: &SessionLifecycleScope) {
        *self.lock() = LifecycleAdmission::Ended(scope.clone());
    }
}

impl LifecycleQueue {
    pub(super) fn reject_queued(&mut self) {
        while let Ok(request) = self.commands.try_recv() {
            if matches!(&*self.admission.lock(), LifecycleAdmission::Ended(scope) if scope == &request.scope)
            {
                request
                    .completion
                    .finish(Err(not_sent(LifecycleCallNotSent::EnrollmentEnded)));
            }
        }
    }
}

impl Drop for LifecycleQueue {
    fn drop(&mut self) {
        self.admission.withdraw();
        self.commands.close();
        self.reject_queued();
    }
}

impl LifecycleCallHandle {
    fn submit(
        &self,
        scope: &SessionLifecycleScope,
        connection: &ConnectionAttempt,
        call: CentralCall,
        expected: LifecycleReplyCheck,
        send_before: ClockInstant,
    ) -> Result<oneshot::Receiver<LifecycleResult>, LifecycleCallError> {
        // Admission and queue insertion are one operation with respect to socket withdrawal.
        let admission = self.admission.lock();
        match &*admission {
            LifecycleAdmission::Admitted {
                scope: current,
                admitted,
            } if scope == current && connection == &admitted.connection => {}
            LifecycleAdmission::Ended(ended) if scope == ended => {
                return Err(not_sent(LifecycleCallNotSent::EnrollmentEnded));
            }
            _ => return Err(not_sent(LifecycleCallNotSent::LinkNotReady)),
        }
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| not_sent(LifecycleCallNotSent::Busy))?;
        let (answer, received) = oneshot::channel();
        self.commands
            .try_send(LifecycleRequest {
                scope: scope.clone(),
                connection: connection.clone(),
                send_before,
                call: Box::new(call),
                expected,
                completion: LifecycleCompletion {
                    answer: Some(answer),
                    fallback: not_sent(LifecycleCallNotSent::LinkNotReady),
                    _slot: slot,
                },
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => not_sent(LifecycleCallNotSent::Busy),
                mpsc::error::TrySendError::Closed(_) => {
                    not_sent(LifecycleCallNotSent::LinkNotReady)
                }
            })?;
        Ok(received)
    }

    async fn call(
        &self,
        scope: &SessionLifecycleScope,
        connection: &ConnectionAttempt,
        call: CentralCall,
        expected: LifecycleReplyCheck,
        send_before: ClockInstant,
    ) -> LifecycleResult {
        self.submit(scope, connection, call, expected, send_before)?
            .await
            .unwrap_or(Err(LifecycleCallError::AnswerLost))
    }
}

impl AdmittedLifecycleLink for LifecycleCallHandle {
    fn lifecycle_readiness(&self, scope: &SessionLifecycleScope) -> LifecycleLinkReadiness {
        match &*self.admission.lock() {
            LifecycleAdmission::Admitted {
                scope: current,
                admitted,
            } if current == scope => LifecycleLinkReadiness::Ready(admitted.clone()),
            LifecycleAdmission::Ended(ended) if ended == scope => {
                LifecycleLinkReadiness::EnrollmentEnded
            }
            _ => LifecycleLinkReadiness::NotReady,
        }
    }

    fn create_fleet_session<'call>(
        &'call self,
        scope: &'call SessionLifecycleScope,
        connection: &'call ConnectionAttempt,
        request: CreateFleetSession,
        send_before: ClockInstant,
    ) -> LifecycleCallFuture<'call, CheckedCreateReply> {
        Box::pin(async move {
            let expected = LifecycleReplyCheck::Create(CreateReplyContext::from_request(&request));
            let call = CentralCall::CreateFleetSession(Envelope {
                target: scope.organization.clone(),
                body: request,
            });
            match self
                .call(scope, connection, call, expected, send_before)
                .await?
            {
                CheckedLifecycleReply::Create(reply) => Ok(reply),
                _ => Err(LifecycleCallError::ContradictedReply),
            }
        })
    }

    fn bind_fleet_session<'call>(
        &'call self,
        scope: &'call SessionLifecycleScope,
        connection: &'call ConnectionAttempt,
        request: BindFleetSession,
        send_before: ClockInstant,
    ) -> LifecycleCallFuture<'call, BindFleetSessionReply> {
        Box::pin(async move {
            let expected = LifecycleReplyCheck::Bind(SessionRef {
                machine_id: scope.machine.clone(),
                session_id: request.harness_session_id.clone(),
            });
            let call = CentralCall::BindFleetSession(Envelope {
                target: scope.organization.clone(),
                body: request,
            });
            match self
                .call(scope, connection, call, expected, send_before)
                .await?
            {
                CheckedLifecycleReply::Bind(reply) => Ok(reply),
                _ => Err(LifecycleCallError::ContradictedReply),
            }
        })
    }

    fn end_fleet_session<'call>(
        &'call self,
        scope: &'call SessionLifecycleScope,
        connection: &'call ConnectionAttempt,
        request: EndFleetSession,
        send_before: ClockInstant,
    ) -> LifecycleCallFuture<'call, EndSessionOutcome> {
        Box::pin(async move {
            let expected = LifecycleReplyCheck::End(EndReplyContext::from_request(&request));
            let call = CentralCall::EndFleetSession(Envelope {
                target: scope.organization.clone(),
                body: request,
            });
            match self
                .call(scope, connection, call, expected, send_before)
                .await?
            {
                CheckedLifecycleReply::End(reply) => Ok(reply),
                _ => Err(LifecycleCallError::ContradictedReply),
            }
        })
    }
}

pub(super) struct LifecycleRequest {
    pub(super) scope: SessionLifecycleScope,
    pub(super) connection: ConnectionAttempt,
    pub(super) send_before: ClockInstant,
    pub(super) call: Box<CentralCall>,
    pub(super) expected: LifecycleReplyCheck,
    pub(super) completion: LifecycleCompletion,
}

pub(super) struct LifecycleCompletion {
    answer: Option<oneshot::Sender<LifecycleResult>>,
    fallback: LifecycleCallError,
    // The same slot covers queue residence, a write, and answer retention.
    _slot: OwnedSemaphorePermit,
}

impl LifecycleCompletion {
    pub(super) fn caller_is_waiting(&self) -> bool {
        self.answer
            .as_ref()
            .is_some_and(|answer| !answer.is_closed())
    }

    pub(super) fn may_have_written(&mut self) {
        self.fallback = LifecycleCallError::AnswerLost;
    }

    pub(super) fn finish(mut self, result: LifecycleResult) {
        if let Some(answer) = self.answer.take() {
            let _ = answer.send(result);
        }
    }
}

impl Drop for LifecycleCompletion {
    fn drop(&mut self) {
        if let Some(answer) = self.answer.take() {
            let _ = answer.send(Err(self.fallback));
        }
    }
}

pub(super) const fn not_sent(reason: LifecycleCallNotSent) -> LifecycleCallError {
    LifecycleCallError::NotSent(reason)
}
