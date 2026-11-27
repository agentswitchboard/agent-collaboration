//! Address harness observations to the owner and its exact run.

use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        CallbackAdmission, EventPort, EventRefusal, EventSendFuture, HarnessOwnerEvents,
        HarnessProvider, OwnerHarnessBinding, SessionHarnessPayloads, SessionLifecycleFact,
        SessionLifecyclePort, StreamTurn,
    },
    session_lifecycle_machine::{LoadAttempt, SessionLifecycle},
    session_owner::{OwnerAddress, OwnerEvent, RunAttempt, SessionSnapshot},
    session_turn_machine::SessionTurn,
};
use session_event_model::{ProviderAuthStatus, SessionEvent};
use std::sync::Arc;
use switchboard_session_task::session_owner_task::{
    OwnerCompletionPort, OwnerTaskError, SessionOwnerHandle,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub(crate) type OwnerHarnessPayloads = SessionHarnessPayloads<
    crate::external_provider_approval_context::ExternalProviderApprovalContext,
>;
pub(crate) type SessionHarnessBinding = Box<dyn OwnerHarnessBinding<OwnerHarnessPayloads>>;
pub(crate) type SessionHarnessProvider = dyn HarnessProvider<OwnerHarnessPayloads>;

#[cfg(test)]
type HarnessLossHold = Box<dyn Fn() -> Option<tokio::sync::oneshot::Receiver<()>> + Send + Sync>;

pub(crate) struct BoundSessionHarness {
    pub(crate) binding: SessionHarnessBinding,
    pub(crate) observations: Arc<SessionOwnerLifecycleReports>,
}

pub(crate) struct SessionOwnerLifecycleReports {
    address: OwnerAddress,
    completions: OwnerCompletionPort,
    snapshots: watch::Receiver<SessionSnapshot>,
    stopped: CancellationToken,
    #[cfg(test)]
    harness_loss_hold: Option<HarnessLossHold>,
}

#[derive(Clone, Copy)]
pub(crate) enum LifecycleObservationScope {
    Creating,
    Restoring(RunAttempt<LoadAttempt>),
    Activated(SessionGeneration),
}

impl SessionOwnerLifecycleReports {
    pub(crate) fn new(
        address: OwnerAddress,
        handle: &SessionOwnerHandle,
        stopped: CancellationToken,
    ) -> Self {
        Self {
            address,
            completions: handle.completion_port(),
            snapshots: handle.snapshots(),
            stopped,
            #[cfg(test)]
            harness_loss_hold: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_harness_loss_hold(mut self, hold: HarnessLossHold) -> Self {
        self.harness_loss_hold = Some(hold);
        self
    }

    pub(crate) fn observed_at(
        self: &Arc<Self>,
        scope: LifecycleObservationScope,
        retired: CancellationToken,
    ) -> SessionLifecyclePort {
        Arc::new(SessionRunLifecycleReports {
            owner: Arc::clone(self),
            scope,
            retired,
        })
    }
}

impl HarnessOwnerEvents<ProviderAuthStatus> for SessionOwnerLifecycleReports {
    fn report_harness_gone(&self) -> EventSendFuture<'_> {
        Box::pin(async move {
            #[cfg(test)]
            if let Some(hold) = self.harness_loss_hold.as_ref().and_then(|hold| hold()) {
                let _test_released = hold.await;
            }
            self.completions
                .send_lifecycle_observation(self.address, OwnerEvent::ConnectionLost, &self.stopped)
                .await
        })
    }

    fn report_connection_auth(&self, status: ProviderAuthStatus) -> CallbackAdmission {
        let snapshot = self.snapshots.borrow().clone();
        let (SessionLifecycle::Live { generation }, Some(mut capabilities)) =
            (snapshot.lifecycle, snapshot.projection.capabilities)
        else {
            return CallbackAdmission::Ended;
        };
        capabilities.auth_status = status;
        inline_admission(self.completions.reserve(self.address).map(|reply| {
            reply.send(OwnerEvent::Stream {
                run: generation,
                expected_turn: None,
                event: SessionEvent::CapabilitiesChanged { capabilities },
            });
        }))
    }
}

pub(crate) fn inline_admission(result: Result<(), OwnerTaskError>) -> CallbackAdmission {
    match result {
        Ok(()) => CallbackAdmission::Admitted,
        Err(_) => CallbackAdmission::Unavailable,
    }
}

struct SessionRunLifecycleReports {
    owner: Arc<SessionOwnerLifecycleReports>,
    scope: LifecycleObservationScope,
    retired: CancellationToken,
}

impl EventPort<SessionLifecycleFact> for SessionRunLifecycleReports {
    fn send<'port>(&'port self, fact: SessionLifecycleFact) -> EventSendFuture<'port>
    where
        SessionLifecycleFact: 'port,
    {
        Box::pin(async move {
            let event = self.event(fact)?;
            self.owner
                .completions
                .send_lifecycle_observation(self.owner.address, event, &self.retired)
                .await
        })
    }
}

impl SessionRunLifecycleReports {
    fn event(&self, fact: SessionLifecycleFact) -> Result<OwnerEvent, EventRefusal> {
        let LifecycleObservationScope::Activated(generation) = self.scope else {
            return match (self.scope, fact) {
                (
                    LifecycleObservationScope::Restoring(attempt),
                    SessionLifecycleFact::Replay { run, events },
                ) if attempt == run => Ok(OwnerEvent::Replay { run, events }),
                _ => Err(EventRefusal::Retired),
            };
        };
        match fact {
            SessionLifecycleFact::Stream { run, turn, event } if run == generation => {
                let expected_turn = match turn {
                    StreamTurn::Observed(turn) => turn,
                    StreamTurn::AtIngress => {
                        let snapshot = self.owner.snapshots.borrow().clone();
                        if !matches!(snapshot.lifecycle,
                            SessionLifecycle::Live { generation: live }
                            | SessionLifecycle::Closing { generation: live, .. } if live == run)
                        {
                            return Err(EventRefusal::Retired);
                        }
                        match snapshot.turn {
                            SessionTurn::Idle => None,
                            SessionTurn::Running { turn } | SessionTurn::Cancelling { turn } => {
                                Some(turn)
                            }
                        }
                    }
                };
                Ok(OwnerEvent::Stream {
                    run,
                    expected_turn,
                    event,
                })
            }
            SessionLifecycleFact::CatalogueObserved { run, update } if run == generation => {
                Ok(OwnerEvent::CatalogueObserved { run, update })
            }
            SessionLifecycleFact::SkillView { run, view } if run == generation => {
                Ok(OwnerEvent::SkillView { run, view })
            }
            SessionLifecycleFact::ObservedTurnEnded { run, turn, outcome } if run == generation => {
                Ok(OwnerEvent::ObservedTurnEnded { run, turn, outcome })
            }
            SessionLifecycleFact::OutputLimit { run, turn } if run == generation => {
                Ok(OwnerEvent::OutputLimit { run, turn })
            }
            SessionLifecycleFact::RunObservationFailed { run } if run == generation => {
                Ok(OwnerEvent::RunLost(run))
            }
            _ => Err(EventRefusal::Retired),
        }
    }
}
