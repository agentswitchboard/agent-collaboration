use super::TokioWorkspaceStores;
use crate::{machine_push_registry::MachinePushRegistry, object_storage::ObjectStorage};
use central_link_call_composition::{ApprovalEndingTransport, SelectedApprovalSocket};
use central_link_protocol::{
    FreshCorrelationId, MacCallAddress, MacLinkRpc,
    approval_rpc::{ApprovalHolderTarget, DeliverEndingRpc},
};
use collaboration_central_core::{
    MachineLinkRequestAuthority, TYPED_LINK_CALL_TIMEOUT, WorkspaceRuntime, finish_mac_call,
};
use fleet_rpc::{CallFailure, Envelope};
use router_clock::RuntimeClock;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock, Weak},
};

type Runtime = WorkspaceRuntime<ObjectStorage, TokioWorkspaceStores>;

#[derive(Clone, Default)]
pub(super) struct ApprovalDeadlineTasks {
    tasks: tokio_util::task::TaskTracker,
    stop: tokio_util::sync::CancellationToken,
}

impl ApprovalDeadlineTasks {
    pub(super) fn schedule(&self, completion: collaboration_central_core::LinkCallCompletion) {
        let stop = self.stop.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                () = stop.cancelled() => {}
                () = completion => {}
            }
        });
    }

    pub(super) async fn close(&self) {
        self.stop.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }
}

pub(crate) struct NativeApprovalDelivery {
    registry: MachinePushRegistry,
    clock: Arc<dyn RuntimeClock>,
    runtime: OnceLock<Weak<Runtime>>,
}

impl NativeApprovalDelivery {
    pub(crate) fn new(registry: MachinePushRegistry, clock: Arc<dyn RuntimeClock>) -> Self {
        Self {
            registry,
            clock,
            runtime: OnceLock::new(),
        }
    }

    pub(super) fn install_runtime(&self, runtime: &Arc<Runtime>) {
        let _ = self.runtime.set(Arc::downgrade(runtime));
    }
}

impl ApprovalEndingTransport for NativeApprovalDelivery {
    #[cfg(feature = "test-support")]
    fn fixture_delivery_answer_applied(&self) -> collaboration_central_core::LinkCallCompletion {
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        Box::pin(async move {
            if let Some(runtime) = runtime {
                runtime
                    .runtime()
                    .pause_points()
                    .reached_central(fleet_runtime::PausePoint::ApprovalEndingAnswerApplied)
                    .await;
            }
        })
    }

    fn select(
        &self,
        target: ApprovalHolderTarget,
    ) -> Pin<Box<dyn Future<Output = Result<SelectedApprovalSocket, CallFailure>> + Send>> {
        let registry = self.registry.clone();
        let socket = registry.select_approval_socket(&target);
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        let clock = self.clock.clone();
        Box::pin(async move {
            let (attachment, sender, ended) = socket?;
            let runtime = runtime.ok_or(CallFailure::NeverReached)?;
            let Ok(MachineLinkRequestAuthority::Admitted(machine)) =
                runtime.restore_machine_link_request(&attachment).await
            else {
                return Err(CallFailure::NeverReached);
            };
            let answer_runtime = runtime.clone();
            let answer_target = target.clone();
            let answer_sender = sender.clone();
            Ok(SelectedApprovalSocket {
                machine,
                ended: Box::pin(ended.cancelled_owned()),
                apply_answer: Box::new(move |apply| {
                    Box::pin(async move {
                        let _turn = answer_runtime
                            .runtime()
                            .take_central_turn_for_admission()
                            .await;
                        let Some(attachment) =
                            registry.current_approval_attachment(&answer_target, &answer_sender)
                        else {
                            return;
                        };
                        if let Ok(Ok(current)) = answer_runtime
                            .restore_machine_in_held_turn(&attachment)
                            .await
                        {
                            apply(current).await;
                        }
                    })
                }),
                send: Box::new(move |request| {
                    Box::pin(async move {
                        let deadline = clock.monotonic_now().after(TYPED_LINK_CALL_TIMEOUT);
                        #[cfg(feature = "test-support")]
                        runtime
                            .runtime()
                            .pause_points()
                            .reached_central(fleet_runtime::PausePoint::BeforeApprovalEndingEnqueue)
                            .await;
                        let correlation = FreshCorrelationId::mint_at(clock.now().unix_millis());
                        let call = DeliverEndingRpc::request(Envelope {
                            target: target.clone(),
                            body: *request,
                        });
                        let address = MacCallAddress::from(&call);
                        let response = match sender.enqueue(correlation, deadline, call, address) {
                            Ok(pending) => pending.wait(clock.as_ref()).await,
                            Err(failure) => Err(failure),
                        };
                        #[cfg(feature = "test-support")]
                        if matches!(response, Err(CallFailure::NeverReached)) {
                            runtime
                                .runtime()
                                .pause_points()
                                .reached_central(
                                    fleet_runtime::PausePoint::ApprovalEndingNeverReached,
                                )
                                .await;
                        }
                        finish_mac_call::<DeliverEndingRpc>(&target, response?)
                    })
                }),
            })
        })
    }
}
