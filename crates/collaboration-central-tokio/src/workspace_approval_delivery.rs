use super::TokioWorkspaceStores;
use crate::{
    machine_call_queue::MacCallSender, machine_push_registry::MachinePushRegistry,
    object_storage::ObjectStorage,
};
use central_link_call_composition::{
    ApprovalDeliveryTransport, ApprovalMacCall, SelectedApprovalSocket,
};
use central_link_protocol::{
    FreshCorrelationId, MAX_SOCKET_MAC_CALL_ITEMS, MacCallAddress, MacCallDeliveryKey, MacCallSkip,
    MacLinkRpc, TypedCallResult,
    approval_rpc::{
        ApprovalHolderTarget, ApprovalNoticeRpc, ApprovalNoticeTarget, DeliverEndingRpc,
    },
};
use collaboration_central_core::{
    LinkCallCompletion, MachineLinkRequestAuthority, TYPED_LINK_CALL_TIMEOUT, WorkspaceRuntime,
    finish_mac_call,
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

impl ApprovalDeliveryTransport for NativeApprovalDelivery {
    fn offer_notice(
        &self,
        notice: central_link_call_composition::CommittedApprovalNotice,
    ) -> Pin<Box<dyn Future<Output = Result<ApprovalMacCall<()>, CallFailure>> + Send>> {
        let (organization_id, destination, request_id) = notice.into_destination();
        let target = ApprovalNoticeTarget {
            organization_id,
            machine_id: destination.enrollment.machine_id,
            enrollment_id: destination.enrollment.enrollment_id,
            session_id: destination.session_id,
        };
        let address = MacCallAddress::ApprovalNotice(target.clone());
        let item = MacCallDeliveryKey::Notice(request_id.clone(), target.session_id.clone());
        let registry = self.registry.clone();
        let clock = self.clock.clone();
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        Box::pin(async move {
            let (attachment, socket, ended) =
                registry.select_delivery_socket(&target.organization_id, &address)?;
            let approval_attempt = match claim_attempt(&socket, item) {
                Some(attempt) => attempt,
                None => return Ok(ApprovalMacCall::Skipped),
            };
            let request = ApprovalNoticeRpc::request(Envelope {
                target: target.clone(),
                body: central_link_protocol::approval_rpc::ApprovalNotice { request_id },
            });
            let pending = socket.enqueue(
                FreshCorrelationId::mint_at(clock.now().unix_millis()),
                clock.monotonic_now().after(TYPED_LINK_CALL_TIMEOUT),
                request,
                address.clone(),
            );
            let result = match pending {
                Ok(pending) => pending.wait(clock.as_ref()).await,
                Err(failure) => Err(failure),
            };
            if approval_attempt.finish()
                && let Some(runtime) = runtime
            {
                resume_socket_recovery(&runtime, &registry, &socket, &address, attachment, ended)
                    .await;
            }
            match finish_mac_call::<ApprovalNoticeRpc>(&target, result?)? {
                TypedCallResult::Answered {
                    reply:
                        Envelope {
                            body:
                                central_link_protocol::approval_rpc::ApprovalNoticeReply::Received {}
                                | central_link_protocol::approval_rpc::ApprovalNoticeReply::NotHere {},
                            ..
                        },
                } => Ok(ApprovalMacCall::Attempted(())),
                TypedCallResult::Refused { .. } => Err(CallFailure::AnswerLost),
            }
        })
    }

    fn reclaim_mac_call(
        &self,
        address: MacCallAddress,
        item: MacCallDeliveryKey,
    ) -> LinkCallCompletion {
        let registry = self.registry.clone();
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        Box::pin(async move {
            let Some(organization) = address.organization_id() else {
                return;
            };
            let Ok((attachment, socket, ended)) =
                registry.select_delivery_socket(organization, &address)
            else {
                return;
            };
            if socket.reclaim_settled_item(&item)
                && let Some(runtime) = runtime
            {
                resume_socket_recovery(&runtime, &registry, &socket, &address, attachment, ended)
                    .await;
            }
        })
    }

    #[cfg(feature = "test-support")]
    fn fixture_mac_delivery_event(
        &self,
        event: central_link_call_composition::FixtureMacDeliveryEvent,
    ) -> LinkCallCompletion {
        use central_link_call_composition::FixtureMacDeliveryEvent as Event;
        use fleet_runtime::PausePoint;
        let point = match event {
            Event::BeforeMacEndingOffer => PausePoint::BeforeMacEndingOffer,
            Event::AfterMacEndingOffer => PausePoint::AfterMacEndingOffer,
            Event::BeforeMacNoticeOffer => PausePoint::BeforeMacNoticeOffer,
            Event::AfterMacNoticeOffer => PausePoint::AfterMacNoticeOffer,
            Event::MacEndingRecoveryStarted => PausePoint::MacEndingRecoveryStarted,
            Event::MacEndingRecoveryFinished => PausePoint::MacEndingRecoveryFinished,
        };
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        Box::pin(async move {
            if let Some(runtime) = runtime {
                runtime
                    .runtime()
                    .pause_points()
                    .reached_central(point)
                    .await;
            }
        })
    }

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
        request_id: workspace_ids::ApprovalRequestId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ApprovalMacCall<SelectedApprovalSocket>, CallFailure>>
                + Send,
        >,
    > {
        let registry = self.registry.clone();
        let item = MacCallDeliveryKey::Ending(request_id, target.router_run.clone());
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        let clock = self.clock.clone();
        Box::pin(async move {
            let (attachment, sender, ended) = registry.select_approval_socket(&target)?;
            let Some(approval_attempt) = claim_attempt(&sender, item.clone()) else {
                return Ok(ApprovalMacCall::Skipped);
            };
            let runtime = runtime.ok_or(CallFailure::NeverReached)?;
            let Ok(MachineLinkRequestAuthority::Admitted(machine)) =
                runtime.restore_machine_link_request(&attachment).await
            else {
                return Err(CallFailure::NeverReached);
            };
            let send_attachment = attachment.clone();
            let send_ended = ended.clone();
            let send_registry = registry.clone();
            let answer_runtime = runtime.clone();
            let answer_target = target.clone();
            let answer_sender = sender.clone();
            Ok(ApprovalMacCall::Attempted(SelectedApprovalSocket {
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
                        let send_address = address.clone();
                        let response = match sender.enqueue_claimed_approval(
                            correlation,
                            deadline,
                            call,
                            address,
                            item,
                        ) {
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
                        if approval_attempt.finish() {
                            resume_socket_recovery(
                                &runtime,
                                &send_registry,
                                &sender,
                                &send_address,
                                send_attachment,
                                send_ended,
                            )
                            .await;
                        }
                        finish_mac_call::<DeliverEndingRpc>(&target, response?)
                    })
                }),
            }))
        })
    }
}

struct NativeApprovalAttempt {
    socket: MacCallSender,
    item: Option<MacCallDeliveryKey>,
}

fn claim_attempt(
    socket: &MacCallSender,
    item: MacCallDeliveryKey,
) -> Option<NativeApprovalAttempt> {
    match socket.claim_approval_item(item.clone()) {
        Ok(()) => Some(NativeApprovalAttempt {
            socket: socket.clone(),
            item: Some(item),
        }),
        Err(MacCallSkip::FirstSaturation) => {
            crate::native_logging::observe(
                fleet_contracts::CentralObservation::MacCallLedgerSaturated {
                    capacity: MAX_SOCKET_MAC_CALL_ITEMS,
                },
            );
            None
        }
        Err(
            MacCallSkip::KindNotAdvertised | MacCallSkip::AlreadyOffered | MacCallSkip::Saturated,
        ) => None,
    }
}

impl NativeApprovalAttempt {
    fn finish(mut self) -> bool {
        self.item
            .take()
            .is_some_and(|item| self.socket.finish_approval_item(&item))
    }
}

impl Drop for NativeApprovalAttempt {
    fn drop(&mut self) {
        if let Some(item) = self.item.take() {
            self.socket.finish_approval_item(&item);
        }
    }
}

async fn resume_socket_recovery(
    runtime: &Runtime,
    registry: &MachinePushRegistry,
    socket: &MacCallSender,
    address: &MacCallAddress,
    attachment: fleet_contracts::MachineConnectionAttachment,
    ended: tokio_util::sync::CancellationToken,
) {
    let recovery = async {
        let Some(organization) = address.organization_id() else {
            return;
        };
        let Ok((current, selected, _)) = registry.select_delivery_socket(organization, address)
        else {
            return;
        };
        if !socket.same_socket(&selected) || current != attachment {
            return;
        }
        let Ok(MachineLinkRequestAuthority::Admitted(machine)) =
            runtime.restore_machine_link_request(&current).await
        else {
            return;
        };
        let (holder_run, notices) = socket.recovery_kinds();
        if let Some(run) = holder_run
            && let Some(recovery) = runtime.recover_approval_endings(machine.clone(), run)
        {
            recovery.await;
        }
        if notices && let Some(recovery) = runtime.recover_approval_notices(machine) {
            recovery.await;
        }
    };
    tokio::select! {
        biased;
        () = ended.cancelled() => {}
        () = recovery => {}
    }
}
