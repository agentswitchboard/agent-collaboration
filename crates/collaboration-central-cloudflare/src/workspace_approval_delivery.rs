use super::{
    MacCallAddress, MacLinkRpc, OrgRuntime, PendingMacCalls, is_current_socket,
    matching_attachment, socket_open, socket_tag,
};
use crate::worker_clock::WorkspaceClock;
use central_link_call_composition::{ApprovalEndingTransport, SelectedApprovalSocket};
use central_link_protocol::approval_rpc::{ApprovalHolderTarget, DeliverEndingRpc};
use collaboration_central_core::{TYPED_LINK_CALL_TIMEOUT, finish_mac_call};
use fleet_rpc::{CallFailure, Envelope};
use fleet_runtime_ports::Clock;
use futures_util::future::{Either, FutureExt, select};
use std::{
    future::Future,
    pin::{Pin, pin},
    sync::{Arc, OnceLock, Weak},
};
use worker::{
    State,
    send::{SendFuture, SendWrapper},
};

pub(in super::super) struct WorkerApprovalDelivery {
    state: Arc<SendWrapper<State>>,
    clock: WorkspaceClock,
    pending: PendingMacCalls,
    runtime: OnceLock<Weak<OrgRuntime>>,
}

impl WorkerApprovalDelivery {
    pub(in super::super) fn new(
        state: Arc<SendWrapper<State>>,
        clock: WorkspaceClock,
        pending: PendingMacCalls,
    ) -> Self {
        Self {
            state,
            clock,
            pending,
            runtime: OnceLock::new(),
        }
    }

    pub(in super::super) fn install_runtime(&self, runtime: &Arc<OrgRuntime>) {
        let _ = self.runtime.set(Arc::downgrade(runtime));
    }
}

impl ApprovalEndingTransport for WorkerApprovalDelivery {
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
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        let state = self.state.clone();
        let clock = self.clock.clone();
        let pending_calls = self.pending.clone();
        Box::pin(SendFuture::new(async move {
            let runtime = runtime.ok_or(CallFailure::NeverReached)?;
            let address = MacCallAddress::DeliverEnding(target.clone());
            let socket = state
                .get_websockets_with_tag(target.machine_id.as_str())
                .into_iter()
                .find(socket_open)
                .ok_or(CallFailure::NeverReached)?;
            let authority = matching_attachment(&socket, &address, &target.organization_id)?;
            let tag = socket_tag(&state, &socket).ok_or(CallFailure::NeverReached)?;
            let stopped = pending_calls.socket_ended(tag.clone()).map(|_| ()).shared();
            let stopped_before_write = stopped.clone();
            let Ok(collaboration_central_core::MachineLinkRequestAuthority::Admitted(machine)) =
                runtime
                    .restore_machine_link_request(&authority.authority)
                    .await
            else {
                return Err(CallFailure::NeverReached);
            };
            let socket = Arc::new(SendWrapper::new(socket));
            let answer_socket = socket.clone();
            let answer_state = state.clone();
            let answer_runtime = runtime.clone();
            let answer_target = target.clone();
            let answer_address = address.clone();
            Ok(SelectedApprovalSocket {
                machine,
                ended: Box::pin(stopped),
                apply_answer: Box::new(move |apply| {
                    Box::pin(SendFuture::new(async move {
                        let _turn = answer_runtime
                            .runtime()
                            .take_central_turn_for_admission()
                            .await;
                        if !is_current_socket(
                            &answer_state,
                            &answer_socket,
                            &answer_target.machine_id,
                        ) {
                            return;
                        }
                        let Ok(attachment) = matching_attachment(
                            &answer_socket,
                            &answer_address,
                            &answer_target.organization_id,
                        ) else {
                            return;
                        };
                        if let Ok(Ok(current)) = answer_runtime
                            .restore_machine_in_held_turn(&attachment.authority)
                            .await
                        {
                            apply(current).await;
                        }
                    }))
                }),
                send: Box::new(move |request| {
                    Box::pin(SendFuture::new(async move {
                        let deadline = clock.now().after(TYPED_LINK_CALL_TIMEOUT);
                        let call = DeliverEndingRpc::request(Envelope {
                            target: target.clone(),
                            body: *request,
                        });
                        let write = async {
                            #[cfg(feature = "test-support")]
                            if let Some(canceled) = pending_calls.approval_before_write() {
                                let held = runtime.runtime().pause_points().reached_central(
                                    fleet_runtime::PausePoint::BeforeApprovalEndingWrite,
                                );
                                if matches!(select(canceled, pin!(held)).await, Either::Left(_)) {
                                    return Err(CallFailure::NeverReached);
                                }
                            }
                            let _turn = runtime.runtime().take_central_turn_for_admission().await;
                            if !is_current_socket(&state, &socket, &target.machine_id) {
                                return Err(CallFailure::NeverReached);
                            }
                            let current =
                                matching_attachment(&socket, &address, &target.organization_id)?;
                            if !matches!(
                                runtime
                                    .restore_machine_in_held_turn(&current.authority)
                                    .await,
                                Ok(Ok(_))
                            ) {
                                return Err(CallFailure::NeverReached);
                            }
                            pending_calls.begin_mac_call(
                                &clock,
                                &socket,
                                tag,
                                Box::new(call),
                                address,
                                deadline,
                            )
                        };
                        let timeout = pin!(clock.wait_until(deadline));
                        let stop = select(stopped_before_write, timeout);
                        let write = match select(pin!(stop), pin!(write)).await {
                            Either::Left(_) => Err(CallFailure::NeverReached),
                            Either::Right((write, _)) => write,
                        };
                        #[cfg(feature = "test-support")]
                        if matches!(write, Err(CallFailure::NeverReached)) {
                            runtime
                                .runtime()
                                .pause_points()
                                .reached_central(
                                    fleet_runtime::PausePoint::ApprovalEndingNeverReached,
                                )
                                .await;
                        }
                        let written = write?;
                        let answer =
                            match select(written.answer, pin!(clock.wait_until(deadline))).await {
                                Either::Left((answer, _)) => {
                                    answer.map_err(|_| CallFailure::AnswerLost)
                                }
                                Either::Right(_) => Err(CallFailure::AnswerLost),
                            };
                        drop(written.pending);
                        finish_mac_call::<DeliverEndingRpc>(&target, answer?)
                    }))
                }),
            })
        }))
    }
}
