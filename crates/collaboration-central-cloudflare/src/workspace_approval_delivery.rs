use super::super::workspace_pending_mac_calls::PendingMacCalls;
use super::{
    MacCallAddress, MacLinkRpc, OrgRuntime, SocketAttachment, is_current_socket,
    matching_attachment, socket_open, socket_tag,
};
use crate::worker_clock::WorkspaceClock;
use central_link_call_composition::{
    ApprovalDeliveryTransport, ApprovalMacCall, SelectedApprovalSocket,
};
use central_link_protocol::{
    MAX_SOCKET_MAC_CALL_ITEMS, MacCall, MacCallDeliveryKey, MacCallKind, MacCallSkip,
    TypedCallResult,
    approval_rpc::{
        ApprovalHolderTarget, ApprovalNoticeRpc, ApprovalNoticeTarget, DeliverEndingRpc,
    },
};
use collaboration_central_core::{
    LinkCallCompletion, MachineLinkRequestAuthority, TYPED_LINK_CALL_TIMEOUT, finish_mac_call,
};
use fleet_rpc::{CallFailure, Envelope};
use fleet_runtime_ports::Clock;
use futures_util::future::{Either, FutureExt, select};
use std::{
    future::Future,
    pin::{Pin, pin},
    sync::{Arc, OnceLock, Weak},
};
use worker::{
    State, WebSocket,
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

impl ApprovalDeliveryTransport for WorkerApprovalDelivery {
    fn offer_notice(
        &self,
        notice: central_link_call_composition::CommittedApprovalNotice,
    ) -> Pin<Box<dyn Future<Output = Result<ApprovalMacCall<()>, CallFailure>> + Send>> {
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        let state = self.state.clone();
        let clock = self.clock.clone();
        let pending_calls = self.pending.clone();
        Box::pin(SendFuture::new(async move {
            let runtime = runtime.ok_or(CallFailure::NeverReached)?;
            let (organization_id, destination, request_id) = notice.into_destination();
            let target = ApprovalNoticeTarget {
                organization_id,
                machine_id: destination.enrollment.machine_id,
                enrollment_id: destination.enrollment.enrollment_id,
                session_id: destination.session_id,
            };
            let item = MacCallDeliveryKey::Notice(request_id.clone(), target.session_id.clone());
            let socket = state
                .get_websockets_with_tag(target.machine_id.as_str())
                .into_iter()
                .find(socket_open)
                .ok_or(CallFailure::NeverReached)?;
            let Some(approval_attempt) = claim_worker_attempt(&socket, item)? else {
                return Ok(ApprovalMacCall::Skipped);
            };
            let deadline = clock.now().after(TYPED_LINK_CALL_TIMEOUT);
            let write = async {
                #[cfg(feature = "test-support")]
                runtime
                    .runtime()
                    .pause_points()
                    .reached_central(fleet_runtime::PausePoint::BeforeApprovalNoticeWrite)
                    .await;
                let _turn = runtime.runtime().take_central_turn_for_admission().await;
                let request = ApprovalNoticeRpc::request(Envelope {
                    target: target.clone(),
                    body: central_link_protocol::approval_rpc::ApprovalNotice { request_id },
                });
                let address = MacCallAddress::from(&request);
                let tag = socket_tag(&state, &socket).ok_or(CallFailure::NeverReached)?;
                let attachment = matching_attachment(&socket, &address, &target.organization_id)?;
                restore_before_write(&runtime, &pending_calls, &state, &socket, &attachment)
                    .await?;
                let MacCall::ApprovalNotice(notice) = &request else {
                    return Err(CallFailure::NeverReached);
                };
                match runtime.approval_notice_deadline_in_held_turn(notice).await {
                    Ok(Some(approval_deadline))
                        if clock.now().unix_millis() < approval_deadline.timestamp_millis() => {}
                    Ok(_) => return Err(CallFailure::NeverReached),
                    Err(failure) => {
                        worker::console_warn!(
                            "could not validate queued approval notice: {failure:?}"
                        );
                        return Err(CallFailure::NeverReached);
                    }
                }
                if !is_current_socket(&state, &socket, &target.machine_id) {
                    return Err(CallFailure::NeverReached);
                }
                pending_calls.begin_mac_call(
                    &clock,
                    &socket,
                    tag,
                    Box::new(request),
                    address,
                    deadline,
                )
            };
            let attempt = match select(pin!(clock.wait_for_mac_call(deadline)), pin!(write)).await {
                Either::Left(_) => Err(CallFailure::NeverReached),
                Either::Right((attempt, _)) => attempt,
            };
            #[cfg(feature = "test-support")]
            runtime
                .runtime()
                .pause_points()
                .reached_central(fleet_runtime::PausePoint::AfterApprovalNoticeAttempt)
                .await;
            let answer = async {
                let written = attempt?;
                let answer =
                    match select(written.answer, pin!(clock.wait_for_mac_call(deadline))).await {
                        Either::Left((answer, _)) => answer.map_err(|_| CallFailure::AnswerLost),
                        Either::Right(_) => Err(CallFailure::AnswerLost),
                    };
                drop(written.pending);
                answer
            }
            .await;
            if approval_attempt.finish() {
                resume_worker_recovery(&runtime, &state, &socket, &pending_calls).await;
            }
            match finish_mac_call::<ApprovalNoticeRpc>(&target, answer?)? {
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
        }))
    }

    fn reclaim_mac_call(
        &self,
        address: MacCallAddress,
        item: MacCallDeliveryKey,
    ) -> LinkCallCompletion {
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        let state = self.state.clone();
        let pending_calls = self.pending.clone();
        Box::pin(SendFuture::new(async move {
            let Some(organization) = address.organization_id() else {
                return;
            };
            let Some(socket) = state
                .get_websockets_with_tag(address.machine_id().as_str())
                .into_iter()
                .find(socket_open)
            else {
                return;
            };
            let Ok(mut attachment) = matching_attachment(&socket, &address, organization) else {
                return;
            };
            let resume = attachment.mac_calls.reclaim_settled(&item);
            if attachment.save(&socket).is_ok()
                && resume
                && let Some(runtime) = runtime
            {
                resume_worker_recovery(&runtime, &state, &socket, &pending_calls).await;
            }
        }))
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
        Box::pin(SendFuture::new(async move {
            if let Some(runtime) = runtime {
                runtime
                    .runtime()
                    .pause_points()
                    .reached_central(point)
                    .await;
            }
        }))
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
            let item = MacCallDeliveryKey::Ending(request_id, target.router_run.clone());
            let Some(approval_attempt) = claim_worker_attempt(&socket, item)? else {
                return Ok(ApprovalMacCall::Skipped);
            };
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
            Ok(ApprovalMacCall::Attempted(SelectedApprovalSocket {
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
                            restore_before_write(
                                &runtime,
                                &pending_calls,
                                &state,
                                &socket,
                                &current,
                            )
                            .await?;
                            let MacCall::DeliverEnding(ending) = &call else {
                                return Err(CallFailure::NeverReached);
                            };
                            if !runtime
                                .approval_ending_is_owed_in_held_turn(
                                    &ending.target,
                                    ending.body.request_id(),
                                )
                                .await
                                .map_err(|_| CallFailure::NeverReached)?
                            {
                                return Err(CallFailure::NeverReached);
                            }
                            if !is_current_socket(&state, &socket, &target.machine_id) {
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
                        let timeout = pin!(clock.wait_for_mac_call(deadline));
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
                            match select(written.answer, pin!(clock.wait_for_mac_call(deadline)))
                                .await
                            {
                                Either::Left((answer, _)) => {
                                    answer.map_err(|_| CallFailure::AnswerLost)
                                }
                                Either::Right(_) => Err(CallFailure::AnswerLost),
                            };
                        drop(written.pending);
                        if approval_attempt.finish() {
                            resume_worker_recovery(&runtime, &state, &socket, &pending_calls).await;
                        }
                        finish_mac_call::<DeliverEndingRpc>(&target, answer?)
                    }))
                }),
            }))
        }))
    }
}

struct WorkerApprovalAttempt {
    socket: SendWrapper<WebSocket>,
    item: Option<MacCallDeliveryKey>,
}

fn claim_worker_attempt(
    socket: &WebSocket,
    item: MacCallDeliveryKey,
) -> Result<Option<WorkerApprovalAttempt>, CallFailure> {
    let mut attachment = SocketAttachment::read(socket)
        .map_err(|_| CallFailure::NeverReached)?
        .ok_or(CallFailure::NeverReached)?;
    let admission = attachment.mac_calls.begin(item.clone());
    match admission {
        Ok(()) => {
            attachment
                .save(socket)
                .map_err(|_| CallFailure::NeverReached)?;
            Ok(Some(WorkerApprovalAttempt {
                socket: SendWrapper::new(socket.clone()),
                item: Some(item),
            }))
        }
        Err(MacCallSkip::FirstSaturation | MacCallSkip::Saturated) => {
            attachment
                .save(socket)
                .map_err(|_| CallFailure::NeverReached)?;
            if admission == Err(MacCallSkip::FirstSaturation) {
                crate::signin_observation::observe_signin(
                    fleet_contracts::CentralObservation::MacCallLedgerSaturated {
                        capacity: MAX_SOCKET_MAC_CALL_ITEMS,
                    },
                );
            }
            Ok(None)
        }
        Err(MacCallSkip::KindNotAdvertised | MacCallSkip::AlreadyOffered) => Ok(None),
    }
}

impl WorkerApprovalAttempt {
    fn finish(mut self) -> bool {
        self.finish_item()
    }

    fn finish_item(&mut self) -> bool {
        let Some(item) = self.item.take() else {
            return false;
        };
        let Ok(Some(mut current)) = SocketAttachment::read(&self.socket) else {
            return false;
        };
        let resume = current.mac_calls.finish(&item);
        current.save(&self.socket).is_ok() && resume
    }
}

impl Drop for WorkerApprovalAttempt {
    fn drop(&mut self) {
        self.finish_item();
    }
}

async fn resume_worker_recovery(
    runtime: &OrgRuntime,
    state: &State,
    socket: &WebSocket,
    pending_calls: &PendingMacCalls,
) {
    let Ok(Some(current)) = SocketAttachment::read(socket) else {
        return;
    };
    if !is_current_socket(state, socket, current.authority.machine_id()) {
        return;
    }
    let Ok(MachineLinkRequestAuthority::Admitted(machine)) = runtime
        .restore_machine_link_request(&current.authority)
        .await
    else {
        return;
    };
    if !is_current_socket(state, socket, machine.machine_id()) {
        return;
    }
    let Some(tag) = socket_tag(state, socket) else {
        return;
    };
    let recovery = async {
        if current.mac_calls.handles(MacCallKind::DeliverEnding)
            && let Some(run) = current.registered_run
            && let Some(recovery) = runtime.recover_approval_endings(machine.clone(), run)
        {
            recovery.await;
        }
        if current.mac_calls.handles(MacCallKind::ApprovalNotice)
            && let Some(recovery) = runtime.recover_approval_notices(machine)
        {
            recovery.await;
        }
    };
    let _ = select(pending_calls.socket_ended(tag), pin!(recovery)).await;
}

async fn restore_before_write(
    runtime: &OrgRuntime,
    pending: &PendingMacCalls,
    state: &worker::State,
    socket: &worker::WebSocket,
    attachment: &SocketAttachment,
) -> Result<(), CallFailure> {
    match runtime
        .restore_machine_in_held_turn(&attachment.authority)
        .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(cause)) => {
            let _ = pending.close(state, socket, cause.code(), None);
            Err(CallFailure::NeverReached)
        }
        Err(_) => Err(CallFailure::NeverReached),
    }
}
