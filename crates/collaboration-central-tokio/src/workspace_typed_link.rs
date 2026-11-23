//! Typed calls over the active Tokio socket, using the owning link's send and close paths.
use super::{LinkWrites, Runtime};
use crate::machine_call_queue::{MacCallReceiver, QueuedMacCall};
use axum::extract::ws::{Message, WebSocket, close_code};
use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, FrameDirection, LinkFrame, MachineLinkCloseCode,
    TypedCallResult, encode_text,
};
use collaboration_central_core::{
    TypedCallCarrier, ValidatedMachineConnection, is_approval_holder_call,
};
use std::{
    future::{Future, poll_fn},
    task::{Context, Poll},
};

pub(super) async fn reply_to_typed_call(
    socket: &mut WebSocket,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    calls: &MacCallReceiver,
    correlation_id: CorrelationId,
    request: Option<CentralCall>,
) -> bool {
    let (carrier, turn) = if is_approval_holder_call(request.as_ref()) {
        #[cfg(feature = "test-support")]
        runtime
            .runtime()
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeApprovalCarrierTurn)
            .await;
        let turn = runtime.runtime().take_central_turn_for_admission().await;
        if writes.stop.is_cancelled() || writes.replaced.is_cancelled() {
            return false;
        }
        (
            TypedCallCarrier::Approval {
                registered_run: calls.registered_run(),
            },
            Some(turn),
        )
    } else {
        (TypedCallCarrier::Session, None)
    };
    let dispatched = match runtime
        .dispatch_typed_central_call(connection.attachment_ref(), request, carrier)
        .await
    {
        Ok(dispatched) => dispatched,
        Err(_) => {
            writes.close(socket, close_code::ERROR, "central unavailable");
            return false;
        }
    };
    if let Some(run) = dispatched.registered_run {
        calls.bind_run(run);
    }
    drop(turn);
    if !send_central_call_result(socket, writes, correlation_id, dispatched.result).await {
        return false;
    }
    if let Some(completion) = dispatched.completion {
        writes.schedule_completion(completion);
    }
    true
}

#[cfg(feature = "test-support")]
pub(super) async fn reply_to_fixture_probe(
    socket: &mut WebSocket,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    correlation_id: CorrelationId,
    request: central_link_protocol::fixture_central_probe::ProbeRequestEnvelope,
) -> bool {
    match runtime
        .dispatch_fixture_central_probe(connection.attachment_ref(), request)
        .await
    {
        Ok(answer @ TypedCallResult::Answered { .. }) => {
            let Ok(text) = central_link_protocol::fixture_central_probe::encode_probe_reply(
                correlation_id,
                answer,
            ) else {
                return false;
            };
            writes.send_text(socket, text).await
        }
        Ok(TypedCallResult::Refused { reason }) => {
            send_central_call_result(
                socket,
                writes,
                correlation_id,
                TypedCallResult::Refused { reason },
            )
            .await
        }
        Err(_) => {
            writes.close(socket, close_code::ERROR, "central unavailable");
            false
        }
    }
}

async fn send_central_call_result(
    socket: &mut WebSocket,
    writes: &LinkWrites<'_>,
    correlation_id: CorrelationId,
    result: TypedCallResult<central_link_protocol::CentralCallReply>,
) -> bool {
    let lapsed = match &result {
        TypedCallResult::Refused { reason } => reason.authority_close(),
        TypedCallResult::Answered { .. } => None,
    };
    let reply = LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result: Box::new(result),
    });
    if let Some(cause) = lapsed {
        writes.attempt_terminal_refusal(socket, &reply).await;
        writes.close_authority(socket, cause);
        return false;
    }
    writes.send(socket, &reply).await
}

pub(super) async fn offer_mac_call(
    socket: &mut WebSocket,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    calls: &MacCallReceiver,
    queued: QueuedMacCall,
) -> bool {
    #[cfg(feature = "test-support")]
    if queued.is_approval_ending() {
        runtime
            .runtime()
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeApprovalEndingWrite)
            .await;
    }
    let deadline = queued.deadline();
    let Ok(text) = encode_text(&queued.frame(), FrameDirection::ToRouter) else {
        return false;
    };
    let prevented = queued.prewrite_cancellation();
    let attachment = connection.attachment();
    let waker = poll_fn(|context| Poll::Ready(context.waker().clone())).await;
    let outcome = 'write: {
        let send = socket.send(Message::Text(text.into()));
        tokio::pin!(send);
        // Keep the organization turn through the first poll. Backpressure after that
        // poll belongs to the socket, so subsequent polls do not hold the turn.
        let first = tokio::select! {
            biased;
            () = writes.stop.cancelled() => break 'write WriteOutcome::LinkEnded,
            () = writes.replaced.cancelled() => break 'write WriteOutcome::LinkEnded,
            () = prevented.cancelled() => break 'write WriteOutcome::Skipped,
            () = tokio::task::unconstrained(writes.clock.sleep_until(deadline)) => break 'write WriteOutcome::Skipped,
            first = runtime.register_active_machine(&attachment, || {
                if writes.stop.is_cancelled() || writes.replaced.is_cancelled() || prevented.is_cancelled() {
                    return Ok(None);
                }
                if writes.clock.now().unix_millis().div_euclid(1_000) >= connection.effective_expires_at().0 {
                    return Err(MachineLinkCloseCode::Expired);
                }
                let mut context = Context::from_waker(&waker);
                Ok(calls.start_write(queued, writes.clock, connection, || {
                    send.as_mut().poll(&mut context).map(|result| result.is_ok())
                }))
            }) => first.map(|authority| authority.and_then(std::convert::identity)),
        };
        match first {
            Ok(Ok(None)) => WriteOutcome::Skipped,
            Ok(Ok(Some(Poll::Ready(sent)))) => WriteOutcome::Finished(sent),
            Ok(Ok(Some(Poll::Pending))) => {
                let send = writes.send_until_stopped(async { send.await.is_ok() });
                let sent = router_clock::timeout_at(writes.clock, deadline, send)
                    .await
                    .unwrap_or(false);
                WriteOutcome::Finished(sent)
            }
            Ok(Err(cause)) => WriteOutcome::AuthorityLapsed(cause),
            Err(_) => WriteOutcome::Unavailable,
        }
    };
    match outcome {
        WriteOutcome::Skipped | WriteOutcome::Finished(true) => true,
        WriteOutcome::Finished(false) => {
            writes.close_if_authority_cancelled(socket);
            false
        }
        WriteOutcome::LinkEnded => {
            writes.close(socket, close_code::NORMAL, "machine link ended");
            false
        }
        WriteOutcome::AuthorityLapsed(cause) => {
            writes.close_authority(socket, cause);
            false
        }
        WriteOutcome::Unavailable => {
            writes.close(socket, close_code::ERROR, "central unavailable");
            false
        }
    }
}

enum WriteOutcome {
    Skipped,
    Finished(bool),
    LinkEnded,
    AuthorityLapsed(MachineLinkCloseCode),
    Unavailable,
}
