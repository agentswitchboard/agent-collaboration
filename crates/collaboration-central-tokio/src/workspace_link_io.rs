//! Input limits and the shared delivery path for a native machine link.

use super::{LinkState, LinkWrites, Runtime};
use axum::extract::{
    WebSocketUpgrade,
    ws::{CloseFrame, Message, Utf8Bytes, WebSocket, close_code},
};
use central_link_protocol::{
    DataFrame, LINK_MAX_TEXT_BYTES, LinkFrame, LinkSizeRefusal, SocketDirectMailOffers, TimeLeft,
};
use collaboration_central_core::{MachineLinkRequestAuthority, ValidatedMachineConnection};
use fleet_contracts::mail_link::{
    WELCOME_PAGE_SIZE, WelcomeDeliveryPage, WindowedDeliveryDecision,
};
use tokio_tungstenite::tungstenite::{Error, error::CapacityError};

pub(super) fn configure_upgrade(upgrade: WebSocketUpgrade) -> WebSocketUpgrade {
    upgrade
        .max_frame_size(LINK_MAX_TEXT_BYTES)
        .max_message_size(LINK_MAX_TEXT_BYTES)
}

pub(super) enum IncomingText {
    Text(Utf8Bytes),
    Control,
    Ended,
}

pub(super) async fn receive_text(
    socket: &mut WebSocket,
    writes: &LinkWrites<'_>,
    incoming: Option<Result<Message, axum::Error>>,
) -> IncomingText {
    match incoming {
        Some(Ok(Message::Text(text))) => IncomingText::Text(text),
        Some(Ok(Message::Ping(_) | Message::Pong(_))) => IncomingText::Control,
        Some(Ok(Message::Binary(_))) => {
            writes.close(socket, close_code::UNSUPPORTED, "text frames required");
            IncomingText::Ended
        }
        Some(Err(error)) => {
            if matches!(
                error.into_inner().downcast_ref::<Error>(),
                Some(Error::Capacity(CapacityError::MessageTooLong { .. }))
            ) {
                let refusal = LinkSizeRefusal::MessageTooBig;
                close_oversized_input(socket, writes, refusal).await;
            }
            IncomingText::Ended
        }
        Some(Ok(Message::Close(_))) | None => IncomingText::Ended,
    }
}

async fn close_oversized_input(
    socket: &mut WebSocket,
    writes: &LinkWrites<'_>,
    refusal: LinkSizeRefusal,
) {
    // The frame parser stops at the oversized header. Keep the transport
    // alive long enough for the peer to read the close while it is writing.
    let deadline = writes
        .clock
        .monotonic_now()
        .after(std::time::Duration::from_millis(250));
    let frame = Message::Close(Some(CloseFrame {
        code: refusal.code(),
        reason: refusal.reason().into(),
    }));
    let send = writes.send_until_stopped(async { socket.send(frame).await.is_ok() });
    if !matches!(
        router_clock::timeout_at(writes.clock, deadline, send).await,
        Ok(true)
    ) {
        return;
    }
    tokio::select! {
        () = writes.stop.cancelled() => {}
        () = writes.replaced.cancelled() => {}
        () = writes.clock.sleep_until(deadline) => {}
    }
}

pub(super) async fn handle_heartbeat(
    socket: &mut WebSocket,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
) -> bool {
    match runtime
        .restore_machine_link_request(connection.attachment_ref())
        .await
    {
        Ok(MachineLinkRequestAuthority::Admitted(_)) => writes.send(socket, &LinkFrame::Pong).await,
        Ok(MachineLinkRequestAuthority::Refused(cause)) => {
            writes.close_authority(socket, cause);
            false
        }
        Ok(MachineLinkRequestAuthority::TemporarilyUnavailable) | Err(_) => {
            writes.close(socket, close_code::ERROR, "central unavailable");
            false
        }
    }
}

pub(super) async fn send_welcome_backlog(
    socket: &mut WebSocket,
    state: &LinkState,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    offers: &mut SocketDirectMailOffers,
) -> bool {
    let attachment = connection.attachment();
    let mut cursor = None;
    loop {
        let page = tokio::select! {
            biased;
            () = state.stop.cancelled() => { writes.close_if_authority_cancelled(socket); return false; }
            () = writes.replaced.cancelled() => { writes.close_if_authority_cancelled(socket); return false; }
            page = runtime.windowed_welcome_page(&attachment, cursor.as_ref()) => page,
        };
        let rows = match page {
            Ok(WelcomeDeliveryPage::Ready(rows)) => rows,
            Ok(WelcomeDeliveryPage::AuthorityLapsed(cause)) => {
                writes.close_authority(socket, cause);
                return false;
            }
            Err(_) => return false,
        };
        cursor = rows
            .last()
            .map(fleet_contracts::mail_link::WindowedDelivery::cursor);
        let count = rows.len();
        for row in rows {
            if !offer_windowed_delivery(
                socket,
                state,
                writes,
                runtime,
                connection,
                offers,
                &row.message.message_id,
            )
            .await
            {
                return false;
            }
        }
        if count < WELCOME_PAGE_SIZE {
            return true;
        }
    }
}

pub(super) async fn offer_windowed_delivery(
    socket: &mut WebSocket,
    state: &LinkState,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    offers: &mut SocketDirectMailOffers,
    message_id: &workspace_ids::MessageId,
) -> bool {
    let attachment = connection.attachment();
    let decision = tokio::select! {
        biased;
        () = state.stop.cancelled() => { writes.close_if_authority_cancelled(socket); return false; }
        () = writes.replaced.cancelled() => { writes.close_if_authority_cancelled(socket); return false; }
        decision = runtime.prepare_windowed_delivery(&attachment, message_id) => decision,
    };
    let delivery = match decision {
        Ok(WindowedDeliveryDecision::Ready(delivery)) => delivery,
        Ok(WindowedDeliveryDecision::Ineligible) => {
            return true;
        }
        Ok(WindowedDeliveryDecision::AuthorityLapsed(cause)) => {
            writes.close_authority(socket, cause);
            return false;
        }
        Err(_) => return false,
    };
    let Some(now) =
        chrono::DateTime::from_timestamp_millis(state.host.current_time().unix_millis())
    else {
        return false;
    };
    if now >= delivery.expires_at {
        return true;
    }
    let time_left = if matches!(
        delivery.message.body,
        workspace_model::central_mail::MessageBody::Direct { .. }
    ) {
        let Some(time_left) =
            TimeLeft::at_handoff(delivery.message.created_at, delivery.expires_at, now)
        else {
            return true;
        };
        Some(time_left)
    } else {
        None
    };
    if time_left.is_some() && !offers.claim(message_id) {
        return true;
    }
    let frame = LinkFrame::Data(DataFrame::Deliver {
        message: Box::new(central_link_protocol::DeliveryMessage::from(
            *delivery.message,
        )),
        sender: delivery.sender,
        time_left,
    });
    writes.send(socket, &frame).await
}

#[cfg(feature = "test-support")]
pub(super) async fn hold_welcome(runtime: &Runtime, point: fleet_runtime::PausePoint) {
    runtime
        .runtime()
        .pause_points()
        .reached_central(point)
        .await;
}
