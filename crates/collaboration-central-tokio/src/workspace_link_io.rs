//! Input limits and the shared delivery path for a native machine link.

use super::Runtime;
use axum::extract::{
    WebSocketUpgrade,
    ws::{CloseFrame, Message, Utf8Bytes, WebSocket, close_code},
};
use central_link_protocol::{
    LINK_MAX_TEXT_BYTES, LinkFrame, LinkSizeRefusal, LiveOfferOutcome, MachineLinkCloseCode,
    SocketDirectMailOffers, check_central_egress_length, encode_central_egress,
};
use collaboration_central_core::{
    MachineLinkRequestAuthority, PreparedMailOffer, ValidatedMachineConnection,
};
use fleet_contracts::mail_link::{WELCOME_PAGE_SIZE, WelcomeDeliveryPage};
use futures_util::FutureExt;
use router_clock::RuntimeClock;
use std::{
    future::{Future, poll_fn},
    sync::OnceLock,
    task::{Context, Poll},
    time::Duration,
};
use tokio_tungstenite::tungstenite::{Error, error::CapacityError};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use workspace_model::central_mail::MessageBody;

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
            writes
                .close(socket, close_code::UNSUPPORTED, "text frames required")
                .await;
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
            writes.close_authority(socket, cause).await;
            false
        }
        Ok(MachineLinkRequestAuthority::TemporarilyUnavailable) | Err(_) => {
            writes
                .close(socket, close_code::ERROR, "central unavailable")
                .await;
            false
        }
    }
}

pub(super) async fn send_welcome_backlog(
    socket: &mut WebSocket,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    offers: &mut SocketDirectMailOffers,
    direct_only: bool,
) -> bool {
    let attachment = connection.attachment();
    let mut cursor = None;
    loop {
        let page = tokio::select! {
            biased;
            () = writes.stop.cancelled() => { writes.close_if_authority_cancelled(socket).await; return false; }
            () = writes.replaced.cancelled() => { writes.close_if_authority_cancelled(socket).await; return false; }
            page = runtime.windowed_welcome_page(&attachment, cursor.as_ref()) => page,
        };
        let rows = match page {
            Ok(WelcomeDeliveryPage::Ready(rows)) => rows,
            Ok(WelcomeDeliveryPage::AuthorityLapsed(cause)) => {
                writes.close_authority(socket, cause).await;
                return false;
            }
            Err(_) => return false,
        };
        cursor = rows
            .last()
            .map(fleet_contracts::mail_link::WindowedDelivery::cursor);
        let count = rows.len();
        for row in rows {
            if direct_only && !matches!(row.message.body, MessageBody::Direct { .. }) {
                continue;
            }
            if !offer_windowed_delivery(
                socket,
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
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    offers: &mut SocketDirectMailOffers,
    message_id: &workspace_ids::MessageId,
) -> bool {
    let attachment = connection.attachment();
    let waker = poll_fn(|context| Poll::Ready(context.waker().clone())).await;
    let outcome = 'offer: {
        let Some((prepared, turn)) = writes
            .until_stopped(async {
                let turn = runtime.runtime().take_central_turn_for_admission().await;
                let prepared = runtime
                    .prepare_mail_offer_in_held_turn(&attachment, message_id, offers)
                    .await;
                (prepared, turn)
            })
            .await
        else {
            writes.close_if_authority_cancelled(socket).await;
            return false;
        };
        let text = match prepared {
            Ok(PreparedMailOffer::Ready(text)) => text,
            Ok(PreparedMailOffer::NotOffered(reason)) => {
                break 'offer LiveOfferOutcome::DefinitivelyNotOffered(reason);
            }
            Ok(PreparedMailOffer::AuthorityLapsed(cause)) => {
                drop(turn);
                writes.close_authority(socket, cause).await;
                return false;
            }
            Err(failure) => {
                drop(turn);
                tracing::warn!(?failure, "central could not prepare a direct mail offer");
                writes
                    .close(socket, close_code::ERROR, "central unavailable")
                    .await;
                return false;
            }
        };
        let send = async {
            #[cfg(feature = "test-support")]
            if runtime.take_mail_offer_write_failure() {
                return false;
            }
            socket.send(Message::Text(text.into())).await.is_ok()
        };
        tokio::pin!(send);
        let first = send.as_mut().poll(&mut Context::from_waker(&waker));
        drop(turn);
        let written = match first {
            Poll::Ready(written) => written,
            Poll::Pending => writes.send_until_stopped(send).await,
        };
        LiveOfferOutcome::from_write(written)
    };
    match outcome {
        LiveOfferOutcome::Offered | LiveOfferOutcome::DefinitivelyNotOffered(_) => true,
        LiveOfferOutcome::Ambiguous => {
            writes
                .close(socket, close_code::ERROR, "mail delivery outcome unknown")
                .await;
            false
        }
    }
}

#[cfg(feature = "test-support")]
pub(super) async fn hold_welcome(runtime: &Runtime, point: fleet_runtime::PausePoint) {
    runtime
        .runtime()
        .pause_points()
        .reached_central(point)
        .await;
}

pub(super) struct LinkWrites<'a> {
    pub(super) stop: &'a CancellationToken,
    pub(super) replaced: &'a CancellationToken,
    pub(super) authority_close: &'a OnceLock<MachineLinkCloseCode>,
    pub(super) clock: &'a dyn RuntimeClock,
    pub(super) tasks: &'a TaskTracker,
    #[cfg(feature = "test-support")]
    pub(super) pauses: &'a fleet_runtime::PausePoints,
}

impl LinkWrites<'_> {
    pub(super) fn schedule_completion(
        &self,
        completion: collaboration_central_core::LinkCallCompletion,
    ) {
        let stop = self.stop.clone();
        let replaced = self.replaced.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                () = stop.cancelled() => {}
                () = replaced.cancelled() => {}
                () = completion => {}
            }
        });
    }

    pub(super) async fn send(&self, socket: &mut WebSocket, frame: &LinkFrame) -> bool {
        let Ok(encoded) = encode_central_egress(frame) else {
            return false;
        };
        if let Some(bytes) = encoded.oversized_reply_bytes {
            tracing::warn!(bytes, "central substituted an oversized link reply");
        }
        self.send_text(socket, encoded.text).await
    }

    pub(super) async fn send_text(&self, socket: &mut WebSocket, text: String) -> bool {
        if check_central_egress_length(text.len()).is_err() {
            return false;
        }
        let sent = self
            .send_until_stopped(async { socket.send(Message::Text(text.into())).await.is_ok() })
            .await;
        if !sent {
            self.close_if_authority_cancelled(socket).await;
        }
        sent
    }

    pub(super) async fn send_until_stopped(&self, send: impl Future<Output = bool>) -> bool {
        self.until_stopped(send).await.unwrap_or(false)
    }

    async fn until_stopped<Output>(
        &self,
        operation: impl Future<Output = Output>,
    ) -> Option<Output> {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => None,
            () = self.replaced.cancelled() => None,
            result = operation => Some(result),
        }
    }

    pub(super) async fn close_if_authority_cancelled(&self, socket: &mut WebSocket) {
        if let Some(cause) = self.authority_close.get() {
            self.close_authority(socket, *cause).await;
        }
    }

    pub(super) async fn attempt_terminal_refusal(&self, socket: &mut WebSocket, frame: &LinkFrame) {
        const REFUSAL_SEND_BUDGET: Duration = Duration::from_millis(250);
        let Ok(encoded) = encode_central_egress(frame) else {
            return;
        };
        tokio::select! {
            biased;
            () = self.stop.cancelled() => {}
            _ = router_clock::timeout(self.clock, REFUSAL_SEND_BUDGET, socket.send(Message::Text(encoded.text.into()))) => {}
        }
    }

    pub(super) async fn close_authority(
        &self,
        socket: &mut WebSocket,
        cause: MachineLinkCloseCode,
    ) {
        let cause = self.authority_close.get().copied().unwrap_or(cause);
        let close = async {
            #[cfg(feature = "test-support")]
            self.pauses
                .reached_central(fleet_runtime::PausePoint::BeforeMachineAuthorityCloseWrite)
                .await;
            socket.send(authority_close_frame(cause)).await
        };
        let _ = router_clock::timeout(self.clock, AUTHORITY_CLOSE_BUDGET, close).await;
    }

    pub(super) async fn close(&self, socket: &mut WebSocket, code: u16, reason: &'static str) {
        if let Some(cause) = self.authority_close.get() {
            self.close_authority(socket, *cause).await;
        } else if self.replaced.is_cancelled() {
            close_link(socket, self.stop, close_code::NORMAL, "machine link ended");
        } else {
            close_link(socket, self.stop, code, reason);
        }
    }
}

const AUTHORITY_CLOSE_BUDGET: Duration = Duration::from_millis(250);

fn authority_close_frame(cause: MachineLinkCloseCode) -> Message {
    Message::Close(Some(CloseFrame {
        code: cause.code(),
        reason: "".into(),
    }))
}

pub(super) async fn send_authority_close(
    socket: &mut WebSocket,
    clock: &dyn RuntimeClock,
    cause: MachineLinkCloseCode,
) {
    let _ = router_clock::timeout(
        clock,
        AUTHORITY_CLOSE_BUDGET,
        socket.send(authority_close_frame(cause)),
    )
    .await;
}

pub(super) fn close_link(
    socket: &mut WebSocket,
    stop: &CancellationToken,
    code: u16,
    reason: &'static str,
) {
    if !stop.is_cancelled() {
        attempt_close(socket, code, reason);
    }
}

fn attempt_close(socket: &mut WebSocket, code: u16, reason: &'static str) {
    // Once polled on a writable socket the transport can deliver the typed reason.
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
        .now_or_never();
}
