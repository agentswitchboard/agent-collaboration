//! Physical Workerd socket I/O shared by backlog and live delivery.

use central_link_protocol::{DataFrame, LinkSizeRefusal, SocketDirectMailOffers, TimeLeft};
use fleet_contracts::MachineConnectionAttachment;
use fleet_runtime_ports::Clock;
use std::{
    collections::BTreeMap,
    sync::{Mutex, PoisonError},
};
use worker::{Result, WebSocket, WebSocketIncomingMessage};
use workspace_ids::MessageId;

use super::workspace_machine_link::send;
use super::{
    CentralObject, OrgRuntime, workspace_socket_attachment::SocketAttachment,
    workspace_typed_link::socket_tag,
};

#[derive(Default)]
pub(super) struct WorkerMailOffers {
    sockets: Mutex<BTreeMap<String, SocketMailPhase>>,
}

enum SocketMailPhase {
    AwaitingWelcome,
    Open(SocketDirectMailOffers),
}

impl WorkerMailOffers {
    pub(super) fn begin_welcome(&self, tag: String) {
        self.sockets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tag, SocketMailPhase::AwaitingWelcome);
    }

    pub(super) fn finish_welcome(&self, tag: String) {
        self.sockets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                tag,
                SocketMailPhase::Open(SocketDirectMailOffers::default()),
            );
    }

    pub(super) fn forget(&self, tag: &str) {
        self.sockets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(tag);
    }

    fn can_offer(&self, tag: &str) -> bool {
        !matches!(
            self.sockets
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(tag),
            Some(SocketMailPhase::AwaitingWelcome)
        )
    }

    fn claim(&self, tag: String, message: &MessageId) -> bool {
        let mut sockets = self.sockets.lock().unwrap_or_else(PoisonError::into_inner);
        let phase = sockets
            .entry(tag)
            .or_insert_with(|| SocketMailPhase::Open(SocketDirectMailOffers::default()));
        match phase {
            SocketMailPhase::AwaitingWelcome => false,
            SocketMailPhase::Open(offers) => offers.claim(message),
        }
    }
}

impl CentralObject {
    pub(super) fn forget_mail_offers(&self, socket: &WebSocket) {
        if let Some(tag) = socket_tag(&self.state, socket) {
            self.mail_offers.forget(&tag);
        }
    }

    pub(super) fn machine_text(
        &self,
        socket: &WebSocket,
        incoming: WebSocketIncomingMessage,
    ) -> Result<Option<String>> {
        let WebSocketIncomingMessage::String(text) = incoming else {
            self.close_machine_link(socket, 1008, Some("text frame required"))?;
            return Ok(None);
        };
        if let Err(refusal) = LinkSizeRefusal::check_length(text.len()) {
            self.close_machine_link(socket, refusal.code(), Some(refusal.reason()))?;
            return Ok(None);
        }
        Ok(Some(text))
    }

    pub(super) async fn answer_heartbeat(&self, socket: &WebSocket) -> Result<()> {
        let attachment = match SocketAttachment::read(socket) {
            Ok(Some(attachment)) => attachment.authority,
            _ => return self.close_machine_link(socket, 1008, Some("connection identity missing")),
        };
        let organization = attachment.organization_id().clone();
        if self.check_physical_organization(&organization).is_err() {
            return self.close_machine_link(socket, 1008, Some("connection organization mismatch"));
        }
        #[cfg(feature = "test-support")]
        super::signin_launcher_controls::sample_clock(self, &organization).await?;
        let runtime = self.runtime(organization).await?;
        let _turn = runtime.runtime().take_central_turn_for_admission().await;
        match runtime.restore_machine_in_held_turn(&attachment).await {
            Ok(Ok(_)) => socket.send_with_str("pong"),
            Ok(Err(cause)) => self.close_machine_authority(socket, cause),
            Err(_) => self.close_machine_link(socket, 1011, Some("central unavailable")),
        }
    }
}

impl CentralObject {
    pub(super) async fn send_welcome_mail(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        attachment: &MachineConnectionAttachment,
    ) -> Result<()> {
        use fleet_contracts::mail_link::{WELCOME_PAGE_SIZE, WelcomeDeliveryPage};
        let mut cursor = None;
        loop {
            let page = runtime
                .windowed_welcome_page(attachment, cursor.as_ref())
                .await
                .map_err(|error| worker::Error::RustError(error.to_string()))?;
            let rows = match page {
                WelcomeDeliveryPage::Ready(rows) => rows,
                WelcomeDeliveryPage::AuthorityLapsed(cause) => {
                    return self.close_machine_authority(socket, cause);
                }
            };
            cursor = rows
                .last()
                .map(fleet_contracts::mail_link::WindowedDelivery::cursor);
            let count = rows.len();
            for row in rows {
                if !self
                    .offer_windowed_mail(runtime, socket, attachment, &row.message.message_id)
                    .await?
                {
                    return Ok(());
                }
            }
            if count < WELCOME_PAGE_SIZE {
                return Ok(());
            }
        }
    }

    pub(super) async fn flush_ready_mail(&self, runtime: &OrgRuntime) {
        let ready = std::mem::take(
            &mut *self
                .mail_pushes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for message in ready {
            for socket in self
                .state
                .get_websockets_with_tag(message.machine_id.as_str())
            {
                if socket.as_ref().ready_state() != web_sys::WebSocket::OPEN {
                    continue;
                }
                if let Ok(Some(attachment)) = SocketAttachment::read(&socket)
                    && let Err(error) = self
                        .offer_windowed_mail(
                            runtime,
                            &socket,
                            &attachment.authority,
                            &message.message_id,
                        )
                        .await
                {
                    worker::console_warn!("central delivery offer failed: {error}");
                }
            }
        }
    }

    async fn offer_windowed_mail(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        attachment: &MachineConnectionAttachment,
        message_id: &workspace_ids::MessageId,
    ) -> Result<bool> {
        use fleet_contracts::mail_link::WindowedDeliveryDecision;
        let Some(tag) = socket_tag(&self.state, socket) else {
            return Ok(false);
        };
        // A live hint before Welcome is covered by the backlog that follows it.
        if !self.mail_offers.can_offer(&tag) {
            return Ok(true);
        }
        let decision = runtime
            .prepare_windowed_delivery(attachment, message_id)
            .await
            .map_err(|error| worker::Error::RustError(error.to_string()))?;
        match decision {
            WindowedDeliveryDecision::Ready(delivery) => {
                if socket.as_ref().ready_state() != web_sys::WebSocket::OPEN {
                    return Ok(false);
                }
                let now = chrono::DateTime::from_timestamp_millis(self.clock.now().unix_millis())
                    .ok_or_else(|| {
                    worker::Error::RustError("offer clock unavailable".into())
                })?;
                if now >= delivery.expires_at {
                    return Ok(true);
                }
                let time_left = if matches!(
                    delivery.message.body,
                    workspace_model::central_mail::MessageBody::Direct { .. }
                ) {
                    let Some(time_left) =
                        TimeLeft::at_handoff(delivery.message.created_at, delivery.expires_at, now)
                    else {
                        return Ok(true);
                    };
                    Some(time_left)
                } else {
                    None
                };
                if time_left.is_some() && !self.mail_offers.claim(tag, message_id) {
                    return Ok(true);
                }
                send(
                    socket,
                    DataFrame::Deliver {
                        message: Box::new(central_link_protocol::DeliveryMessage::from(
                            *delivery.message,
                        )),
                        sender: delivery.sender,
                        time_left,
                    },
                )?;
                Ok(true)
            }
            WindowedDeliveryDecision::Ineligible => Ok(true),
            WindowedDeliveryDecision::AuthorityLapsed(cause) => {
                self.close_machine_authority(socket, cause)?;
                Ok(false)
            }
        }
    }
}

#[cfg(feature = "test-support")]
pub(super) async fn hold_welcome(runtime: &OrgRuntime, point: fleet_runtime::PausePoint) {
    runtime
        .runtime()
        .pause_points()
        .reached_central(point)
        .await;
}
