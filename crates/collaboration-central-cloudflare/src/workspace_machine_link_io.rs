//! Physical Workerd socket I/O shared by backlog and live delivery.

use central_link_protocol::{DataFrame, LinkSizeRefusal, LiveOfferOutcome, ReceiptResult};
use collaboration_central_core::{PreparedMailOffer, RenewalTermination, project_renewal_refusal};
use fleet_contracts::MachineConnectionAttachment;
use fleet_contracts::mail_link::{WELCOME_PAGE_SIZE, WelcomeDeliveryPage, WindowedDelivery};
use std::{
    collections::BTreeSet,
    sync::{Mutex, PoisonError},
};
use worker::{Result, WebSocket, WebSocketIncomingMessage};
use workspace_ids::MessageId;
use workspace_model::central_mail::MessageBody;

use super::{
    CentralObject, OrgRuntime,
    workspace_socket_attachment::{SocketAttachment, socket_open, socket_tag},
};

#[derive(Default)]
pub(super) struct WorkerMailOffers {
    awaiting_welcome: Mutex<BTreeSet<String>>,
}

impl WorkerMailOffers {
    pub(super) fn begin_welcome(&self, tag: String) {
        self.awaiting_welcome
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tag);
    }

    pub(super) fn forget(&self, tag: &str) {
        self.awaiting_welcome
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(tag);
    }

    fn can_offer(&self, tag: &str) -> bool {
        !self
            .awaiting_welcome
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(tag)
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

    pub(super) fn close_machine_link(
        &self,
        socket: &WebSocket,
        code: u16,
        reason: Option<&str>,
    ) -> Result<()> {
        self.forget_mail_offers(socket);
        self.pending_mac_calls
            .close(&self.state, socket, code, reason)
    }

    pub(super) fn close_machine_authority(
        &self,
        socket: &WebSocket,
        cause: central_link_protocol::MachineLinkCloseCode,
    ) -> Result<()> {
        self.close_machine_link(socket, cause.code(), None)
    }
}

impl CentralObject {
    pub(super) async fn send_welcome_mail(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        direct_only: bool,
    ) -> Result<()> {
        let mut cursor = None;
        loop {
            let page = {
                let _turn = runtime.runtime().take_central_turn_for_admission().await;
                let Some(attachment) = SocketAttachment::read(socket)? else {
                    return self.close_machine_link(
                        socket,
                        1008,
                        Some("connection identity missing"),
                    );
                };
                runtime
                    .windowed_welcome_page_in_held_turn(&attachment.authority, cursor.as_ref())
                    .await
            };
            let rows = match page {
                Ok(WelcomeDeliveryPage::Ready(rows)) => rows,
                Ok(WelcomeDeliveryPage::AuthorityLapsed(cause)) => {
                    return self.close_machine_authority(socket, cause);
                }
                Err(failure) => {
                    worker::console_warn!("central could not read the mail backlog: {failure}");
                    return self.close_machine_link(socket, 1011, Some("central unavailable"));
                }
            };
            cursor = rows.last().map(WindowedDelivery::cursor);
            let count = rows.len();
            for row in rows {
                if direct_only && !matches!(row.message.body, MessageBody::Direct { .. }) {
                    continue;
                }
                if !self
                    .offer_windowed_mail(runtime, socket, &row.message.message_id)
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
                .unwrap_or_else(PoisonError::into_inner),
        );
        for message in ready {
            for socket in self
                .state
                .get_websockets_with_tag(message.machine_id.as_str())
            {
                if !socket_open(&socket) {
                    continue;
                }
                let offered = self
                    .offer_windowed_mail(runtime, &socket, &message.message_id)
                    .await;
                let result = match offered {
                    Ok(true) => self.resume_deferred_mail(runtime, &socket).await,
                    Ok(false) => continue,
                    Err(error) => Err(error),
                };
                if let Err(error) = result {
                    worker::console_warn!("central delivery offer failed: {error}");
                    let _ = self.close_machine_link(&socket, 1011, Some("central unavailable"));
                }
            }
        }
    }

    pub(super) async fn release_mail_receipt(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        message_id: &MessageId,
        result: ReceiptResult,
    ) -> Result<()> {
        if result == ReceiptResult::NotApplicable {
            return Ok(());
        }
        let _turn = runtime.runtime().take_central_turn_for_admission().await;
        if let Some(mut attachment) = SocketAttachment::read(socket)? {
            attachment.mail_offers.release(message_id);
            attachment.save(socket)?;
        }
        Ok(())
    }

    pub(super) async fn resume_deferred_mail(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
    ) -> Result<()> {
        if !socket_open(socket) {
            return Ok(());
        }
        let needs_scan = {
            let _turn = runtime.runtime().take_central_turn_for_admission().await;
            let Some(mut attachment) = SocketAttachment::read(socket)? else {
                return self.close_machine_link(socket, 1008, Some("connection identity missing"));
            };
            if let Err(failure) = runtime.observe_mail_offer_time(&mut attachment.mail_offers) {
                worker::console_warn!("central could not observe mail offer time: {failure}");
                return self.close_machine_link(socket, 1011, Some("central unavailable"));
            }
            let needs_scan = attachment.mail_offers.take_deferred();
            if needs_scan {
                attachment.save(socket)?;
            }
            needs_scan
        };
        if needs_scan {
            self.send_welcome_mail(runtime, socket, true).await?;
        }
        Ok(())
    }

    async fn offer_windowed_mail(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        message_id: &MessageId,
    ) -> Result<bool> {
        let Some(tag) = socket_tag(&self.state, socket) else {
            return Ok(false);
        };
        // A live hint before Welcome is covered by the backlog that follows it.
        if !self.mail_offers.can_offer(&tag) {
            return Ok(true);
        }
        let _turn = runtime.runtime().take_central_turn_for_admission().await;
        if !socket_open(socket) {
            return Ok(false);
        }
        let (current, prepared) = loop {
            let Some(mut snapshot) = SocketAttachment::read(socket)? else {
                self.close_machine_link(socket, 1008, Some("connection identity missing"))?;
                return Ok(false);
            };
            let prepared = runtime
                .prepare_mail_offer_in_held_turn(
                    &snapshot.authority,
                    message_id,
                    &mut snapshot.mail_offers,
                )
                .await;
            if !socket_open(socket) {
                return Ok(false);
            }
            let Some(mut current) = SocketAttachment::read(socket)? else {
                self.close_machine_link(socket, 1008, Some("connection identity missing"))?;
                return Ok(false);
            };
            if current.authority != snapshot.authority {
                continue;
            }
            // Call completions can update their ledger while the store is
            // awaited. Save only the mailbox decision into the latest snapshot.
            current.mail_offers = snapshot.mail_offers;
            break (current, prepared);
        };
        let outcome = match prepared {
            Ok(PreparedMailOffer::Ready(text)) => {
                if let Err(error) = current.save(socket) {
                    worker::console_warn!("central could not save the socket mail claims: {error}");
                    self.close_machine_link(socket, 1011, Some("central unavailable"))?;
                    return Ok(false);
                }
                #[cfg(feature = "test-support")]
                let fail_send = runtime.take_mail_offer_write_failure();
                #[cfg(not(feature = "test-support"))]
                let fail_send = false;
                LiveOfferOutcome::from_write(
                    !fail_send && socket_open(socket) && socket.send_with_str(&text).is_ok(),
                )
            }
            Ok(PreparedMailOffer::NotOffered(reason)) => {
                current.save(socket)?;
                LiveOfferOutcome::DefinitivelyNotOffered(reason)
            }
            Ok(PreparedMailOffer::AuthorityLapsed(cause)) => {
                self.close_machine_authority(socket, cause)?;
                return Ok(false);
            }
            Err(failure) => {
                worker::console_warn!("central could not prepare a mail offer: {failure}");
                self.close_machine_link(socket, 1011, Some("central unavailable"))?;
                return Ok(false);
            }
        };
        match outcome {
            LiveOfferOutcome::Offered | LiveOfferOutcome::DefinitivelyNotOffered(_) => Ok(true),
            LiveOfferOutcome::Ambiguous => {
                self.close_machine_link(socket, 1011, Some("mail delivery outcome unknown"))?;
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

pub(super) fn send(socket: &WebSocket, frame: DataFrame) -> Result<()> {
    let encoded = central_link_protocol::encode_central_egress(
        &central_link_protocol::LinkFrame::Data(frame),
    )
    .map_err(|failure| worker::Error::RustError(failure.to_string()))?;
    if let Some(bytes) = encoded.oversized_reply_bytes {
        worker::console_warn!(
            "central substituted an oversized link reply ({} bytes)",
            bytes
        );
    }
    socket.send_with_str(&encoded.text)
}

/// R40/V32 renewal on the open hibernating socket. A renewed connection's
/// attachment replaces the old one, so a restored socket carries the renewed
/// expiry; on refusal the old attachment and its expiry stand.
pub(super) async fn renew(
    socket: &WebSocket,
    runtime: &OrgRuntime,
    attachment: &MachineConnectionAttachment,
    token: &str,
) -> Result<(
    central_link_protocol::TokenRenewalResult,
    Option<RenewalTermination>,
)> {
    use central_link_protocol::{TokenRenewalRefusal, TokenRenewalResult};
    let refused = |reason| (TokenRenewalResult::Refused { reason }, None);
    Ok(
        match runtime.renew_machine_connection(attachment, token).await {
            Ok(Ok(renewed)) => {
                let Some(effective_expires_at) = chrono::DateTime::from_timestamp(
                    renewed.connection.effective_expires_at().0,
                    0,
                ) else {
                    return Ok(refused(TokenRenewalRefusal::CentralUnavailable));
                };
                let Some(mut current) = SocketAttachment::read(socket)? else {
                    return Ok(refused(TokenRenewalRefusal::CentralUnavailable));
                };
                // A registration may have completed while the authority turn was awaited.
                current.authority = renewed.connection.attachment();
                current.save(socket)?;
                (
                    TokenRenewalResult::Renewed {
                        effective_expires_at,
                        central_time: renewed.central_time,
                    },
                    None,
                )
            }
            Ok(Err(refusal)) => {
                let (reason, termination) = project_renewal_refusal(refusal);
                (TokenRenewalResult::Refused { reason }, termination)
            }
            Err(_) => refused(TokenRenewalRefusal::CentralUnavailable),
        },
    )
}
