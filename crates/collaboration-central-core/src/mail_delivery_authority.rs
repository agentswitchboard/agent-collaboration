//! Central turns and authority for the job's mailbox operations.
use crate::{WorkspaceCaller, WorkspaceRuntime};
use central_link_protocol::{
    DataFrame, DeliveryMessage, LinkFrame, NotOffered, SocketDirectMailOffers, TimeLeft,
    encode_central_egress,
};
use central_storage::StoreFailure;
use fleet_contracts::{
    MachineConnectionAttachment,
    mail_link::{
        MachineLinkCloseCode, MailReceiptSettlement, WelcomeDeliveryCursor, WelcomeDeliveryPage,
        WindowedDeliveryDecision,
    },
};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use workspace_ids::{MessageId, SessionRef};
use workspace_model::{
    RefusalReason, Timestamp,
    central_mail::{MessageBody, MessageStatus, ReceiptKind, SessionQueue},
};

pub enum PreparedMailOffer {
    Ready(String),
    NotOffered(NotOffered),
    AuthorityLapsed(MachineLinkCloseCode),
}

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub async fn apply_delivery_receipt(
        &self,
        connection: &MachineConnectionAttachment,
        message_id: &MessageId,
        receipt: ReceiptKind,
    ) -> Result<MailReceiptSettlement, StoreFailure> {
        let result = self.settle_receipt(connection, message_id, receipt).await;
        #[cfg(feature = "test-support")]
        if matches!(&result, Ok(MailReceiptSettlement::Applied)) {
            self.execution
                .pause_points()
                .reached_engine(
                    fleet_runtime::PausePoint::AfterEngineCommit,
                    message_id.as_str(),
                )
                .await;
        }
        if matches!(
            &result,
            Ok(MailReceiptSettlement::Applied) | Err(StoreFailure::AnswerLost)
        ) {
            let _ = self.after_event().await;
        }
        result
    }

    async fn settle_receipt(
        &self,
        connection: &MachineConnectionAttachment,
        message_id: &MessageId,
        receipt: ReceiptKind,
    ) -> Result<MailReceiptSettlement, StoreFailure> {
        let _turn = self.execution.take_central_turn().await;
        if connection.organization_id() != self.execution.organization() {
            return Ok(MailReceiptSettlement::AuthorityLapsed(
                MachineLinkCloseCode::Inactive,
            ));
        }
        fleet_machine_login::link_authority::validate_shape(connection)?;
        let time = self
            .machine_login_time(connection.organization_id())
            .await?;
        let receipt = match self
            .machine_login_service()
            .restore_receipt(connection, time, message_id.clone(), receipt)
            .await?
        {
            Ok(restored) => restored,
            Err(refusal) => return Ok(MailReceiptSettlement::AuthorityLapsed(refusal.close)),
        };
        let ids = self.execution.shared_ids();
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeReceiptTransaction)
            .await;

        let applied = self
            .direct_messages
            .apply_receipt(receipt, ids)
            .await
            .map_err(StoreFailure::from)?;
        Ok(match applied {
            fleet_mail::ApplyReceipt::Applied { ready } => {
                if let Some(ready) = ready {
                    (self.mail_delivery_ready)(ready);
                }
                MailReceiptSettlement::Applied
            }
            fleet_mail::ApplyReceipt::Stop(result) => MailReceiptSettlement::Stop(result),
            fleet_mail::ApplyReceipt::AuthorityLapsed(cause) => {
                MailReceiptSettlement::AuthorityLapsed(cause)
            }
        })
    }

    /// Read the next page of direct and system messages with an open window.
    pub async fn windowed_welcome_page(
        &self,
        attachment: &MachineConnectionAttachment,
        cursor: Option<&WelcomeDeliveryCursor>,
    ) -> Result<WelcomeDeliveryPage, StoreFailure> {
        let _turn = self.execution.take_central_turn().await;
        self.windowed_welcome_page_in_held_turn(attachment, cursor)
            .await
    }

    pub async fn windowed_welcome_page_in_held_turn(
        &self,
        attachment: &MachineConnectionAttachment,
        cursor: Option<&WelcomeDeliveryCursor>,
    ) -> Result<WelcomeDeliveryPage, StoreFailure> {
        if let Err(refusal) = self.restore_live_authority_held(attachment).await? {
            return Ok(WelcomeDeliveryPage::AuthorityLapsed(refusal.close));
        }
        let now = self.execution.now()?;
        let page = self
            .direct_messages
            .read_offer_page(fleet_mail::OfferPageLookup {
                recipient: attachment.machine_id().clone(),
                after: cursor.cloned(),
                now,
            })
            .await
            .map_err(StoreFailure::from)?;
        Ok(WelcomeDeliveryPage::Ready(page))
    }

    /// Prepare the exact bytes, then claim the direct row. The caller owns this
    /// socket until its first write attempt in the same organization turn.
    pub async fn prepare_mail_offer_in_held_turn(
        &self,
        attachment: &MachineConnectionAttachment,
        message_id: &MessageId,
        offers: &mut SocketDirectMailOffers,
    ) -> Result<PreparedMailOffer, StoreFailure> {
        let decision = match self
            .read_mail_offer_in_held_turn(attachment, message_id, self.execution.now()?)
            .await
        {
            Err(StoreFailure::Unavailable | StoreFailure::AnswerLost) => {
                self.read_mail_offer_in_held_turn(attachment, message_id, self.execution.now()?)
                    .await?
            }
            result => result?,
        };
        let delivery = match decision {
            WindowedDeliveryDecision::Ready(delivery) => delivery,
            WindowedDeliveryDecision::Ineligible => {
                return Ok(PreparedMailOffer::NotOffered(NotOffered::NoLongerQueued));
            }
            WindowedDeliveryDecision::AuthorityLapsed(cause) => {
                return Ok(PreparedMailOffer::AuthorityLapsed(cause));
            }
        };
        let now = self.execution.now()?;
        let time_left = if matches!(delivery.message.body, MessageBody::Direct { .. }) {
            let Some(time_left) =
                TimeLeft::at_handoff(delivery.message.created_at, delivery.expires_at, now)
            else {
                return Ok(PreparedMailOffer::NotOffered(NotOffered::NoLongerQueued));
            };
            Some(time_left)
        } else {
            if now >= delivery.expires_at {
                return Ok(PreparedMailOffer::NotOffered(NotOffered::NoLongerQueued));
            }
            None
        };
        let encoded = encode_central_egress(&LinkFrame::Data(DataFrame::Deliver {
            message: Box::new(DeliveryMessage::from(*delivery.message)),
            sender: delivery.sender,
            time_left,
        }))
        .map_err(|_| StoreFailure::Unavailable)?;
        if self.execution.now()?.timestamp() >= attachment.effective_expires_at().0 {
            return Ok(PreparedMailOffer::AuthorityLapsed(
                MachineLinkCloseCode::Expired,
            ));
        }
        if time_left.is_some()
            && let Err(reason) = offers.claim(message_id, delivery.expires_at)
        {
            return Ok(PreparedMailOffer::NotOffered(reason));
        }
        Ok(PreparedMailOffer::Ready(encoded.text))
    }

    /// A passed wall-clock cutoff can move back into the future. Recheck a
    /// claim at a timestamp inside its original window, so only durable
    /// settlement or removal can make the offer lookup absent.
    pub async fn refill_deferred_mail_in_held_turn(
        &self,
        attachment: &MachineConnectionAttachment,
        offers: &mut SocketDirectMailOffers,
    ) -> Result<bool, StoreFailure> {
        let elapsed = offers
            .elapsed_claims(self.execution.now()?)
            .map(|(message, cutoff)| (message.clone(), cutoff))
            .collect::<Vec<_>>();
        for (message, cutoff) in elapsed {
            let Some(inside_window) = cutoff.checked_sub_signed(chrono::TimeDelta::milliseconds(1))
            else {
                continue;
            };
            let queued = self
                .direct_messages
                .read_offer(fleet_mail::DirectOfferLookup {
                    message_id: message.clone(),
                    recipient: attachment.machine_id().clone(),
                    now: inside_window,
                })
                .await
                .map_err(StoreFailure::from)?;
            if queued.is_none() {
                offers.release(&message);
            }
        }
        Ok(offers.take_deferred())
    }

    /// Re-observe the durable row, including after a page was listed.
    pub async fn prepare_windowed_delivery(
        &self,
        attachment: &MachineConnectionAttachment,
        message_id: &MessageId,
    ) -> Result<WindowedDeliveryDecision, StoreFailure> {
        let _turn = self.execution.take_central_turn().await;
        self.read_mail_offer_in_held_turn(attachment, message_id, self.execution.now()?)
            .await
    }

    async fn read_mail_offer_in_held_turn(
        &self,
        attachment: &MachineConnectionAttachment,
        message_id: &MessageId,
        now: Timestamp,
    ) -> Result<WindowedDeliveryDecision, StoreFailure> {
        if let Err(refusal) = self.restore_live_authority_held(attachment).await? {
            return Ok(WindowedDeliveryDecision::AuthorityLapsed(refusal.close));
        }
        #[cfg(feature = "test-support")]
        if self
            .mail_offer_read_failure
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Err(StoreFailure::Unavailable);
        }
        let offer = self
            .direct_messages
            .read_offer(fleet_mail::DirectOfferLookup {
                message_id: message_id.clone(),
                recipient: attachment.machine_id().clone(),
                now,
            })
            .await
            .map_err(StoreFailure::from)?;
        Ok(match offer {
            Some(offer) => {
                WindowedDeliveryDecision::Ready(fleet_contracts::mail_link::ResolvedMailDelivery {
                    message: Box::new(offer.message),
                    sender: offer.sender,
                    expires_at: offer.cutoff,
                })
            }
            None => WindowedDeliveryDecision::Ineligible,
        })
    }

    pub(crate) async fn direct_message_status(
        &self,
        caller: &WorkspaceCaller,
        message_id: &MessageId,
    ) -> Result<Result<MessageStatus, RefusalReason>, crate::ApplyingRequestFailure> {
        let _turn = self.execution.take_central_turn().await;
        if let Err(refusal) = self
            .access()
            .admit_workspace(
                self,
                caller.caller(),
                caller.connection().attachment_ref(),
                None,
            )
            .await?
        {
            return crate::applying_request_failure::applying_refusal(refusal, Err);
        }
        let Some(actor) = self.access().existing_caller_actor(caller.caller()).await? else {
            return Ok(Ok(MessageStatus::NotFound {}));
        };
        let applied = self
            .direct_messages
            .settle_and_read_status(fleet_mail::OwnedDirectStatus {
                message_id: message_id.clone(),
                sender: actor,
                clock: self.execution.shared_clock(),
                ids: self.execution.shared_ids(),
            })
            .await
            .map_err(StoreFailure::from)?;
        if let Some(ready) = applied.ready {
            (self.mail_delivery_ready)(ready);
        }
        Ok(Ok(applied.status))
    }

    /// Reads queued messages without consuming them, in stable message-UUIDv7 order.
    pub async fn queued_for_session(
        &self,
        session: &SessionRef,
    ) -> Result<SessionQueue, StoreFailure> {
        let _turn = self.execution.take_central_turn().await;
        fleet_mail::queued_for_session_held(self.execution.statement_host(), session).await
    }
}
