//! Central turns and authority for the job's mailbox operations.
use crate::{WorkspaceCaller, WorkspaceRuntime};
use central_storage::StoreFailure;
use fleet_contracts::{
    MachineConnectionAttachment,
    mail_link::{
        MailReceiptSettlement, WelcomeDeliveryCursor, WelcomeDeliveryPage, WindowedDeliveryDecision,
    },
};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use workspace_ids::{MessageId, SessionRef};
use workspace_model::{
    RefusalReason,
    central_mail::{MessageStatus, ReceiptKind, SessionQueue},
};

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
        let authority = match self.restore_live_authority_held(connection).await? {
            Ok(authority) => authority,
            Err(refusal) => return Ok(MailReceiptSettlement::AuthorityLapsed(refusal.close)),
        };

        let admitted_receipt = fleet_mail::AdmittedReceipt::new(
            authority,
            message_id.clone(),
            receipt,
            self.execution.shared_ids(),
        );
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeReceiptTransaction)
            .await;

        let applied = self
            .direct_messages
            .apply_receipt(admitted_receipt)
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

    /// Re-observe the durable row at push time, including after a page was listed.
    pub async fn prepare_windowed_delivery(
        &self,
        attachment: &MachineConnectionAttachment,
        message_id: &MessageId,
    ) -> Result<WindowedDeliveryDecision, StoreFailure> {
        let _turn = self.execution.take_central_turn().await;
        if let Err(refusal) = self.restore_live_authority_held(attachment).await? {
            return Ok(WindowedDeliveryDecision::AuthorityLapsed(refusal.close));
        }
        let offer = self
            .direct_messages
            .read_offer(fleet_mail::DirectOfferLookup {
                message_id: message_id.clone(),
                recipient: attachment.machine_id().clone(),
                now: self.execution.now()?,
            })
            .await
            .map_err(StoreFailure::from)?;
        if let Some(offer) = offer {
            return Ok(WindowedDeliveryDecision::Ready(
                fleet_contracts::mail_link::ResolvedMailDelivery {
                    message: Box::new(offer.message),
                    sender: offer.sender,
                    expires_at: offer.cutoff,
                },
            ));
        }
        Ok(WindowedDeliveryDecision::Ineligible)
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
