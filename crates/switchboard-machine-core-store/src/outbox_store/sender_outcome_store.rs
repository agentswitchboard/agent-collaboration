//! Retains a late outcome and recovers the original direct-message acceptance.
use super::{
    MacOutbox, OutboxStoreError,
    outbox_rows::{StoredPayload, outbox_content, payload_content_digest, read_owned_outbox},
    validate_write_timestamp,
};
use crate::{
    OutboxAccepted, OutboxCancellation, OutboxRequestStatus, OutboxWriteTarget,
    StoredSenderOutcome, outbox_item_state::OutboxItemState, outbox_receipt_profile::AnsweredWrite,
    sender_outcome::OutcomePrecedence,
};
use sqlite_storage_maintenance::TransactionStart;
use workspace_ids::RequestId;
use workspace_model::Caller;

impl MacOutbox<'_> {
    pub async fn record_sender_delivery(
        &mut self,
        caller: &Caller,
        notice: &StoredSenderOutcome,
        now_ms: i64,
    ) -> Result<bool, OutboxStoreError> {
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let Some(row) = read_owned_outbox(&mut transaction, caller, &notice.request_id).await?
        else {
            return Ok(false);
        };
        let may_have_left = matches!(
            row.state,
            OutboxItemState::Sent
                | OutboxItemState::Resent
                | OutboxItemState::SentAnswerLost
                | OutboxItemState::Unconfirmed
        );
        let accepted = confirmed_request(&row.status, may_have_left, notice)?;
        let mut profile = match row.payload {
            StoredPayload::Answered(profile) => profile,
            StoredPayload::Unanswered(payload) => AnsweredWrite {
                request_id: notice.request_id.clone(),
                target: outbox_content(notice.request_id.as_str(), &payload)?.target,
                content_digest: payload_content_digest(&payload)?,
                delivery_outcome: None,
            },
        };
        if !matches!(profile.target, OutboxWriteTarget::Session { .. }) {
            return Err(mismatch(notice));
        }
        match profile.delivery_outcome.as_ref() {
            Some(previous) if previous == notice => return Ok(true),
            Some(previous) if previous.inbox_message_id == notice.inbox_message_id => {
                return Err(mismatch(notice));
            }
            Some(previous) => match notice.precedence_over(previous) {
                OutcomePrecedence::Keep => return Ok(false),
                OutcomePrecedence::Replace => {}
                OutcomePrecedence::ContradictoryFinal => {
                    return Err(OutboxStoreError::ContradictoryFinalOutcome {
                        request: notice.request_id.clone(),
                    });
                }
            },
            None => {}
        }
        profile.delivery_outcome = Some(notice.clone());
        let item_state = OutboxItemState::Answered.as_str();
        let payload_json = serde_json::to_string(&profile)?;
        let outcome_json = serde_json::to_string(&accepted)?;
        sqlx::query!(
        "UPDATE outbox_items SET item_state = ?, payload_json = ?, outcome_json = ?, updated_at = ?
         WHERE request_id = ?",
        item_state,
        payload_json,
        outcome_json,
        now_ms,
        notice.request_id.as_str(),
    )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// A retained sender outcome, scoped to the original caller and request.
    pub async fn sender_outcome(
        &mut self,
        caller: &Caller,
        request: &RequestId,
    ) -> Result<Option<StoredSenderOutcome>, OutboxStoreError> {
        let Some(row) = read_owned_outbox(&mut self.store.connection, caller, request).await?
        else {
            return Ok(None);
        };
        match row.payload {
            StoredPayload::Answered(profile) => Ok(profile.delivery_outcome),
            StoredPayload::Unanswered(_) => Ok(None),
        }
    }
}

fn confirmed_request(
    status: &OutboxRequestStatus,
    may_have_left: bool,
    notice: &StoredSenderOutcome,
) -> Result<OutboxAccepted, OutboxStoreError> {
    match status {
        OutboxRequestStatus::Answered(reply @ OutboxAccepted::SendMessage { message_id })
            if *message_id == notice.message_id =>
        {
            Ok(reply.clone())
        }
        OutboxRequestStatus::Sent
        | OutboxRequestStatus::InOutbox { .. }
        | OutboxRequestStatus::Ended {
            reason: OutboxCancellation::NotConfirmedWithin15Minutes,
        } if may_have_left => Ok(OutboxAccepted::SendMessage {
            message_id: notice.message_id.clone(),
        }),
        _ => Err(mismatch(notice)),
    }
}

fn mismatch(notice: &StoredSenderOutcome) -> OutboxStoreError {
    OutboxStoreError::DeliveryOutcomeMismatch {
        request: notice.request_id.clone(),
    }
}
