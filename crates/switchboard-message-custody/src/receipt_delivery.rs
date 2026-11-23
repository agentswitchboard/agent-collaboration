//! Wire receipts for decisions the recipient inbox has committed.

use crate::custody_operator::{CustodyOperatorEvent, inbox_scan};
use central_link_client::{ConnectionAttempt, LinkLaneCommand, LinkLaneDisposition, LinkLaneReply};
use central_link_protocol::{
    CorrelationId, FreshCorrelationId, MAX_RECEIPTS, ProtocolError, Receipt, ReceiptBatch,
    ReceiptKind, ReceiptResult,
};
use std::collections::BTreeSet;
use switchboard_machine_core_store::{
    InboxDecision, InboxReceipt, InboxRefusalReason, InboxStore, InboxStoreError,
};
use workspace_ids::MessageId;
use workspace_model::{Timestamp, central_mail::MessageRefusal};

/// A frame the receipt pump prepares for one ready connection from committed decisions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptFrame {
    connection: ConnectionAttempt,
    correlation_id: CorrelationId,
    receipts: ReceiptBatch,
}

impl ReceiptFrame {
    #[must_use]
    pub fn command(&self) -> LinkLaneCommand {
        LinkLaneCommand::SubmitReceipts {
            connection: self.connection.clone(),
            correlation_id: self.correlation_id.clone(),
            receipts: self.receipts.clone(),
        }
    }
}

/// What custody learned from trying to confirm one receipt frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReceiptFrameOutcome {
    Confirmed { results: Vec<ReceiptResult> },
    RetryPending,
    StaleCompletion,
}

/// A local store or encoding failure reported by the receipt pump.
#[derive(Debug, thiserror::Error)]
pub enum ReceiptDeliveryError {
    #[error("unable to read or confirm stored inbox receipts")]
    Store(#[from] InboxStoreError),
    #[error("invalid receipt batch")]
    InvalidBatch(#[from] ProtocolError),
    #[error("invalid arrival time on the stored receipt for {0}")]
    InvalidArrivalTime(MessageId),
}

#[derive(Default)]
enum ReceiptFlight {
    #[default]
    Idle,
    AwaitingAnswer(ReceiptFrame),
    WaitingForRetry(ReceiptFrame),
}

/// The custody task owns one pump. It retries a pending frame on the same connection and reads durable decisions on reconnect.
#[derive(Default)]
pub struct ReceiptDelivery {
    flight: ReceiptFlight,
}

impl ReceiptDelivery {
    /// Prepare at most one frame. The caller rechecks readiness when handing it to the driver.
    pub async fn prepare_frame(
        &mut self,
        store: &mut InboxStore,
        operator_events: &mut Vec<CustodyOperatorEvent>,
        connection: &ConnectionAttempt,
        now_ms: i64,
    ) -> Result<Option<ReceiptFrame>, ReceiptDeliveryError> {
        if matches!(&self.flight, ReceiptFlight::AwaitingAnswer(frame) if &frame.connection == connection)
        {
            return Ok(None);
        }
        let scanned = store.pending_receipts().await?;
        let retry = match &self.flight {
            ReceiptFlight::WaitingForRetry(frame)
                if &frame.connection == connection
                    && !scanned.corrupt_rows.iter().any(|corrupt| {
                        frame
                            .receipts
                            .receipts()
                            .iter()
                            .any(|receipt| receipt.message_id.as_str() == corrupt.key)
                    }) =>
            {
                let frame = frame.clone();
                Some(frame)
            }
            ReceiptFlight::Idle
            | ReceiptFlight::AwaitingAnswer(_)
            | ReceiptFlight::WaitingForRetry(_) => None,
        };
        let pending = inbox_scan(scanned, operator_events);
        if let Some(frame) = retry {
            self.flight = ReceiptFlight::AwaitingAnswer(frame.clone());
            return Ok(Some(frame));
        }
        if pending.is_empty() {
            self.flight = ReceiptFlight::Idle;
            return Ok(None);
        }
        let receipts = pending
            .into_iter()
            .take(MAX_RECEIPTS)
            .map(receipt_for_decision)
            .collect::<Result<Vec<_>, _>>()?;
        let frame = ReceiptFrame {
            connection: connection.clone(),
            correlation_id: FreshCorrelationId::mint_at(now_ms).wire_id().clone(),
            receipts: ReceiptBatch::new(receipts)?,
        };
        self.flight = ReceiptFlight::AwaitingAnswer(frame.clone());
        Ok(Some(frame))
    }

    /// Retire durable receipts only after an exact answer on the still-current connection.
    pub async fn settle_frame(
        &mut self,
        store: &mut InboxStore,
        operator_events: &mut Vec<CustodyOperatorEvent>,
        submitted: ReceiptFrame,
        current_connection: Option<&ConnectionAttempt>,
        disposition: LinkLaneDisposition,
        now_ms: i64,
    ) -> Result<ReceiptFrameOutcome, ReceiptDeliveryError> {
        let ReceiptFlight::AwaitingAnswer(active) = &self.flight else {
            return Ok(ReceiptFrameOutcome::StaleCompletion);
        };
        if active != &submitted {
            return Ok(ReceiptFrameOutcome::StaleCompletion);
        }
        // Retain the same frame on a malformed answer or a store failure. A new
        // connection will prepare a new frame using the pending rows instead.
        self.flight = ReceiptFlight::WaitingForRetry(submitted.clone());
        if current_connection != Some(&submitted.connection) {
            return Ok(ReceiptFrameOutcome::StaleCompletion);
        }
        let LinkLaneDisposition::Answered {
            reply:
                LinkLaneReply::ReceiptResults {
                    correlation_id,
                    results,
                },
        } = disposition
        else {
            return Ok(ReceiptFrameOutcome::RetryPending);
        };
        if correlation_id != submitted.correlation_id
            || results.len() != submitted.receipts.receipts().len()
        {
            return Ok(ReceiptFrameOutcome::RetryPending);
        }

        let scanned = store.pending_receipts().await?;
        let pending: BTreeSet<_> = inbox_scan(scanned, operator_events)
            .into_iter()
            .map(|receipt| receipt.message_id)
            .collect();
        for receipt in submitted.receipts.receipts() {
            // A retry after a partially committed local confirmation can
            // receive the same checked answer. Confirm only the pending rows.
            if pending.contains(&receipt.message_id) {
                store
                    .mark_receipt_confirmed(&receipt.message_id, now_ms)
                    .await?;
            }
        }
        self.flight = ReceiptFlight::Idle;
        Ok(ReceiptFrameOutcome::Confirmed { results })
    }
}

fn receipt_for_decision(stored: InboxReceipt) -> Result<Receipt, ReceiptDeliveryError> {
    let kind = match stored.decision {
        InboxDecision::Accepted => {
            let arrived_at =
                Timestamp::from_timestamp_millis(stored.received_at_ms).ok_or_else(|| {
                    ReceiptDeliveryError::InvalidArrivalTime(stored.message_id.clone())
                })?;
            ReceiptKind::Delivered {
                arrived_at,
                waiting: None,
            }
        }
        InboxDecision::Refused { reason } => {
            let reason = match reason {
                InboxRefusalReason::UnknownSession => MessageRefusal::UnknownSession,
                InboxRefusalReason::AmbiguousSession => MessageRefusal::AmbiguousSession,
            };
            ReceiptKind::Refused { reason }
        }
    };
    Ok(Receipt {
        message_id: stored.message_id,
        kind,
    })
}
