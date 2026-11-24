//! Wire receipts for decisions the recipient inbox has committed.

use crate::custody_operator::{CustodyOperatorEvent, inbox_scan};
use central_link_client::{
    ConnectionAttempt, CorrelatedReceiptResults, LinkLaneDisposition, SubmitReceiptsCommand,
};
use central_link_protocol::{
    CorrelationId, FreshCorrelationId, MAX_RECEIPTS, ProtocolError, Receipt, ReceiptBatch,
    ReceiptKind, ReceiptResult,
};
use router_clock::ClockInstant;
use std::collections::BTreeSet;
use switchboard_machine_core_store::{
    DirectHandoff, DirectHandoffChange, HandoffCommit, InboxDecision, InboxReceipt,
    InboxRefusalReason, InboxStore, InboxStoreError, SeenReceiptRejection, SeenReceiptState,
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
    pub fn command(&self, send_before: ClockInstant) -> SubmitReceiptsCommand {
        SubmitReceiptsCommand {
            connection: self.connection.clone(),
            send_before,
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
    #[error("invalid local receipt settlement time")]
    InvalidSettlementTime,
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
        let mut receipts = pending
            .into_iter()
            .take(MAX_RECEIPTS)
            .map(receipt_for_decision)
            .collect::<Result<Vec<_>, _>>()?;
        if receipts.len() < MAX_RECEIPTS && store.has_seen_receipt_owed().await? {
            let rows = inbox_scan(store.scan_direct_handoffs().await?, operator_events);
            let free = MAX_RECEIPTS - receipts.len();
            let initial_in_frame: BTreeSet<_> = receipts
                .iter()
                .map(|receipt| receipt.message_id.clone())
                .collect();
            receipts.extend(
                rows.into_iter()
                    .filter_map(|row| {
                        let DirectHandoff::Seen {
                            completed_at,
                            receipt: SeenReceiptState::Owed,
                        } = row.handoff
                        else {
                            return None;
                        };
                        (row.delivery_receipt_confirmed
                            || initial_in_frame.contains(&row.message_id))
                        .then_some(Receipt {
                            message_id: row.message_id,
                            kind: ReceiptKind::Seen { completed_at },
                        })
                    })
                    .take(free),
            );
        }
        if receipts.is_empty() {
            self.flight = ReceiptFlight::Idle;
            return Ok(None);
        }
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
        disposition: LinkLaneDisposition<CorrelatedReceiptResults>,
        now_ms: i64,
    ) -> Result<ReceiptFrameOutcome, ReceiptDeliveryError> {
        let ReceiptFlight::AwaitingAnswer(active) = &self.flight else {
            return Ok(ReceiptFrameOutcome::StaleCompletion);
        };
        if active != &submitted {
            return Ok(ReceiptFrameOutcome::StaleCompletion);
        }
        // Keep the same decisions. The socket may have spent this correlation
        // as soon as the frame started, even if confirmation fails locally.
        let mut retry = submitted.clone();
        if !matches!(&disposition, LinkLaneDisposition::NotReached { .. }) {
            retry.correlation_id = FreshCorrelationId::mint_at(now_ms).wire_id().clone();
        }
        self.flight = ReceiptFlight::WaitingForRetry(retry);
        if current_connection != Some(&submitted.connection) {
            return Ok(ReceiptFrameOutcome::StaleCompletion);
        }
        let LinkLaneDisposition::Answered {
            reply:
                CorrelatedReceiptResults {
                    correlation_id,
                    results,
                },
            ..
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
        let mut seen_changes = vec![];
        for (receipt, result) in submitted
            .receipts
            .receipts()
            .iter()
            .zip(results.iter().copied())
        {
            // A retry after a partially committed local confirmation can
            // receive the same checked answer. Confirm only the pending rows.
            match receipt.kind {
                ReceiptKind::Delivered { .. } | ReceiptKind::Refused { .. } => {
                    if pending.contains(&receipt.message_id) {
                        store
                            .mark_receipt_confirmed(&receipt.message_id, now_ms)
                            .await?;
                    }
                }
                ReceiptKind::Seen { completed_at } => {
                    seen_changes.push((receipt.message_id.clone(), completed_at, result));
                }
            }
        }
        if !seen_changes.is_empty() {
            let now = Timestamp::from_timestamp_millis(now_ms)
                .ok_or(ReceiptDeliveryError::InvalidSettlementTime)?;
            let rows = inbox_scan(store.scan_direct_handoffs().await?, operator_events);
            for (message_id, completed_at, result) in seen_changes {
                let Some(row) = rows.iter().find(|row| row.message_id == message_id) else {
                    continue;
                };
                let expected = DirectHandoff::Seen {
                    completed_at,
                    receipt: SeenReceiptState::Owed,
                };
                if row.handoff != expected {
                    continue;
                }
                let next = DirectHandoff::Seen {
                    completed_at,
                    receipt: seen_confirmation(result),
                };
                if store
                    .compare_and_set_direct_handoffs(
                        &row.target,
                        &[DirectHandoffChange {
                            message_id,
                            expected,
                            next,
                        }],
                        now,
                    )
                    .await?
                    != HandoffCommit::Committed
                {
                    return Ok(ReceiptFrameOutcome::RetryPending);
                }
            }
        }
        self.flight = ReceiptFlight::Idle;
        Ok(ReceiptFrameOutcome::Confirmed { results })
    }
}

fn seen_confirmation(result: ReceiptResult) -> SeenReceiptState {
    match result {
        ReceiptResult::Recorded => SeenReceiptState::Recorded,
        ReceiptResult::AlreadySettled => SeenReceiptState::Rejected {
            reason: SeenReceiptRejection::AlreadySettled,
        },
        ReceiptResult::NoSuchMessage => SeenReceiptState::Rejected {
            reason: SeenReceiptRejection::NoSuchMessage,
        },
        ReceiptResult::NotApplicable => SeenReceiptState::Rejected {
            reason: SeenReceiptRejection::NotApplicable,
        },
    }
}

fn receipt_for_decision(stored: InboxReceipt) -> Result<Receipt, ReceiptDeliveryError> {
    let kind = match stored.decision {
        InboxDecision::Accepted => {
            let arrived_at =
                Timestamp::from_timestamp_millis(stored.received_at_ms).ok_or_else(|| {
                    ReceiptDeliveryError::InvalidArrivalTime(stored.message_id.clone())
                })?;
            ReceiptKind::Delivered { arrived_at }
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
