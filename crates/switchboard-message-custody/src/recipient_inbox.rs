//! The Mac's admission of messages that central offered to a local session.

use central_link_protocol::{DeliveryMessage, DirectOfferBody, DirectOfferMessage, TimeLeft};
use router_clock::RuntimeClock;
use state_machine_core::StateMachine;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxDecision, InboxRefusalReason, InboxStore, InboxStoreError, RecordDelivery,
};
use tokio::sync::Mutex;
use workspace_ids::{MachineId, MessageId, SessionRef};
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageRecord},
};

use crate::{
    custody_operator::{CustodyOperatorEvent, inbox_scan},
    delivery_custody::{
        DeliveryCustodyEffect, DeliveryCustodyEvent, DeliveryCustodyMachine, DeliveryCustodyState,
    },
    receipt_delivery::{ReceiptDelivery, ReceiptDeliveryError, ReceiptFrame, ReceiptFrameOutcome},
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};

const RECIPIENT_LOOKUP_BUDGET: Duration = Duration::from_secs(30);

/// A custody offer assembled from a checked central Deliver frame and the local arrival time.
pub(crate) enum InboxOffer {
    Direct {
        message: Box<DirectOfferMessage>,
        time_left: TimeLeft,
        arrived_at: Timestamp,
    },
    System {
        message: Box<MessageRecord>,
        arrived_at: Timestamp,
    },
}

struct OfferLookup {
    directory: Arc<dyn RecipientSessionDirectory>,
    clock: Arc<dyn RuntimeClock>,
    session: SessionRef,
}

impl OfferLookup {
    async fn resolve(self) -> RecipientSessionAnswer {
        let deadline = self.clock.monotonic_now().after(RECIPIENT_LOOKUP_BUDGET);
        tokio::select! {
            biased;
            () = self.clock.sleep_until(deadline) => RecipientSessionAnswer::CouldNotTell,
            answer = self.directory.query_recipient_session(&self.session.session_id) => answer,
        }
    }
}

impl InboxOffer {
    /// Checks the message and duration pairing on the link boundary.
    fn from_delivery(
        delivery: DeliveryMessage,
        time_left: Option<TimeLeft>,
        arrived_at: Timestamp,
    ) -> Result<Self, InboxCustodyError> {
        match (delivery, time_left) {
            (DeliveryMessage::Direct(message), Some(time_left)) => Ok(Self::Direct {
                message: Box::new(message),
                time_left,
                arrived_at,
            }),
            (DeliveryMessage::System(message), None)
                if matches!(
                    message.body,
                    MessageBody::Notice { .. } | MessageBody::DeliveryOutcome { .. }
                ) =>
            {
                Ok(Self::System {
                    message: Box::new(message),
                    arrived_at,
                })
            }
            (DeliveryMessage::Direct(_), None) | (DeliveryMessage::System(_), _) => {
                Err(InboxCustodyError::InvalidOffer)
            }
        }
    }

    fn message_id(&self) -> &MessageId {
        match self {
            Self::Direct { message, .. } => &message.message_id,
            Self::System { message, .. } => &message.message_id,
        }
    }

    fn target_session(&self) -> &SessionRef {
        match self {
            Self::Direct { message, .. } => &message.target,
            Self::System { message, .. } => &message.target,
        }
    }

    fn arrived_at(&self) -> Timestamp {
        match self {
            Self::Direct { arrived_at, .. } | Self::System { arrived_at, .. } => *arrived_at,
        }
    }

    fn record_to_keep(&self) -> (MessageRecord, Option<TimeLeft>) {
        match self {
            Self::Direct {
                message,
                time_left,
                arrived_at,
            } => {
                let DirectOfferBody::Direct {
                    body,
                    delivery,
                    reply_to,
                } = &message.body;
                // The direct wire record has no central timestamps. Its local
                // inbox record starts at the injected arrival time.
                (
                    MessageRecord {
                        message_id: message.message_id.clone(),
                        sender: message.sender.clone(),
                        target: message.target.clone(),
                        body: MessageBody::Direct {
                            body: body.clone(),
                            delivery: *delivery,
                            reply_to: reply_to.clone(),
                        },
                        state: message.state.clone(),
                        source: message.source.clone(),
                        created_at: *arrived_at,
                        updated_at: *arrived_at,
                    },
                    Some(*time_left),
                )
            }
            Self::System { message, .. } => ((**message).clone(), None),
        }
    }
}

/// The local custody result reported after checking or recording the inbox's decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboxOfferResult {
    Recorded { decision: InboxDecision },
    AlreadyDecided { decision: InboxDecision },
    LookupDeferred,
    DamagedRowIgnored,
}

/// Admission failures reported by the recipient inbox to the custody task.
#[derive(Debug, thiserror::Error)]
pub enum InboxCustodyError {
    #[error("invalid central delivery offer")]
    InvalidOffer,
    #[error("central offered a delivery addressed to machine {0}")]
    WrongMachine(MachineId),
    #[error("inbox storage unavailable")]
    Store(#[from] InboxStoreError),
    #[error("the inbox could not read the earlier decision for message {0}")]
    MissingDecision(MessageId),
    #[error("the delivery policy did not queue a receipt after a committed decision")]
    MissingReceiptEffect,
}

/// Custody constructs this from the Part A inbox, the local identity and the Host's session lookup.
pub struct RecipientInbox {
    store: InboxStore,
    machine_id: MachineId,
    directory: Arc<dyn RecipientSessionDirectory>,
    clock: Arc<dyn RuntimeClock>,
    uncommitted: BTreeMap<MessageId, InboxOffer>,
    receipts: ReceiptDelivery,
    operator_events: Vec<CustodyOperatorEvent>,
}

impl RecipientInbox {
    #[must_use]
    pub fn new(
        store: InboxStore,
        machine_id: MachineId,
        directory: Arc<dyn RecipientSessionDirectory>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            store,
            machine_id,
            directory,
            clock,
            uncommitted: BTreeMap::new(),
            receipts: ReceiptDelivery::default(),
            operator_events: Vec::new(),
        }
    }

    /// Validate and keep an offer or its permanent refusal, retaining temporary failures for retry.
    pub async fn receive_offer(
        &mut self,
        incoming: central_link_client::ValidatedDeliveryOffer,
    ) -> Result<InboxOfferResult, InboxCustodyError> {
        let offer = self.start_offer(incoming)?;
        self.keep_or_retain(offer).await
    }

    pub(crate) fn start_offer(
        &mut self,
        incoming: central_link_client::ValidatedDeliveryOffer,
    ) -> Result<InboxOffer, InboxCustodyError> {
        let (message, time_left) = incoming.into_parts();
        let offer = InboxOffer::from_delivery(*message, time_left, self.clock.utc_now())?;
        let target = offer.target_session();
        if target.machine_id != self.machine_id {
            return Err(InboxCustodyError::WrongMachine(target.machine_id.clone()));
        }
        Ok(self.uncommitted.remove(offer.message_id()).unwrap_or(offer))
    }

    /// The custody task schedules another local attempt at the next minute mark while this is true.
    #[must_use]
    pub fn needs_local_retry(&self) -> bool {
        !self.uncommitted.is_empty()
    }

    /// Report damaged inbox rows to the operator, separately from session reads.
    pub fn take_operator_events(&mut self) -> Vec<CustodyOperatorEvent> {
        std::mem::take(&mut self.operator_events)
    }

    /// Retry each offered message once, using the first arrival and payload.
    pub async fn retry_uncommitted(&mut self) -> Vec<InboxCustodyError> {
        let mut failures = Vec::new();
        for offer in self.take_uncommitted().into_values() {
            if let Err(error) = self.keep_or_retain(offer).await {
                failures.push(error);
            }
        }
        failures
    }

    pub(crate) fn take_uncommitted(&mut self) -> BTreeMap<MessageId, InboxOffer> {
        std::mem::take(&mut self.uncommitted)
    }

    pub(crate) async fn keep_shared_offer(
        inbox: &Mutex<Self>,
        offer: InboxOffer,
    ) -> Result<InboxOfferResult, InboxCustodyError> {
        let (first_decision, lookup) = {
            let mut inbox = inbox.lock().await;
            (
                inbox.prior_decision(offer.message_id()).await,
                inbox.offer_lookup(&offer),
            )
        };
        let result = match first_decision {
            Ok(None) => {
                let answer = lookup.resolve().await;
                inbox.lock().await.commit_offer(&offer, answer).await
            }
            Ok(Some(decided)) => Ok(decided),
            Err(error) => Err(error),
        };
        inbox.lock().await.retain_on_failure(offer, &result);
        result
    }

    /// Read one session's retained messages in the order supplied by the inbox store.
    pub async fn waiting_for_session(
        &mut self,
        session: &SessionRef,
    ) -> Result<Vec<MessageRecord>, InboxCustodyError> {
        let scan = self.store.pending_handoffs().await?;
        let mut waiting = inbox_scan(scan, &mut self.operator_events);
        waiting.retain(|message| &message.target == session);
        Ok(waiting)
    }

    /// Prepare durable decisions for the connection the driver declares ready.
    pub async fn prepare_receipts(
        &mut self,
        connection: &central_link_client::ConnectionAttempt,
    ) -> Result<Option<ReceiptFrame>, ReceiptDeliveryError> {
        let now_ms = self.clock.now().unix_millis();
        self.receipts
            .prepare_frame(
                &mut self.store,
                &mut self.operator_events,
                connection,
                now_ms,
            )
            .await
    }

    /// Observe the driver's answer to a frame prepared by this inbox.
    pub async fn settle_receipts(
        &mut self,
        submitted: ReceiptFrame,
        current_connection: Option<&central_link_client::ConnectionAttempt>,
        disposition: central_link_client::LinkLaneDisposition<
            central_link_client::CorrelatedReceiptResults,
        >,
    ) -> Result<ReceiptFrameOutcome, ReceiptDeliveryError> {
        let now_ms = self.clock.now().unix_millis();
        self.receipts
            .settle_frame(
                &mut self.store,
                &mut self.operator_events,
                submitted,
                current_connection,
                disposition,
                now_ms,
            )
            .await
    }

    async fn keep_or_retain(
        &mut self,
        offer: InboxOffer,
    ) -> Result<InboxOfferResult, InboxCustodyError> {
        let result = match self.prior_decision(offer.message_id()).await {
            Ok(None) => {
                let answer = self.offer_lookup(&offer).resolve().await;
                self.commit_offer(&offer, answer).await
            }
            Ok(Some(decided)) => Ok(decided),
            Err(error) => Err(error),
        };
        self.retain_on_failure(offer, &result);
        result
    }

    fn retain_on_failure(
        &mut self,
        offer: InboxOffer,
        result: &Result<InboxOfferResult, InboxCustodyError>,
    ) {
        if matches!(
            result,
            Ok(InboxOfferResult::LookupDeferred) | Err(InboxCustodyError::Store(_))
        ) {
            self.uncommitted.insert(offer.message_id().clone(), offer);
        }
    }

    fn offer_lookup(&self, offer: &InboxOffer) -> OfferLookup {
        OfferLookup {
            directory: self.directory.clone(),
            clock: self.clock.clone(),
            session: offer.target_session().clone(),
        }
    }

    async fn commit_offer(
        &mut self,
        offer: &InboxOffer,
        answer: RecipientSessionAnswer,
    ) -> Result<InboxOfferResult, InboxCustodyError> {
        let message_id = offer.message_id();
        if let Some(previous) = self.prior_decision(message_id).await? {
            return Ok(previous);
        }
        let decision = match answer {
            RecipientSessionAnswer::KnownOnce => InboxDecision::Accepted,
            RecipientSessionAnswer::UnknownSession => InboxDecision::Refused {
                reason: InboxRefusalReason::UnknownSession,
            },
            RecipientSessionAnswer::AmbiguousSession => InboxDecision::Refused {
                reason: InboxRefusalReason::AmbiguousSession,
            },
            RecipientSessionAnswer::CouldNotTell => return Ok(InboxOfferResult::LookupDeferred),
        };
        // Part A's store has no expiry column. Keep the duration until this
        // boundary so Part C can supply the expiry when it owns session handoff.
        let (message, _time_left) = offer.record_to_keep();
        let arrival_ms = offer.arrived_at().timestamp_millis();
        match self
            .store
            .record_delivery(&message, decision, arrival_ms)
            .await?
        {
            RecordDelivery::Inserted => {
                let (_, effects) = DeliveryCustodyMachine::transition(
                    &DeliveryCustodyState::AwaitingDecision,
                    DeliveryCustodyEvent::DecisionCommitted(decision),
                )
                .map_err(|_| InboxCustodyError::MissingReceiptEffect)?;
                let [DeliveryCustodyEffect::ReceiptPending(decision)] = effects.as_slice() else {
                    return Err(InboxCustodyError::MissingReceiptEffect);
                };
                Ok(InboxOfferResult::Recorded {
                    decision: *decision,
                })
            }
            RecordDelivery::Duplicate => self
                .prior_decision(message_id)
                .await?
                .ok_or_else(|| InboxCustodyError::MissingDecision(message_id.clone())),
        }
    }

    async fn prior_decision(
        &mut self,
        message_id: &MessageId,
    ) -> Result<Option<InboxOfferResult>, InboxCustodyError> {
        let scanned = self.store.receipt_for(message_id).await?;
        let damaged = !scanned.corrupt_rows.is_empty();
        let stored = inbox_scan(scanned, &mut self.operator_events);
        Ok(if damaged {
            Some(InboxOfferResult::DamagedRowIgnored)
        } else {
            stored.map(|row| InboxOfferResult::AlreadyDecided {
                decision: row.decision,
            })
        })
    }
}
