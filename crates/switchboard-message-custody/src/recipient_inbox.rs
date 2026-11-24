//! The Mac's admission of messages that central offered to a local session.

use central_link_protocol::{DeliveryMessage, DirectOfferBody, DirectOfferMessage, TimeLeft};
use router_clock::RuntimeClock;
use std::{collections::BTreeMap, future::Future, sync::Arc};
use switchboard_machine_core_store::{
    InboxDecision, InboxOfferExpiry, InboxRefusalReason, InboxStore, RecordDelivery,
};
use tokio::sync::{Mutex, MutexGuard};
use workspace_ids::{MachineId, MessageId, SessionRef};
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageRecord},
};

use crate::{
    custody_operator::{CustodyOperatorEvent, inbox_scan},
    receipt_delivery::{ReceiptDelivery, ReceiptDeliveryError, ReceiptFrame, ReceiptFrameOutcome},
    recipient_sessions::{
        RECIPIENT_LOOKUP_BUDGET, RecipientSessionAnswer, RecipientSessionDirectory,
    },
};

pub use crate::inbox_error::InboxCustodyError;

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

enum PreparedKeep<'inbox> {
    AlreadyDecided(InboxOfferResult),
    Write {
        inbox: MutexGuard<'inbox, RecipientInbox>,
        answer: RecipientSessionAnswer,
    },
}

impl OfferLookup {
    async fn resolve(self) -> RecipientSessionAnswer {
        let deadline = self.clock.monotonic_now().after(RECIPIENT_LOOKUP_BUDGET);
        self.directory
            .query_until(&self.session.session_id, self.clock.as_ref(), deadline)
            .await
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
        arrived_at: Timestamp,
    ) -> Result<InboxOfferResult, InboxCustodyError> {
        let offer = self.start_offer(incoming, arrived_at)?;
        self.keep_or_retain(offer).await
    }

    pub(crate) fn start_offer(
        &mut self,
        incoming: central_link_client::ValidatedDeliveryOffer,
        arrived_at: Timestamp,
    ) -> Result<InboxOffer, InboxCustodyError> {
        let (message, time_left) = incoming.into_parts();
        let offer = InboxOffer::from_delivery(*message, time_left, arrived_at)?;
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
        stop: impl Future<Output = ()>,
    ) -> Result<InboxOfferResult, InboxCustodyError> {
        // A stopped lookup is left for Central to re-offer. After preparation
        // selects a durable write, keep it outside the cancellable wait.
        let prepared = tokio::select! {
            biased;
            () = stop => return Ok(InboxOfferResult::LookupDeferred),
            prepared = Self::prepare_shared_keep(inbox, &offer) => prepared,
        };
        let result = match prepared {
            Ok(PreparedKeep::AlreadyDecided(decided)) => Ok(decided),
            Ok(PreparedKeep::Write { mut inbox, answer }) => {
                inbox.write_offer(&offer, answer).await
            }
            Err(error) => Err(error),
        };
        inbox.lock().await.retain_on_failure(offer, &result);
        result
    }

    async fn prepare_shared_keep<'inbox>(
        inbox: &'inbox Mutex<Self>,
        offer: &InboxOffer,
    ) -> Result<PreparedKeep<'inbox>, InboxCustodyError> {
        let (first_decision, lookup) = {
            let mut inbox = inbox.lock().await;
            (
                inbox.prior_decision(offer.message_id()).await?,
                inbox.offer_lookup(offer),
            )
        };
        if let Some(decided) = first_decision {
            return Ok(PreparedKeep::AlreadyDecided(decided));
        }
        let answer = lookup.resolve().await;
        let mut inbox = inbox.lock().await;
        if let Some(decided) = inbox.prior_decision(offer.message_id()).await? {
            return Ok(PreparedKeep::AlreadyDecided(decided));
        }
        Ok(PreparedKeep::Write { inbox, answer })
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

    pub(crate) fn recipient_directory(&self) -> Arc<dyn RecipientSessionDirectory> {
        self.directory.clone()
    }

    pub(crate) async fn direct_handoffs(
        &mut self,
    ) -> Result<Vec<switchboard_machine_core_store::DirectHandoffRow>, InboxCustodyError> {
        let scan = self.store.scan_direct_handoffs().await?;
        Ok(inbox_scan(scan, &mut self.operator_events))
    }

    pub(crate) async fn waiting_notice_handoffs(
        &mut self,
    ) -> Result<Vec<switchboard_machine_core_store::DirectHandoffRow>, InboxCustodyError> {
        let scan = self.store.scan_waiting_notice_handoffs().await?;
        Ok(inbox_scan(scan, &mut self.operator_events))
    }

    pub(crate) async fn commit_direct_handoffs(
        &mut self,
        target: &SessionRef,
        changes: &[switchboard_machine_core_store::DirectHandoffChange],
    ) -> Result<switchboard_machine_core_store::HandoffCommit, InboxCustodyError> {
        Ok(self
            .store
            .compare_and_set_direct_handoffs(target, changes, self.clock.utc_now())
            .await?)
    }

    pub(crate) async fn read_inbox(
        &mut self,
        session: &crate::inbox_read::AdmittedLocalSession,
        limit: usize,
    ) -> Result<crate::inbox_read::RecipientInboxPage, crate::inbox_read::InboxReadError> {
        let target = session.address();
        let scan = self
            .store
            .direct_inbox_messages(target, limit)
            .await
            .map_err(InboxCustodyError::from)?;
        let messages = inbox_scan(scan, &mut self.operator_events);
        Ok(crate::inbox_read::RecipientInboxPage::new(
            target.clone(),
            messages,
        ))
    }

    pub(crate) async fn record_inbox_read(
        &mut self,
        session: &crate::inbox_read::AdmittedLocalSession,
        read: crate::inbox_read::RecipientInboxRead,
    ) -> Result<crate::inbox_read::InboxReadSettlement, crate::inbox_read::InboxReadError> {
        use crate::inbox_read::{InboxReadError, InboxReadSettlement};
        use switchboard_machine_core_store::{DirectHandoff, HandoffCommit};

        let target = session.address();
        if &read.target != target {
            return Err(InboxReadError::RecipientChanged);
        }
        let unique: std::collections::BTreeSet<_> = read.returned.iter().collect();
        if unique.len() != read.returned.len() {
            return Err(InboxReadError::MessageNotKept);
        }
        let mut rows = Vec::new();
        for message_id in &read.returned {
            let row = self
                .store
                .direct_handoff_for_recipient(target, message_id)
                .await
                .map_err(InboxCustodyError::from)?
                .ok_or(InboxReadError::MessageNotKept)?;
            if matches!(
                row.handoff,
                DirectHandoff::Waiting | DirectHandoff::NotKnown
            ) {
                rows.push(row);
            }
        }
        let (changes, _) = crate::handover::changes_for(
            &rows,
            crate::handover::HandoffEvent::ReadAcknowledged {
                completed_at: self.clock.utc_now(),
            },
        )?;
        if self.commit_direct_handoffs(target, &changes).await? != HandoffCommit::Committed {
            return Err(InboxReadError::ChangedSinceRead);
        }
        Ok(InboxReadSettlement {
            changed: changes.len(),
            unchanged: read.returned.len() - changes.len(),
        })
    }

    pub(crate) async fn pending_sender_notices(
        &mut self,
    ) -> Result<Vec<MessageRecord>, InboxCustodyError> {
        let scan = self.store.pending_handoffs().await?;
        Ok(inbox_scan(scan, &mut self.operator_events)
            .into_iter()
            .filter(|message| matches!(message.body, MessageBody::DeliveryOutcome { .. }))
            .collect())
    }

    pub(crate) async fn finish_sender_notice(
        &mut self,
        notice: &MessageId,
    ) -> Result<(), InboxCustodyError> {
        self.store
            .record_handoff(notice, self.clock.now().unix_millis())
            .await?;
        Ok(())
    }

    pub(crate) async fn next_retention_deadline(
        &mut self,
    ) -> Result<Option<i64>, InboxCustodyError> {
        let scan = self.store.next_prune_at_ms().await?;
        Ok(inbox_scan(scan, &mut self.operator_events))
    }

    pub(crate) async fn prune_past_retention(&mut self) -> Result<(), InboxCustodyError> {
        let now_ms = self.clock.now().unix_millis();
        let scan = self.store.prune_past_retention(now_ms).await?;
        inbox_scan(scan, &mut self.operator_events);
        Ok(())
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
        if let Some(previous) = self.prior_decision(offer.message_id()).await? {
            return Ok(previous);
        }
        self.write_offer(offer, answer).await
    }

    async fn write_offer(
        &mut self,
        offer: &InboxOffer,
        answer: RecipientSessionAnswer,
    ) -> Result<InboxOfferResult, InboxCustodyError> {
        let message_id = offer.message_id();
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
        let (message, time_left) = offer.record_to_keep();
        let arrival_ms = offer.arrived_at().timestamp_millis();
        let expiry = match time_left {
            Some(duration) => InboxOfferExpiry::DirectWithExpiry(
                arrival_ms
                    .checked_add(i64::from(u32::from(duration)))
                    .and_then(Timestamp::from_timestamp_millis)
                    .ok_or(InboxCustodyError::ExpiryOutOfRange)?,
            ),
            None => InboxOfferExpiry::SystemWithoutExpiry,
        };
        match self
            .store
            .record_delivery(&message, decision, arrival_ms, expiry)
            .await?
        {
            // The store commits the first decision and its pending receipt together.
            RecordDelivery::Inserted => Ok(InboxOfferResult::Recorded { decision }),
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
