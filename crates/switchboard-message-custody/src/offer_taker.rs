//! The socket's event consumer. It keeps the delivery locally before reading the next event.

use central_link_client::{
    AdmittedConnection, ConnectionAttempt, LinkLaneEvent, LinkLocalFault, ReadyLinkLane,
};
use central_link_protocol::DeliveryMessage;
use std::sync::Arc;
use switchboard_machine_core_store::InboxDecision;
use tokio::sync::{Mutex, mpsc, watch};
use workspace_ids::{MessageId, SessionRef};
use workspace_model::central_mail::MessageBody;

use crate::recipient_inbox::{InboxCustodyError, InboxOfferResult, RecipientInbox};

pub(crate) type SharedRecipientInbox = Arc<Mutex<RecipientInbox>>;

/// The observation custody makes after reading one event from the admitted lane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CustodyLinkObservation {
    AdmissionChanged,
    ConnectionFaulted {
        connection: ConnectionAttempt,
        cause: LinkLocalFault,
    },
    Delivery {
        connection: ConnectionAttempt,
        result: InboxOfferResult,
    },
    LocalRetryCompleted,
    DriverClosed,
}

pub(crate) struct StoredNoticeKey {
    pub(crate) target: SessionRef,
    pub(crate) message_id: MessageId,
}

pub(crate) struct TakenEvent {
    pub(crate) observation: CustodyLinkObservation,
    pub(crate) sender_notice: Option<StoredNoticeKey>,
}

pub(crate) fn admitted_for(lane: &dyn ReadyLinkLane) -> Option<AdmittedConnection> {
    lane.watch_admitted().borrow().clone()
}

pub(crate) async fn take_event(
    inbox: &SharedRecipientInbox,
    event: Option<LinkLaneEvent>,
) -> Result<TakenEvent, InboxCustodyError> {
    let observation = match event {
        Some(
            LinkLaneEvent::ConnectionAdmitted(_) | LinkLaneEvent::ConnectionNotAdmitted { .. },
        ) => CustodyLinkObservation::AdmissionChanged,
        Some(LinkLaneEvent::ConnectionFaulted { connection, cause }) => {
            CustodyLinkObservation::ConnectionFaulted { connection, cause }
        }
        None => CustodyLinkObservation::DriverClosed,
        Some(LinkLaneEvent::Deliver {
            connection,
            offer,
            sender: _,
        }) => {
            // The stamp attests admission when the socket delivered this offer.
            // The current watch is for new commands and can already name its replacement.
            let notice = match offer.message() {
                DeliveryMessage::System(record)
                    if matches!(record.body, MessageBody::DeliveryOutcome { .. }) =>
                {
                    Some(StoredNoticeKey {
                        target: record.target.clone(),
                        message_id: record.message_id.clone(),
                    })
                }
                DeliveryMessage::System(_) | DeliveryMessage::Direct(_) => None,
            };
            let first_offer = inbox.lock().await.start_offer(offer)?;
            let kept = RecipientInbox::keep_shared_offer(inbox, first_offer).await?;
            let accepted = matches!(
                kept,
                InboxOfferResult::Recorded {
                    decision: InboxDecision::Accepted
                } | InboxOfferResult::AlreadyDecided {
                    decision: InboxDecision::Accepted
                }
            );
            return Ok(TakenEvent {
                observation: CustodyLinkObservation::Delivery {
                    connection,
                    result: kept,
                },
                sender_notice: notice.filter(|_| accepted),
            });
        }
    };
    Ok(TakenEvent {
        observation,
        sender_notice: None,
    })
}

pub(crate) async fn run_offer_taker(
    lane: Arc<dyn ReadyLinkLane>,
    inbox: SharedRecipientInbox,
    mut stop: watch::Receiver<bool>,
    retries: mpsc::Receiver<()>,
    observations: mpsc::UnboundedSender<Result<TakenEvent, InboxCustodyError>>,
) {
    tokio::select! {
        biased;
        () = async { let _ = stop.wait_for(|stop| *stop).await; } => {}
        () = consume_offers(lane, inbox, retries, observations) => {}
    }
}

async fn consume_offers(
    lane: Arc<dyn ReadyLinkLane>,
    inbox: SharedRecipientInbox,
    mut retries: mpsc::Receiver<()>,
    observations: mpsc::UnboundedSender<Result<TakenEvent, InboxCustodyError>>,
) {
    let mut lane_open = true;
    loop {
        let taken = tokio::select! {
            event = lane.next_event(), if lane_open => {
                lane_open = event.is_some();
                take_event(&inbox, event).await
            }
            requested = retries.recv() => {
                if requested.is_none() {
                    return;
                }
                let offered = inbox.lock().await.take_uncommitted();
                for offer in offered.into_values() {
                    if let Err(error) = RecipientInbox::keep_shared_offer(&inbox, offer).await
                        && observations.send(Err(error)).is_err()
                    {
                        return;
                    }
                }
                Ok(TakenEvent {
                    observation: CustodyLinkObservation::LocalRetryCompleted,
                    sender_notice: None,
                })
            }
        };
        if matches!(&taken, Ok(saved) if saved.sender_notice.is_none()
            && matches!(saved.observation, CustodyLinkObservation::Delivery { result, .. }
                if result != InboxOfferResult::LookupDeferred))
        {
            continue;
        }
        if observations.send(taken).is_err() {
            return;
        }
    }
}
