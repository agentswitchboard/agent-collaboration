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
            let kept = inbox.lock().await.receive_offer(offer).await?;
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
    observations: mpsc::UnboundedSender<Result<TakenEvent, InboxCustodyError>>,
) {
    loop {
        let event = tokio::select! {
            biased;
            () = async { let _ = stop.wait_for(|stop| *stop).await; } => return,
            event = lane.next_event() => event,
        };
        let stream_closed = event.is_none();
        let taken = take_event(&inbox, event).await;
        if matches!(&taken, Ok(saved) if saved.sender_notice.is_none()
            && matches!(saved.observation, CustodyLinkObservation::Delivery { .. }))
        {
            continue;
        }
        if observations.send(taken).is_err() || stream_closed {
            return;
        }
    }
}
