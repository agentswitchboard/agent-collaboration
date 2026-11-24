//! The socket's event consumer. It keeps the delivery locally before reading the next event.

use central_link_client::{
    AdmittedConnection, ConnectionAttempt, LinkLaneEvent, LinkLocalFault, ReadyLinkLane,
};
use central_link_protocol::DeliveryMessage;
use std::{future::Future, sync::Arc};
use switchboard_machine_core_store::InboxDecision;
use tokio::sync::{Mutex, mpsc, watch};
use workspace_model::central_mail::MessageBody;

use crate::{
    custody_run_group::{CustodyRunState, wait_for_stop},
    recipient_inbox::{InboxCustodyError, InboxOfferResult, RecipientInbox},
};

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

/// The offer consumer sends an observation and a durable-work wake to the command task.
pub(crate) struct TakenEvent {
    pub(crate) observation: CustodyLinkObservation,
    pub(crate) sender_notice_saved: bool,
}

pub(crate) fn admitted_for(lane: &dyn ReadyLinkLane) -> Option<AdmittedConnection> {
    lane.watch_admitted().borrow().clone()
}

pub(crate) async fn take_event(
    inbox: &SharedRecipientInbox,
    event: Option<LinkLaneEvent>,
    stop: impl Future<Output = ()>,
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
            arrived_at,
            offer,
            sender: _,
        }) => {
            // The stamp attests admission when the socket delivered this offer.
            // The current watch is for new commands and can already name its replacement.
            let sender_notice = matches!(offer.message(),
                DeliveryMessage::System(record) if matches!(record.body, MessageBody::DeliveryOutcome { .. }));
            let first_offer = inbox.lock().await.start_offer(offer, arrived_at)?;
            let kept = RecipientInbox::keep_shared_offer(inbox, first_offer, stop).await?;
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
                sender_notice_saved: accepted && sender_notice,
            });
        }
    };
    Ok(TakenEvent {
        observation,
        sender_notice_saved: false,
    })
}

enum OfferInput {
    Event(Option<LinkLaneEvent>),
    Retry(Option<()>),
}

pub(crate) async fn run_offer_taker(
    lane: Arc<dyn ReadyLinkLane>,
    inbox: SharedRecipientInbox,
    mut stop: watch::Receiver<CustodyRunState>,
    mut retries: mpsc::Receiver<()>,
    observations: mpsc::UnboundedSender<Result<TakenEvent, InboxCustodyError>>,
) {
    let mut lane_open = true;
    loop {
        // Stop can interrupt input and lookup waits. The inbox owns the
        // boundary after which a durable keep runs to completion.
        let input = tokio::select! {
            biased;
            () = wait_for_stop(&mut stop) => return,
            input = async {
                tokio::select! {
                    event = lane.next_event(), if lane_open => OfferInput::Event(event),
                    retry = retries.recv() => OfferInput::Retry(retry),
                }
            } => input,
        };
        let taken = match input {
            OfferInput::Event(event) => {
                lane_open = event.is_some();
                take_event(&inbox, event, wait_for_stop(&mut stop)).await
            }
            OfferInput::Retry(None) => return,
            OfferInput::Retry(Some(())) => {
                let offered = inbox.lock().await.take_uncommitted();
                for offer in offered.into_values() {
                    if stop.borrow().stopped {
                        return;
                    }
                    if let Err(error) =
                        RecipientInbox::keep_shared_offer(&inbox, offer, wait_for_stop(&mut stop))
                            .await
                        && observations.send(Err(error)).is_err()
                    {
                        return;
                    }
                }
                Ok(TakenEvent {
                    observation: CustodyLinkObservation::LocalRetryCompleted,
                    sender_notice_saved: true,
                })
            }
        };
        if matches!(&taken, Ok(saved) if !saved.sender_notice_saved
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
