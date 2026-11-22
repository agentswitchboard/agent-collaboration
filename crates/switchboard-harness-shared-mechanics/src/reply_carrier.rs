//! One bounded FIFO for exact replies and retirement-scoped observations.

use std::{num::NonZeroUsize, sync::Arc};
use switchboard_harness_contracts::{
    EventPort, EventRefusal, EventSendFuture, ReplyAdmission, ReservedReply, WriterState,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

/// The receiver's exact stamp, message, and any credit held for the work.
pub struct StampedMessage<Address, Message> {
    address: Address,
    message: Message,
    stream_credit: Option<OwnedSemaphorePermit>,
}

impl<Address, Message> StampedMessage<Address, Message> {
    /// Retains stream credit through synchronous processing, including error returns.
    pub fn process<Output>(self, process: impl FnOnce(Address, Message) -> Output) -> Output {
        let Self {
            address,
            message,
            stream_credit,
        } = self;
        let output = process(address, message);
        drop(stream_credit);
        output
    }
}

/// Producer access to one ordered ingress and its shared stream budget.
pub struct CarrierSender<Address, Message> {
    sender: mpsc::Sender<StampedMessage<Address, Message>>,
    stream_credits: Arc<Semaphore>,
}

impl<Address, Message> Clone for CarrierSender<Address, Message> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            stream_credits: Arc::clone(&self.stream_credits),
        }
    }
}

/// The owner receives replies and observations in their enqueue order.
pub struct CarrierReceiver<Address, Message> {
    receiver: mpsc::Receiver<StampedMessage<Address, Message>>,
}

impl<Address, Message> CarrierReceiver<Address, Message> {
    pub async fn recv(&mut self) -> Option<StampedMessage<Address, Message>> {
        self.receiver.recv().await
    }
}

/// The stream budget leaves room for reply reservations when capacity exceeds one.
pub fn reply_carrier<Address, Message>(
    capacity: NonZeroUsize,
) -> (
    CarrierSender<Address, Message>,
    CarrierReceiver<Address, Message>,
) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    let stream_credits = Arc::new(Semaphore::new((capacity.get() / 2).max(1)));
    (
        CarrierSender {
            sender,
            stream_credits,
        },
        CarrierReceiver { receiver },
    )
}

impl<Address: Send + 'static, Message: Send + 'static> CarrierSender<Address, Message> {
    /// Binds the reserved queue slot to the original address and reply mapping.
    pub fn reserve<Reply>(
        &self,
        address: Address,
        map_reply: impl FnOnce(Reply) -> Message + Send + 'static,
    ) -> ReplyAdmission<Reply> {
        match self.sender.clone().try_reserve_owned() {
            Ok(permit) => ReplyAdmission::Reserved(Box::new(CarrierReply {
                address,
                permit,
                map_reply,
            })),
            Err(mpsc::error::TrySendError::Full(_)) => ReplyAdmission::Refused(WriterState::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                ReplyAdmission::Refused(WriterState::Stopped)
            }
        }
    }

    /// Binds a typed stream to its address and the caller's existing retirement handle.
    pub fn event_port<Event, MapEvent>(
        &self,
        address: Address,
        retired: &CancellationToken,
        map_event: MapEvent,
    ) -> impl EventPort<Event> + use<Address, Message, Event, MapEvent>
    where
        Address: Clone + Sync,
        Event: Send,
        MapEvent: Fn(Event) -> Message + Send + Sync,
    {
        CarrierEventPort {
            carrier: self.clone(),
            address,
            retired: retired.clone(),
            map_event,
        }
    }
}

struct CarrierReply<Address, Message, MapReply> {
    address: Address,
    permit: mpsc::OwnedPermit<StampedMessage<Address, Message>>,
    map_reply: MapReply,
}

impl<Address, Message, MapReply, Reply> ReservedReply<Reply>
    for CarrierReply<Address, Message, MapReply>
where
    Address: Send,
    Message: Send,
    MapReply: FnOnce(Reply) -> Message + Send,
{
    fn send(self: Box<Self>, reply: Reply) {
        let Self {
            address,
            permit,
            map_reply,
        } = *self;
        permit.send(StampedMessage {
            address,
            message: map_reply(reply),
            stream_credit: None,
        });
    }
}

struct CarrierEventPort<Address, Message, MapEvent> {
    carrier: CarrierSender<Address, Message>,
    address: Address,
    retired: CancellationToken,
    map_event: MapEvent,
}

impl<Address, Message, MapEvent, Event> EventPort<Event>
    for CarrierEventPort<Address, Message, MapEvent>
where
    Address: Clone + Send + Sync,
    Message: Send,
    MapEvent: Fn(Event) -> Message + Send + Sync,
    Event: Send,
{
    fn send<'a>(&'a self, event: Event) -> EventSendFuture<'a>
    where
        Event: 'a,
    {
        Box::pin(async move {
            let send = async {
                let credit = Arc::clone(&self.carrier.stream_credits)
                    .acquire_owned()
                    .await
                    .map_err(|_| EventRefusal::Stopped)?;
                self.carrier
                    .sender
                    .send(StampedMessage {
                        address: self.address.clone(),
                        message: (self.map_event)(event),
                        stream_credit: Some(credit),
                    })
                    .await
                    .map_err(|_| EventRefusal::Stopped)
            };
            tokio::select! {
                biased;
                () = self.retired.cancelled() => Err(EventRefusal::Retired),
                result = send => result,
            }
        })
    }
}

#[cfg(test)]
#[path = "reply_carrier_test.rs"]
mod tests;
