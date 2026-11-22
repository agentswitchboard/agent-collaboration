//! One bounded FIFO for exact replies and retirement-scoped observations.

use std::{num::NonZeroUsize, sync::Arc};
use switchboard_harness_contracts::{
    EventPort, EventRefusal, EventSendFuture, ReplyAdmission, ReservedReply, WriterState,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

/// The carrier attaches the receiver's exact stamp and any credit held for the message.
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

/// The carrier creates this producer handle for its ordered ingress and stream budget.
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

/// The carrier creates this handle for the owner to receive in enqueue order.
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
    /// Reserves one inline permit for a message addressed to this receiver.
    pub fn reserve_inline(
        &self,
        address: Address,
    ) -> Result<CarrierReply<Address, Message>, WriterState> {
        self.sender
            .clone()
            .try_reserve_owned()
            .map(|permit| CarrierReply { address, permit })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => WriterState::Full,
                mpsc::error::TrySendError::Closed(_) => WriterState::Stopped,
            })
    }

    /// Reserves and binds a typed reply mapping for an object-safe producer.
    pub fn reserve<Reply>(
        &self,
        address: Address,
        map_reply: impl FnOnce(Reply) -> Message + Send + 'static,
    ) -> ReplyAdmission<Reply> {
        match self.reserve_inline(address) {
            Ok(receipt) => ReplyAdmission::Reserved(Box::new(MappedReply { receipt, map_reply })),
            Err(reason) => ReplyAdmission::Refused(reason),
        }
    }

    /// Enqueues an observation with borrowed retirement and no trait erasure.
    pub async fn send_stream<Event>(
        &self,
        address: Address,
        event: Event,
        retired: &CancellationToken,
        map_event: impl FnOnce(Event) -> Message,
    ) -> Result<(), EventRefusal> {
        let send = async {
            let credit = Arc::clone(&self.stream_credits)
                .acquire_owned()
                .await
                .map_err(|_| EventRefusal::Stopped)?;
            if self.sender.is_closed() {
                return Err(EventRefusal::Stopped);
            }
            self.sender
                .send(StampedMessage {
                    address,
                    message: map_event(event),
                    stream_credit: Some(credit),
                })
                .await
                .map_err(|_| EventRefusal::Stopped)
        };
        tokio::select! {
            biased;
            () = retired.cancelled() => Err(EventRefusal::Retired),
            result = send => result,
            // Only a pending send needs a receiver-closure waiter.
            () = self.sender.closed() => Err(EventRefusal::Stopped),
        }
    }

    /// Creates an owned event port once for one address and retirement scope.
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

/// The carrier issues this inline receipt for one addressed message.
pub struct CarrierReply<Address, Message> {
    address: Address,
    permit: mpsc::OwnedPermit<StampedMessage<Address, Message>>,
}

impl<Address, Message> CarrierReply<Address, Message> {
    /// Consumes the permit and publishes the message with its original address.
    pub fn send(self, message: Message) {
        self.permit.send(StampedMessage {
            address: self.address,
            message,
            stream_credit: None,
        });
    }
}

impl<Address: Send, Message: Send> ReservedReply<Message> for CarrierReply<Address, Message> {
    fn send(self: Box<Self>, message: Message) {
        (*self).send(message);
    }
}

struct MappedReply<Address, Message, MapReply> {
    receipt: CarrierReply<Address, Message>,
    map_reply: MapReply,
}

impl<Address: Send, Message: Send, Reply, MapReply: FnOnce(Reply) -> Message + Send>
    ReservedReply<Reply> for MappedReply<Address, Message, MapReply>
{
    fn send(self: Box<Self>, reply: Reply) {
        self.receipt.send((self.map_reply)(reply));
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
    Address: Clone + Send + Sync + 'static,
    Message: Send + 'static,
    MapEvent: Fn(Event) -> Message + Send + Sync,
    Event: Send,
{
    fn send<'a>(&'a self, event: Event) -> EventSendFuture<'a>
    where
        Event: 'a,
    {
        Box::pin(self.carrier.send_stream(
            self.address.clone(),
            event,
            &self.retired,
            &self.map_event,
        ))
    }
}

#[cfg(test)]
#[path = "reply_carrier_test.rs"]
mod tests;
