//! Declarations for admitted replies and backpressured observations.

use std::{future::Future, pin::Pin};

/// The writer reports why it could not reserve room.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterState {
    Full,
    Stopped,
}

/// The receiving runtime reserves one place for this exact typed reply.
///
/// Dropping the endpoint releases the reservation. Using it guarantees writer
/// room while the receiver lives, with no guarantee of observation after shutdown.
pub trait ReservedReply<Reply>: Send {
    fn send(self: Box<Self>, reply: Reply);
}

/// The carrier reports whether it set aside room for the requested reply.
pub enum ReplyAdmission<Reply> {
    Reserved(Box<dyn ReservedReply<Reply>>),
    Refused(WriterState),
}

/// The event port reports why it did not enqueue an observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventRefusal {
    Retired,
    Stopped,
}

/// An event producer sends observations under the scope bound by the receiver.
///
/// Success means the receiver's ingress accepted the event. Retirement can stop
/// an outstanding send, and an already enqueued observation remains ordered.
pub trait EventPort<Event>: Send + Sync {
    fn send<'a>(&'a self, event: Event) -> EventSendFuture<'a>
    where
        Event: 'a;
}

/// An event producer receives this standard future from a backpressured send.
pub type EventSendFuture<'a> = Pin<Box<dyn Future<Output = Result<(), EventRefusal>> + Send + 'a>>;
