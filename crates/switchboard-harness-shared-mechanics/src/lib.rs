//! Shared runtime mechanics for harnesses and session controllers.

mod reply_carrier;

pub use reply_carrier::{
    CarrierReceiver, CarrierReply, CarrierSender, StampedMessage, reply_carrier,
};
