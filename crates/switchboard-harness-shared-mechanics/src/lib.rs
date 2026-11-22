//! Shared runtime mechanics for harnesses and session controllers.

mod reply_carrier;

pub use reply_carrier::{CarrierReceiver, CarrierSender, StampedMessage, reply_carrier};
