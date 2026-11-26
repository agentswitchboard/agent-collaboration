//! Shared runtime mechanics for harnesses and session controllers.

mod reply_carrier;

#[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
pub mod work_observation;

pub use reply_carrier::{
    CarrierReceiver, CarrierReply, CarrierSender, StampedMessage, reply_carrier,
};
