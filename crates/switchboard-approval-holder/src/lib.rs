//! Pure state machines for the Mac's side of approval custody.
//!
//! One owner must hold the gate and all waits of a run and apply their steps in order.
//! The checks refuse stale inputs and candidates under the current gate,
//! relying on the executor to serialize access.

pub mod waiting_request;
