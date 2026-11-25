//! Pure registration and authority policy for one Router run.

mod run_gate;

pub use run_gate::{
    AuthorityEnd, CallAttempt, CallKey, DialAttempt, LinkAdmission, LinkAttempt, MatchedRunRefusal,
    RunAuthority, RunGate, RunGateEffect, RunGateEvent, RunGateRefusal, RunGateState,
};
