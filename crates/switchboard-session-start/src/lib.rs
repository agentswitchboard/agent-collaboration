//! The pure reducer and persistence contracts for one Mac session start.

#![deny(clippy::wildcard_enum_match_arm)]

mod make_progress;
mod make_transitions;
mod start_cleanup;
mod start_events;
mod start_machine;
mod start_snapshot;
mod start_transition_table;

pub use make_progress::{
    DurableFirstInput, FirstInputId, FirstInputIntent, FirstInputObservation, LaunchProgress,
    MakePlan,
};
pub use start_events::{
    BindObservation, CentralEndObservation, CreateObservation, HarnessCloseObservation,
    HarnessLaunchObservation, StartEffect, StartEvent, StartReadiness, StartTransitionRefusal,
};
pub use start_machine::StartMachine;
pub use start_snapshot::{
    BindRefusal, CentralCloseProgress, CentralEndPurpose, CentralEndUnavailable, ClosingReason,
    ClosingSession, CreateRefusal, EndCallCertainty, EndOutcome, EndedCentralSession,
    HarnessCloseProgress, InvalidStartSnapshot, IssuedSessionProof, LaunchUncertainty,
    ObservedSettlement, RegistrationContradiction, RegistrationProgress, SealedSessionSecretRef,
    StartProgress, StartRefusal, StartScope, StartSnapshot,
};
