//! The pure reducer and persistence contracts for one Mac session start.

#![deny(clippy::wildcard_enum_match_arm)]

mod envelope;
mod facts;
mod machine;
mod outcome;
mod ports;
mod protocol;
mod reason;
mod snapshot;
mod transition_table;

pub use envelope::{
    CandidateAsker, EnvelopeRefusal, LaunchSpecRef, LocalStartDigest, StartAsker, StartCandidate,
    StartEnvelope, StartScope,
};
pub use facts::{
    ConversationFact, EndLeg, EndNotDue, EndNotSent, Exposure, LaunchFact, PointerFact, Recording,
    Reservation, SecretFact, SecretIssue, StartFacts,
};
pub use machine::{StartMachine, StartStateKind};
pub use outcome::{
    CentralStanding, CreateReplyKind, FaultContext, FinishedOutcome, NotSentNowReason, StartAnswer,
    StartFault,
};
pub use ports::{
    OpeningCommit, OpeningNotCommitted, OpeningOutcome, SecretPutRefusal, SecretReadError,
    SessionSecretStore, StartRecord, StartRecordError, StartRevision, StartSaveRefusal,
};
pub use protocol::{
    BindNotTaken, CloseResult, Directed, DirectiveId, DirectiveResult, EnrollmentEnded,
    LaunchNotTaken, LaunchResult, LinkNotTaken, LinkResult, PutSecretResult, SaveResult,
    SetUpNotTaken, SetUpResult, StartDirective, StartEffect, StartInput, StartInstanceId,
    StartNotification, StartRefusal, WaiterSlot,
};
pub use reason::{
    BindDispute, CleanupCall, FailureReason, HoldLostDuring, StopReason, UnknownReason,
};
pub use snapshot::{CleanupOutcome, OpenedSession, SnapshotFacts, SnapshotKind, StartSnapshot};
