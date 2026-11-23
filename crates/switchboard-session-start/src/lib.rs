//! The pure reducer and persistence contracts for one Mac session start.

#![deny(clippy::wildcard_enum_match_arm)]

mod envelope;
mod facts;
mod machine;
mod outcome;
mod ports;
mod protocol;
mod reason;
mod reply;
mod snapshot;
mod transition_table;

pub use envelope::{
    CandidateAsker, EnvelopeRefusal, LaunchSpecRef, LocalStartDigest, StartAsker, StartCandidate,
    StartEnvelope, StartScope,
};
pub use facts::{
    ConversationFact, EndLeg, EndNotDue, EndNotSent, Exposure, LaunchFact, PointerFact, Recording,
    Reservation, SecretFact, SecretIssue, StartFacts, StoredFacts,
};
pub use machine::{StartMachine, StartStateKind};
pub use outcome::{
    CentralStanding, FaultContext, FinishedOutcome, NotSentNowReason, StartAnswer, StartFault,
};
pub use ports::{
    OpeningCommit, OpeningNotCommitted, OpeningOutcome, SecretPutRefusal, SecretReadError,
    SessionSecretStore, StartRecord, StartRecordError, StartRevision, StartSaveRefusal,
};
pub use protocol::{
    CloseResult, Directed, DirectiveId, DirectiveResult, LaunchResult, PutSecretNotTaken,
    PutSecretResult, SaveResult, SetUpNotTaken, SetUpResult, StartDirective, StartEffect,
    StartInput, StartInstanceId, StartNotification, StartRefusal, WaiterSlot,
};
pub use reason::{
    BindDispute, CleanupCall, CorruptStartRecord, FailureReason, HoldLostDuring, IntendedOutcome,
    LaunchNotTaken, StopReason, UnknownReason,
};
pub use reply::{
    BindContradiction, BindNotTaken, BindReplyKind, CreateAnswer, CreateContradiction,
    CreateReplyKind, EndContradiction, EndReplyKind, LinkContradiction, LinkNotTaken, LinkResult,
    ObservedReservation,
};
pub use snapshot::{
    CleanupOutcome, OpenedSession, SnapshotFacts, SnapshotKind, SnapshotPhase, StartSnapshot,
    StoredSnapshot,
};
