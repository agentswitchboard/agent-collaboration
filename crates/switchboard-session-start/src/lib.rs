//! The pure reducer and persistence contracts for one Mac session start.

mod envelope;
mod facts;
mod machine;
mod outcome;
mod ports;
mod protocol;
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
    BindDispute, CleanupCall, CreateReplyKind, FailureReason, FaultContext, FinishedOutcome,
    HoldLostDuring, NotSentNowReason, StartAnswer, StartFault, StopReason, UnknownReason,
};
pub use ports::{
    OpeningCommit, OpeningNotCommitted, OpeningOutcome, SecretPutRefusal, SecretReadError,
    SessionSecretStore, StartRecord, StartRecordError, StartRevision, StartSaveRefusal,
};
pub use protocol::{
    BindNotTaken, CentralStanding, CloseResult, Directed, DirectiveId, DirectiveResult,
    EnrollmentEnded, LaunchNotTaken, LaunchResult, LinkNotTaken, LinkResult, PutSecretResult,
    SaveResult, SetUpNotTaken, SetUpResult, StartDirective, StartEffect, StartInput,
    StartInstanceId, StartNotification, StartRefusal, WaiterSlot,
};
pub use snapshot::{CleanupOutcome, OpenedSession, SnapshotFacts, SnapshotKind, StartSnapshot};
