//! Closed causes shared by the durable snapshots and the runtime answers.

use workspace_ids::{CentralSessionId, SessionRef};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupCall {
    Create,
    Bind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HoldLostDuring {
    SetUp,
    Bind,
    Opening,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindDispute {
    WrongSecret,
    AlreadyBoundToDifferentId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FailureReason {
    CouldNotSave,
    CreatorNotAllowed,
    Conflict,
    ActiveWithoutLocalProof {
        session_id: CentralSessionId,
        address: SessionRef,
    },
    NoSecret,
    CentralFault {
        call: CleanupCall,
    },
    ReservationRanOut,
    NotLaunched,
    SecretNotStored,
    SecretStoreNotKnown,
    SecretIssueCollision,
    LaunchFailed,
    DuplicateHarnessId,
    SetUpFailed,
    HoldLost {
        during: HoldLostDuring,
    },
    AddressTaken,
    AssociationDisputed(BindDispute),
    NoSuchStart,
    OpeningNotPrepared,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownReason {
    AttemptCeiling,
    Launch,
    SetUp,
    Opening,
    Contradicted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    RecordNotCommitted,
    RecordConflict,
    RecordNotKnown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntendedOutcome {
    Failed(FailureReason),
    Settled,
    Fenced,
    Unknown(UnknownReason),
    CleaningUp(FailureReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LaunchNotTaken {
    LinkNotReady,
    EnrollmentEnded,
    ReservationElapsed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotKind {
    CreateIntended,
    CreateUnknown,
    Reserved,
    LaunchIntended,
    Held,
    BindIntended,
    BindUnknown,
    CleaningUp,
    Opened,
    Failed,
    Settled,
    Fenced,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FactName {
    Reservation,
    StoredSecret,
    CommittedPointer,
    LaunchIntended,
    LaunchStarted,
    HarnessSessionId,
    HeldConversation,
    CleanupReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CorruptStartRecord {
    #[error("the start envelope does not match the record scope")]
    ScopeMismatch,
    #[error("the issue does not belong to the envelope enrollment")]
    IssueEnrollmentMismatch,
    #[error("the issue does not name the reserved central session")]
    IssueSessionMismatch,
    #[error("the issue does not match the reservation attempt")]
    IssueAttemptMismatch,
    #[error("the pointer has no reservation")]
    PointerWithoutReservation,
    #[error("the opened address belongs to a different machine")]
    AssociationMachineMismatch,
    #[error("the opened address does not name the held harness session")]
    AssociationHarnessMismatch,
    #[error("the opened association does not name the reserved central session")]
    AssociationSessionMismatch,
    #[error("the opened association does not match the committed issue")]
    AssociationIssueMismatch,
    #[error("the reservation does not match the snapshot attempt")]
    ReservationAttemptMismatch,
    #[error("the stored or possibly stored secret has no reservation")]
    SecretWithoutReservation,
    #[error("an opened record must hold the conversation")]
    OpenedWithoutHeldConversation,
    #[error("an opened record must carry a committed pointer")]
    OpenedWithoutCommittedPointer,
    #[error("a reserved snapshot must carry a reservation")]
    ReservedWithoutReservation,
    #[error("a reserved snapshot must carry the secret write intent")]
    ReservedWithoutSecretIntent,
    #[error("an opened record must carry the stored secret for its committed issue")]
    OpenedWithoutStoredSecret,
    #[error("an opened record must carry evidence of a started launch")]
    OpenedWithoutLaunch,
    #[error("an opened record must carry a witnessed bind")]
    OpenedWithoutWitnessedBind,
    #[error("an opened record already has a settlement")]
    OpenedWithSettlement,
    #[error("a settled record must carry its exact settlement")]
    SettledWithoutSettlement,
    #[error("the end leg disagrees with the exact settlement")]
    EndSettlementDisagrees,
    #[error("an opened record must carry its exact Active proof")]
    OpenedWithoutActiveProof,
    #[error("the Active proof does not name the opened address")]
    OpenedActiveProofAddressMismatch,
    #[error("an opened record has an end leg")]
    OpenedWithEndLeg,
    #[error("the {phase:?} phase requires {fact:?}")]
    PhaseFactMissing { phase: SnapshotKind, fact: FactName },
}
